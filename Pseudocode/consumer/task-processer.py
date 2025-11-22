# worker/consumer.py

import asyncio
import time
import orjson
from typing import Any, Dict

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import OffsetAndMetadata, TopicPartition

KAFKA_BOOTSTRAP = "kafka-cluster:9092"
INPUT_TOPIC = "tasks.received"
HOT_TOPIC = "tasks.hot"
COLD_TOPIC = "tasks.cold"
GROUP_ID = "task-receiver-group"

POSTGRES_DSN = "postgresql://user:pass@postgres/llm"

MAX_RETRIES = 3

async def process_message(
    msg_value: Dict[str, Any],
    pg_conn: asyncpg.Connection,
    producer: AIOKafkaProducer,
) -> None:
    """
    Process a single task.received message:
      1) insert into Postgres `tasks` table
      2) forward to hot or cold topic based on priority
    """
    task_id = msg_value["task_id"]
    prompt = msg_value["prompt"]
    priority = msg_value.get("priority", "cold")

    # 1) Insert into tasks table
    # Adjust SQL to your schema
    await pg_conn.execute(
        """
        INSERT INTO tasks (id, prompt, priority, created_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (id) DO NOTHING
        """,
        task_id,
        prompt,
        priority,
    )

    # 2) Produce to next topic
    forward_topic = HOT_TOPIC if priority == "hot" else COLD_TOPIC

    # The key can be priority or anything else
    key_bytes = priority.encode("utf-8")
    await producer.send_and_wait(
        forward_topic,
        key=key_bytes,
        value=msg_value,
    )

async def consume_loop():
    # Create DB pool / connection
    pg_pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=1, max_size=10)

    # Create producer (to forward)
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k,  # already bytes
        value_serializer=lambda v: orjson.dumps(v),
    )
    await producer.start()

    # Create consumer
    consumer = AIOKafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: orjson.loads(v),
    )
    await consumer.start()

    try:
        async for msg in consumer:
            tp = msg.topic_partition
            offset = msg.offset
            try:
                async with pg_pool.acquire() as conn:
                    await process_message(msg.value, conn, producer)

                # Commit offset after success
                await consumer.commit({tp: OffsetAndMetadata(offset + 1, None)})

            except Exception as e:
                print(f"Failed to process message at {tp}, offset {offset}: {e}")
    finally:
        await consumer.stop()
        await producer.stop()
        await pg_pool.close()

if __name__ == "__main__":
    asyncio.run(consume_loop())
