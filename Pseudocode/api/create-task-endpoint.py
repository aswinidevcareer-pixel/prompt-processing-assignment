import asyncio
import uuid
from fastapi import FastAPI, HTTPException
import orjson

import asyncpg
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

app = FastAPI()

# Configure your Postgres and Kafka
POSTGRES_DSN = "postgresql://user:pass@postgres/dbname"
KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC_HOT = "task.hot"
KAFKA_TOPIC_COLD = "tasks.cold"

pg_pool: asyncpg.Pool = None
kafka_producer: AIOKafkaProducer = None

@app.on_event("startup")
async def startup():
    global pg_pool, kafka_producer
    pg_pool = await asyncpg.create_pool(dsn=POSTGRES_DSN)

    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        enable_idempotence=True,          
        acks="all",                       
        request_timeout_ms=30_000,        
        retry_backoff_ms=200,             
        key_serializer=lambda k: k,       
        value_serializer=lambda v: v,
    )
    await kafka_producer.start()

@app.on_event("shutdown")
async def shutdown():
    await kafka_producer.stop()
    await pg_pool.close()

@app.post("/tasks")
async def create_task(req: NewTaskRequest):
    task_id = str(uuid.uuid4())

    kafka_msg = {
        "id": task_id,
        "prompt": req.prompt,
        "priority": req.priority
    }
    kafka_value = orjson.dumps(kafka_msg)

    async with pg_pool.acquire() as conn:
        # Start a DB transaction
        async with conn.transaction():
            # 1) Insert into Postgres
            row = await conn.fetchrow(
                """
                INSERT INTO tasks (id, prompt, model, priority, metadata, status)
                VALUES ($1, $2, $3, $4, $5, 'pending')
                RETURNING id
                """,
                task_id, req.prompt, req.priority or {}
            )

            # 2) Try to send to Kafka
            try:
                await asyncio.wait_for(
                    kafka_producer.send_and_wait(
                        KAFKA_TOPIC_HOT if req.priority == "high" else KAFKA_TOPIC_COLD,
                        key=task_id.encode("utf-8"),
                        value=kafka_value,
                    ),
                    timeout=10  # seconds, adapt as needed
                )
            except (KafkaError, asyncio.TimeoutError) as exc:
                # Kafka failed → raise, which causes rollback of the DB transaction
                raise HTTPException(
                    status_code=503,
                    detail="Could not enqueue task, try again later"
                ) from exc

    return {"id": task_id, "status": "queued"}
