import asyncio  
import time  
import random  
import json  
import orjson  
from typing import Dict, Any, List, Tuple  
from itertools import groupby  

import aioredis  
import asyncpg  
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  
from aiokafka.structs import OffsetAndMetadata  

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "kafka-cluster:9092"
HOT_TOPIC = "task.hot"
COLD_TOPIC = "task.cold"
DLQ_TOPIC = "task.dlq"
GROUP_ID = "llm-workers-v2025"

REDIS_URL = "redis://redis-cluster"
POSTGRES_DSN = "postgresql://user:pass@postgres/llm"

MAX_HOT_RETRIES = 10
MAX_BATCH_RETRIES = 5
BASE_BACKOFF = 1.0   # seconds
BACKOFF_MULT = 2.0
JITTER = 0.2         # seconds

MODEL_LIST_CACHE_KEY = "model_config:all_enabled"
MODEL_LIST_TTL = 300  # seconds

redis = aioredis.from_url(REDIS_URL, decode_responses=False)
pool: asyncpg.Pool

# Token bucket script (unchanged)
TOKEN_BUCKET_SCRIPT = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local data = redis.call('HMGET', key, 'tokens', 'last')
local tokens = tonumber(data[1]) or burst
local last = tonumber(data[2]) or now
local elapsed = now - last
tokens = math.min(burst, tokens + elapsed * refill)
if tokens >= 1 then
    tokens = tokens - 1
    redis.call('HSET', key, 'tokens', tokens, 'last', now)
    redis.call('EXPIRE', key, 3600)
    return 1
else
    return 0
end
"""


async def load_all_model_configs_from_db() -> List[Dict[str, Any]]:
    """Load all enabled model configs from DB."""
    rows = await pool.fetch(
        "SELECT model_id, target_rpm, burst, weight, tier FROM model_config WHERE enabled"
    )
    result = []
    for r in rows:
        result.append({
            "model_id": r["model_id"],
            "target_rpm": r["target_rpm"],
            "burst": r["burst"],
            "weight": r["weight"],
            "tier": r["tier"],
        })
    return result

async def get_all_model_configs() -> List[Dict[str, Any]]:
    # Try Redis first
    raw = await redis.get(MODEL_LIST_CACHE_KEY)
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            # If parsing fails, treat as cache miss
            pass

    # Cache miss: load from DB
    configs = await load_all_model_configs_from_db()
    # Store in Redis
    await redis.set(MODEL_LIST_CACHE_KEY, json.dumps(configs), ex=MODEL_LIST_TTL)
    return configs

async def choose_model(priority: str) -> int:
    configs = await get_all_model_configs()
    # Filter based on priority
    if priority == "hot":
        valid = [c for c in configs if c["tier"] == "premium"]
    else:
        valid = [c for c in configs if c["tier"] in ("premium", "standard")]

    if not valid:
        raise RuntimeError("No models available for priority " + priority)

    models = [c["model_id"] for c in valid]
    weights = [c["weight"] for c in valid]
    return random.choices(models, weights=weights, k=1)[0]

async def acquire_token(model_id: str) -> bool:
    configs = await get_all_model_configs()
    config = next((c for c in configs if c["model_id"] == model_id), None)
    if not config:
        return False

    refill = config["target_rpm"] / 60.0
    burst = config["burst"]
    now = time.time()
    key = f"tb:{model_id}"
    res = await redis.eval(
        TOKEN_BUCKET_SCRIPT,
        1,
        key,
        now,
        refill,
        burst,
    )
    return bool(res)

def use_batch_endpoint(priority: str, prompt_tokens: int) -> bool:
    if priority in ("critical", "high"):
        return False
    if priority == "normal" and prompt_tokens > 3000:
        return False
    return True

async def send_to_dlq(producer: AIOKafkaProducer, item: Dict[str, Any], error: str):
    dlq_msg = {
        "task_id": item["task_id"],
        "model": item["model"],
        "error": error,
        "dlq_at": time.time()
    }
    await producer.send_and_wait(DLQ_TOPIC, dlq_msg)

# ─────────────────────────────────────────────────────────────
# WORKER CLASSES
# ─────────────────────────────────────────────────────────────

class HotWorker:
    def __init__(self, consumer: AIOKafkaConsumer, producer: AIOKafkaProducer):
        self.consumer = consumer
        self.producer = producer
        self.retry_counts: Dict[str, int] = {}

    async def start(self):
        await self.consumer.start()
        try:
            while True:
                msg = await self.consumer.getone()
                await self.handle_message(msg)
        finally:
            await self.consumer.stop()

    async def handle_message(self, msg):
        payload = msg.value
        task_id = payload["task_id"]
        prompt = payload["prompt"]
        priority = payload["priority"]
        model = await choose_model(priority)

        # token bucket
        while not await acquire_token(model):
            await asyncio.sleep(0.005)

        attempt = self.retry_counts.get(task_id, 0) + 1

        try:
            # Process single
            answer = await models_backend.single(prompt, model=model, timeout=90)

            # Write to DB
            await pool.execute(
                "UPDATE tasks SET status='solved', answer=$1, solved_at=NOW() WHERE id=$2",
                answer, task_id
            )

            # Commit offset
            tp = msg.topic_partition
            await self.consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, None)})

            # Clear retry count
            self.retry_counts.pop(task_id, None)

        except Exception as e:
            if attempt > MAX_HOT_RETRIES:
                #Update Status
                await pool.execute(
                    "UPDATE tasks SET status='failed' WHERE id=$1",
                    task_id
                )    

                # Permanent failure → DLQ
                await send_to_dlq(self.producer, payload, f"hot_permanent: {e}")

                # Commit so we don’t block
                tp = msg.topic_partition
                await self.consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, None)})
                self.retry_counts.pop(task_id, None)
            else:
                # Schedule retry with backoff
                self.retry_counts[task_id] = attempt
                delay = BASE_BACKOFF * (BACKOFF_MULT ** (attempt - 1)) + random.uniform(0, JITTER)
                print(f"[HotWorker] Retry {attempt}/{MAX_HOT_RETRIES} for task {task_id}, delay {delay:.2f}s: {e}")
                await asyncio.sleep(delay)
                # Recursively retry
                await self.handle_message(msg)


class ColdWorker:
    def __init__(self, consumer: AIOKafkaConsumer, producer: AIOKafkaProducer):
        self.consumer = consumer
        self.producer = producer
        self.retry_counts: Dict[Tuple[str, ...], int] = {}

    async def start(self):
        await self.consumer.start()
        try:
            while True:
                msgs_by_tp = await self.consumer.getmany(timeout_ms=1000)
                msgs = []
                for tp, part_msgs in msgs_by_tp.items():
                    msgs.extend(part_msgs)
                if not msgs:
                    await asyncio.sleep(0.1)
                    continue
                await self.handle_batch(msgs)
        finally:
            await self.consumer.stop()

    async def handle_batch(self, msgs: List[Any]):
        # Build items
        items = []
        for m in msgs:
            payload = m.value
            task_id = payload["task_id"]
            prompt = payload["prompt"]
            priority = payload["priority"]
            model = await choose_model(priority)

            # token bucket
            while not await acquire_token(model):
                await asyncio.sleep(0.005)

            items.append({
                "msg": m,
                "task_id": task_id,
                "prompt": prompt,
                "model": model,
                "priority": priority,
                "tokens": tokens,
            })

        # Identify a batch key (e.g., the sorted task_ids)
        batch_key = tuple(sorted(item["task_id"] for item in items))
        attempt = self.retry_counts.get(batch_key, 0) + 1

        try:
            # Process in batches per model
            items.sort(key=lambda it: it["model"])
            for model, group in groupby(items, key=lambda it: it["model"]):
                group = list(group)
                prompts = [it["prompt"] for it in group]
                answers = await models_backend.batch(prompts, model=model, timeout=1200)

                async with pool.acquire() as conn:
                    async with conn.transaction():
                        for it, ans in zip(group, answers):
                            # TODO: Refactor this logic to use bulk update (updateMany)
                            await conn.execute(
                                "UPDATE tasks SET status='solved', answer=$1, solved_at=NOW() WHERE id=$2",
                                ans, it["task_id"]
                            )

            # Commit offsets
            offsets = {
                item["msg"].topic_partition: OffsetAndMetadata(item["msg"].offset + 1, None)
                for item in items
            }
            await self.consumer.commit(offsets)
            self.retry_counts.pop(batch_key, None)

        except Exception as e:
            if attempt > MAX_BATCH_RETRIES:
                #Update Status
                await pool.execute(
                    "UPDATE tasks SET status='failed' WHERE id=$1",
                    task_id
                )    
                
                print(f"[ColdWorker] Batch permanent failure after {attempt-1} retries, size={len(items)}: {e}")
                for it in items:
                    await send_to_dlq(self.producer, it,
                                      f"batch_permanent: {e}")
                offsets = {
                    item["msg"].topic_partition: OffsetAndMetadata(item["msg"].offset + 1, None)
                    for item in items
                }
                await self.consumer.commit(offsets)
                self.retry_counts.pop(batch_key, None)
            else:
                self.retry_counts[batch_key] = attempt
                delay = BASE_BACKOFF * (BACKOFF_MULT ** (attempt - 1)) + random.uniform(0, JITTER)
                print(f"[ColdWorker] Retry {attempt}/{MAX_BATCH_RETRIES} for batch size {len(items)}, delay {delay:.2f}s: {e}")
                await asyncio.sleep(delay)
                # Then retry the same batch
                await self.handle_batch([it["msg"] for it in items])

# ─────────────────────────────────────────────────────────────
# MAIN RUNNER
# ─────────────────────────────────────────────────────────────
async def main():
    global pool

    # Setup DB pool
    pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=10, max_size=100)

    # Setup producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=orjson.dumps,
    )
    await producer.start()

    # Setup consumers
    hot_consumer = AIOKafkaConsumer(
        HOT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=orjson.loads,
        max_poll_records=1,
        max_poll_interval_ms=600000,
    )
    cold_consumer = AIOKafkaConsumer(
        COLD_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=orjson.loads,
        max_poll_records=500,
        max_poll_interval_ms=600000,
    )

    hot_worker = HotWorker(hot_consumer, producer)
    cold_worker = ColdWorker(cold_consumer, producer)

    try:
        await asyncio.gather(
            hot_worker.start(),
            cold_worker.start()
        )
    finally:
        await producer.stop()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
