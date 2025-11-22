# worker/api.py

import asyncio
import uuid
from typing import Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import orjson
from aiokafka import AIOKafkaProducer

KAFKA_BOOTSTRAP = "kafka-cluster:9092"
KAFKA_TOPIC = "tasks.received"  # your “tasks received” topic

class CreateTaskRequest(BaseModel):
    prompt: str
    priority: Literal["hot", "cold", "normal", "high", "critical"]  # adapt priorities


class CreateTaskResponse(BaseModel):
    task_id: str

app = FastAPI()
producer: AIOKafkaProducer = None  # will init later


@app.on_event("startup")
async def startup_event():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: orjson.dumps(v),
    )
    await producer.start()


@app.on_event("shutdown")
async def shutdown_event():
    global producer
    if producer:
        await producer.stop()


@app.post("/tasks", response_model=CreateTaskResponse)
async def create_task(req: CreateTaskRequest):
    task_id = str(uuid.uuid4())

    # Build the message payload
    msg = {
        "task_id": task_id,
        "prompt": req.prompt,
        "priority": req.priority,
        "created_at": int(uuid.uuid1().time),  # or use time.time(), whatever you need
    }

    # Use priority as the key (so partition = based on key)
    key = req.priority  # "hot", "cold", etc.

    try:
        # Send message to Kafka
        await producer.send_and_wait(KAFKA_TOPIC, value=msg, key=key)
    except Exception as e:
        # If Kafka fails, return 500
        raise HTTPException(status_code=500, detail=f"Failed to produce task: {e}")

    return CreateTaskResponse(task_id=task_id)
