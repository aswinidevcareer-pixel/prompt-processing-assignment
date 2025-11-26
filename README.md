
# LLM Task Solver – Architecture Redesign  
**From Airflow to Real-Time, Controllable & Efficient Inference Queue**

## Current System (Airflow-Based)

### Components
- **PostgreSQL** – stores all tasks (`prompt`, `status`, `answer`, etc.)
- **Models Backend (FastAPI)** – exposes:
  - `POST /single` – solves one prompt
  - `POST /batch` – solves many prompts in parallel (returns only when all are done)
- **Airflow** – the only component that actually calls the Models Backend

### Current Workflow
1. Airflow DAG starts (on schedule or manual trigger)  
2. Pulls **all** unsolved tasks from Postgres  
3. Splits them into exactly 20 batches  
4. Spawns 20 mapped workers  
5. Each worker repeatedly:  
   - Takes 10 tasks → calls `/batch` → updates Postgres  
   - Repeats until no tasks left  
6. Hard 30-minute timeout → DAG ends and restarts later

### Major Bottlenecks
- **Uneven model traffic**: No per-model rate control means some models may be overused or underused.
- **Database contention**: High parallel writes/reads to Postgres can cause transaction bottlenecks. Also pulling **all** unsolved tasks from Postgres.
- **Batch tail latency**: Slowest task in a batch delays the entire batch response.
- **Cold starts** – tasks wait minutes just for the DAG to begin  
- **Fixed worker timeout**: Rigid 30-minute timeouts may abort slow workers prematurely.
- **Fixed batch size of 10** – too small for cheap models, too risky for slow ones  
- **No backpressure** – all workers hammer models 
- **No priority support** – urgent tasks blocked by bulk jobs  

## Proposed System 
### Diagram
#### High Level Architectural diagram

<img width="881" height="900" alt="image" src="https://github.com/user-attachments/assets/04287271-dc5d-40d3-9728-2ceddbed15db" />

> For an interactive version or better readability, view the diagram in [Miro board](https://miro.com/app/board/uXjVJmbknk4=/?moveToWidget=3458764649616160272)

#### System architecture - sequence diagram

<img width="1364" height="912" alt="image" src="https://github.com/user-attachments/assets/ed53c743-2365-45e5-9c5b-d11dd684be73" />

> For an interactive version or better readability, view the diagram in [Miro board](https://miro.com/app/board/uXjVJmbknk4=/?moveToWidget=3458764649616160333&cot=14)

### Architecture Overview
Tasks are ingested via an HTTP edge endpoint, which accepts any type of task and performs the following steps:
 1. Persists the task into a PostgreSQL database.
 2. Determines the priority of each task, and forwards it to one of two Kafka topics:
    - **tasks.hot** — for critical or high-priority tasks
    - **tasks.cold** — for normal, best-effort tasks

Latter A **single unified worker fleet** (one Python binary) consumes both topics:
- **Hot path** → calls the `/single` endpoint instantly for low-latency results.
- **Cold path** → dynamically groups prompts by model and sends large, optimal batches (last 1 sec) to the `/batch` endpoint for maximum throughput and cost efficiency.

All model calls pass through a **global Redis + Lua token bucket** that atomically enforces exact RPM/TPM limits per model, performs weighted model selection, and applies quota changes instantly without restarts. Once the model responds, the worker updates PostgreSQL and commits the Kafka offset, guaranteeing exactly-once processing.

The `PUT /model-config/{model_id}` endpoint is used to update the full configuration of a model, replacing its current settings.

### Rate Limiting (Token Bucket)

<img width="722" height="681" alt="image" src="https://github.com/user-attachments/assets/624161be-2a83-401d-9425-57a2e1bca878" />

> For an interactive version or better readability, view the diagram in [Miro board](https://miro.com/app/board/uXjVJmbknk4=/?moveToWidget=3458764649616160411&cot=14)

It use a **token bucket** algorithm backed by Redis + a Lua script to enforce rate limiting:
- Each model (or client) has its own “bucket” in Redis, stored as a hash with two fields:
  - `tokens`: the current number of tokens  
  - `last`: the timestamp of the last refill  
- Tokens are refilled over time at a fixed rate (`refill`), but never beyond a configured maximum capacity (`burst`).
- On each request, we run a Lua script that:
  1. Reads the current token count and `last` timestamp — if none exist (first use), it initializes the bucket to full.  
  2. Computes how much time has passed since the last refill, then adds tokens accordingly (capped at `burst`).  
  3. Checks if there is at least **1** token available:  
     - **If yes**: subtracts one token, updates `last = now`, sets a 1‑hour TTL on the Redis key, and returns `1` (allowed)  
     - **If no**: returns `0` (rate limit exceeded)
- Because the logic is executed inside a **Lua script**, the read → refill → consume → update steps are **atomic**, meaning no race conditions even under concurrent requests.

### Pseudocode for key/important parts:
#### Python Pseudocode
1. api/create-task-endpoint.py → Pseudocode for `POST: /task` endpoint
3. consumer/cold-hot-task-processer.py → Pseudocode for **single unified worker fleet for both hot and cold tasks**
4. api/update-model-config-endpoint.py → Pseudocode for `PUT: /model-config` endpoint
#### SQL Pseudocode
1. dlq_events.sql
2. model_config.sql
3. tasks.sql

### Recommended Further Enhancements
- **Introduce adaptive batching**: dynamically adjust the flush interval (or batch size) based on real‑time metrics (queue depth, arrival rate, latency), improving throughput under high load and reducing latency when traffic is low. 
- **Add an adaptive model routing layer**: select which model to call for each task based on current load, cost, and performance metrics. This can optimize for cost, latency, and quality by shifting weight dynamically between models.
