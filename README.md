
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
- **Cold starts** – tasks wait minutes just for the DAG to begin  
- **No per-model traffic control** – impossible to enforce “model 1 ≤ 800 RPM”  
- **Quota changes** require code + DAG redeploy  
- **Fixed batch size of 10** – too small for cheap models, too risky for slow ones  
- **30-minute timeout** silently drops long prompts  
- **No backpressure** – all workers hammer models 
- **No priority support** – urgent tasks blocked by bulk jobs  

## Proposed System 
### Diagram
1. High Level Architectural diagram
2. System architecture - sequence diagram
Can be found here: "https://miro.com/app/board/uXjVJoAx08I=/?moveToWidget=3458764649382668269"
### Description 
New tasks are inserted into the task processing plateform via a edeg endpoint which is menat to receive any kind sof tasks and immidetely produce a event to a kafka topic her the partition key is the task priority just to make sure the hot events are processed quickely, latter the events from this kafka topic are processed by a lightweight, always-on **Router Service** continuously watches for new rows, determines each task’s priority, and immediately publishes the task to one of two Kafka topics: `tasks.hot` for high/critical priority or `tasks.cold` for normal/best-effort work.

Latter A **single unified worker fleet** (one Python binary) consumes both topics:
- **Hot path** → calls the `/single` endpoint instantly for low-latency results.
- **Cold path** → dynamically groups prompts by model and sends large, optimal batches (last 1 sec) to the `/batch` endpoint for maximum throughput and cost efficiency.
All model calls pass through a **global Redis + Lua token bucket** that atomically enforces exact RPM/TPM limits per model, performs weighted model selection, and applies quota changes instantly without restarts. Once the model responds, the worker updates PostgreSQL and commits the Kafka offset, guaranteeing exactly-once processing.


### Architecture Overview
Tasks are ingested via an HTTP edge endpoint, which accepts any type of task and immediately produces an event to a Kafka topic(task.received). Events are partitioned by task priority, ensuring that high-priority (“hot”) tasks are processed first.

A lightweight, always-on `Task Ingestion Processor` continuously consumes from this input topic, and performs the following steps:
 1. Persists the task into a PostgreSQL database.
 2. Determines the priority of each task, and forwards it to one of two Kafka topics:
    - **tasks.hot** — for critical or high-priority tasks
    - **tasks.cold** — for normal, best-effort tasks

Latter A **single unified worker fleet** (one Python binary) consumes both topics:
- **Hot path** → calls the `/single` endpoint instantly for low-latency results.
- **Cold path** → dynamically groups prompts by model and sends large, optimal batches (last 1 sec) to the `/batch` endpoint for maximum throughput and cost efficiency.

All model calls pass through a **global Redis + Lua token bucket** that atomically enforces exact RPM/TPM limits per model, performs weighted model selection, and applies quota changes instantly without restarts. Once the model responds, the worker updates PostgreSQL and commits the Kafka offset, guaranteeing exactly-once processing.

### Pseudocode for key/important parts:
#### Python Pseudocode
1. api/create-task-endpoint.py → Pseudocode for `POST: /task` endpoint
2. consumer/task-processer.py → Pseudocode for **single unified worker fleet**
3. consumer/cold-hot-task-processer.py → Pseudocode for **single unified worker fleet**
4. api/update-model-config-endpoint.py → Pseudocode for `PUT: /model-config` endpoint
#### SQL Pseudocode
1. dlq_events.sql
2. model_config.sql
3. tasks.sql
