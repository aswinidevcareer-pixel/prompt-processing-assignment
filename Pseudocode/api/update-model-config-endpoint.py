from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional

import asyncpg

# ─────────────────────────────────────────────────────────────
# CONFIG / SETTINGS
# ─────────────────────────────────────────────────────────────
POSTGRES_DSN = "postgresql://user:pass@postgres/llm"

app = FastAPI()
pool: asyncpg.Pool = None

class ModelConfigUpdate(BaseModel):
    target_rpm: Optional[int] = None
    weight: Optional[float] = None
    tier: Optional[str] = None
    burst: Optional[int] = None
    enabled: Optional[bool] = None

@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=1, max_size=10)

@app.on_event("shutdown")
async def shutdown():
    await pool.close()

@app.put("/model-config/{model_id}")
async def update_model_config(model_id: str, cfg: ModelConfigUpdate):
    fields = []
    values = []
    if cfg.target_rpm is not None:
        fields.append("target_rpm = $%d" % (len(values) + 1))
        values.append(cfg.target_rpm)
    if cfg.weight is not None:
        fields.append("weight = $%d" % (len(values) + 1))
        values.append(cfg.weight)
    if cfg.tier is not None:
        fields.append("tier = $%d" % (len(values) + 1))
        values.append(cfg.tier)
    if cfg.burst is not None:
        fields.append("burst = $%d" % (len(values) + 1))
        values.append(cfg.burst)
    if cfg.enabled is not None:
        fields.append("enabled = $%d" % (len(values) + 1))
        values.append(cfg.enabled)

    if not fields:
        raise HTTPException(status_code=400, detail="No fields provided for update")

    values.append(model_id)
    sql = "UPDATE model_config SET " + ", ".join(fields) + " WHERE model_id = $" + str(len(values))
    async with pool.acquire() as conn:
        result = await conn.execute(sql, *values)
        # result is something like "UPDATE 1" if one row was updated
        if result != "UPDATE 1":
            raise HTTPException(status_code=404, detail="Model config not found")

    return {"model_id": model_id, "updated": True}
