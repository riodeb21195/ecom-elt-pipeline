import os
import random
import time
from datetime import UTC, datetime, timedelta

from fastapi import FastAPI, HTTPException, Query, Request

app = FastAPI(title="Mock Orders Source API")

RATE_LIMIT_PER_MINUTE = int(os.environ.get("RATE_LIMIT_PER_MINUTE", "60"))
SCHEMA_DRIFT_ENABLED = os.environ.get("SCHEMA_DRIFT_ENABLED", "false").lower() == "true"
TRANSIENT_ERROR_RATE = float(os.environ.get("TRANSIENT_ERROR_RATE", "0.03"))

_request_log: list[float] = []
_orders_cache: list[dict] | None = None

def _rate_limit_check():
    now = time.time()
    window_start = now - 60
    while _request_log and _request_log[0] < window_start:
        _request_log.pop(0)
    if len(_request_log) >= RATE_LIMIT_PER_MINUTE:
        raise HTTPException(status_code=429, detail="Rate Limit exceeded, back off")
    _request_log.append(now)

def _generate_orders(n: int = 500) -> list[dict]:
    random.seed(42)
    categories = ["electronics", "furniture", "toys", "beauty", "sports", "books"]
    states = ["SP", "RJ", "MG", "BA", "RS", "PR"]
    orders = []
    base_time = datetime(2024, 1, 1, tzinfo=UTC)
    for i in range(n):
        order_ts = base_time + timedelta(hours= i * 3, minutes=random.randint(0,59))
        delivered_offset_days = random.choice([2,3,4,5,7,10,14])
        estimated_days = random.choice([3,5,7])
        field_name = "order_status_v2" if SCHEMA_DRIFT_ENABLED and i > n * 0.8 else "order_status"
        order = {
            "order_id": f"ord_{i:06d}",
            "customer_state": random.choice(states),
            "product_category": random.choice(categories),
            field_name: random.choice(["delivered", "delivered", "delivered", "shipped"]),
            "order_purchase_timestamp": order_ts.isoformat(),
            "order_delivered_timestamp": (order_ts + timedelta(days=delivered_offset_days)).isoformat(),
            "order_estimated_delivery_days": estimated_days,
            "review_score": random.choice([1, 2, 3, 4, 4, 5, 5, 5]) if delivered_offset_days <= estimated_days
            else random.choice([1, 1, 2, 2, 3]),
            "updated_at": order_ts.isoformat(),
        }
        orders.append(order)
    return orders

def _get_orders_cache() -> list[dict]:
    global _orders_cache
    if _orders_cache is None:
        _orders_cache = _generate_orders()
    return _orders_cache

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/orders")
def get_orders(
    request: Request,
    limit: int = Query(100, le=500),
    offset: int = Query(0, ge=0),
    updated_after: str | None = Query(None, description="ISO timestamp, inkr. Extraktion"),
):
    _rate_limit_check()

    if random.random() < TRANSIENT_ERROR_RATE:
        raise HTTPException(status_code=503, detail="Transient error, try again")
    
    orders = _get_orders_cache()

    if updated_after:
        cutoff = datetime.fromisoformat(updated_after)
        orders = [o for o in orders if datetime.fromisoformat(o["updated_at"]) > cutoff]
    
    page = orders[offset: offset + limit]
    return {
        "data": page,
        "pagination": {
            "limit": limit,
            "offset": offset,
            "total": len(orders),
            "has_more": offset + limit < len(orders),
        }
    }




                             
                                           