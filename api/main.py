"""Fake sales API - stage 1: cursor pagination, deterministic data.

Serves fact_sales_transactions rows over REST so the ingestion client
(your exercise) has something realistic to pull from. Same determinism
rules as daily.ipynb: seed = date, disjoint ID block per day.

Run from the REPO ROOT (imports reference_data from there):

    uvicorn api.main:app --reload

Interactive docs: http://127.0.0.1:8000/docs
"""

import base64
import random
import sys
from datetime import date as Date
from functools import lru_cache
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from reference_data import PAYMENT_METHODS, PAYMENT_WEIGHTS  # noqa: E402

EPOCH = Date(2020, 1, 1)   # same origin as daily.ipynb ID blocks
ID_BLOCK = 10_000_000
ROWS_PER_DAY = 2_000       # API-sized volume; daily.ipynb does ~100K
WALK_IN_RATIO = 0.30       # customer_id = None -> walk-in customer
MAX_PAGE_SIZE = 500


class Sale(BaseModel):
    transaction_id: int
    transaction_code: str
    location_id: int
    customer_id: int | None   # None = walk-in (~30%)
    employee_id: int
    transaction_date: Date
    payment_method: str
    receipt_number: str


class SalesPage(BaseModel):
    data: list[Sale]
    count: int
    next_cursor: str | None   # opaque - clients must not parse it


@lru_cache(maxsize=32)
def generate_day(day: Date) -> tuple[Sale, ...]:
    """Deterministic full day of sales; cached so paging doesn't regenerate."""
    rng = random.Random(day.isoformat())
    id_start = (day - EPOCH).days * ID_BLOCK
    rows = []
    for i in range(ROWS_PER_DAY):
        tid = id_start + i
        rows.append(Sale(
            transaction_id=tid,
            transaction_code=f"TRX-{day:%Y%m%d}-{i:06d}",
            location_id=rng.randint(1, 100),
            customer_id=None if rng.random() < WALK_IN_RATIO else rng.randint(1, 50_000),
            employee_id=rng.randint(1, 2_000),
            transaction_date=day,
            payment_method=rng.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0],
            receipt_number=f"RCP/{day:%Y/%m/%d}/{rng.randint(100000, 999999)}",
        ))
    return tuple(rows)


def encode_cursor(day: Date, offset: int) -> str:
    return base64.urlsafe_b64encode(f"{day.isoformat()}:{offset}".encode()).decode()


def decode_cursor(cursor: str, expected_day: Date) -> int:
    """Cursors are opaque to clients but WE validate them strictly."""
    try:
        day_str, offset_str = base64.urlsafe_b64decode(cursor.encode()).decode().split(":")
        day, offset = Date.fromisoformat(day_str), int(offset_str)
    except (ValueError, UnicodeDecodeError):
        raise HTTPException(status_code=400, detail="Malformed cursor")
    if day != expected_day:
        raise HTTPException(status_code=400, detail="Cursor belongs to a different date")
    if offset < 0:
        raise HTTPException(status_code=400, detail="Malformed cursor")
    return offset


app = FastAPI(title="Car Workshop Sales API", version="0.1.0")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/sales", response_model=SalesPage)
def get_sales(
    date: Date = Query(..., description="Business date, YYYY-MM-DD"),
    page_size: int = Query(100, ge=1, le=MAX_PAGE_SIZE),
    cursor: str | None = Query(None, description="Opaque cursor from previous page"),
) -> SalesPage:
    if date < EPOCH or date > Date.today():
        raise HTTPException(status_code=422, detail=f"date must be within [{EPOCH}, today]")

    day_rows = generate_day(date)
    offset = decode_cursor(cursor, date) if cursor else 0
    page = day_rows[offset:offset + page_size]
    next_offset = offset + page_size
    return SalesPage(
        data=list(page),
        count=len(page),
        next_cursor=encode_cursor(date, next_offset) if next_offset < len(day_rows) else None,
    )
