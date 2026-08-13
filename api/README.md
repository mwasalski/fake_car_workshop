# Sales API — ingestion sparring ground

Stage 1: a FastAPI app serving `fact_sales_transactions` rows with cursor
pagination. Deterministic (seed = date, same ID-block scheme as `daily.ipynb`),
so any date returns identical data on every call — your client is fully testable.

## Run

```bash
cd <repo root>
python3 -m venv .venv && source .venv/bin/activate
pip install -r api/requirements.txt
uvicorn api.main:app --reload
```

Then open http://127.0.0.1:8000/docs — interactive Swagger UI, free with FastAPI.

```bash
curl 'http://127.0.0.1:8000/sales?date=2026-08-10&page_size=5'
# -> {"data": [...], "count": 5, "next_cursor": "MjAyNi0wOC0xMDo1"}
curl 'http://127.0.0.1:8000/sales?date=2026-08-10&page_size=5&cursor=MjAyNi0wOC0xMDo1'
```

## Contract

| endpoint | params | returns |
| --- | --- | --- |
| `GET /health` | — | `{"status": "ok"}` |
| `GET /sales` | `date` (required), `page_size` (≤500), `cursor` (opaque) | `{data, count, next_cursor}` |

`next_cursor == null` means last page. Cursors are opaque — do not parse them
client-side. 2 000 rows per day, ~30% `customer_id = null` (walk-ins).

## Exercise (stage 2 — write this yourself)

`ingest/pull_sales.py`: pull **all** pages for a given date and write them to
one JSONL file. Requirements:

- `httpx`, timeout on every request,
- retry with exponential backoff on 5xx / connection errors (max 5 attempts),
- follow `next_cursor` until null,
- output file: `ingest/out/sales_<date>.jsonl`, overwrite-safe (idempotent re-run),
- log: pages fetched, rows written, duration.

Roadmap: stage 3 = client writes to ADLS landing → Auto Loader picks it up;
stage 4 = API grows auth token, 429 + `Retry-After`, random 500s, duplicate
pages — and your client has to survive all of it.
