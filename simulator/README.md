# simulator/ — local ERP simulator & data lab

The data-generation core of the platform, extracted from the notebooks into
portable modules, plus a small web app to run it on localhost — **no Spark, no
Databricks** required. This is the "ERP system" of the use case: it lives
outside the lakehouse and produces the files the pipeline ingests.

## Layout

| file | role |
|---|---|
| `dims_generation.py` | 7 dimension tables, seed 42 — logic 1:1 from `initial_dims.ipynb` |
| `fact_generation.py` | 13 daily fact tables, seed = date — logic 1:1 from `daily.ipynb` |
| `local_io.py` | pyarrow read/write with schemas enforced from `table_schemas.py` |
| `app.py` | FastAPI: buttons to generate, endpoint to preview parquet |
| `index.html` | the UI served at `/` |
| `smoke_test.py` | end-to-end check: schemas + date32 + determinism (runs in CI) |

The notebooks import these modules (thin wrappers now), so there is **one**
generator implementation for Databricks and local runs.

## Run with Docker

```bash
# from the REPO ROOT (image needs reference_data.py / table_schemas.py)
docker build -f simulator/Dockerfile -t car-workshop-sim .
docker run -p 8000:1289 -v "$PWD/data:/data" -e HOST_DATA_DIR="$PWD/data" car-workshop-sim
# open http://localhost:8000
```

Generated parquet lands in `./data` on the host (volume mount), mirroring the
Databricks volume layout:

```text
data/
  dim_landing/<dim_table>/snapshot.parquet
  fact_files/<fact_table>/[year=…/month=…/]*.parquet
```

## Run without Docker

```bash
pip install -r simulator/requirements.txt
uvicorn simulator.app:app --reload     # from the repo root
```

## API

| endpoint | what it does |
|---|---|
| `GET /` | the UI |
| `GET /api/status` | data dir + generated tables with row counts |
| `POST /api/generate/dims?scale=0.05` | 7 dims; `scale=1.0` = the exact Databricks dataset (50K customers, slower) |
| `POST /api/generate/day?day=2026-08-10&scale=0.05` | one day of 13 fact tables; blank day = yesterday |
| `POST /api/generate/backfill?start=…&end=…&scale=0.02` | one `generate_day` per date in the range (max 1 year) — local `backfill.ipynb` |
| `GET /api/preview/{table}?n=1000` | first n rows of any generated table as JSON |

Notes:

- `scale` < 1.0 on dims is a **local-testing convenience** — it changes RNG
  consumption, so rows differ from the full dataset. Fact determinism holds for
  a given (date, scale, dims) triple: the smoke test regenerates a day and
  asserts identical rows.
- Re-generating a day writes **duplicate files** (same behaviour as the
  notebook) — that is deliberate exercise material for dedup in silver.
- Known determinism leak: `fact_payments.transaction_number` uses `uuid4`
  (pre-existing in daily.ipynb, tracked in the journal).

## Roadmap (iteration 2+)

- SQL box in the UI (duckdb over the parquet folders).
- Weekly mutator endpoint (sheets-as-source redesign: typos, deletes, new
  employees/locations) — this app becomes the "HR/Controlling" simulator too.
- Push generated files straight to ADLS landing (azure-storage-blob) so the
  Databricks Auto Loader ingests simulator output end-to-end.
