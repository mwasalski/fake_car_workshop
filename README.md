# Car Workshop – Fake Data Platform (Databricks)

Synthetic dataset generator for practising Spark and Data Engineering on Databricks.
Business scenario: a Polish network of 100 car workshops and accessory shops.
Star schema: **7 dimension tables** (static, loaded once) + **13 fact tables**
(generated daily as parquet, ingested incrementally with Auto Loader).

## Architecture

```text
create_tables.sql   (once)   catalog, schemas, volumes, empty Delta tables
initial_dims.ipynb  (once)   7 dim tables -> car_workshop.dim.*  (direct Delta write)
daily.ipynb         (daily)  13 fact tables -> parquet on /Volumes/car_workshop/fact/fact_files
autoloader.ipynb    (daily)  Auto Loader ingest -> car_workshop.fact.*  (Delta)
run_all.ipynb                orchestrator: setup + daily cycle via %run
```

## Files

| file | purpose |
| --- | --- |
| `run_all.ipynb` | entry point – one-time setup + daily cycle via `%run` |
| `create_tables.sql` | DDL: catalog `car_workshop`, schemas `dim`/`fact`, volumes, 20 Delta tables |
| `initial_dims.ipynb` | one-time dimension load (overwrite – safe to re-run, deterministic seed) |
| `daily.ipynb` | daily fact generator (parquet -> volume) |
| `backfill.ipynb` | historical backfill – runs `daily.ipynb` per date in parallel |
| `autoloader.ipynb` | incremental fact ingest (`cloudFiles`, checkpointed) |
| `silver/` | silver layer: DDL + bronze -> silver streaming jobs (see `silver/README.md`) |
| `reference_data.py` | all reference data: cities, car makes, products, services, statuses, weights |
| `table_schemas.py` | single source of truth for table schemas + partition layout |
| `testing/` | data checks: row counts & sizes, NULL audit, duplicate checks (see `testing/README.md`) |
| `learning/` | exam-prep notebooks: skew, salting, shuffle, Auto Loader, CDC, CDF, Delta internals (uses a disposable `car_workshop.lab` schema) |
| `delete_truncate.sql` | cleanup snippets: DELETE / TRUNCATE / DROP for all tables |
| `car_workshop_ab/` | Databricks Asset Bundle scaffold (`databricks bundle init`) – DABs learning ground, still contains template sample code |
| `.claude/skills/` | Claude Code sparring-partner skills: `/de-sparring` (senior DE) and `/da-mentor` (Data Architect path) |

## Setup on a new environment

Prerequisites:

- Databricks workspace with **Unity Catalog** enabled (volumes require it),
- `CREATE CATALOG` privilege on the metastore (or a pre-created `car_workshop` catalog with `ALL PRIVILEGES`),
- cluster with DBR 13+ (pandas / numpy / pyarrow preinstalled).

Steps:

1. Clone this repo into **Databricks Repos**. The notebooks import `reference_data.py`
   and `table_schemas.py` from the same folder, so the whole repo must land together.
2. Open `run_all.ipynb` and run section **1. One-time setup**
   (executes `create_tables.sql`, then `initial_dims.ipynb`).
3. Run section **2. Daily run** (`daily.ipynb` -> `autoloader.ipynb`).

## Daily runs & backfill

- `TARGET_DATE` widget (`YYYY-MM-DD`), blank = yesterday.
- For scheduled runs create a Databricks Job with two tasks: `daily.ipynb` -> `autoloader.ipynb`
  (or schedule `run_all.ipynb` – the setup section is idempotent, just slower).
- **Backfill:** run `backfill.ipynb` – it runs `daily.ipynb` once per date via
  `dbutils.notebook.run` (parallel, `PARALLELISM` widget), skips already generated dates
  and ingests everything with one Auto Loader pass at the end.
  Every day owns a disjoint 10M ID block per table, so IDs never collide across days.
  Seed = date, so each day's data is reproducible.
- Re-running the same date writes duplicate files (new random file names) –
  clean that day's files first if you need a redo.

## Scaling

`SCALE_FACTOR` widget in `daily.ipynb` (blank = `1.0`): `1.0` ≈ 100K rows/day across all fact tables.
Set `10` / `100` for ~1M / ~10M rows per day. The per-table mix lives in `DAILY_BASE_ROWS`.
Upper bound: largest table must stay below `ID_BLOCK` (10M rows/day) – there is an assert.

## Reset (start over on an existing environment)

Tables keep their schema – no need to drop anything:

1. Truncate fact tables (see the TRUNCATE section of `delete_truncate.sql`).
2. Clear volume files and Auto Loader state:

   ```python
   dbutils.fs.rm('/Volumes/car_workshop/dim/dim_files/', recurse=True)              # legacy, unused
   dbutils.fs.rm('/Volumes/car_workshop/dim/autoloader_checkpoints/', recurse=True) # legacy, unused
   dbutils.fs.rm('/Volumes/car_workshop/fact/fact_files/', recurse=True)
   dbutils.fs.rm('/Volumes/car_workshop/fact/autoloader_checkpoints/', recurse=True)
   ```

3. Dimensions: just re-run `initial_dims.ipynb` (overwrite).
