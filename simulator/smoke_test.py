"""End-to-end smoke test of the portable generators - runs anywhere, no Spark.

    python3 -m simulator.smoke_test

Generates small dims + one day of facts into a temp dir, then asserts:
  * every table's parquet columns match table_schemas.py exactly,
  * every DATE column lands as parquet date32 (not timestamp),
  * re-generating the same day yields identical rows (determinism port check).
"""

import sys
import tempfile
from datetime import date
from pathlib import Path

import pyarrow.dataset as pads

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from simulator import local_io
from simulator.dims_generation import generate_dims
from simulator.fact_generation import generate_day
from table_schemas import PARTITIONED_TABLES, TABLE_SCHEMAS


def check_schema(table_name, path):
    dataset = pads.dataset(str(path), format='parquet', partitioning='hive')
    expected = TABLE_SCHEMAS[table_name]
    got = {f.name: f.type for f in dataset.schema}
    got.pop('__index_level_0__', None)
    assert set(got) == set(expected), (
        f'{table_name}: columns differ\n  extra: {set(got) - set(expected)}'
        f'\n  missing: {set(expected) - set(got)}')
    partition_cols = set(PARTITIONED_TABLES.get(table_name, []))
    for col, typ in expected.items():
        if typ == 'DATE' and col not in partition_cols:
            assert str(got[col]) == 'date32[day]', f'{table_name}.{col}: {got[col]} != date32'


def main():
    tmp = Path(tempfile.mkdtemp(prefix='car_workshop_smoke_'))
    dim_dir, fact_dir = tmp / 'dim_landing', tmp / 'fact_files'

    dim_stats = generate_dims(lambda pdf, name: local_io.write_dim(pdf, name, dim_dir), scale=0.02)
    for s in dim_stats:
        check_schema(s['table'], dim_dir / s['table'])
    print(f"dims OK: {len(dim_stats)} tables, {sum(s['rows'] for s in dim_stats):,} rows")

    dims = local_io.load_dims(dim_dir)
    day = date(2026, 1, 15)
    fact_stats = generate_day(day, 0.01, str(fact_dir), dims)
    assert len(fact_stats) == 13, f'expected 13 fact tables, got {len(fact_stats)}'
    for s in fact_stats:
        check_schema(s['table'], fact_dir / s['table'])
    print(f"facts OK: {len(fact_stats)} tables, {sum(s['rows'] for s in fact_stats):,} rows")

    # determinism: same date -> identical rows (fresh output dir, same dims)
    fact_dir2 = tmp / 'fact_files_rerun'
    generate_day(day, 0.01, str(fact_dir2), dims)
    for table in ('fact_sales_transactions', 'fact_work_orders', 'fact_invoices'):
        a = local_io.read_table(fact_dir / table).to_pandas().sort_values(
            TABLE_SCHEMAS[table] and list(TABLE_SCHEMAS[table])[0]).reset_index(drop=True)
        b = local_io.read_table(fact_dir2 / table).to_pandas().sort_values(
            list(TABLE_SCHEMAS[table])[0]).reset_index(drop=True)
        assert a.astype(str).equals(b[a.columns].astype(str)), f'{table}: re-run rows differ'
    print('determinism OK: identical rows on re-run')
    print(f'smoke test PASSED (workdir: {tmp})')


if __name__ == '__main__':
    main()
