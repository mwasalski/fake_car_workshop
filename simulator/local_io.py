"""Local parquet IO for the simulator - what Spark does for the notebooks.

Dims are written with an explicit pyarrow schema derived from table_schemas.py,
so local files carry exactly the declared types (DATE -> date32, BIGINT ->
int64, ...) - same contract the Delta tables enforce on Databricks.
"""

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as pads
import pyarrow.parquet as pq

from table_schemas import TABLE_SCHEMAS

_PA_TYPES = {
    'BIGINT': pa.int64(),
    'INT': pa.int32(),
    'STRING': pa.string(),
    'DATE': pa.date32(),
    'DOUBLE': pa.float64(),
    'BOOLEAN': pa.bool_(),
}


def arrow_schema(table_name):
    return pa.schema([(col, _PA_TYPES[typ]) for col, typ in TABLE_SCHEMAS[table_name].items()])


def write_dim(pdf, table_name, dim_dir):
    """Write one dim snapshot as a single parquet file under dim_dir/<table>/."""
    schema = arrow_schema(table_name)
    table = pa.Table.from_pandas(pdf, preserve_index=False).select(schema.names).cast(schema)
    out = Path(dim_dir) / table_name
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out / 'snapshot.parquet')


def read_table(path, n=None):
    """Read a parquet folder (hive partitions welcome) -> pyarrow Table."""
    dataset = pads.dataset(str(path), format='parquet', partitioning='hive')
    return dataset.head(n) if n else dataset.to_table()


def count_rows(path):
    return pads.dataset(str(path), format='parquet', partitioning='hive').count_rows()


def load_dims(dim_dir):
    """Read local dim snapshots into the pool dict for generate_day."""
    from simulator.fact_generation import dims_from_frames

    def pdf(table):
        p = Path(dim_dir) / table
        if not p.exists():
            raise FileNotFoundError(f'{table} not found under {dim_dir} - generate dims first')
        return read_table(p).to_pandas()

    def ids(table, col):
        return pdf(table)[col].to_numpy()

    return dims_from_frames(
        locations=pdf('dim_locations')[['location_id', 'type']],
        employees=pdf('dim_employees')[['employee_id', 'position', 'location_id']],
        customer_ids=ids('dim_customers', 'customer_id'),
        vehicle_ids=ids('dim_vehicles', 'vehicle_id'),
        product_ids=ids('dim_products', 'product_id'),
        service_ids=ids('dim_services', 'service_id'),
        supplier_ids=ids('dim_suppliers', 'supplier_id'),
    )


def list_tables(data_dir):
    """Enumerate generated tables on disk: {name: {'rows': int, 'kind': ...}}."""
    out = {}
    for kind, sub in (('dim', 'dim_landing'), ('fact', 'fact_files')):
        base = Path(data_dir) / sub
        if not base.exists():
            continue
        for p in sorted(base.iterdir()):
            if p.is_dir() and any(p.rglob('*.parquet')):
                out[p.name] = {'kind': kind, 'rows': count_rows(p), 'path': str(p)}
    return out
