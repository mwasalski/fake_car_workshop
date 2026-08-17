"""Local lab: generate the car-workshop dataset on localhost and browse it.

    uvicorn simulator.app:app --reload        (from the repo root)
    # or: docker build -f simulator/Dockerfile -t car-workshop-sim . \
    #     && docker run -p 8000:8000 -v "$PWD/data:/data" car-workshop-sim

Data layout under CAR_WORKSHOP_DATA (default ./data):
    dim_landing/<dim_table>/snapshot.parquet      - same shape as the ADLS volume
    fact_files/<fact_table>/[year=/month=/]*.parquet
"""

import json
import os
import platform
import subprocess
import time
from datetime import date, timedelta
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from simulator import local_io
from simulator.dims_generation import generate_dims
from simulator.fact_generation import DAILY_BASE_ROWS, generate_day
from table_schemas import TABLE_SCHEMAS

DATA_DIR = Path(os.environ.get('CAR_WORKSHOP_DATA', './data'))
DIM_DIR = DATA_DIR / 'dim_landing'
FACT_DIR = DATA_DIR / 'fact_files'

app = FastAPI(title='car-workshop simulator', version='0.1')


@app.get('/', response_class=HTMLResponse)
def index():
    return (Path(__file__).parent / 'index.html').read_text()


@app.get('/api/status')
def status():
    return {
        'data_dir': str(DATA_DIR.resolve()),
        'tables': local_io.list_tables(DATA_DIR),
        'daily_base_rows': DAILY_BASE_ROWS,
    }


@app.post('/api/generate/dims')
def api_generate_dims(scale: float = Query(1.0, gt=0, le=1.0)):
    t0 = time.time()
    stats = generate_dims(lambda pdf, name: local_io.write_dim(pdf, name, DIM_DIR), scale=scale)
    return {'scale': scale, 'seconds': round(time.time() - t0, 1), 'tables': stats}


@app.post('/api/generate/day')
def api_generate_day(day: str = Query(None, description='YYYY-MM-DD, blank = yesterday'),
                     scale: float = Query(0.05, gt=0)):
    target = date.fromisoformat(day) if day else date.today() - timedelta(days=1)
    if target < date(2020, 1, 1):
        raise HTTPException(422, 'dates before 2020-01-01 break the ID-block scheme')
    try:
        dims = local_io.load_dims(DIM_DIR)
    except FileNotFoundError as e:
        raise HTTPException(409, str(e))
    t0 = time.time()
    stats = generate_day(target, scale, str(FACT_DIR), dims)
    return {'date': target.isoformat(), 'scale': scale,
            'seconds': round(time.time() - t0, 1), 'tables': stats}


@app.post('/api/open-folder')
def api_open_folder():
    """Open the data folder in the host file manager (only possible off-Docker).

    Inside a container there is no host GUI to talk to - we return the host
    path instead (docker run ... -e HOST_DATA_DIR="$PWD/data") and the UI
    copies it to the clipboard.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if Path('/.dockerenv').exists():
        return {'opened': False,
                'path': os.environ.get('HOST_DATA_DIR', ''),
                'reason': 'running inside Docker - open the mounted folder on the host'}
    path = str(DATA_DIR.resolve())
    opener = {'Darwin': 'open', 'Windows': 'explorer', 'Linux': 'xdg-open'}.get(platform.system())
    if not opener:
        raise HTTPException(501, f'unsupported platform: {platform.system()}')
    subprocess.Popen([opener, path])
    return {'opened': True, 'path': path}


@app.post('/api/generate/backfill')
def api_generate_backfill(start: str, end: str, scale: float = Query(0.05, gt=0)):
    """Local equivalent of backfill.ipynb: one generate_day per date in [start, end]."""
    d0, d1 = date.fromisoformat(start), date.fromisoformat(end)
    if d0 < date(2020, 1, 1):
        raise HTTPException(422, 'dates before 2020-01-01 break the ID-block scheme')
    if d0 > d1:
        raise HTTPException(422, 'start must be <= end')
    if (d1 - d0).days > 366:
        raise HTTPException(422, 'more than a year per request - split the backfill')
    try:
        dims = local_io.load_dims(DIM_DIR)
    except FileNotFoundError as e:
        raise HTTPException(409, str(e))
    t0, days, total = time.time(), 0, 0
    d = d0
    while d <= d1:
        total += sum(s['rows'] for s in generate_day(d, scale, str(FACT_DIR), dims))
        days += 1
        d += timedelta(days=1)
    return {'start': start, 'end': end, 'scale': scale, 'days': days,
            'rows': total, 'seconds': round(time.time() - t0, 1)}


@app.get('/api/preview/{table}')
def api_preview(table: str, n: int = Query(1000, ge=1, le=10_000)):
    if table not in TABLE_SCHEMAS:
        raise HTTPException(404, f'unknown table {table!r}')
    tables = local_io.list_tables(DATA_DIR)
    if table not in tables:
        raise HTTPException(404, f'{table} not generated yet')
    df = local_io.read_table(tables[table]['path'], n).to_pandas()
    payload = json.loads(df.to_json(orient='split', date_format='iso', index=False))
    payload['total_rows'] = tables[table]['rows']
    return payload
