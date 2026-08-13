"""Smoke test for the sales API (CI + local). Needs api/requirements.txt installed.

    python3 scripts/api_smoke.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)

assert client.get('/health').json() == {'status': 'ok'}

page1 = client.get('/sales', params={'date': '2026-01-01', 'page_size': 5})
assert page1.status_code == 200, page1.text
body = page1.json()
assert body['count'] == 5 and body['next_cursor'], body

# determinism: same request twice -> identical payload
again = client.get('/sales', params={'date': '2026-01-01', 'page_size': 5})
assert again.json() == body, 'API is not deterministic'

# pathological inputs must be rejected, not 500
assert client.get('/sales', params={'date': '2026-01-01', 'cursor': 'zzz'}).status_code == 400
assert client.get('/sales', params={'date': '2030-01-01'}).status_code == 422

print('API smoke: OK')
