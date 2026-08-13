"""Regenerate the derived data baked into docs/index.html.

Docs-as-code: the page never states facts it can derive. Two blocks are
rewritten in place between marker comments:

  DOCS  - table counts/lists imported straight from table_schemas.py
  PAGES - fresh /sales responses captured from the FastAPI app (TestClient)

Run locally after changing schemas or the API (needs api/requirements.txt
installed), and it also runs in the docs deploy workflow:

    python3 scripts/build_docs_data.py
"""

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import table_schemas as ts  # noqa: E402


def derive_docs_data() -> dict:
    def tbl(name: str, prefix: str) -> str:
        return name.removeprefix(prefix).removesuffix('_SCHEMA').lower()

    schemas = [n for n in dir(ts) if n.endswith('_SCHEMA')]
    facts = sorted(tbl(n, 'FACT_') for n in schemas if n.startswith('FACT_'))
    dims = sorted(tbl(n, 'DIM_') for n in schemas if n.startswith('DIM_'))
    assert facts and dims, 'table_schemas.py yielded no tables - refusing to build'
    return {
        'table_count': len(schemas),
        'fact_tables': [f'fact_{t}' for t in facts],
        'dim_tables': [f'dim_{t}' for t in dims],
        'partitioned': sorted(ts.PARTITIONED_TABLES),
    }


def capture_api_pages(n_pages: int = 3, page_size: int = 5) -> list:
    from fastapi.testclient import TestClient

    from api.main import app

    client = TestClient(app)
    pages, cursor = [], None
    for _ in range(n_pages):
        params = {'date': '2026-08-10', 'page_size': page_size}
        if cursor:
            params['cursor'] = cursor
        resp = client.get('/sales', params=params)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        pages.append(body)
        cursor = body['next_cursor']
    return pages


def replace_block(html: str, start: str, end: str, payload: str) -> str:
    pattern = re.compile(re.escape(start) + '.*?' + re.escape(end), re.S)
    assert pattern.search(html), f'marker {start} not found in docs/index.html'
    return pattern.sub(f'{start}\n{payload}\n{end}', html, count=1)


def main() -> None:
    docs = derive_docs_data()
    pages = capture_api_pages()

    path = ROOT / 'docs' / 'index.html'
    html = original = path.read_text()
    html = replace_block(html, '/*__DOCS_DATA_START__*/', '/*__DOCS_DATA_END__*/',
                         f'const DOCS = {json.dumps(docs)};')
    html = replace_block(html, '/*__API_PAGES_START__*/', '/*__API_PAGES_END__*/',
                         f'const PAGES = {json.dumps(pages)};')

    if html == original:
        print('docs/index.html already up to date')
        return
    path.write_text(html)
    print(f"docs/index.html rebuilt: {docs['table_count']} tables "
          f"({len(docs['fact_tables'])} fact / {len(docs['dim_tables'])} dim), "
          f"{len(pages)} API pages")


if __name__ == '__main__':
    main()
