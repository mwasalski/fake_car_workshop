"""Syntax-check every code cell in every notebook (CI + local).

Skips magic cells (%/!) and the untouched DABs scaffold. Run from repo root:

    python3 scripts/check_notebooks.py
"""

import glob
import json
import sys

EXCLUDED = ('car_workshop_ab/', '.venv/')

failed = False
checked = 0
for path in sorted(glob.glob('**/*.ipynb', recursive=True)):
    if any(x in path for x in EXCLUDED):
        continue
    nb = json.load(open(path))
    for i, cell in enumerate(nb['cells']):
        if cell['cell_type'] != 'code':
            continue
        src = cell['source'] if isinstance(cell['source'], str) else ''.join(cell['source'])
        if src.lstrip().startswith(('%', '!')):
            continue
        checked += 1
        try:
            compile(src, f'{path}:cell{i}', 'exec')
        except SyntaxError as e:
            print(f'FAIL {path} cell {i}: {e}')
            failed = True

print(f'{checked} cells checked: ' + ('FAILED' if failed else 'OK'))
sys.exit(1 if failed else 0)
