# `print()` & f-string formatting tips

## `print()` itself

```python
print(a, b, c, sep=', ', end='\n', file=sys.stdout, flush=False)
```

- `sep` — separator between args (default `' '`).
- `end` — what to print after everything (default `'\n'`) — set `end=''` to build a
  line incrementally, or `end='\r'` for an in-place progress counter.
- `flush=True` — force the buffer to write immediately (useful in long-running loops
  on Databricks where output can lag).

```python
for i in range(total):
    print(f'\r{i}/{total}', end='', flush=True)
```

## f-string format spec mini-language

```
{value:[[fill]align][sign][#][0][width][grouping][.precision][type]}
```

The part after `:` is the **format spec** — same mini-language used by `str.format()`
and `format()`, just inline in an f-string. This is what you're likely thinking of
(the `:` key sits right next to `;` on most keyboards, or is Shift+`;` on US layout —
easy to mistype).

## Float-specific specs (the useful ones)

| Spec | Effect | Example | Result |
|---|---|---|---|
| `.2f` | fixed decimals | `f'{3.14159:.2f}'` | `3.14` |
| `,` | thousands separator | `f'{1987691:,}'` | `1,987,691` |
| `,.2f` | thousands + decimals combined | `f'{1234567.891:,.2f}'` | `1,234,567.89` |
| `_` | thousands separator with `_` (good for readability in code, not display) | `f'{1987691:_}'` | `1_987_691` |
| `.0f` | round to integer, keep float formatting (no decimal point) | `f'{2.7:.0f}'` | `3` |
| `%` | multiply by 100, append `%` | `f'{0.3049:.1%}'` | `30.5%` |
| `e` | scientific notation | `f'{123456.0:.2e}'` | `1.23e+05` |
| `+` | force sign on positives too | `f'{5:+}'` | `+5` |
| `>10` / `<10` / `^10` | align right/left/center, width 10 | `f'{5:>10}'` | `'         5'` |
| `0>10` | pad with `0` instead of space | `f'{5:010.2f}'` | `'0000005.00'` |

They combine in order — sign, then width/fill, then grouping, then precision, then type:

```python
f'{-1234.5:>+15,.2f}'   # '       -1,234.50'
```

## Debug specifier `=` (Python 3.8+)

Prints the expression **and** its value — the single best print-debugging trick:

```python
ratio = stats['max_cnt'] / stats['median_cnt']
print(f'{ratio=}')             # ratio=200000.0
print(f'{ratio=:.1f}')         # ratio=200000.0  (with a format spec too)
print(f'{stats["max_cnt"]=:,}') # stats["max_cnt"]=1,987,691
```

No more `print(f'ratio: {ratio}')` typos where the label doesn't match the variable.

## Gotchas

- **`,`/`_` are grouping specs for the *display*, not locale-aware.** They always use
  `,` / `_` regardless of system locale. For real locale-aware formatting (e.g. `.`
  as thousands separator, Polish convention) use the `locale` module — rarely needed
  in notebooks, but don't assume `:,` adapts automatically.
- **`.Nf` rounds, it doesn't truncate**, and uses round-half-to-even (banker's
  rounding) on the underlying binary float — `f'{2.5:.0f}'` → `'2'`, not `'3'`, because
  2.5 in binary floating point plus round-half-to-even lands there. If you need
  predictable rounding for money, use `Decimal` first.
- **Type mismatch raises**: `f'{"abc":,}'` → `ValueError` — grouping (`,`) only works
  on numeric types.
- **f-strings > `.format()` > `%`-formatting** — f-strings are evaluated at parse
  time, are the fastest, and are the standard since Python 3.6. Use `.format()` only
  when the template string itself is dynamic (e.g. loaded from a config/i18n file).

## Quick reference used in this repo

Straight from `learning/spark_deep_dive.ipynb`:

```python
print(f"hottest key: {stats['max_cnt']:,} rows | median: {stats['median_cnt']:,} "
      f"| skew ratio: {stats['max_cnt'] / stats['median_cnt']:.0f}x")
# hottest key: 1,987,691 rows | median: 9 | skew ratio: 220855x

print(f'{label}: {time.time() - t0:.1f}s')
# skewed SMJ, AQE skew join OFF: 4.2s
```

(Origin: follow-up to the `percentile_approx.md` note, while reading format specs
used in the skew-check prints.)
