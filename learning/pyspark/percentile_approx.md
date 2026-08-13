# `percentile_approx`

`percentile_approx(col, percentage [, accuracy])` computes an approximate percentile
without fully sorting the data — it builds a compressed sketch per partition
(Greenwald-Khanna style algorithm), then **merges** sketches across partitions.
This makes it a combinable/associative aggregation — one pass, no sort-shuffle.

```python
F.expr("percentile_approx(cnt, 0.5)")                    # default accuracy=10000
F.expr("percentile_approx(cnt, 0.5, 100)")                # less accurate, less memory
F.expr("percentile_approx(cnt, array(0.25, 0.5, 0.95))")  # several percentiles from ONE sketch
```

## Trade-offs

| | `percentile_approx` | exact (`percentile`, `median()`, window `percentile_cont`) |
|---|---|---|
| Mechanism | sketch, mergeable, single-pass | requires a **full sort** of the column |
| Cost | O(N), no sort-shuffle | O(N log N) + wide shuffle |
| Error | formally bounded: `error ≈ 1/accuracy` (not randomly "could be way off") | zero |
| Scalability per group | one sketch per group — memory grows linearly with group cardinality in a `groupBy` | sort per group — expensive already at moderate cardinality |
| Determinism | deterministic given the data and `accuracy` | always exact |

**The `accuracy` parameter** (default 10000) is the memory/precision ↔ error trade-off
knob: higher value = bigger sketch = smaller relative error, but more expensive.
Same philosophy as `approx_count_distinct` (HyperLogLog) — Spark uses "exact is too
expensive at scale" in several places.

## When exact vs. approx

Use exact when the number goes into an invoice/financial report where error is
unacceptable. Use approx anywhere the number is **diagnostic** — e.g. answering
"is my hot key roughly 200,000x bigger than typical" rather than "what is the exact
median down to the row". Paying for a full sort just to decide whether to broadcast
or salt would be wasteful — `percentile_approx` is the right tool for that job.

(Origin: discussion while working through `learning/spark_deep_dive.ipynb`, section 1 —
the skew-check cell uses `percentile_approx(cnt, 0.5)` to get a median group size.)
