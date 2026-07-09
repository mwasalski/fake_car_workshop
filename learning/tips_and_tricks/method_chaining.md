# Why PySpark chains are wrapped in parentheses

```python
(items
 .withColumn('product_id',
             F.when(F.rand(seed=42) < 0.8, F.lit(hot_product)).otherwise(F.col('product_id')))
 .write.mode('overwrite').saveAsTable(f'{LAB}.skewed_sales_items'))
```

Pure Python, not Spark: **parentheses allow one expression to span multiple lines.**

Python ends a statement at the end of a line. Without the outer parens:

```python
items
 .withColumn(...)   # SyntaxError / IndentationError
```

...the first line is a lone `items` (valid, does nothing) and the second starts with a
dot — a syntax error. But **inside an open `()`, `[]` or `{}` newlines are ignored**
(*implicit line joining*) — the parser reads everything up to the closing bracket as a
single expression.

## Three ways to break a method chain across lines

```python
# 1. parentheses - idiomatic, recommended by PEP 8
result = (items
          .withColumn(...)
          .filter(...)
          .select(...))

# 2. backslash - works, but fragile (a space after \ = SyntaxError, ugly diffs)
result = items \
    .withColumn(...) \
    .filter(...)

# 3. intermediate variables - fine when the steps are business-meaningful on their own
step1 = items.withColumn(...)
result = step1.filter(...)
```

Style #1 is the de facto standard in PySpark codebases: transformations read like a
pipeline, every line starts with `.method(...)`, so the steps scan top-to-bottom and
adding/reordering a step is a one-line diff.

## Bonus: adjacent string literals

The same implicit-joining rule explains this pattern in prints:

```python
print(f"hottest key: {stats['max_cnt']:,} rows "
      f"| skew ratio: {ratio:.0f}x")
```

Two string literals next to each other inside parentheses are concatenated at compile
time — no `+` needed, and each fragment keeps its own `f` prefix.

(Origin: question about the skewed-table cell in `learning/spark_deep_dive.ipynb`.)
