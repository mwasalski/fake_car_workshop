"""Portable data-generation core + local lab app.

The generation logic for dimensions and daily facts lives here and is shared by:
  * the Databricks notebooks (initial_dims.ipynb / daily.ipynb - thin wrappers), and
  * the local Docker lab (simulator/app.py - FastAPI on localhost).

Pure pandas/numpy/faker/pyarrow - no spark, no dbutils.
"""
