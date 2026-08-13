#!/usr/bin/env python3
"""Local Spark UI lab - the real Spark UI, no Databricks needed.

Standalone counterpart of learning/spark_ui_diagnosis.ipynb for machines where
only serverless (no classic cluster / no Spark UI) is available, e.g. the
Databricks Free Edition. Runs plain local PySpark and serves the FULL Spark UI
at http://localhost:4040 - Jobs timeline, Stages, Summary Metrics, SQL tab.

Usage:
    python3 learning/spark_ui_local_lab.py

The script runs 4 demos (same story as the notebook: partition cap, skew
straggler, driver-bound work, tiny tasks) and PAUSES after each one so you can
click through the UI while the jobs are fresh. Press Enter to continue.

Knobs (env vars):
    UI_LAB_ROWS=6000000   fact size; raise if demos finish too fast to see
    UI_LAB_AUTO=1         no pauses (smoke-test mode)

Notes:
    - needs only `pip install pyspark` + a JVM (Java 17/21); no repo imports,
      data is generated on the fly - reading the UI needs pathological job
      shapes, not the car_workshop tables
    - local[8] = ONE executor (the driver) with 8 task slots, so the Executors
      tab is boring here; everything else (Stages, timeline, Summary Metrics,
      SQL plans, AQE) looks exactly like on a real cluster
"""

import math
import os
import shutil
import tempfile
import time

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

ROWS = int(os.environ.get("UI_LAB_ROWS", "6000000"))
INTERACTIVE = os.environ.get("UI_LAB_AUTO") != "1"
CORES = 8

spark = (
    SparkSession.builder
    .master(f"local[{CORES}]")
    .appName("spark_ui_diagnosis_local")
    .config("spark.ui.retainedJobs", "300")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)
sc = spark.sparkContext
WORK_DIR = tempfile.mkdtemp(prefix="spark_ui_lab_")

print(f"\nSpark {spark.version} | local[{CORES}] -> {CORES} task slots")
print(f"Spark UI:  http://localhost:4040")
print(f"work dir:  {WORK_DIR}")
print(f"fact size: {ROWS:,} rows (UI_LAB_ROWS to change)")


def banner(title, *look_for):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)
    for line in look_for:
        print(f"  UI: {line}")
    print()


def timed(label, fn):
    t0 = time.time()
    result = fn()
    print(f"{label}: {time.time() - t0:.1f}s")
    return result


def pause():
    if INTERACTIVE:
        input("\n>>> click through http://localhost:4040 now - Enter for next demo... ")


# --------------------------------------------------------------------- data
# grp_key: ~2M groups -> real shuffle; product_id: 85% one hot key -> skew;
# value: payload to aggregate. All deterministic-ish, generated on the fly.
fact = (
    spark.range(ROWS)
    .withColumn("grp_key", F.pmod(F.hash("id"), F.lit(2_000_000)))
    .withColumn(
        "product_id",
        F.when(F.rand(seed=42) < 0.85, F.lit(7))
        .otherwise(F.pmod(F.hash("id", F.lit(1)), F.lit(200_000))),
    )
    .withColumn("value", F.rand(seed=42) * 100)
)

dim_products = (
    spark.range(200_000)
    .withColumnRenamed("id", "product_id")
    .withColumn("product_name", F.sha2(F.col("product_id").cast("string"), 256))
)


# --------------------------------------------------------------- demo 1
banner(
    "DEMO 1 - parallelism capped by PARTITION COUNT, not core count",
    "Jobs tab: job '1A' -> second stage has 2 tasks; job '1B' -> 16 tasks",
    "Stage detail -> Event Timeline: 2 busy lanes vs all 8 (this cluster = 8 slots)",
    "same data, same 'cluster' - only the task count changed the wall clock",
)
spark.conf.set("spark.sql.adaptive.enabled", "false")


def capped_agg():
    return (
        fact.groupBy("grp_key")
        .agg(F.sum("value").alias("sum_value"))
        # chained sha2 AFTER the shuffle = CPU work trapped in the capped stage
        .withColumn("fp", F.sha2(F.concat_ws("|", "grp_key", "sum_value"), 256))
        .withColumn("fp", F.sha2(F.concat_ws("|", "fp", "sum_value"), 256))
        .withColumn("fp", F.sha2(F.concat_ws("|", "fp", "grp_key"), 256))
        .agg(F.count("*"), F.max("fp"))
        .collect()
    )


sc.setJobDescription("1A: shuffle.partitions=2 (6 of 8 cores idle)")
spark.conf.set("spark.sql.shuffle.partitions", "2")
timed("1A: shuffle.partitions=2 ", capped_agg)

sc.setJobDescription("1B: shuffle.partitions=16 (all cores busy)")
spark.conf.set("spark.sql.shuffle.partitions", "16")
timed("1B: shuffle.partitions=16", capped_agg)
sc.setJobDescription(None)
pause()


# --------------------------------------------------------------- demo 2
banner(
    "DEMO 2 - skew: one straggler task pins the whole stage",
    "job '2A' -> longest stage -> Summary Metrics: Duration Max >> Median",
    "same table, Shuffle Read Size: one task read ~85% of the data",
    "Tasks list: sort by Duration desc -> meet the straggler",
    "job '2B' -> SQL tab -> join node: AQE split the skewed partition (skew=true)",
)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # force sort-merge join


def skewed_join():
    return (
        fact.join(dim_products, "product_id")
        .agg(F.sum("value"), F.count("*"))
        .collect()
    )


sc.setJobDescription("2A: skewed SMJ, AQE OFF (find the straggler)")
spark.conf.set("spark.sql.adaptive.enabled", "false")
timed("2A: skewed join, no rescue   ", skewed_join)

sc.setJobDescription("2B: skewed SMJ, AQE skew rescue ON")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "8MB")
timed("2B: skewed join, AQE rescue  ", skewed_join)

sc.setJobDescription(None)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")
pause()


# --------------------------------------------------------------- demo 3
banner(
    "DEMO 3 - the cluster isn't doing the work at all",
    "Jobs tab -> Event Timeline (top of page): a blank GAP after the collect",
    "job - that gap IS the pandas loop; every worker idle, driver at 100%",
    "job '3C' (coalesce(1) write): final stage = 1 task; '3D' = parallel write",
)
sample = fact.select("value").limit(1_500_000)

sc.setJobDescription("3A: collect 1.5M rows to the driver")
pdf = timed("3A: toPandas 1.5M rows           ", sample.toPandas)


def driver_loop():
    # pure-Python math on the driver: no Spark job exists while this runs.
    # Sized to ~5s so the blank gap in the jobs timeline is unmissable.
    total = 0.0
    values = pdf["value"].tolist()
    t0 = time.time()
    while time.time() - t0 < 5:
        for v in values:
            total += math.sqrt(abs(v))
    return total


timed("3B: python loop on driver (GAP!) ", driver_loop)

sc.setJobDescription("3B': same aggregation, distributed")
timed("3B': same math as Spark agg      ",
      lambda: sample.agg(F.sum(F.sqrt(F.abs("value")))).collect())

sc.setJobDescription("3C: coalesce(1) write - ONE task")
timed("3C: coalesce(1) write            ",
      lambda: fact.coalesce(1).write.mode("overwrite")
                  .parquet(f"{WORK_DIR}/single_file"))

sc.setJobDescription("3D: normal parallel write")
timed("3D: parallel write               ",
      lambda: fact.write.mode("overwrite").parquet(f"{WORK_DIR}/parallel"))
sc.setJobDescription(None)
pause()


# --------------------------------------------------------------- demo 4
banner(
    "DEMO 4 - tiny tasks: scheduling overhead beats compute",
    "job '4A' -> scan stage: 600 tasks, Median duration in MILLISECONDS",
    "Summary Metrics: Scheduler Delay + Task Deserialization = real share of task",
    "job '4B': 8 right-sized tasks, overhead share collapses",
)
sc.setJobDescription("4-prep: write 600 tiny files vs 8 files")
fact.repartition(600).write.mode("overwrite").parquet(f"{WORK_DIR}/many_files")
fact.repartition(8).write.mode("overwrite").parquet(f"{WORK_DIR}/few_files")

# Spark normally PACKS small files into ~128MB tasks and hides the problem at
# this data size -> shrink the pack size so the scan launches ~1 task per file
sc.setJobDescription("4A: scan 600 tiny files (1 task/file)")
spark.conf.set("spark.sql.files.maxPartitionBytes", "1MB")
timed("4A: scan 600 tiny files      ",
      lambda: spark.read.parquet(f"{WORK_DIR}/many_files")
                   .agg(F.sum("value")).collect())

sc.setJobDescription("4B: scan 8 right-sized files")
spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")
timed("4B: scan 8 right-sized files ",
      lambda: spark.read.parquet(f"{WORK_DIR}/few_files")
                   .agg(F.sum("value")).collect())
sc.setJobDescription(None)


# --------------------------------------------------------------- wrap-up
print("\n" + "=" * 72)
print("THE SENIOR UI WALK (what you just saw, in interview order)")
print("=" * 72)
print("""\
 1. Executors tab      - is task time spread or concentrated? (thin here: local
                         mode has one executor - on a cluster this is step one)
 2. Jobs timeline      - gaps between jobs = driver/external work   (demo 3)
 3. stage task count   - fewer tasks than cores = partition cap     (demo 1)
 4. Max vs Median      - straggler task = skew                      (demo 2)
 5. median in millis   - overhead-bound tiny tasks                  (demo 4)

Extra workers only help when the Stages tab shows a deep queue of uniform,
healthy-sized tasks - every other picture above means autoscaling buys idle.""")

if INTERACTIVE:
    input("\n>>> UI stays alive at http://localhost:4040 - Enter to shut down... ")

shutil.rmtree(WORK_DIR, ignore_errors=True)
spark.stop()
print("done - work dir cleaned, Spark stopped")
