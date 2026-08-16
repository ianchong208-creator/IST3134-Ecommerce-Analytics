#!/usr/bin/env python3
"""
IST3134 Group Assignment - Spark Scalability Benchmark
Measures execution time for the three analytical operations (Event
Distribution, Daily Purchase & Revenue, Top 10 Brands) at four increasing
sample sizes, for direct comparison against the Pandas implementation at
the same sizes.

Usage (via spark-submit on the EMR primary node):
  spark-submit --master yarn --deploy-mode client \
    --num-executors 4 --executor-memory 3g --executor-cores 2 \
    spark_scalability_benchmark.py \
    s3://<bucket>/ecommerce/2019-Nov.csv \
    s3://<bucket>/ecommerce-results/spark-scalability
"""
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

if len(sys.argv) != 3:
    print("Usage: spark_scalability_benchmark.py <input_csv_path> <output_path>")
    sys.exit(1)

input_path = sys.argv[1]
output_path = sys.argv[2]

SAMPLE_SIZES = [100000, 500000, 1000000, 5000000]

spark = SparkSession.builder.appName("SparkScalabilityBenchmark-Nov2019").getOrCreate()

# Same explicit schema as the main job - avoids inferSchema's extra pass.
schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True),
])

print(f"Reading {input_path} (schema applied, not caching the full file) ...")
df = (spark.read
      .option("header", True)
      .schema(schema)
      .csv(input_path))

df = df.withColumn(
    "event_date",
    F.to_date(F.substring(F.col("event_time"), 1, 10), "yyyy-MM-dd")
)

results = []

for n in SAMPLE_SIZES:
    print("\n" + "=" * 70)
    print(f"SAMPLE SIZE: {n:,} rows")
    print("=" * 70)

    # Draw the first n rows (matches pandas' read_csv(..., nrows=n) for a
    # fair comparison) and materialise them BEFORE timing starts, so the
    # sampling/read cost is not mixed into the operation timings below.
    sample = df.limit(n)
    sample.cache()
    actual_rows = sample.count()
    print(f"Materialised {actual_rows:,} rows into cache.")

    # --- 1. Event Distribution ---
    t0 = time.time()
    event_dist = (
        sample.groupBy("event_type")
              .agg(F.count("*").alias("event_count"))
              .withColumn("pct_of_total", F.round(F.col("event_count") * 100.0 / actual_rows, 2))
              .orderBy(F.desc("event_count"))
    )
    event_dist.collect()  # force real execution - a DataFrame plan alone is lazy and times ~0s
    t_event_dist = time.time() - t0
    print(f"[1/3] Event Distribution: {t_event_dist:.2f}s")

    # --- 2. Daily Purchase and Revenue Analysis ---
    t0 = time.time()
    purchases = sample.filter(F.col("event_type") == "purchase")
    daily_purchase_revenue = (
        purchases.groupBy("event_date")
                 .agg(
                     F.count("*").alias("num_purchases"),
                     F.round(F.sum("price"), 2).alias("total_revenue"),
                     F.round(F.avg("price"), 2).alias("avg_order_value"),
                 )
                 .orderBy("event_date")
    )
    daily_purchase_revenue.collect()
    t_daily_purchase = time.time() - t0
    print(f"[2/3] Daily Purchase and Revenue Analysis: {t_daily_purchase:.2f}s")

    # --- 3. Top 10 Brands by Estimated Revenue ---
    t0 = time.time()
    top_brands = (
        purchases.filter(F.col("brand").isNotNull() & (F.col("brand") != ""))
                 .groupBy("brand")
                 .agg(
                     F.round(F.sum("price"), 2).alias("estimated_revenue"),
                     F.count("*").alias("num_purchases"),
                 )
                 .orderBy(F.desc("estimated_revenue"))
                 .limit(10)
    )
    top_brands.collect()
    t_top_brands = time.time() - t0
    print(f"[3/3] Top 10 Brands by Estimated Revenue: {t_top_brands:.2f}s")

    total_time = t_event_dist + t_daily_purchase + t_top_brands
    print(f"Total analysis time for {actual_rows:,} rows: {total_time:.2f}s")

    results.append((
        actual_rows,
        round(t_event_dist, 2),
        round(t_daily_purchase, 2),
        round(t_top_brands, 2),
        round(total_time, 2),
    ))

    # Remove sample from cache before moving to the next size, so results
    # don't build up in memory and each size starts from a clean slate.
    sample.unpersist()

# ============================================================
# CREATE SPARK RESULT DATAFRAME
# ============================================================
result_columns = [
    "rows",
    "event_distribution_sec",
    "daily_purchase_revenue_sec",
    "top10_brands_sec",
    "total_analysis_time_sec"
]

results_df = spark.createDataFrame(results, result_columns)

print("\n")
print("=" * 70)
print("FINAL AWS SPARK SCALABILITY RESULTS")
print("=" * 70)

results_df.show(truncate=False)

# ============================================================
# SAVE RESULTS TO S3
# ============================================================
(
    results_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(output_path)
)

print("\nResults saved to:")
print(output_path)

# ============================================================
# CLEAN UP
# ============================================================
df.unpersist()
spark.stop()
