#!/usr/bin/env python3
"""
IST3134 Group Assignment - eCommerce Behavior Data Analysis
Dataset : 2019-Nov.csv (REES46 multi-category store events, ~9GB)
Engine  : PySpark on Amazon EMR (YARN)

Produces three outputs, written as single CSV files to S3:
  1. Event Distribution            -> <output>/event_distribution
  2. Daily Purchase & Revenue      -> <output>/daily_purchase_revenue
  3. Top 10 Brands by Revenue      -> <output>/top10_brands

Usage (via spark-submit on the EMR primary node):
  spark-submit --master yarn --deploy-mode client \
    --num-executors 4 --executor-memory 3g --executor-cores 2 \
    ecommerce_analysis.py \
    s3://<BUCKET>/ecommerce/2019-Nov.csv \
    s3://<BUCKET>/ecommerce-results
"""
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

if len(sys.argv) != 3:
    print("Usage: ecommerce_analysis.py <input_csv_path> <output_base_path>")
    sys.exit(1)

input_path = sys.argv[1]
output_path = sys.argv[2]

spark = SparkSession.builder.appName("EcommerceAnalysis-Nov2019").getOrCreate()

# Explicit schema avoids a full extra pass over the 9GB file that
# .option("inferSchema", True) would trigger.
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

print(f"Reading {input_path} ...")
df = (spark.read
      .option("header", True)
      .schema(schema)
      .csv(input_path))

df = df.withColumn(
    "event_date",
    F.to_date(F.substring(F.col("event_time"), 1, 10), "yyyy-MM-dd")
)

# We scan this DataFrame three times below - cache it once.
df.cache()
total_events = df.count()
print(f"Total events: {total_events:,}")

# ---------------------------------------------------------------------
# 1. Event Distribution - count and share of each event_type
# ---------------------------------------------------------------------
t0 = time.time()
event_dist = (
    df.groupBy("event_type")
      .agg(F.count("*").alias("event_count"))
      .withColumn("pct_of_total", F.round(F.col("event_count") * 100.0 / total_events, 2))
      .orderBy(F.desc("event_count"))
)
event_dist.show(truncate=False)
event_dist.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{output_path}/event_distribution")
print(f"[1/3] Event Distribution done in {time.time() - t0:.1f}s")

# ---------------------------------------------------------------------
# 2. Daily Purchase and Revenue Analysis - only 'purchase' events
# ---------------------------------------------------------------------
t0 = time.time()
purchases = df.filter(F.col("event_type") == "purchase")

daily_purchase_revenue = (
    purchases.groupBy("event_date")
      .agg(
          F.count("*").alias("num_purchases"),
          F.round(F.sum("price"), 2).alias("total_revenue"),
          F.round(F.avg("price"), 2).alias("avg_order_value"),
      )
      .orderBy("event_date")
)
daily_purchase_revenue.show(31, truncate=False)
daily_purchase_revenue.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{output_path}/daily_purchase_revenue")
print(f"[2/3] Daily Purchase and Revenue Analysis done in {time.time() - t0:.1f}s")

# ---------------------------------------------------------------------
# 3. Top 10 Brands by Estimated Revenue - sum(price) on purchase events
# ---------------------------------------------------------------------
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
top_brands.show(10, truncate=False)
top_brands.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{output_path}/top10_brands")
print(f"[3/3] Top 10 Brands by Estimated Revenue done in {time.time() - t0:.1f}s")

print("All three analyses complete. Results written under:", output_path)
spark.stop()
