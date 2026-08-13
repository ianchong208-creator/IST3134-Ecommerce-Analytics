# IST3134 Group Assignment — eCommerce Behavior Analytics on Amazon EMR

Big Data Analytics in the Cloud (IST3134), May Semester 2026. A PySpark pipeline running on Amazon EMR that analyzes real-world eCommerce clickstream data to surface event behavior, revenue trends, and top-performing brands.

## Problem

Online retailers generate huge volumes of clickstream data (views, cart adds, purchases) every day. Understanding how that raw event stream converts into revenue — which days are strongest, which brands drive the most sales, and how much of all traffic actually converts to a purchase — is a classic Big Data problem: the data is too large for a single machine to process quickly, but the questions themselves are simple aggregations that benefit enormously from distributed, in-memory computation.

## Dataset

**eCommerce behavior data from multi category store** (REES46 Marketing Platform), via Kaggle:
https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

Real behavioral data from a mid-sized multi-category online store, collected in October and November 2019. Each row is one user event.

| Column | Description |
|---|---|
| `event_time` | UTC timestamp of the event |
| `event_type` | `view`, `cart`, or `purchase` |
| `product_id` | Product identifier |
| `category_id` / `category_code` | Product category (taxonomy code) |
| `brand` | Brand name (lowercase, may be blank) |
| `price` | Price in USD |
| `user_id` | Anonymized user identifier |
| `user_session` | Session identifier |

This project uses **`2019-Nov.csv`** (~9 GB, 67,501,979 events) — November was chosen over October specifically because it includes Black Friday and the run-up to Cyber Monday, giving the daily revenue analysis a genuine peak-demand story rather than a flat baseline.

## Approach

The pipeline runs as a single PySpark job (`ecommerce_analysis.py`) submitted to a managed Amazon EMR cluster:

1. **Read** the 9 GB CSV from S3 into a Spark DataFrame using an explicit schema (avoids the extra full-file pass that `inferSchema` would trigger on a file this size).
2. **Cache** the DataFrame once, since all three analyses scan it independently — this is the concrete advantage Spark has over disk-based MapReduce: each subsequent aggregation reads from memory instead of re-reading and re-parsing the source file.
3. Run three independent `groupBy`/`agg` transformations (Spark SQL DataFrame API):
   - **Event Distribution** — count and share of each `event_type`.
   - **Daily Purchase & Revenue Analysis** — purchases, total revenue, and average order value per calendar day, filtered to `event_type = 'purchase'`.
   - **Top 10 Brands by Estimated Revenue** — brands ranked by summed purchase revenue.
4. **Write** each result back to S3 as a single CSV (`coalesce(1)`), independent of the EMR cluster's lifecycle — HDFS on EMR disappears when the cluster terminates, S3 does not.

Distributing the read and the three aggregations across the cluster's core nodes is what makes this tractable at this scale — the same logic expressed as classic Java MapReduce would require separate map/reduce stages with disk I/O between each one, whereas Spark keeps the cached DataFrame in memory across all three passes.

## Repository Contents

```
.
├── ecommerce_analysis.py       # The PySpark job (see Approach above)
├── AWS_Commands_Runbook.md     # Full, copy-pasteable AWS Academy Learner Lab setup:
│                                #   S3 upload -> EMR cluster launch -> SSH -> spark-submit -> teardown
└── results/
    ├── event_distribution/
    ├── daily_purchase_revenue/
    └── top10_brands/            # Each folder holds the job's output CSV
```

## How to Reproduce

Full step-by-step commands (AWS CLI + CloudShell + SSH, written for AWS Academy Learner Lab) are in [`AWS_Commands_Runbook.md`](./AWS_Commands_Runbook.md). Summary:

1. Upload `2019-Nov.csv` to S3.
2. Launch an EMR cluster (EMR 7.13.0, Hadoop 3.4.2, Spark 3.5.6; 1 primary + 2 core `m5.xlarge`).
3. SSH into the primary node.
4. `spark-submit ecommerce_analysis.py s3://<bucket>/ecommerce/2019-Nov.csv s3://<bucket>/ecommerce-results`
5. Pull the three result CSVs back down from S3.
6. Terminate the cluster.

## Results

### 1. Event Distribution

| Event type | Count | % of total |
|---|---:|---:|
| view | 63,556,110 | 94.15% |
| cart | 3,028,930 | 4.49% |
| purchase | 916,939 | 1.36% |

Roughly **1 in 70 views** (916,939 / 63,556,110) ends in a purchase — a useful conversion-rate baseline.

### 2. Daily Purchase & Revenue Analysis

29 days of November (see `results/daily_purchase_revenue/` for the full table). Headline numbers:

- Typical day: ~22,000–28,000 purchases, ~$6.3M–$8.0M revenue, ~$285–$315 average order value.
- **Nov 16–17 stand out sharply**: 68,247 and 185,195 purchases respectively (vs. a normal day's ~25,000), with Nov 17 alone bringing in ~$57.7M — more than 7x a typical day.
- **Nov 15 is entirely absent** from the data.
- Nov 29 (Black Friday) shows a smaller but visible bump: 32,107 purchases, ~$9.6M.

### 3. Top 10 Brands by Estimated Revenue

| Brand | Estimated revenue | Purchases |
|---|---:|---:|
| apple | $127,512,524.88 | 166,064 |
| samsung | $54,869,880.87 | 200,027 |
| xiaomi | $11,259,865.96 | 68,292 |
| lg | $5,239,018.76 | 12,879 |
| huawei | $4,780,682.35 | 23,703 |
| sony | $3,862,886.30 | 10,309 |
| lucente | $3,527,545.57 | 14,559 |
| oppo | $3,488,540.76 | 15,080 |
| acer | $3,347,306.53 | 6,402 |
| lenovo | $2,698,106.30 | 6,547 |

### 4. Performance Characteristics (Solution 1: PySpark on EMR)

Measured by re-running the identical job (same cluster shape, same executor config, same input file) with the driver log captured and bookended with UTC timestamps.

**Execution time** — 159 s (2 min 39 s) end to end:

| Stage | Duration |
|---|---:|
| Total job (start to finish) | 159 s (2 min 39 s) |
| Spark/YARN startup + initial read, parse, and cache | ≈ 130.5 s (one-time) |
| Event Distribution | 10.7 s |
| Daily Purchase and Revenue Analysis | 9.9 s |
| Top 10 Brands by Estimated Revenue | 7.9 s |

The three aggregations together take only 28.5 of the 159 seconds — the remaining ~130 s is the one-time cost of starting the Spark application and reading, parsing, and caching the full 67,501,979-row file. Every aggregation after the first is cheap specifically because it reuses that cached DataFrame instead of repeating the read.

**Scalability** — horizontal by design: 1 primary + 2 core `m5.xlarge` nodes, with YARN auto-partitioning the cached DataFrame and all three stages across 4 executors (`--num-executors 4 --executor-memory 3g --executor-cores 2`). Adding capacity is a config change, not a code change.

**Memory limitations** — measured via the aggregated driver+executor logs (`yarn logs -applicationId ...`): cached DataFrame partitions landed in executor memory at ~42–55 MiB each, with free memory decreasing smoothly as partitions accumulated and no spill-to-disk, eviction, or "not enough memory" warnings anywhere in the log. The full 9 GB file fit comfortably in the two core nodes' combined memory. (Spark's default `MEMORY_AND_DISK` storage level for cached DataFrames would spill to local disk automatically if it didn't fit, rather than failing.)

**Ability to handle increasing data volume** — additive, not architectural: pointing the same job at both months combined (`s3://<bucket>/ecommerce/2019-*.csv`, ~14.6 GB) needs no code change, just enough core nodes to hold throughput steady. Not empirically tested this round (single data volume only).

Solution 2 (pandas, single-machine) will be measured against the same four dimensions once complete, for a direct comparison.

## Notable Findings

- **Samsung outsells Apple on volume, Apple wins on revenue.** Samsung had 20% more purchases than Apple (200,027 vs. 166,064) but Apple generated more than double the revenue ($127.5M vs. $54.9M) — average selling price, not unit volume, is the dominant factor here, and it's the kind of pattern a single `groupBy`/`agg` on Spark surfaces immediately at this scale.
- **The Nov 15 gap and Nov 16–17 spike are very unlikely to be a processing artifact.** The date-parsing logic is a straightforward substring extraction applied uniformly to every row, so it has no mechanism to selectively distort two specific days — this points to a genuine irregularity in the source data (most plausibly a collection or export gap on the platform's side that batched the 15th's events into the following days). It's flagged here as a data-quality observation rather than an error to be fixed.

## Tech Stack

Amazon EMR 7.13.0 (Hadoop 3.4.2, Spark 3.5.6) · PySpark DataFrame API · Amazon S3 · AWS CLI · AWS Academy Learner Lab

## Authors

- Ian Chong Yi Ren
- Tan Yan Bin

Course: IST3134 — Big Data Analytics in the Cloud, Sunway University, May Semester 2026.
