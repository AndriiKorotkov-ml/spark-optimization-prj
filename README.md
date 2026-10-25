# PySpark Sales Analytics

A scalable **Apache Spark / PySpark** batch pipeline for e-commerce sales data.
A Spark-based generator produces millions of intentionally dirty and skewed order lines;
the pipeline ingests them into a **medallion architecture** (bronze → silver → gold),
enforces data quality and builds analytical tables. A benchmark module measures the effect
of every optimization against a naive implementation.

```
data/raw/                      output/
 ├─ orders/*.csv   ─ ingest ─►  bronze/orders, bronze/customers       (Parquet, zstd)
 ├─ customers/*.csv            silver/order_lines/order_month=YYYY-MM/ (partitioned, sorted)
 └─ products.json              silver/customers/
                               quarantine/order_lines/   (rejected rows + reason + source file)
                               gold/
                                ├─ daily_revenue            (7-day moving average)
                                ├─ monthly_category_growth  (MoM growth, share of month)
                                ├─ top_products             (top-N per group, dense_rank)
                                ├─ country_x_category       (pivot)
                                ├─ return_rate_by_category  (conditional aggregation)
                                ├─ customer_rfm             (RFM via approximate quartiles)
                                ├─ customer_running_total   (cumulative sums, lag)
                                └─ cohort_retention         (Spark SQL, window functions)
                               _run_summary.json            (quality metrics, stage timings)
                               benchmark/results.md         (naive vs optimized)
```

## Scale

| Dataset | Raw order lines | Raw size | Pipeline run time |
|---|---:|---:|---:|
| default (1M orders, 200k customers) | 2.5M | 157 MB | ~22 s |
| large (5M orders, 1M customers) | 12.7M | 785 MB | ~60 s |

Measured on a MacBook Air (10 cores, 16 GB RAM) in `local[*]` mode, starting from raw CSV
(bronze is rebuilt on every run). Stage split for the large run: bronze 6 s, silver 30 s,
gold 24 s. The same code runs unchanged on a cluster with `SPARK_MASTER=yarn` /
`k8s://...` or via `spark-submit`.

The generator is itself a Spark job (`spark.range` + column expressions + `explode`), so it
scales the same way the pipeline does: the large dataset is generated in ~9 s. Customer
activity follows a power law: the top customer owns ~1.7% of all order lines, which gives a
realistic skewed key distribution.

## Optimizations

| # | Technique | Where |
|---|---|---|
| 1 | Raw CSV is parsed once into a **Parquet bronze** layer; downstream stages get column pruning and predicate pushdown | `pipeline.ingest_bronze` |
| 2 | **Broadcast hash join** for dimensions; AQE converts the customer join to broadcast at runtime when it is small enough | `transforms.enrich_orders`, `spark.py` |
| 3 | **Single-pass data quality**: one broadcast left join labels every row, then a persisted DataFrame is split into valid/rejected instead of separate anti- and semi-joins | `transforms.label_orders` |
| 4 | Quality counts come from one `groupBy(reject_reason)` over the persisted labels instead of several `count()` actions | `quality.build_report` |
| 5 | **Persist** reused DataFrames (`MEMORY_AND_DISK_DESER`) and materialize them before fan-out; unpersist when done | `pipeline.run` |
| 6 | **Pre-aggregated order facts** (one row per order) feed order-level KPIs, replacing `countDistinct` over line items | `transforms.order_facts` |
| 7 | **No global windows on large data**: RFM uses `percentile_approx` cut-offs instead of `ntile()` over an unpartitioned window that pulls every row into one partition | `transforms.customer_rfm` |
| 8 | Cohort SQL uses `MIN() OVER (PARTITION BY customer)` instead of a self-join | `pipeline.COHORT_SQL` |
| 9 | **Parallel gold writes**: independent jobs are submitted from a thread pool into a FAIR scheduler pool | `pipeline.write_gold` |
| 10 | **File layout**: `repartition(order_month)` + `sortWithinPartitions` before `partitionBy` avoids small files and improves compression; `maxRecordsPerFile` caps file size; small gold tables are written as one file | `pipeline.write` |
| 11 | **AQE tuning** (AQE itself is on by default in Spark 4): skew-join thresholds, 64 MB advisory partition size, 32 MB broadcast threshold, shuffle partitions sized to cores | `spark.py` |
| 12 | ANSI-safe parsing (`try_cast`, `try_to_timestamp`) so a single bad value never fails a multi-million-row job | `transforms.label_orders` |

## Benchmark

`uv run sales-benchmark` runs each pair on the same data with the `noop` sink (full computation, no
output I/O) and keeps the best run.

**Default dataset:** 2,548,984 raw order lines, Spark 4.2.0, local[*], best of 3 runs.

| Experiment | Baseline | Optimized | Baseline, s | Optimized, s | Speed-up |
|---|---|---|---:|---:|---:|
| scan: csv vs parquet | raw CSV, full parse | bronze Parquet, column pruning | 0.31 | 0.08 | x3.9 |
| join: products | sort-merge join | broadcast hash join | 0.56 | 0.25 | x2.2 |
| data quality split | anti + semi join, 3 passes | single labelled pass, persisted | 2.48 | 1.78 | x1.4 |
| gold reuse (4 tables) | recompute lineage each time | persisted enriched | 6.68 | 4.46 | x1.5 |
| rfm scoring | ntile over global window | percentile_approx cut-offs | 0.41 | 0.72 | x0.6 |
| cohort retention | self-join on first order | MIN() OVER window | 0.84 | 0.48 | x1.8 |
| daily kpis | countDistinct on order lines | pre-aggregated order facts | 0.85 | 0.37 | x2.3 |

**Large dataset:** 12,747,623 raw order lines, Spark 4.2.0, local[*], best of 2 runs.

| Experiment | Baseline | Optimized | Baseline, s | Optimized, s | Speed-up |
|---|---|---|---:|---:|---:|
| scan: csv vs parquet | raw CSV, full parse | bronze Parquet, column pruning | 1.24 | 0.22 | x5.6 |
| join: products | sort-merge join | broadcast hash join | 4.34 | 1.03 | x4.2 |
| data quality split | anti + semi join, 3 passes | single labelled pass, persisted | 12.11 | 8.49 | x1.4 |
| gold reuse (4 tables) | recompute lineage each time | persisted enriched | 29.91 | 18.91 | x1.6 |
| rfm scoring | ntile over global window | percentile_approx cut-offs | 1.75 | 2.39 | x0.7 |
| cohort retention | self-join on first order | MIN() OVER window | 2.37 | 1.53 | x1.5 |
| daily kpis | countDistinct on order lines | pre-aggregated order facts | 4.29 | 1.13 | x3.8 |

The gains from Parquet, broadcast joins and pre-aggregation grow with the data size.

**About RFM:** with up to 1M customers the global-window `ntile` is still faster, because
sorting that many rows in one partition is cheap and the approximate version needs an extra
aggregation pass. The point of the optimized version is that it has no single-partition
step, so its cost grows with the cluster rather than with one executor's memory. Global
windows are the classic reason Spark jobs hit OOM once the data grows; Spark itself warns
with `No Partition Defined for Window operation`.

Timings on a laptop are noisy. Other running applications, swap and thermal throttling
can move numbers by 2x, so compare runs made under the same conditions.

## Other Spark features shown

- Explicit `StructType` schemas; CSV in `PERMISSIVE` mode with `_corrupt_record`
- Source-file lineage via the `_metadata.file_name` column
- `left_anti` / `left_semi` joins, `pivot`, `count_if`, `dense_rank`, `lag`, `row_number`, row frames
- Spark SQL over temp views with CTEs and window functions
- Typed config + CLI, structured logging, per-stage and per-table timings
- uv-managed package (`uv_build`, lockfile, console scripts); 18 unit and end-to-end tests

## Run it

Requirements: [uv](https://docs.astral.sh/uv/) and Java 17+ (`java -version`).
uv installs Python 3.12 (pinned in `.python-version`) and all dependencies by itself.

```bash
uv sync                  # create .venv from uv.lock (pyspark + pytest)
uv run pytest -q         # 18 tests
uv run sales-generate    # 1M orders (~2.5M lines) -> data/raw
uv run sales-pipeline    # bronze -> silver -> gold -> output/
uv run sales-benchmark   # naive vs optimized -> output/benchmark/results.md
```

Large dataset (5M orders, ~12.7M lines):

```bash
uv run sales-generate --out data/large --orders 5000000 --customers 1000000
uv run sales-pipeline --input data/large --output output/large --quiet
uv run sales-benchmark --input data/large --bronze output/large/bronze --output output/large/benchmark
```

Pipeline options:

```bash
uv run sales-pipeline --input data/raw --output output --max-reject-ratio 0.05 --reuse-bronze --quiet
```

Tuning values that rarely change are constants at the top of `pipeline.py`:
`TOP_N = 3`, `MAX_RECORDS_PER_FILE = 1_000_000`, `GOLD_PARALLELISM = 4`.

Each command has `--help`. Driver memory and master are taken from the environment:
`SPARK_DRIVER_MEMORY=8g uv run sales-pipeline`, `SPARK_MASTER=local[4] uv run sales-pipeline`.

To start over, delete the generated folders: `rm -rf data output`.

While a job is running, the Spark UI is at <http://localhost:4040>. Gold jobs appear there
under the `gold` scheduler pool with descriptive job names.

## Project layout

```
pyproject.toml      project, scripts, dev group, uv settings
uv.lock             pinned dependency versions
.python-version     Python 3.12 for uv
src/sales_pipeline/
  spark.py          SparkSession factory and tuned defaults
  config.py         PipelineConfig dataclass + CLI parsing
  schemas.py        explicit schemas
  generate_data.py  distributed synthetic data generator
  transforms.py     cleaning + gold aggregations (pure DataFrame functions)
  quality.py        data quality report and threshold gate
  pipeline.py       ingestion, orchestration, writes, Spark SQL
  benchmark.py      naive vs optimized comparisons
tests/
  conftest.py          session-scoped SparkSession fixture
  test_transforms.py   transformation unit tests
  test_quality.py      quality gate tests
  test_pipeline.py     generator + end-to-end run
```

## Possible next steps

- Delta Lake tables with `MERGE` for incremental daily loads
- Structured Streaming ingestion from Kafka
- Airflow DAG and a Docker / Kubernetes deployment with `spark-submit`
