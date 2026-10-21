import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import quality
from . import transforms as T
from .config import PipelineConfig
from .schemas import PRODUCTS, RAW_CUSTOMERS, RAW_ORDERS
from .spark import get_spark

log = logging.getLogger("sales_pipeline")

COHORT_SQL = """
WITH monthly AS (
    SELECT DISTINCT customer_id, order_month
    FROM order_facts
    WHERE status = 'completed'
),
activity AS (
    SELECT customer_id,
           order_month,
           MIN(order_month) OVER (PARTITION BY customer_id) AS cohort_month
    FROM monthly
)
SELECT cohort_month,
       CAST(months_between(to_date(order_month || '-01'), to_date(cohort_month || '-01')) AS INT)
           AS month_number,
       COUNT(*) AS active_customers,
       ROUND(100 * COUNT(*) / FIRST_VALUE(COUNT(*)) OVER (
           PARTITION BY cohort_month ORDER BY MIN(order_month)), 1) AS retention_pct
FROM activity
GROUP BY cohort_month, order_month
ORDER BY cohort_month, month_number
"""

SMALL_GOLD_TABLES = {
    "daily_revenue",
    "monthly_category_growth",
    "top_products",
    "country_x_category",
    "return_rate_by_category",
    "cohort_retention",
}


class StageTimer:
    def __init__(self) -> None:
        self.durations: dict[str, float] = {}

    @contextmanager
    def stage(self, name: str):
        log.info("stage %s: started", name)
        started = time.perf_counter()
        yield
        self.durations[name] = round(time.perf_counter() - started, 2)
        log.info("stage %s: done in %.2fs", name, self.durations[name])


def read_raw_orders(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.schema(RAW_ORDERS)
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(f"{path}/orders")
        .withColumn("_source_file", F.col("_metadata.file_name"))
    )


def read_raw_customers(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(RAW_CUSTOMERS).option("header", True).csv(f"{path}/customers")


def read_products(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(PRODUCTS).json(f"{path}/products.json")


def write(
    df: DataFrame,
    path: str,
    partition_by: str | None = None,
    files: int | None = None,
    max_records_per_file: int | None = None,
) -> None:
    if files:
        df = df.coalesce(files)
    writer = df.write.mode("overwrite")
    if max_records_per_file:
        writer = writer.option("maxRecordsPerFile", max_records_per_file)
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.parquet(path)


def ingest_bronze(spark: SparkSession, cfg: PipelineConfig) -> None:
    bronze = f"{cfg.output_path}/bronze"
    if cfg.reuse_bronze and Path(bronze, "orders", "_SUCCESS").exists():
        log.info("bronze: reusing %s", bronze)
        return
    write(read_raw_orders(spark, cfg.input_path), f"{bronze}/orders")
    write(read_raw_customers(spark, cfg.input_path), f"{bronze}/customers")


def write_gold(spark: SparkSession, gold: dict[str, DataFrame], out: str, parallelism: int) -> None:
    def job(item: tuple[str, DataFrame]) -> tuple[str, float]:
        name, df = item
        started = time.perf_counter()
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "gold")
        spark.sparkContext.setJobDescription(f"gold/{name}")
        write(df, f"{out}/gold/{name}", files=1 if name in SMALL_GOLD_TABLES else None)
        return name, time.perf_counter() - started

    with ThreadPoolExecutor(max_workers=max(1, parallelism)) as pool:
        for name, seconds in pool.map(job, gold.items()):
            log.info("gold table written: %-24s %.2fs", name, seconds)


def show(title: str, df: DataFrame, n: int = 10) -> None:
    print(f"\n=== {title} ===")
    df.show(n, truncate=False)


def build_gold(
    spark: SparkSession, enriched: DataFrame, facts: DataFrame, categories: list[str], cfg: PipelineConfig
) -> dict[str, DataFrame]:
    facts.createOrReplaceTempView("order_facts")
    return {
        "daily_revenue": T.daily_revenue(facts),
        "monthly_category_growth": T.monthly_category_growth(enriched),
        "top_products": T.top_products_per_category(enriched, cfg.top_n),
        "country_x_category": T.revenue_by_country_category(enriched, categories),
        "return_rate_by_category": T.return_rate_by_category(enriched),
        "customer_rfm": T.customer_rfm(facts, cfg.as_of),
        "customer_running_total": T.customer_running_total(facts),
        "cohort_retention": spark.sql(COHORT_SQL),
    }


def print_results(gold: dict[str, DataFrame], report: quality.QualityReport, cfg: PipelineConfig) -> None:
    print(f"\n=== Rejected rows by reason ({report.duplicate_rows} duplicates removed) ===")
    for reason, rows in report.rejected_by_reason.items():
        print(f"  {reason:<24}{rows:>10}")
    show("Daily revenue (last 7 days)", gold["daily_revenue"].orderBy(F.desc("order_date")), 7)
    show(f"Top-{cfg.top_n} products per category", gold["top_products"], 5 * cfg.top_n)
    show(
        "Month-over-month growth (Electronics)",
        gold["monthly_category_growth"].filter("category = 'Electronics'"),
        12,
    )
    show("Revenue: country x category", gold["country_x_category"])
    show("Return and cancel rate by category", gold["return_rate_by_category"])
    show(
        "RFM segments",
        gold["customer_rfm"]
        .groupBy("segment")
        .agg(F.count("*").alias("customers"), F.round(F.avg("monetary"), 2).alias("avg_spend"))
        .orderBy(F.desc("customers")),
    )
    show("Top customers by lifetime spend", gold["customer_rfm"].orderBy(F.desc("monetary")), 5)
    first_cohort = gold["cohort_retention"].agg(F.min("cohort_month")).first()[0]
    show(
        f"Cohort retention ({first_cohort})",
        gold["cohort_retention"].filter(F.col("cohort_month") == F.lit(first_cohort)),
        12,
    )


def run(spark: SparkSession, cfg: PipelineConfig) -> dict:
    timer = StageTimer()
    out = cfg.output_path

    with timer.stage("bronze"):
        ingest_bronze(spark, cfg)
        products = read_products(spark, cfg.input_path).cache()
        categories = sorted(r.category for r in products.select("category").distinct().collect())

    with timer.stage("silver"):
        bronze_orders = spark.read.parquet(f"{out}/bronze/orders")
        bronze_customers = spark.read.parquet(f"{out}/bronze/customers")

        labeled = T.label_orders(bronze_orders, products).persist(StorageLevel.MEMORY_AND_DISK_DESER)
        valid, rejected = T.split_orders(labeled)
        customers = T.clean_customers(bronze_customers)
        enriched = T.enrich_orders(valid, customers, products).persist(StorageLevel.MEMORY_AND_DISK_DESER)

        report = quality.build_report(labeled, enriched.count())
        log.info(
            "quality: %d raw, %d valid, %d rejected (%.2f%%), %d duplicates",
            report.raw_rows, report.valid_rows, report.rejected_rows,
            report.reject_ratio * 100, report.duplicate_rows,
        )
        quality.enforce(report, cfg.max_reject_ratio)

        write(
            enriched.repartition("order_month").sortWithinPartitions("customer_id", "order_ts"),
            f"{out}/silver/order_lines",
            partition_by="order_month",
            max_records_per_file=cfg.max_records_per_file,
        )
        write(rejected, f"{out}/quarantine/order_lines", files=1)
        write(customers, f"{out}/silver/customers")
        labeled.unpersist()

    with timer.stage("gold"):
        facts = T.order_facts(enriched).persist(StorageLevel.MEMORY_AND_DISK_DESER)
        log.info("order facts: %d orders", facts.count())
        gold = build_gold(spark, enriched, facts, categories, cfg)
        write_gold(spark, gold, out, cfg.gold_parallelism)

    if cfg.show_results:
        print_results(gold, report, cfg)
        print("\n=== Physical plan of the enrichment join ===")
        T.enrich_orders(valid, customers, products).explain()

    facts.unpersist()
    enriched.unpersist()
    products.unpersist()

    summary = {
        "quality": report.to_dict(),
        "stage_seconds": timer.durations,
        "total_seconds": round(sum(timer.durations.values()), 2),
        "gold_tables": sorted(gold),
        "spark": {
            "version": spark.version,
            "master": spark.sparkContext.master,
            "default_parallelism": spark.sparkContext.defaultParallelism,
            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        },
    }
    Path(out).mkdir(parents=True, exist_ok=True)
    Path(out, "_run_summary.json").write_text(json.dumps(summary, indent=2))
    return summary


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = PipelineConfig.from_args(argv)
    spark = get_spark()
    try:
        summary = run(spark, cfg)
        log.info("finished in %.1fs: %s", summary["total_seconds"], json.dumps(summary["stage_seconds"]))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
