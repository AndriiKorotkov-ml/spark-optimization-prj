import argparse
import json
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import transforms as T
from .pipeline import COHORT_SQL, read_products, read_raw_orders
from .spark import get_spark

NAIVE_COHORT_SQL = """
WITH first_order AS (
    SELECT customer_id, MIN(order_month) AS cohort_month
    FROM order_facts WHERE status = 'completed' GROUP BY customer_id
),
activity AS (
    SELECT DISTINCT o.customer_id, f.cohort_month,
           CAST(months_between(to_date(o.order_month || '-01'),
                               to_date(f.cohort_month || '-01')) AS INT) AS month_number
    FROM order_facts o JOIN first_order f USING (customer_id)
    WHERE o.status = 'completed'
)
SELECT cohort_month, month_number, COUNT(*) AS active_customers
FROM activity GROUP BY cohort_month, month_number
"""


@dataclass
class Result:
    experiment: str
    baseline: str
    optimized: str
    baseline_s: float
    optimized_s: float

    @property
    def speedup(self) -> float:
        return self.baseline_s / self.optimized_s if self.optimized_s else float("inf")


def noop(*dfs: DataFrame) -> Callable[[], None]:
    def action() -> None:
        for df in dfs:
            df.write.format("noop").mode("overwrite").save()

    return action


@contextmanager
def conf(spark: SparkSession, **settings: str):
    keys = {k.replace("__", "."): v for k, v in settings.items()}
    previous = {k: spark.conf.get(k, None) for k in keys}
    for k, v in keys.items():
        spark.conf.set(k, v)
    try:
        yield
    finally:
        for k, v in previous.items():
            if v is None:
                spark.conf.unset(k)
            else:
                spark.conf.set(k, v)


def measure(spark: SparkSession, fn: Callable[[], None], repeats: int) -> float:
    timings = []
    for _ in range(repeats):
        spark.catalog.clearCache()
        started = time.perf_counter()
        fn()
        timings.append(time.perf_counter() - started)
    return round(min(timings), 2)


def naive_clean_orders(raw: DataFrame, products: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = raw.withColumns(
        {
            "customer_id": F.nullif(F.trim("customer_id"), F.lit("")),
            "quantity": F.col("quantity").try_cast("int"),
            "order_ts": F.try_to_timestamp("order_ts", F.lit("yyyy-MM-dd HH:mm:ss")),
        }
    )
    bad = (
        F.col("_corrupt_record").isNotNull()
        | F.col("customer_id").isNull()
        | F.col("quantity").isNull()
        | (F.col("quantity") <= 0)
        | F.col("order_ts").isNull()
    )
    known = products.select("product_id")
    passed = typed.filter(~bad)
    rejected = typed.filter(bad).unionByName(passed.join(known, "product_id", "left_anti"))
    valid = passed.join(known, "product_id", "left_semi").dropDuplicates(T.ORDER_KEY)
    return valid, rejected


def naive_rfm(facts: DataFrame) -> DataFrame:
    base = T.completed(facts).groupBy("customer_id").agg(
        F.max("order_date").alias("last"),
        F.count("*").alias("frequency"),
        F.sum("order_total").alias("monetary"),
    )
    return base.withColumns(
        {
            "r_score": F.ntile(4).over(Window.orderBy("last")),
            "f_score": F.ntile(4).over(Window.orderBy("frequency")),
            "m_score": F.ntile(4).over(Window.orderBy("monetary")),
        }
    )


def gold_from(enriched: DataFrame, categories: list[str]) -> list[DataFrame]:
    return [
        T.monthly_category_growth(enriched),
        T.top_products_per_category(enriched),
        T.revenue_by_country_category(enriched, categories),
        T.return_rate_by_category(enriched),
    ]


def run(spark: SparkSession, raw_path: str, bronze_path: str, repeats: int) -> list[Result]:
    results: list[Result] = []

    def compare(name: str, baseline: str, base_fn, optimized: str, opt_fn) -> None:
        b = measure(spark, base_fn, repeats)
        o = measure(spark, opt_fn, repeats)
        results.append(Result(name, baseline, optimized, b, o))
        print(f"{name:<22} {b:>7.2f}s -> {o:>7.2f}s  x{b / o:.1f}", flush=True)

    products = read_products(spark, raw_path)
    categories = sorted(r.category for r in products.select("category").distinct().collect())
    bronze = spark.read.parquet(f"{bronze_path}/orders")
    customers = T.clean_customers(spark.read.parquet(f"{bronze_path}/customers"))

    def status_totals(df: DataFrame) -> DataFrame:
        return df.groupBy("status").agg(F.sum(F.col("quantity").try_cast("int")).alias("units"))

    compare(
        "scan: csv vs parquet",
        "raw CSV, full parse",
        noop(status_totals(read_raw_orders(spark, raw_path))),
        "bronze Parquet, column pruning",
        noop(status_totals(bronze)),
    )

    def product_join() -> DataFrame:
        return bronze.join(products.drop("stock"), "product_id")

    with conf(
        spark,
        spark__sql__autoBroadcastJoinThreshold="-1",
        spark__sql__adaptive__autoBroadcastJoinThreshold="-1",
    ):
        smj = measure(spark, noop(product_join()), repeats)
    bhj = measure(spark, noop(bronze.join(F.broadcast(products.drop("stock")), "product_id")), repeats)
    results.append(Result("join: products", "sort-merge join", "broadcast hash join", smj, bhj))
    print(f"{'join: products':<22} {smj:>7.2f}s -> {bhj:>7.2f}s  x{smj / bhj:.1f}", flush=True)

    def naive_dq() -> None:
        valid, rejected = naive_clean_orders(bronze, products)
        noop(valid, rejected)()
        rejected.count()

    def single_pass_dq() -> None:
        labeled = T.label_orders(bronze, products).persist(StorageLevel.MEMORY_AND_DISK_DESER)
        valid, rejected = T.split_orders(labeled)
        noop(valid, rejected)()
        labeled.groupBy("reject_reason").count().collect()
        labeled.unpersist()

    compare(
        "data quality split",
        "anti + semi join, 3 passes",
        naive_dq,
        "single labelled pass, persisted",
        single_pass_dq,
    )

    def enriched_df() -> DataFrame:
        valid, _ = T.clean_orders(bronze, products)
        return T.enrich_orders(valid, customers, products)

    def gold_no_cache() -> None:
        noop(*gold_from(enriched_df(), categories))()

    def gold_cached() -> None:
        enriched = enriched_df().persist(StorageLevel.MEMORY_AND_DISK_DESER)
        enriched.count()
        noop(*gold_from(enriched, categories))()
        enriched.unpersist()

    compare(
        "gold reuse (4 tables)",
        "recompute lineage each time",
        gold_no_cache,
        "persisted enriched",
        gold_cached,
    )

    enriched = enriched_df().persist(StorageLevel.MEMORY_AND_DISK_DESER)
    facts = T.order_facts(enriched).persist(StorageLevel.MEMORY_AND_DISK_DESER)

    enriched.count()
    facts.count()
    facts.createOrReplaceTempView("order_facts")

    def no_clear(fn: Callable[[], None]) -> float:
        timings = []
        for _ in range(repeats):
            started = time.perf_counter()
            fn()
            timings.append(time.perf_counter() - started)
        return round(min(timings), 2)

    for name, baseline, base_fn, optimized, opt_fn in [
        (
            "rfm scoring",
            "ntile over global window",
            noop(naive_rfm(facts)),
            "percentile_approx cut-offs",
            lambda: noop(T.customer_rfm(facts))(),
        ),
        (
            "cohort retention",
            "self-join on first order",
            noop(spark.sql(NAIVE_COHORT_SQL)),
            "MIN() OVER window",
            noop(spark.sql(COHORT_SQL)),
        ),
        (
            "daily kpis",
            "countDistinct on order lines",
            noop(
                T.completed(enriched)
                .groupBy("order_date")
                .agg(
                    F.sum("line_revenue"),
                    F.countDistinct("order_id"),
                    F.countDistinct("customer_id"),
                )
            ),
            "pre-aggregated order facts",
            noop(T.daily_revenue(facts)),
        ),
    ]:
        b = no_clear(base_fn)
        o = no_clear(opt_fn)
        results.append(Result(name, baseline, optimized, b, o))
        print(f"{name:<22} {b:>7.2f}s -> {o:>7.2f}s  x{b / o:.1f}", flush=True)

    facts.unpersist()
    enriched.unpersist()
    return results


def to_markdown(results: list[Result], meta: dict) -> str:
    lines = [
        f"Dataset: {meta['raw_rows']:,} raw order lines, Spark {meta['spark']}, {meta['master']}, "
        f"best of {meta['repeats']} runs.",
        "",
        "| Experiment | Baseline | Optimized | Baseline, s | Optimized, s | Speed-up |",
        "|---|---|---|---:|---:|---:|",
    ]
    for r in results:
        lines.append(
            f"| {r.experiment} | {r.baseline} | {r.optimized} | {r.baseline_s:.2f} | "
            f"{r.optimized_s:.2f} | x{r.speedup:.1f} |"
        )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Compare naive and optimized Spark implementations")
    parser.add_argument("--input", default="data/raw")
    parser.add_argument("--bronze", default="output/bronze")
    parser.add_argument("--output", default="output/benchmark")
    parser.add_argument("--repeats", type=int, default=2)
    args = parser.parse_args(argv)

    if not Path(args.bronze, "orders", "_SUCCESS").exists():
        raise SystemExit(f"{args.bronze} not found: run the pipeline first")

    spark = get_spark("sales-benchmark")
    try:
        results = run(spark, args.input, args.bronze, args.repeats)
        meta = {
            "raw_rows": spark.read.parquet(f"{args.bronze}/orders").count(),
            "spark": spark.version,
            "master": spark.sparkContext.master,
            "repeats": args.repeats,
        }
    finally:
        spark.stop()

    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    (out / "results.json").write_text(
        json.dumps({"meta": meta, "results": [asdict(r) | {"speedup": round(r.speedup, 2)} for r in results]}, indent=2)
    )
    (out / "results.md").write_text(to_markdown(results, meta))
    print("\n" + to_markdown(results, meta))


if __name__ == "__main__":
    main()
