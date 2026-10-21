from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

VALID_STATUSES = ["completed", "cancelled", "returned"]
ORDER_KEY = ["order_id", "product_id", "quantity", "order_ts"]
ORDER_GRAIN = ["order_id", "customer_id", "country", "order_ts", "order_date", "order_month", "status"]


def _blank_to_null(name: str) -> Column:
    return F.nullif(F.trim(name), F.lit(""))


def _reject_reason() -> Column:
    return (
        F.when(F.col("_corrupt_record").isNotNull(), "malformed_row")
        .when(F.col("order_id").isNull(), "missing_order_id")
        .when(F.col("customer_id").isNull(), "missing_customer_id")
        .when(F.col("quantity").isNull(), "bad_quantity")
        .when(F.col("quantity") <= 0, "non_positive_quantity")
        .when(F.col("order_ts").isNull(), "bad_timestamp")
        .when(~F.col("status").isin(VALID_STATUSES), "unknown_status")
        .when(F.col("_known_product").isNull(), "unknown_product")
    )


def label_orders(raw: DataFrame, products: DataFrame) -> DataFrame:
    known = F.broadcast(products.select("product_id", F.lit(True).alias("_known_product")))
    return (
        raw.withColumns(
            {
                "order_id": _blank_to_null("order_id"),
                "customer_id": _blank_to_null("customer_id"),
                "product_id": _blank_to_null("product_id"),
                "quantity": F.col("quantity").try_cast("int"),
                "order_ts": F.try_to_timestamp("order_ts", F.lit("yyyy-MM-dd HH:mm:ss")),
                "status": F.lower(F.trim("status")),
            }
        )
        .join(known, "product_id", "left")
        .withColumn("reject_reason", _reject_reason())
        .drop("_known_product")
    )


def split_orders(labeled: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = (
        labeled.filter(F.col("reject_reason").isNull())
        .dropDuplicates(ORDER_KEY)
        .drop("_corrupt_record", "reject_reason", "_source_file")
        .withColumns(
            {
                "order_date": F.to_date("order_ts"),
                "order_month": F.date_format("order_ts", "yyyy-MM"),
            }
        )
    )
    rejected = labeled.filter(F.col("reject_reason").isNotNull()).drop("_corrupt_record")
    return valid, rejected


def clean_orders(raw: DataFrame, products: DataFrame) -> tuple[DataFrame, DataFrame]:
    return split_orders(label_orders(raw, products))


def clean_customers(raw: DataFrame) -> DataFrame:
    latest_first = Window.partitionBy("customer_id").orderBy(F.desc_nulls_last("signup_date"))
    return (
        raw.withColumns(
            {
                "customer_id": _blank_to_null("customer_id"),
                "full_name": F.initcap(F.trim("full_name")),
                "country": F.upper(F.trim("country")),
                "email": F.lower(F.trim("email")),
                "signup_date": F.try_to_date("signup_date", "yyyy-MM-dd"),
            }
        )
        .filter(F.col("customer_id").isNotNull())
        .withColumn("_rn", F.row_number().over(latest_first))
        .filter("_rn = 1")
        .drop("_rn")
    )


def enrich_orders(orders: DataFrame, customers: DataFrame, products: DataFrame) -> DataFrame:
    return (
        orders.join(F.broadcast(products.drop("stock")), "product_id", "inner")
        .join(customers.select("customer_id", "country", "signup_date"), "customer_id", "left")
        .withColumn("country", F.coalesce("country", F.lit("UNKNOWN")))
        .withColumn("line_revenue", (F.col("quantity") * F.col("price")).cast("decimal(12,2)"))
    )


def order_facts(enriched: DataFrame) -> DataFrame:
    return enriched.groupBy(*ORDER_GRAIN).agg(
        F.sum("line_revenue").alias("order_total"),
        F.sum("quantity").alias("units"),
        F.count("*").alias("lines"),
    )


def completed(df: DataFrame) -> DataFrame:
    return df.filter(F.col("status") == "completed")


def daily_revenue(orders: DataFrame) -> DataFrame:
    w7 = Window.orderBy("order_date").rowsBetween(-6, 0)
    return (
        completed(orders)
        .groupBy("order_date")
        .agg(
            F.sum("order_total").alias("revenue"),
            F.count("*").alias("orders"),
            F.countDistinct("customer_id").alias("customers"),
        )
        .withColumn("avg_order_value", F.round(F.col("revenue") / F.col("orders"), 2))
        .withColumn("revenue_7d_avg", F.round(F.avg("revenue").over(w7), 2))
        .orderBy("order_date")
    )


def monthly_category_growth(enriched: DataFrame) -> DataFrame:
    w = Window.partitionBy("category").orderBy("order_month")
    return (
        completed(enriched)
        .groupBy("category", "order_month")
        .agg(F.sum("line_revenue").alias("revenue"))
        .withColumn("prev_revenue", F.lag("revenue").over(w))
        .withColumn(
            "mom_growth_pct",
            F.round((F.col("revenue") - F.col("prev_revenue")) / F.col("prev_revenue") * 100, 1),
        )
        .withColumn(
            "share_of_month_pct",
            F.round(F.col("revenue") / F.sum("revenue").over(Window.partitionBy("order_month")) * 100, 1),
        )
        .orderBy("category", "order_month")
    )


def top_products_per_category(enriched: DataFrame, n: int = 3) -> DataFrame:
    w = Window.partitionBy("category").orderBy(F.desc("revenue"))
    return (
        completed(enriched)
        .groupBy("category", "product_id", "product_name")
        .agg(F.sum("line_revenue").alias("revenue"), F.sum("quantity").alias("units"))
        .withColumn("rank", F.dense_rank().over(w))
        .filter(F.col("rank") <= n)
        .orderBy("category", "rank")
    )


def revenue_by_country_category(enriched: DataFrame, categories: list[str] | None = None) -> DataFrame:
    if categories is None:
        categories = sorted(r.category for r in enriched.select("category").distinct().collect())
    return (
        completed(enriched)
        .groupBy("country")
        .pivot("category", categories)
        .agg(F.round(F.sum("line_revenue"), 0))
        .na.fill(0)
        .orderBy("country")
    )


def return_rate_by_category(enriched: DataFrame) -> DataFrame:
    per_order = enriched.select("category", "order_id", "status").distinct()
    return (
        per_order.groupBy("category")
        .agg(
            F.count("*").alias("orders"),
            F.count_if(F.col("status") == "returned").alias("returned"),
            F.count_if(F.col("status") == "cancelled").alias("cancelled"),
        )
        .withColumn("return_rate_pct", F.round(F.col("returned") / F.col("orders") * 100, 2))
        .withColumn("cancel_rate_pct", F.round(F.col("cancelled") / F.col("orders") * 100, 2))
        .orderBy(F.desc("return_rate_pct"))
    )


def _quartile_score(col: str, cuts: list) -> Column:
    q1, q2, q3 = cuts
    c = F.col(col)
    return F.when(c <= q1, 1).when(c <= q2, 2).when(c <= q3, 3).otherwise(4)


def customer_rfm(orders: DataFrame, as_of: str | None = None, accuracy: int = 10_000) -> DataFrame:
    base = (
        completed(orders)
        .groupBy("customer_id", "country")
        .agg(
            F.max("order_date").alias("last_order_date"),
            F.count("*").alias("frequency"),
            F.sum("order_total").alias("monetary"),
        )
        .withColumn("last_order_day", F.unix_date("last_order_date"))
    )
    probs = F.array(F.lit(0.25), F.lit(0.5), F.lit(0.75))
    stats = base.agg(
        F.max("last_order_date").alias("max_date"),
        F.percentile_approx("last_order_day", probs, accuracy).alias("r"),
        F.percentile_approx("frequency", probs, accuracy).alias("f"),
        F.percentile_approx("monetary", probs, accuracy).alias("m"),
    ).first()

    ref = F.lit(as_of).cast("date") if as_of else F.date_add(F.lit(stats.max_date), 1)
    scored = base.withColumns(
        {
            "recency_days": F.datediff(ref, "last_order_date"),
            "r_score": _quartile_score("last_order_day", stats.r),
            "f_score": _quartile_score("frequency", stats.f),
            "m_score": _quartile_score("monetary", stats.m),
        }
    ).drop("last_order_day")
    r, f, m = F.col("r_score"), F.col("f_score"), F.col("m_score")
    return scored.withColumn(
        "segment",
        F.when((r >= 3) & (f >= 3) & (m >= 3), "Champions")
        .when((r >= 3) & (f >= 2), "Loyal")
        .when((r <= 1) & (m >= 3), "At risk (big spenders)")
        .when(r <= 1, "Lost")
        .otherwise("Regular"),
    )


def customer_running_total(orders: DataFrame) -> DataFrame:
    w = Window.partitionBy("customer_id").orderBy("order_ts", "order_id")
    return (
        completed(orders)
        .select("customer_id", "order_id", "order_ts", "order_total")
        .withColumns(
            {
                "order_seq": F.row_number().over(w),
                "cumulative_spend": F.sum("order_total").over(
                    w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
                ),
                "days_since_prev_order": F.datediff("order_ts", F.lag("order_ts").over(w)),
            }
        )
    )
