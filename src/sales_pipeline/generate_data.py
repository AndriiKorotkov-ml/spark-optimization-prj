import argparse
import json
import random
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from .spark import get_spark

CATEGORIES = {
    "Electronics": ["Headphones", "Keyboard", "Monitor", "Mouse", "Webcam", "SSD"],
    "Books": ["Novel", "Cookbook", "Textbook", "Comic", "Biography"],
    "Home": ["Lamp", "Chair", "Mug", "Blanket", "Kettle"],
    "Sports": ["Yoga mat", "Dumbbell", "Bike helmet", "Water bottle"],
    "Toys": ["Puzzle", "Board game", "Lego set", "Drone"],
}
COUNTRIES = ["UA", "PL", "DE", "US", "GB", "FR"]
STATUSES = ["completed"] * 8 + ["cancelled", "returned"]
FIRST = ["Olena", "Andrii", "Iryna", "Taras", "Anna", "Max", "Sofia", "John", "Emma", "Lukas"]
LAST = ["Shevchenko", "Kowalski", "Muller", "Smith", "Brown", "Bondar", "Dubois", "Novak"]
SKEW_POWER = 3


def make_products(seed: int) -> list[dict]:
    rng = random.Random(seed)
    products, pid = [], 1
    for category, names in CATEGORIES.items():
        for name in names:
            for variant in ("Basic", "Pro"):
                base = rng.uniform(5, 300) * (2.5 if variant == "Pro" else 1)
                products.append(
                    {
                        "product_id": f"P{pid:04d}",
                        "product_name": f"{name} {variant}",
                        "category": category,
                        "price": round(base, 2),
                        "stock": rng.randint(0, 500),
                    }
                )
                pid += 1
    return products


def _pick(values: list[str], r: Column) -> Column:
    arr = F.array(*[F.lit(v) for v in values])
    return F.element_at(arr, (F.floor(r * len(values)) + 1).cast("int"))


def generate_customers(spark: SparkSession, n: int, seed: int) -> DataFrame:
    country = F.col("clean_country")
    messy = F.col("r_messy")
    return (
        spark.range(1, n + 1)
        .withColumns(
            {
                "r_country": F.rand(seed),
                "r_messy": F.rand(seed + 1),
                "r_first": F.rand(seed + 2),
                "r_last": F.rand(seed + 3),
                "r_signup": F.rand(seed + 4),
                "r_bad_date": F.rand(seed + 5),
            }
        )
        .withColumns(
            {
                "customer_id": F.format_string("C%07d", "id"),
                "clean_country": _pick(COUNTRIES, F.col("r_country")),
                "full_name": F.concat_ws(" ", _pick(FIRST, F.col("r_first")), _pick(LAST, F.col("r_last"))),
                "signup": F.date_add(
                    F.lit("2024-06-15").cast("date"), F.floor(F.col("r_signup") * 300).cast("int")
                ),
            }
        )
        .withColumns(
            {
                "country": F.when(messy < 0.33, F.lower(country))
                .when(messy < 0.66, F.concat(F.lit(" "), country, F.lit(" ")))
                .otherwise(country),
                "signup_date": F.when(F.col("r_bad_date") < 0.02, F.lit("not-a-date")).otherwise(
                    F.date_format("signup", "yyyy-MM-dd")
                ),
                "email": F.lower(
                    F.concat(F.regexp_replace("full_name", " ", "."), F.col("id"), F.lit("@example.com"))
                ),
            }
        )
        .select("customer_id", "full_name", "email", "country", "signup_date")
    )


def generate_orders(
    spark: SparkSession, n_orders: int, n_customers: int, n_products: int, seed: int, partitions: int
) -> DataFrame:
    start = F.unix_timestamp(F.lit("2025-01-01 00:00:00"))
    customer_idx = F.floor(F.pow(F.rand(seed), SKEW_POWER) * n_customers) + 1
    orders = spark.range(1, n_orders + 1, numPartitions=partitions).withColumns(
        {
            "order_id": F.format_string("O%09d", "id"),
            "customer_id": F.format_string("C%07d", customer_idx.cast("long")),
            "ts": F.timestamp_seconds(start + F.floor(F.rand(seed + 1) * 365 * 86400)),
            "status": _pick(STATUSES, F.rand(seed + 2)),
            "n_lines": (F.floor(F.rand(seed + 3) * 4) + 1).cast("int"),
        }
    )
    noise = F.col("noise")
    lines = (
        orders.withColumn("line_no", F.explode(F.sequence(F.lit(1), F.col("n_lines"))))
        .withColumns(
            {
                "product_idx": F.floor(F.rand(seed + 4) * n_products) + 1,
                "qty": F.floor(F.rand(seed + 5) * 5) + 1,
                "noise": F.rand(seed + 6),
                "copies": F.when(F.rand(seed + 7) < 0.02, F.lit(2)).otherwise(F.lit(1)),
            }
        )
        .withColumn("copy", F.explode(F.sequence(F.lit(1), F.col("copies"))))
        .select(
            "order_id",
            F.when(noise < 0.01, F.lit("")).otherwise(F.col("customer_id")).alias("customer_id"),
            F.when(noise.between(0.025, 0.03), F.lit("P9999"))
            .otherwise(F.format_string("P%04d", F.col("product_idx").cast("long")))
            .alias("product_id"),
            F.when(noise.between(0.01, 0.02), -F.col("qty")).otherwise(F.col("qty")).cast("string").alias("quantity"),
            F.when(noise.between(0.02, 0.025), F.lit("31/02/2025 25:61"))
            .otherwise(F.date_format("ts", "yyyy-MM-dd HH:mm:ss"))
            .alias("order_ts"),
            "status",
        )
    )
    return lines


def generate(
    spark: SparkSession,
    out: str,
    n_customers: int = 200_000,
    n_orders: int = 1_000_000,
    seed: int = 42,
    files: int | None = None,
) -> dict:
    out_path = Path(out)
    out_path.mkdir(parents=True, exist_ok=True)
    files = files or max(1, n_orders // 250_000)

    products = make_products(seed)
    with open(out_path / "products.json", "w") as f:
        for p in products:
            f.write(json.dumps(p) + "\n")

    (
        generate_customers(spark, n_customers, seed)
        .coalesce(max(1, n_customers // 500_000))
        .write.mode("overwrite")
        .option("header", True)
        .csv(str(out_path / "customers"))
    )

    orders = generate_orders(spark, n_orders, n_customers, len(products), seed, files)
    orders.write.mode("overwrite").option("header", True).csv(str(out_path / "orders"))
    (out_path / "orders" / "part-broken.csv").write_text(
        "order_id,customer_id,product_id,quantity,order_ts,status\n"
        "O999999999,C0000001\n"
        "garbage line with no commas\n"
    )

    return {"products": len(products), "customers": n_customers, "orders": n_orders, "files": files}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Generate a synthetic dirty e-commerce dataset with Spark")
    parser.add_argument("--out", default="data/raw")
    parser.add_argument("--customers", type=int, default=200_000)
    parser.add_argument("--orders", type=int, default=1_000_000)
    parser.add_argument("--files", type=int, default=None)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)

    spark = get_spark("generate-data")
    try:
        stats = generate(spark, args.out, args.customers, args.orders, args.seed, args.files)
        print(f"generated {stats} -> {args.out}/")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
