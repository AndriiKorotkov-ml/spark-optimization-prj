from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

RAW_ORDERS = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("product_id", StringType()),
        StructField("quantity", StringType()),
        StructField("order_ts", StringType()),
        StructField("status", StringType()),
        StructField("_corrupt_record", StringType()),
    ]
)

RAW_CUSTOMERS = StructType(
    [
        StructField("customer_id", StringType()),
        StructField("full_name", StringType()),
        StructField("email", StringType()),
        StructField("country", StringType()),
        StructField("signup_date", StringType()),
    ]
)

PRODUCTS = StructType(
    [
        StructField("product_id", StringType(), nullable=False),
        StructField("product_name", StringType()),
        StructField("category", StringType()),
        StructField("price", DecimalType(10, 2)),
        StructField("stock", IntegerType()),
    ]
)
