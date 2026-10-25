import os
import sys

from pyspark.sql import SparkSession

DEFAULT_CONF = {
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64m",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "3",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "32m",
    "spark.sql.autoBroadcastJoinThreshold": "32m",
    "spark.sql.parquet.compression.codec": "zstd",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.session.timeZone": "UTC",
    "spark.scheduler.mode": "FAIR",
    "spark.ui.showConsoleProgress": "false",
}


def get_spark(
    app_name: str = "sales-analytics",
    master: str | None = None,
    driver_memory: str | None = None,
    shuffle_partitions: int | None = None,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    cores = os.cpu_count() or 4
    conf = {
        **DEFAULT_CONF,
        "spark.driver.memory": driver_memory or os.getenv("SPARK_DRIVER_MEMORY", "6g"),
        "spark.sql.shuffle.partitions": str(shuffle_partitions or cores * 4),
        **(extra_conf or {}),
    }
    builder = SparkSession.builder.appName(app_name).master(master or os.getenv("SPARK_MASTER", "local[*]"))
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
