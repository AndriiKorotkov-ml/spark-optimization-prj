import json

import pytest

from sales_pipeline import generate_data
from sales_pipeline.config import PipelineConfig
from sales_pipeline.pipeline import run
from sales_pipeline.quality import DataQualityError


@pytest.fixture(scope="module")
def raw_dir(spark, tmp_path_factory):
    path = tmp_path_factory.mktemp("raw")
    generate_data.generate(spark, str(path), n_customers=50, n_orders=400, seed=7)
    return path


def test_pipeline_end_to_end(spark, raw_dir, tmp_path):
    cfg = PipelineConfig(input_path=str(raw_dir), output_path=str(tmp_path), show_results=False)
    summary = run(spark, cfg)

    q = summary["quality"]
    assert q["valid_rows"] > 0
    assert q["valid_rows"] + q["rejected_rows"] <= q["raw_rows"]
    assert json.loads((tmp_path / "_run_summary.json").read_text()) == summary

    for table in summary["gold_tables"]:
        assert spark.read.parquet(str(tmp_path / "gold" / table)).count() > 0

    partitions = {p.name for p in (tmp_path / "silver" / "order_lines").iterdir() if p.is_dir()}
    assert partitions and all(p.startswith("order_month=") for p in partitions)

    silver = spark.read.parquet(str(tmp_path / "silver" / "order_lines"))
    assert silver.count() == q["valid_rows"]
    assert silver.filter("quantity <= 0 OR customer_id IS NULL").count() == 0
    assert silver.select("order_id", "product_id", "quantity", "order_ts").distinct().count() == silver.count()

    quarantine = spark.read.parquet(str(tmp_path / "quarantine" / "order_lines"))
    assert quarantine.filter("reject_reason = 'malformed_row'").count() == 2
    assert quarantine.filter("_source_file IS NULL").count() == 0


def test_generator_produces_skewed_dirty_data(spark, raw_dir):
    orders = spark.read.option("header", True).csv(str(raw_dir / "orders"))
    top = orders.groupBy("customer_id").count().orderBy("count", ascending=False).first()
    assert top["count"] > orders.count() / 50
    assert orders.filter("product_id = 'P9999'").count() > 0


def test_pipeline_fails_on_strict_quality_threshold(spark, raw_dir, tmp_path):
    cfg = PipelineConfig(
        input_path=str(raw_dir), output_path=str(tmp_path), max_reject_ratio=0.0, show_results=False
    )
    with pytest.raises(DataQualityError):
        run(spark, cfg)
