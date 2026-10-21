import pytest

from sales_pipeline.quality import DataQualityError, QualityReport, build_report, enforce


def test_build_report_counts_reasons_and_duplicates(spark):
    labeled = spark.createDataFrame(
        [(None,)] * 27 + [("bad_timestamp",), ("bad_timestamp",), ("unknown_product",)],
        "reject_reason string",
    )
    report = build_report(labeled, valid_rows=25)
    assert report.raw_rows == 30
    assert report.rejected_by_reason == {"bad_timestamp": 2, "unknown_product": 1}
    assert report.duplicate_rows == 2
    assert report.reject_ratio == pytest.approx(0.1)


def test_enforce_raises_above_threshold():
    report = QualityReport(raw_rows=100, valid_rows=80, rejected_rows=20)
    enforce(report, max_reject_ratio=0.25)
    with pytest.raises(DataQualityError):
        enforce(report, max_reject_ratio=0.1)
