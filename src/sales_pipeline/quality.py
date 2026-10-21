from dataclasses import asdict, dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(RuntimeError):
    pass


@dataclass
class QualityReport:
    raw_rows: int
    valid_rows: int
    rejected_rows: int
    duplicate_rows: int = 0
    rejected_by_reason: dict[str, int] = field(default_factory=dict)

    @property
    def reject_ratio(self) -> float:
        return self.rejected_rows / self.raw_rows if self.raw_rows else 0.0

    def to_dict(self) -> dict:
        return {**asdict(self), "reject_ratio": round(self.reject_ratio, 4)}


def build_report(labeled: DataFrame, valid_rows: int) -> QualityReport:
    counts = {r.reject_reason: r["count"] for r in labeled.groupBy("reject_reason").count().collect()}
    passed = counts.pop(None, 0)
    by_reason = dict(sorted(counts.items(), key=lambda kv: -kv[1]))
    return QualityReport(
        raw_rows=passed + sum(by_reason.values()),
        valid_rows=valid_rows,
        rejected_rows=sum(by_reason.values()),
        duplicate_rows=passed - valid_rows,
        rejected_by_reason=by_reason,
    )


def enforce(report: QualityReport, max_reject_ratio: float) -> None:
    if report.reject_ratio > max_reject_ratio:
        raise DataQualityError(
            f"Reject ratio {report.reject_ratio:.2%} exceeds threshold {max_reject_ratio:.2%}: "
            f"{report.rejected_by_reason}"
        )
