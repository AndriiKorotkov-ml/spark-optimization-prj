import argparse
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    input_path: str = "data/raw"
    output_path: str = "output"
    max_reject_ratio: float = 0.10
    top_n: int = 3
    as_of: str | None = None
    show_results: bool = True
    reuse_bronze: bool = False
    max_records_per_file: int = 1_000_000
    gold_parallelism: int = 4

    @classmethod
    def from_args(cls, argv: list[str] | None = None) -> "PipelineConfig":
        parser = argparse.ArgumentParser(description="Sales analytics Spark pipeline")
        parser.add_argument("--input", default=cls.input_path)
        parser.add_argument("--output", default=cls.output_path)
        parser.add_argument("--max-reject-ratio", type=float, default=cls.max_reject_ratio)
        parser.add_argument("--top-n", type=int, default=cls.top_n)
        parser.add_argument("--as-of", default=None, help="Reference date for RFM, YYYY-MM-DD")
        parser.add_argument("--quiet", action="store_true", help="Do not print result tables")
        parser.add_argument("--reuse-bronze", action="store_true", help="Skip CSV ingestion if bronze exists")
        parser.add_argument("--max-records-per-file", type=int, default=cls.max_records_per_file)
        parser.add_argument("--gold-parallelism", type=int, default=cls.gold_parallelism)
        args = parser.parse_args(argv)
        return cls(
            input_path=args.input,
            output_path=args.output,
            max_reject_ratio=args.max_reject_ratio,
            top_n=args.top_n,
            as_of=args.as_of,
            show_results=not args.quiet,
            reuse_bronze=args.reuse_bronze,
            max_records_per_file=args.max_records_per_file,
            gold_parallelism=args.gold_parallelism,
        )
