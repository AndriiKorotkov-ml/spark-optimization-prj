import argparse
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    input_path: str = "data/raw"
    output_path: str = "output"
    max_reject_ratio: float = 0.10
    show_results: bool = True
    reuse_bronze: bool = False

    @classmethod
    def from_args(cls, argv: list[str] | None = None) -> "PipelineConfig":
        parser = argparse.ArgumentParser(description="Sales analytics Spark pipeline")
        parser.add_argument("--input", default=cls.input_path)
        parser.add_argument("--output", default=cls.output_path)
        parser.add_argument("--max-reject-ratio", type=float, default=cls.max_reject_ratio)
        parser.add_argument("--quiet", action="store_true", help="Do not print result tables")
        parser.add_argument("--reuse-bronze", action="store_true", help="Skip CSV ingestion if bronze exists")
        args = parser.parse_args(argv)
        return cls(
            input_path=args.input,
            output_path=args.output,
            max_reject_ratio=args.max_reject_ratio,
            show_results=not args.quiet,
            reuse_bronze=args.reuse_bronze,
        )
