"""
NYC Taxi Trips — qualink Benchmark Runner
==========================================

Downloads are NOT included in the repo. Run the shell script first:

    ./benchmarks/download_data.sh          # downloads 5 parquet files (~1.1 GB)
    python benchmarks/run_benchmark.py     # runs the full validation suite

Measures wall-clock time and prints results in all three formatter styles.
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

# Ensure the project root is importable when running as a script.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT / "src"))

from qualink.config import build_suite_from_yaml  # noqa: E402
from qualink.formatters import HumanFormatter, JsonFormatter, MarkdownFormatter  # noqa: E402

BENCHMARK_DIR = Path(__file__).resolve().parent
YAML_PATH = BENCHMARK_DIR / "nyc_taxi_validation.yaml"
DATA_DIR = BENCHMARK_DIR / "data"

FORMATTERS = {
    "human": HumanFormatter,
    "markdown": MarkdownFormatter,
    "json": JsonFormatter,
}

SEPARATOR = "=" * 72


def _check_data() -> int:
    """Verify parquet files exist and return the count."""
    if not DATA_DIR.exists():
        return 0
    return len(list(DATA_DIR.glob("*.parquet")))


def _print_header(file_count: int) -> None:
    total_bytes = sum(f.stat().st_size for f in DATA_DIR.glob("*.parquet"))
    total_mb = total_bytes / (1024 * 1024)
    files = sorted(DATA_DIR.glob("*.parquet"))

    print(SEPARATOR)
    print("  qualink Benchmark — NYC Taxi Trips")
    print(SEPARATOR)
    print(f"  Parquet files : {file_count}")
    print(f"  Total size    : {total_mb:,.1f} MB")
    print(f"  Data dir      : {DATA_DIR}")
    print(f"  YAML config   : {YAML_PATH}")
    print()
    for f in files:
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"    • {f.name}  ({size_mb:,.1f} MB)")
    print(SEPARATOR)
    print()


async def _run_benchmark(fmt_name: str = "human") -> None:
    """Execute the YAML validation suite and print results."""

    file_count = _check_data()
    if file_count == 0:
        print("❌  No parquet files found in benchmarks/data/")
        print()
        print("   Run the download script first:")
        print("     ./benchmarks/download_data.sh")
        print()
        print("   Then run the benchmark:")
        print("     uv run python benchmarks/run_benchmark.py")
        print()
        sys.exit(1)

    _print_header(file_count)

    print(f"⏱  Running benchmark with {fmt_name!r} formatter …")
    print()

    t_start = time.perf_counter()
    builder = build_suite_from_yaml(str(YAML_PATH))

    # Query total records from the registered table
    total_records = 0
    if builder._ctx is not None:
        try:
            count_sql = f"SELECT COUNT(*) AS cnt FROM {builder._table_name}"
            rows = builder._ctx.sql(count_sql).collect()
            total_records = int(rows[0].column("cnt")[0].as_py())
        except Exception:
            pass

    result = await builder.run()
    t_end = time.perf_counter()

    elapsed = t_end - t_start

    formatter_cls = FORMATTERS.get(fmt_name, HumanFormatter)
    output = formatter_cls().format(result)
    print(output)

    print()
    print(SEPARATOR)
    print(f"  Status         : {'✅ PASSED' if result.success else '❌ FAILED'}")
    print(f"  Total records  : {total_records / 1_000_000:,.2f}M")
    print(f"  Wall-clock     : {elapsed:,.3f}s")
    print(f"  Checks         : {result.report.metrics.total_checks}")
    print(f"  Constraints    : {result.report.metrics.total_constraints}")
    print(f"  Passed         : {result.report.metrics.passed}")
    print(f"  Failed         : {result.report.metrics.failed}")
    print(f"  Pass rate      : {result.report.metrics.pass_rate:.1%}")
    print(f"  Engine time    : {result.report.metrics.execution_time_ms / 60_000:,.2f}m")
    print(SEPARATOR)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="qualink benchmark runner — NYC Taxi Trips",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python benchmarks/run_benchmark.py                  # human-readable output
  python benchmarks/run_benchmark.py --format markdown
  python benchmarks/run_benchmark.py --format json
        """,
    )
    parser.add_argument(
        "--format",
        choices=list(FORMATTERS.keys()),
        default="human",
        help="output format (default: human)",
    )
    args = parser.parse_args()

    asyncio.run(_run_benchmark(args.format))


if __name__ == "__main__":
    main()
