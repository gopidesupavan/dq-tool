"""Command-line interface for qualink.

Usage::

    uv run qualinkctl checks.yaml
    uv run qualinkctl checks.yaml --format json
    uv run qualinkctl checks.yaml -f markdown --show-passed
    uv run qualinkctl checks.yaml -f human --no-color --verbose
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import click

from qualink.config import run_yaml
from qualink.core.logging_mixin import configure_logging
from qualink.formatters import (
    FormatterConfig,
    HumanFormatter,
    JsonFormatter,
    MarkdownFormatter,
    ResultFormatter,
)

_FORMATTERS: dict[str, type[ResultFormatter]] = {
    "human": HumanFormatter,
    "json": JsonFormatter,
    "markdown": MarkdownFormatter,
}


@click.command(name="qualinkctl")
@click.argument("config", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option(
    "-f",
    "--format",
    "fmt",
    type=click.Choice(["human", "json", "markdown"], case_sensitive=False),
    default="human",
    show_default=True,
    help="Output format.",
)
@click.option(
    "-o",
    "--output",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    default=None,
    help="Write output to a file instead of stdout.",
)
@click.option(
    "--show-passed",
    is_flag=True,
    default=False,
    help="Include passed constraints in the output.",
)
@click.option(
    "--show-metrics/--no-metrics",
    default=True,
    show_default=True,
    help="Include aggregate metrics in the output.",
)
@click.option(
    "--no-color",
    is_flag=True,
    default=False,
    help="Disable coloured output.",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    help="Enable debug logging.",
)
def main(
    config: Path,
    fmt: str,
    output: Path | None,
    show_passed: bool,
    show_metrics: bool,
    no_color: bool,
    verbose: bool,
) -> None:
    log_level = logging.DEBUG if verbose else logging.WARNING
    configure_logging(level=log_level)

    result = asyncio.run(run_yaml(config))

    fmt_config = FormatterConfig(
        show_metrics=show_metrics,
        show_issues=True,
        show_passed=show_passed,
        colorize=not no_color,
    )
    formatter = _FORMATTERS[fmt](config=fmt_config)
    formatted = formatter.format(result)

    if output:
        output.write_text(formatted, encoding="utf-8")
    else:
        click.echo(formatted)

    if not result.success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
