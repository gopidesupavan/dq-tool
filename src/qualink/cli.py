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
from typing import TYPE_CHECKING

import click

from qualink.config import run_yaml
from qualink.config.parser import load_yaml
from qualink.core.logging_mixin import configure_logging
from qualink.formatters import (
    FormatterConfig,
    HumanFormatter,
    JsonFormatter,
    MarkdownFormatter,
    ResultFormatter,
)
from qualink.output import OutputService, normalize_output_specs, write_text_output

if TYPE_CHECKING:
    from qualink.output import OutputSpec

_FORMATTERS: dict[str, type[ResultFormatter]] = {
    "human": HumanFormatter,
    "json": JsonFormatter,
    "markdown": MarkdownFormatter,
}


@click.command(name="qualinkctl")
@click.argument("config", type=str)
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
    type=str,
    default=None,
    help="Write output to a local path or filesystem URI instead of stdout.",
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
    config: str,
    fmt: str,
    output: str | None,
    show_passed: bool,
    show_metrics: bool,
    no_color: bool,
    verbose: bool,
) -> None:
    log_level = logging.DEBUG if verbose else logging.WARNING
    configure_logging(level=log_level)

    try:
        loaded_config = load_yaml(config)
    except (FileNotFoundError, OSError, ValueError) as exc:
        raise click.ClickException(f"Unable to load config '{config}': {exc}") from exc

    result = asyncio.run(run_yaml(config))
    configured_outputs = normalize_output_specs(loaded_config)

    fmt_config = FormatterConfig(
        show_metrics=show_metrics,
        show_issues=True,
        show_passed=show_passed,
        colorize=not no_color,
    )
    formatter = _FORMATTERS[fmt](config=fmt_config)
    formatted = formatter.format(result)

    if output:
        write_text_output(output, formatted)
    else:
        click.echo(formatted)
        if configured_outputs:
            OutputService().emit_many(result, configured_outputs)

    if output and configured_outputs:
        OutputService().emit_many(result, _without_duplicate_destination(configured_outputs, output))

    if not result.success:
        raise SystemExit(1)


def _without_duplicate_destination(specs: list[OutputSpec], destination: str) -> list[OutputSpec]:
    return [spec for spec in specs if spec.destination != destination]


if __name__ == "__main__":
    main()
