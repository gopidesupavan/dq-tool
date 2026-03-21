from __future__ import annotations

from typing import TYPE_CHECKING

from qualink.core.logging_mixin import get_logger
from qualink.formatters import (
    FormatterConfig,
    HumanFormatter,
    JsonFormatter,
    MarkdownFormatter,
    ResultFormatter,
)
from qualink.output.writer import ResultWriter

if TYPE_CHECKING:
    from qualink.core.result import ValidationResult
    from qualink.output.specs import OutputSpec

_logger = get_logger("output.service")

_FORMATTERS: dict[str, type[ResultFormatter]] = {
    "human": HumanFormatter,
    "json": JsonFormatter,
    "markdown": MarkdownFormatter,
}


class OutputService:
    def __init__(self, writer: ResultWriter | None = None) -> None:
        self._writer = writer or ResultWriter()

    def emit(self, result: ValidationResult, spec: OutputSpec) -> None:
        _logger.info(
            "Emitting validation output: format=%s destination=%s",
            spec.format,
            spec.destination,
        )
        self._writer.write_text(spec.destination, self.render(result, spec))

    def emit_many(self, result: ValidationResult, specs: list[OutputSpec]) -> None:
        _logger.debug("Emitting %d configured output(s)", len(specs))
        for spec in specs:
            self.emit(result, spec)

    def render(self, result: ValidationResult, spec: OutputSpec) -> str:
        formatter_cls = _FORMATTERS.get(spec.format)
        if formatter_cls is None:
            _logger.error("Unsupported output format requested: %s", spec.format)
            raise ValueError(f"Unsupported output format: {spec.format!r}")
        _logger.debug(
            "Rendering validation result with formatter '%s' for destination '%s'",
            formatter_cls.__name__,
            spec.destination,
        )
        formatter = formatter_cls(
            config=FormatterConfig(
                show_metrics=spec.show_metrics,
                show_issues=spec.show_issues,
                show_passed=spec.show_passed,
                colorize=spec.colorize,
            )
        )
        rendered = formatter.format(result)
        _logger.debug("Rendered %d characters for destination '%s'", len(rendered), spec.destination)
        return rendered
