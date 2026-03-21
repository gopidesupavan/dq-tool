from __future__ import annotations

from io import StringIO
from typing import TYPE_CHECKING

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from qualink.core.constraint import ConstraintStatus
from qualink.formatters.base import ResultFormatter

if TYPE_CHECKING:
    from qualink.core.constraint import ConstraintResult
    from qualink.core.result import ValidationIssue, ValidationResult


class HumanFormatter(ResultFormatter):
    def format(self, result: ValidationResult) -> str:
        self.logger.debug("Formatting result as human-readable text for suite '%s'", result.report.suite_name)
        buffer = StringIO()
        console = Console(
            file=buffer,
            force_terminal=self._config.colorize,
            color_system="standard" if self._config.colorize else None,
            no_color=not self._config.colorize,
            width=100,
            soft_wrap=False,
        )

        console.print(self._build_header(result))
        if self._config.show_metrics:
            console.print(self._build_summary_table(result))
        checks_table = self._build_checks_table(result.report.check_results)
        if checks_table is not None:
            console.print(checks_table)
        if self._config.show_issues and result.report.issues:
            console.print(self._build_issues_table(result.report.issues))

        output = buffer.getvalue().rstrip()
        self.logger.debug("Human format output: %d chars", len(output))
        return output

    def _build_header(self, result: ValidationResult) -> Panel:
        status_label = "PASS" if result.success else "FAIL"
        status_style = self._status_style(result.success)
        body = Text()
        body.append(result.report.suite_name, style="bold")
        body.append("\n")
        body.append("Validation result: ", style="dim")
        body.append(status_label, style=f"bold {status_style}")
        return Panel(body, border_style=status_style, title="qualink", expand=True)

    def _build_summary_table(self, result: ValidationResult) -> Table:
        metrics = result.report.metrics
        table = Table(title="Summary", box=None, show_header=False, pad_edge=False)
        table.add_column("Metric", style="cyan", no_wrap=True)
        table.add_column("Value")
        table.add_row("Checks", str(metrics.total_checks))
        table.add_row("Constraints", str(metrics.total_constraints))
        table.add_row("Passed", self._styled_count(metrics.passed, "green"))
        table.add_row("Failed", self._styled_count(metrics.failed, "red"))
        table.add_row("Skipped", self._styled_count(metrics.skipped, "yellow"))
        table.add_row("Pass rate", f"{metrics.pass_rate:.1%}")
        table.add_row("Execution time", f"{metrics.execution_time_ms} ms")
        if metrics.error_count or metrics.warning_count:
            table.add_row("Errors", self._styled_count(metrics.error_count, "red"))
            table.add_row("Warnings", self._styled_count(metrics.warning_count, "yellow"))
        return table

    def _build_checks_table(self, check_results: dict[str, list[ConstraintResult]]) -> Table | None:
        table = Table(title="Checks", expand=True, show_lines=False)
        table.add_column("Check", style="bold cyan", no_wrap=True)
        table.add_column("Status", no_wrap=True)
        table.add_column("Constraint", style="bold")
        table.add_column("Message")
        table.add_column("Metric", justify="right", no_wrap=True)

        has_rows = False
        for check_name, results in check_results.items():
            for constraint_result in results:
                if constraint_result.status == ConstraintStatus.SUCCESS and not self._config.show_passed:
                    continue
                has_rows = True
                table.add_row(
                    check_name,
                    self._status_icon(constraint_result.status),
                    constraint_result.constraint_name or "-",
                    constraint_result.message or constraint_result.constraint_name or "-",
                    self._format_metric(constraint_result.metric),
                )
        return table if has_rows else None

    def _build_issues_table(self, issues: list[ValidationIssue]) -> Table:
        table = Table(title="Issues", expand=True, show_lines=False)
        table.add_column("#", style="dim", no_wrap=True)
        table.add_column("Level", no_wrap=True)
        table.add_column("Check", style="bold cyan", no_wrap=True)
        table.add_column("Constraint", style="bold")
        table.add_column("Column", no_wrap=True)
        table.add_column("Message")
        table.add_column("Metric", justify="right", no_wrap=True)

        for index, issue in enumerate(issues, start=1):
            table.add_row(
                str(index),
                self._issue_level_text(issue.level.name),
                issue.check_name,
                issue.constraint_name,
                issue.column or "-",
                self._issue_message(issue),
                self._format_metric(issue.metric),
            )
        return table

    def _status_icon(self, status: ConstraintStatus) -> str:
        match status:
            case ConstraintStatus.SUCCESS:
                return "[bold green]PASS[/]"
            case ConstraintStatus.FAILURE:
                return "[bold red]FAIL[/]"
            case ConstraintStatus.SKIPPED:
                return "[bold yellow]SKIP[/]"

    def _styled_count(self, count: int, color: str) -> str:
        if not self._config.colorize:
            return str(count)
        return f"[bold {color}]{count}[/]"

    def _format_metric(self, metric: float | None) -> str:
        if metric is None:
            return "-"
        return f"{metric:.4f}" if isinstance(metric, float) else str(metric)

    def _status_style(self, success: bool) -> str:
        return "green" if success else "red"

    def _issue_level_text(self, level_name: str) -> str:
        if not self._config.colorize:
            return level_name
        color = "red" if level_name == "ERROR" else "yellow"
        return f"[bold {color}]{level_name}[/]"

    def _issue_message(self, issue: ValidationIssue) -> str:
        details: list[str] = []
        if issue.description:
            details.append(issue.description)
        if issue.metadata_extra:
            details.append(", ".join(f"{key}={value}" for key, value in issue.metadata_extra.items()))
        if not details:
            return issue.message
        return f"{issue.message}\n[dim]{' | '.join(details)}[/]"
