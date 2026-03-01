from __future__ import annotations

from typing import TYPE_CHECKING

from tabulate import tabulate

if TYPE_CHECKING:
    from qualink.core.result import ValidationResult

from qualink.core.constraint import ConstraintStatus
from qualink.formatters.base import ResultFormatter


class HumanFormatter(ResultFormatter):
    _GREEN = "\033[92m"
    _RED = "\033[91m"
    _YELLOW = "\033[93m"
    _RESET = "\033[0m"
    _BOLD = "\033[1m"

    def format(self, result: ValidationResult) -> str:
        self.logger.debug("Formatting result as human-readable text for suite '%s'", result.report.suite_name)
        lines: list[str] = []
        m = result.report.metrics
        status_str = self._colour(
            "PASSED" if result.success else "FAILED",
            self._GREEN if result.success else self._RED,
        )
        lines.append(f"{self._bold('Verification')} {status_str}: {result.report.suite_name}")
        lines.append("")

        if self._config.show_metrics:
            metrics_table = [
                ["Checks", m.total_checks],
                ["Constraints", m.total_constraints],
                ["Passed", self._colour(str(m.passed), self._GREEN)],
                ["Failed", self._colour(str(m.failed), self._RED)],
                ["Skipped", m.skipped],
                ["Pass rate", f"{m.pass_rate:.1%}"],
            ]
            lines.append(tabulate(metrics_table, tablefmt="plain"))
            lines.append("")

        constraint_rows: list[list[str]] = []
        for check_name, results in result.report.check_results.items():
            for cr in results:
                if cr.status == ConstraintStatus.SUCCESS and not self._config.show_passed:
                    continue
                icon = self._status_icon(cr.status)
                msg = cr.message or cr.constraint_name
                constraint_rows.append([icon, check_name, msg])

        if constraint_rows:
            lines.append(tabulate(constraint_rows, headers=["Status", "Check", "Message"], tablefmt="simple"))
            lines.append("")

        if self._config.show_issues and result.report.issues:
            lines.append(self._bold("Issues:"))
            issue_rows: list[list[str]] = []
            for issue in result.report.issues:
                level_colour = self._RED if issue.level.name == "ERROR" else self._YELLOW
                col_info = issue.column or "-"
                desc_info = issue.description if issue.description else "-"
                extra_info = (
                    ", ".join(f"{k}={v}" for k, v in issue.metadata_extra.items())
                    if issue.metadata_extra
                    else "-"
                )
                issue_rows.append(
                    [
                        self._colour(str(issue.level).upper(), level_colour),
                        issue.check_name,
                        issue.constraint_name,
                        col_info,
                        issue.message,
                        desc_info,
                        extra_info,
                    ]
                )
            lines.append(
                tabulate(
                    issue_rows,
                    headers=["Level", "Check", "Constraint", "Column", "Message", "Description", "Extra"],
                    tablefmt="simple",
                )
            )

        output = "\n".join(lines)
        self.logger.debug("Human format output: %d chars", len(output))
        return output

    def _status_icon(self, status: ConstraintStatus) -> str:
        match status:
            case ConstraintStatus.SUCCESS:
                return self._colour("[PASS]", self._GREEN)
            case ConstraintStatus.FAILURE:
                return self._colour("[FAIL]", self._RED)
            case ConstraintStatus.SKIPPED:
                return self._colour("[SKIP]", self._YELLOW)

    def _colour(self, text: str, colour: str) -> str:
        if not self._config.colorize:
            return text
        return f"{colour}{text}{self._RESET}"

    def _bold(self, text: str) -> str:
        if not self._config.colorize:
            return text
        return f"{self._BOLD}{text}{self._RESET}"
