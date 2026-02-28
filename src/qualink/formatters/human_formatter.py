from __future__ import annotations

from typing import TYPE_CHECKING

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
            lines.append(f"  Checks: {m.total_checks}  |  Constraints: {m.total_constraints}")
            lines.append(
                f"  Passed: {self._colour(str(m.passed), self._GREEN)}  |  "
                f"Failed: {self._colour(str(m.failed), self._RED)}  |  "
                f"Skipped: {m.skipped}"
            )
            lines.append(f"  Pass rate: {m.pass_rate:.1%}")
            lines.append("")

        for check_name, results in result.report.check_results.items():
            for cr in results:
                if cr.status == ConstraintStatus.SUCCESS and not self._config.show_passed:
                    continue
                icon = self._status_icon(cr.status)
                msg = cr.message or cr.constraint_name
                lines.append(f"  {icon} [{check_name}] {msg}")

        if self._config.show_issues and result.report.issues:
            lines.append("")
            lines.append(self._bold("Issues:"))
            for issue in result.report.issues:
                level_colour = self._RED if issue.level.name == "ERROR" else self._YELLOW
                lines.append(
                    f"  {self._colour(str(issue.level).upper(), level_colour)} "
                    f"{issue.check_name} / {issue.constraint_name}: {issue.message}"
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
