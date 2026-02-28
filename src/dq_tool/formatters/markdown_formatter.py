from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dq_tool.core.result import ValidationResult

from dq_tool.core.constraint import ConstraintStatus
from dq_tool.formatters.base import ResultFormatter


class MarkdownFormatter(ResultFormatter):
    def format(self, result: ValidationResult) -> str:
        m = result.report.metrics
        status = "PASS" if result.success else "FAIL"
        lines: list[str] = [
            f"# Verification Report: {result.report.suite_name}",
            "",
            f"**Status:** {status}",
            "",
            "## Metrics",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| Total checks | {m.total_checks} |",
            f"| Total constraints | {m.total_constraints} |",
            f"| Passed | {m.passed} |",
            f"| Failed | {m.failed} |",
            f"| Skipped | {m.skipped} |",
            f"| Pass rate | {m.pass_rate:.1%} |",
            "",
            "## Constraint Results",
            "",
            "| Check | Constraint | Status | Metric |",
            "|-------|------------|--------|--------|",
        ]
        for check_name, results in result.report.check_results.items():
            for cr in results:
                icon = {
                    ConstraintStatus.SUCCESS: "PASS",
                    ConstraintStatus.FAILURE: "FAIL",
                    ConstraintStatus.SKIPPED: "SKIP",
                }.get(cr.status, "?")
                metric_str = f"{cr.metric:.4f}" if cr.metric is not None else "-"
                lines.append(f"| {check_name} | {cr.constraint_name} | {icon} | {metric_str} |")

        if result.report.issues:
            lines.extend(["", "## Issues", ""])
            for issue in result.report.issues:
                lines.append(
                    f"- **[{issue.level}]** {issue.check_name} / {issue.constraint_name}: {issue.message}"
                )
        return "\n".join(lines)
