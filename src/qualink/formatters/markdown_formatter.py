from __future__ import annotations

from typing import TYPE_CHECKING

from tabulate import tabulate

if TYPE_CHECKING:
    from qualink.core.result import ValidationResult

from qualink.core.constraint import ConstraintStatus
from qualink.formatters.base import ResultFormatter


class MarkdownFormatter(ResultFormatter):
    def format(self, result: ValidationResult) -> str:
        self.logger.debug("Formatting result as Markdown for suite '%s'", result.report.suite_name)
        m = result.report.metrics
        status = "PASS" if result.success else "FAIL"
        lines: list[str] = [
            f"# Verification Report: {result.report.suite_name}",
            "",
            f"**Status:** {status}",
            "",
            "## Metrics",
            "",
        ]

        metrics_table = [
            ["Total checks", m.total_checks],
            ["Total constraints", m.total_constraints],
            ["Passed", m.passed],
            ["Failed", m.failed],
            ["Skipped", m.skipped],
            ["Pass rate", f"{m.pass_rate:.1%}"],
        ]
        lines.append(tabulate(metrics_table, headers=["Metric", "Value"], tablefmt="github"))

        lines.extend(["", "## Constraint Results", ""])

        constraint_rows: list[list[str]] = []
        for check_name, results in result.report.check_results.items():
            for cr in results:
                icon = {
                    ConstraintStatus.SUCCESS: "PASS",
                    ConstraintStatus.FAILURE: "FAIL",
                    ConstraintStatus.SKIPPED: "SKIP",
                }.get(cr.status, "?")
                metric_str = f"{cr.metric:.4f}" if cr.metric is not None else "-"
                constraint_rows.append([check_name, cr.constraint_name, icon, metric_str])

        lines.append(
            tabulate(
                constraint_rows,
                headers=["Check", "Constraint", "Status", "Metric"],
                tablefmt="github",
            )
        )

        if result.report.issues:
            lines.extend(["", "## Issues", ""])
            issue_rows: list[list[str]] = []
            for issue in result.report.issues:
                col_part = f"`{issue.column}`" if issue.column else "-"
                extra_part = (
                    ", ".join(f"{k}={v}" for k, v in issue.metadata_extra.items())
                    if issue.metadata_extra
                    else "-"
                )
                issue_rows.append(
                    [
                        f"**{issue.level}**",
                        issue.check_name,
                        issue.constraint_name,
                        col_part,
                        issue.message,
                        extra_part,
                    ]
                )
            lines.append(
                tabulate(
                    issue_rows,
                    headers=["Level", "Check", "Constraint", "Column", "Message", "Extra"],
                    tablefmt="github",
                )
            )

        output = "\n".join(lines)
        self.logger.debug("Markdown format output: %d chars", len(output))
        return output
