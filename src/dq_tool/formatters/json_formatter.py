"""JSON formatter."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dq_tool.core.result import ValidationResult

from dq_tool.formatters.base import ResultFormatter


class JsonFormatter(ResultFormatter):
    def format(self, result: ValidationResult) -> str:
        m = result.report.metrics
        payload: dict[str, Any] = {
            "suite": result.report.suite_name,
            "success": result.success,
            "metrics": {
                "total_checks": m.total_checks,
                "total_constraints": m.total_constraints,
                "passed": m.passed,
                "failed": m.failed,
                "skipped": m.skipped,
                "pass_rate": round(m.pass_rate, 4),
            },
        }
        if self._config.show_issues and result.report.issues:
            payload["issues"] = [
                {
                    "check": i.check_name,
                    "constraint": i.constraint_name,
                    "level": str(i.level),
                    "message": i.message,
                    "metric": i.metric,
                }
                for i in result.report.issues
            ]
        return json.dumps(payload, indent=2)
