from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from qualink.core.result import ValidationResult

from qualink.formatters.base import ResultFormatter


class JsonFormatter(ResultFormatter):
    def format(self, result: ValidationResult) -> str:
        self.logger.debug("Formatting result as JSON for suite '%s'", result.report.suite_name)
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
                "execution_time_ms": m.execution_time_ms,
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
                    "column": i.column,
                    "description": i.description,
                    **({"extra": i.metadata_extra} if i.metadata_extra else {}),
                }
                for i in result.report.issues
            ]
        output = json.dumps(payload, indent=2)
        self.logger.debug("JSON format output: %d chars", len(output))
        return output
