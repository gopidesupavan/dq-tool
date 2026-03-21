from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from qualink.core.constraint import ConstraintStatus

if TYPE_CHECKING:
    from qualink.core.constraint import ConstraintResult
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
        check_results = self._serialize_check_results(result.report.check_results)
        if check_results:
            payload["check_results"] = check_results
        output = json.dumps(payload, indent=2)
        self.logger.debug("JSON format output: %d chars", len(output))
        return output

    def _serialize_check_results(
        self,
        check_results: dict[str, list[ConstraintResult]],
    ) -> list[dict[str, Any]]:
        serialized_checks: list[dict[str, Any]] = []
        for check_name, results in check_results.items():
            serialized_constraints = [
                {
                    "constraint": constraint_result.constraint_name,
                    "status": str(constraint_result.status),
                    "message": constraint_result.message,
                    "metric": constraint_result.metric,
                }
                for constraint_result in results
                if self._should_include_constraint(constraint_result)
            ]
            if serialized_constraints:
                serialized_checks.append(
                    {
                        "check": check_name,
                        "constraints": serialized_constraints,
                    }
                )
        return serialized_checks

    def _should_include_constraint(self, constraint_result: ConstraintResult) -> bool:
        return self._config.show_passed or constraint_result.status != ConstraintStatus.SUCCESS
