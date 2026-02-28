from __future__ import annotations

from typing import TYPE_CHECKING

from dq_tool.core.constraint import ConstraintStatus
from dq_tool.core.level import Level
from dq_tool.core.result import (
    CheckStatus,
    ValidationIssue,
    ValidationMetrics,
    ValidationReport,
    ValidationResult,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

    from dq_tool.checks.check import Check, CheckResult


class ValidationSuite:
    """Entry point for running data-quality checks."""

    def __init__(self, name: str = "") -> None:
        self._name = name

    @staticmethod
    def builder(name: str) -> ValidationSuiteBuilder:
        """Create a builder for constructing a validation suite."""

        return ValidationSuiteBuilder(name)

    def on_data(self, ctx: SessionContext, table_name: str) -> ValidationSuiteBuilder:
        """Begin a validation run against *table_name* in the DataFusion *ctx*."""

        return ValidationSuiteBuilder(self._name or "ValidationRun").on_data(ctx, table_name)


class ValidationSuiteBuilder:
    """Fluent builder that collects checks and runs them via DataFusion."""

    def __init__(self, name: str = "ValidationRun") -> None:
        self._name = name
        self._description: str | None = None
        self._ctx: SessionContext | None = None
        self._table_name: str = "data"
        self._checks: list[Check] = []

    def description(self, desc: str) -> ValidationSuiteBuilder:
        self._description = desc
        return self

    def with_name(self, name: str) -> ValidationSuiteBuilder:
        self._name = name
        return self

    def table_name(self, name: str) -> ValidationSuiteBuilder:
        self._table_name = name
        return self

    def on_data(self, ctx: SessionContext, table_name: str) -> ValidationSuiteBuilder:
        self._ctx = ctx
        self._table_name = table_name
        return self

    def add_check(self, check: Check) -> ValidationSuiteBuilder:
        self._checks.append(check)
        return self

    def add_checks(self, checks: list[Check]) -> ValidationSuiteBuilder:
        self._checks.extend(checks)
        return self

    async def run(self) -> ValidationResult:
        """Execute all checks against the DataFusion context."""

        if self._ctx is None:
            raise RuntimeError("No data context set. Call .on_data(ctx, table) before .run().")

        metrics = ValidationMetrics(total_checks=len(self._checks))
        issues: list[ValidationIssue] = []
        check_results_map: dict[str, list] = {}
        overall_success = True
        worst_status = CheckStatus.SUCCESS

        for check in self._checks:
            cr: CheckResult = await check.run(self._ctx, self._table_name)
            check_results_map[check.name] = cr.constraint_results
            metrics.total_constraints += len(cr.constraint_results)

            for result in cr.constraint_results:
                if result.status == ConstraintStatus.SUCCESS:
                    metrics.passed += 1
                elif result.status == ConstraintStatus.FAILURE:
                    metrics.failed += 1
                    if check.level == Level.ERROR:
                        metrics.error_count += 1
                        overall_success = False
                        worst_status = CheckStatus.ERROR
                    elif check.level == Level.WARNING:
                        metrics.warning_count += 1
                        if worst_status != CheckStatus.ERROR:
                            worst_status = CheckStatus.WARNING
                    issues.append(
                        ValidationIssue(
                            check_name=check.name,
                            constraint_name=result.constraint_name,
                            level=check.level,
                            message=result.message,
                            metric=result.metric,
                        )
                    )
                else:
                    metrics.skipped += 1

        report = ValidationReport(
            suite_name=self._name,
            metrics=metrics,
            check_results=check_results_map,
            issues=issues,
        )

        return ValidationResult(
            success=overall_success,
            status=worst_status,
            report=report,
        )

    def build(self) -> ValidationSuite:
        """Return a configured ``ValidationSuite`` (for deferred execution)."""
        suite = ValidationSuite(self._name)
        return suite
