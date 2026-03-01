from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from qualink.core.constraint import Constraint, ConstraintStatus
from qualink.core.level import Level
from qualink.core.logging_mixin import LoggingMixin
from qualink.core.result import (
    CheckStatus,
    ValidationIssue,
    ValidationMetrics,
    ValidationReport,
    ValidationResult,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

    from qualink.checks.check import Check, CheckResult


class ValidationSuite(LoggingMixin):
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


class ValidationSuiteBuilder(LoggingMixin):
    """Fluent builder that collects checks and runs them via DataFusion."""

    def __init__(self, name: str = "ValidationRun") -> None:
        self._name = name
        self._description: str | None = None
        self._ctx: SessionContext | None = None
        self._table_name: str = "data"
        self._checks: list[Check] = []
        self._run_parallel: bool = False
        self.logger.debug("ValidationSuiteBuilder created: name='%s'", name)

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
        self.logger.debug("Data context set: table='%s'", table_name)
        return self

    def add_check(self, check: Check) -> ValidationSuiteBuilder:
        self._checks.append(check)
        self.logger.debug("Added check '%s' to suite '%s'", check.name, self._name)
        return self

    def add_checks(self, checks: list[Check]) -> ValidationSuiteBuilder:
        self._checks.extend(checks)
        self.logger.debug("Added %d check(s) to suite '%s'", len(checks), self._name)
        return self

    def run_parallel(self, enabled: bool = False) -> ValidationSuiteBuilder:
        """Enable or disable concurrent check execution (default: enabled)."""
        self._run_parallel = enabled
        self.logger.debug(
            "Parallel execution %s for suite '%s'", "enabled" if enabled else "disabled", self._name
        )
        return self

    async def run(self) -> ValidationResult:
        """Execute all checks against the DataFusion context."""

        if self._ctx is None:
            self.logger.error("No data context set — call .on_data(ctx, table) before .run()")
            raise RuntimeError("No data context set. Call .on_data(ctx, table) before .run().")

        start_time = time.perf_counter()

        self.logger.info(
            "Suite '%s' started — running %d check(s) on table '%s'",
            self._name,
            len(self._checks),
            self._table_name,
        )

        metrics = ValidationMetrics(total_checks=len(self._checks))
        issues: list[ValidationIssue] = []
        check_results_map: dict[str, list] = {}
        overall_success = True
        worst_status = CheckStatus.SUCCESS

        check_results: list[CheckResult] = []
        if self._run_parallel:
            check_results = await asyncio.gather(
                *(check.run(self._ctx, self._table_name) for check in self._checks)
            )
        else:
            for check in self._checks:
                check_results.append(await check.run(self._ctx, self._table_name))

        for check, cr in zip(self._checks, check_results, strict=False):
            check_results_map[check.name] = cr.constraint_results
            metrics.total_constraints += len(cr.constraint_results)

            # Build a name→constraint lookup so we can grab metadata
            constraint_by_name: dict[str, Constraint] = {c.name(): c for c in check.constraints}

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

                    # Resolve metadata for this constraint
                    meta = None
                    src = constraint_by_name.get(result.constraint_name)
                    if src is not None:
                        meta = src.metadata()

                    issues.append(
                        ValidationIssue(
                            check_name=check.name,
                            constraint_name=result.constraint_name,
                            level=check.level,
                            message=result.message,
                            metric=result.metric,
                            column=meta.column if meta else None,
                            description=meta.description if meta else "",
                            metadata_extra=dict(meta.extra) if meta else {},
                        )
                    )
                else:
                    metrics.skipped += 1

        elapsed_ms = int((time.perf_counter() - start_time) * 1000)
        metrics.execution_time_ms = elapsed_ms

        report = ValidationReport(
            suite_name=self._name,
            metrics=metrics,
            check_results=check_results_map,
            issues=issues,
        )

        result = ValidationResult(
            success=overall_success,
            status=worst_status,
            report=report,
        )

        pass_rate = metrics.pass_rate
        if overall_success:
            self.logger.info(
                "Suite '%s' completed — PASSED (pass_rate=%.1f%%, passed=%d, failed=%d, time=%dms)",
                self._name,
                pass_rate * 100,
                metrics.passed,
                metrics.failed,
                elapsed_ms,
            )
        else:
            self.logger.error(
                "Suite '%s' completed — FAILED "
                "(pass_rate=%.1f%%, passed=%d, failed=%d, errors=%d, warnings=%d, time=%dms)",
                self._name,
                pass_rate * 100,
                metrics.passed,
                metrics.failed,
                metrics.error_count,
                metrics.warning_count,
                elapsed_ms,
            )

        return result

    def build(self) -> ValidationSuite:
        """Return a configured ``ValidationSuite`` (for deferred execution)."""
        suite = ValidationSuite(self._name)
        return suite
