from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext

    from qualink.constraints.assertion import Assertion

from qualink.core.constraint import (
    Constraint,
    ConstraintMetadata,
    ConstraintResult,
    ConstraintStatus,
)


class ComplianceConstraint(Constraint):
    """Validates that the fraction of rows where *predicate* is true satisfies *assertion*."""

    def __init__(
        self,
        name_label: str,
        predicate: str,
        assertion: Assertion,
        *,
        hint: str = "",
    ) -> None:
        self._label = name_label
        self._predicate = predicate
        self._assertion = assertion
        self._hint = hint

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        sql = (
            f"SELECT AVG(CASE WHEN {self._predicate} THEN 1.0 ELSE 0.0 END) AS compliance "
            f"FROM {table_name}"
        )
        self.logger.debug("Executing SQL: %s", sql)
        rows = ctx.sql(sql).collect()
        value: float = rows[0].column("compliance")[0].as_py()
        self.logger.debug("Metric value: %s", value)
        passed = self._assertion.evaluate(value)
        result = ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=value,
            message=""
            if passed
            else (
                f"Compliance '{self._label}' is {value:.4f}, expected {self._assertion}. {self._hint}".strip()
            ),
            constraint_name=self.name(),
        )
        if passed:
            self.logger.info("Constraint %s passed (metric=%.4f)", self.name(), value)
        else:
            self.logger.info(
                "Constraint %s failed (metric=%.4f, expected %s)", self.name(), value, self._assertion
            )
        return result

    def name(self) -> str:
        return f"Compliance({self._label})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(name=self.name(), description=self._predicate)
