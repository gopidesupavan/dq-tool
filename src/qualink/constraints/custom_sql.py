from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext

from qualink.core.constraint import (
    Constraint,
    ConstraintMetadata,
    ConstraintResult,
    ConstraintStatus,
)


class CustomSqlConstraint(Constraint):
    """Validates that *sql_expression* holds true for all rows.

    The expression is evaluated as a WHERE-clause predicate; the constraint
    passes when the ratio of matching rows equals 1.0.
    """

    def __init__(self, sql_expression: str, hint: str = "") -> None:
        self._expression = sql_expression
        self._hint = hint

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        sql = (
            f"SELECT "
            f"CAST(SUM(CASE WHEN {self._expression} THEN 1 ELSE 0 END) AS DOUBLE) "
            f"/ CAST(GREATEST(COUNT(*), 1) AS DOUBLE) AS compliance "
            f"FROM {table_name}"
        )
        self.logger.debug("Executing SQL: %s", sql)
        df = ctx.sql(sql)
        rows = df.collect()
        compliance: float = rows[0].column("compliance")[0].as_py()
        self.logger.debug("Metric value: %s", compliance)

        passed = compliance == 1.0
        result = ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=compliance,
            message=(
                ""
                if passed
                else (f"Custom SQL compliance is {compliance:.4f} (expression: {self._expression})")
            ),
            constraint_name=self.name(),
        )
        if passed:
            self.logger.info("Constraint %s passed (metric=%.4f)", self.name(), compliance)
        else:
            self.logger.info("Constraint %s failed (metric=%.4f)", self.name(), compliance)
        return result

    def name(self) -> str:
        label = self._hint or self._expression[:50]
        return f"CustomSQL({label})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(
            name=self.name(),
            description=f"All rows must satisfy: {self._expression}",
        )
