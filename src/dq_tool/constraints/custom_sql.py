"""Custom SQL constraint: user-provided SQL boolean expression evaluated per row."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext

from dq_tool.core.constraint import (
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
        df = ctx.sql(sql)
        rows = df.collect()
        compliance: float = rows[0].column("compliance")[0].as_py()

        passed = compliance == 1.0
        return ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=compliance,
            message=(
                ""
                if passed
                else (
                    f"Custom SQL compliance is {compliance:.4f} "
                    f"(expression: {self._expression})"
                )
            ),
            constraint_name=self.name(),
        )

    def name(self) -> str:
        label = self._hint or self._expression[:50]
        return f"CustomSQL({label})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(
            name=self.name(),
            description=f"All rows must satisfy: {self._expression}",
        )
