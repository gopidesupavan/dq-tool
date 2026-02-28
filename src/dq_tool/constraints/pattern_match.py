"""Pattern match constraint: fraction of values matching a regex pattern."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext

    from dq_tool.constraints.assertion import Assertion

from dq_tool.core.constraint import (
    Constraint,
    ConstraintMetadata,
    ConstraintResult,
    ConstraintStatus,
)


class PatternMatchConstraint(Constraint):
    """Validates that the fraction of *column* values matching *pattern* satisfies *assertion*.

    Mirrors Deequ's ``hasPattern`` / ``PatternMatch`` analyzer.
    """

    def __init__(
        self,
        column: str,
        pattern: str,
        assertion: Assertion,
        *,
        hint: str = "",
    ) -> None:
        self._column = column
        self._pattern = pattern
        self._assertion = assertion
        self._hint = hint

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        escaped = self._pattern.replace("'", "''")
        col_expr = f'CAST("{self._column}" AS VARCHAR)'
        sql = (
            f"SELECT CAST(SUM(CASE WHEN {col_expr} ~ '{escaped}' "
            f"THEN 1 ELSE 0 END) AS DOUBLE) "
            f"/ CAST(GREATEST(COUNT(\"{self._column}\"), 1) AS DOUBLE) AS match_ratio "
            f"FROM {table_name} WHERE \"{self._column}\" IS NOT NULL"
        )
        rows = ctx.sql(sql).collect()
        value: float = rows[0].column("match_ratio")[0].as_py()
        passed = self._assertion.evaluate(value)
        return ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=value,
            message="" if passed else (
                f"Pattern match on '{self._column}' is {value:.4f}, "
                f"expected {self._assertion}. {self._hint}".strip()
            ),
            constraint_name=self.name(),
        )

    def name(self) -> str:
        return f"PatternMatch({self._column}, {self._pattern!r})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(name=self.name(), column=self._column)
