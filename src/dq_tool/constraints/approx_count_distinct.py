"""Approximate count distinct constraint."""

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


class ApproxCountDistinctConstraint(Constraint):
    """Validates that the approximate distinct count of *column* satisfies *assertion*."""

    def __init__(
        self, column: str, assertion: Assertion, *, hint: str = ""
    ) -> None:
        self._column = column
        self._assertion = assertion
        self._hint = hint

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        sql = (
            f"SELECT CAST(APPROX_DISTINCT(\"{self._column}\") AS DOUBLE) AS acd "
            f"FROM {table_name}"
        )
        rows = ctx.sql(sql).collect()
        value = float(rows[0].column("acd")[0].as_py())
        passed = self._assertion.evaluate(value)
        return ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=value,
            message="" if passed else (
                f"ApproxCountDistinct of '{self._column}' is {value:.0f}, "
                f"expected {self._assertion}. {self._hint}".strip()
            ),
            constraint_name=self.name(),
        )

    def name(self) -> str:
        return f"ApproxCountDistinct({self._column})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(name=self.name(), column=self._column)
