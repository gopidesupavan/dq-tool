"""MaxLength constraint: asserts on the maximum string length of a column."""

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


class MaxLengthConstraint(Constraint):
    """Validates that the maximum string length of *column* satisfies *assertion*."""

    def __init__(
        self, column: str, assertion: Assertion, *, hint: str = ""
    ) -> None:
        self._column = column
        self._assertion = assertion
        self._hint = hint

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        sql = (
            f"SELECT CAST(MAX(LENGTH(\"{self._column}\")) AS DOUBLE) AS max_len "
            f"FROM {table_name} WHERE \"{self._column}\" IS NOT NULL"
        )
        rows = ctx.sql(sql).collect()
        raw = rows[0].column("max_len")[0].as_py()
        if raw is None:
            return ConstraintResult(
                status=ConstraintStatus.FAILURE,
                message=f"Column '{self._column}' has no non-null values",
                constraint_name=self.name(),
            )
        value = float(raw)
        passed = self._assertion.evaluate(value)
        return ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=value,
            message="" if passed else (
                f"MaxLength of '{self._column}' is {value:.0f}, "
                f"expected {self._assertion}. {self._hint}".strip()
            ),
            constraint_name=self.name(),
        )

    def name(self) -> str:
        return f"MaxLength({self._column})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(name=self.name(), column=self._column)
