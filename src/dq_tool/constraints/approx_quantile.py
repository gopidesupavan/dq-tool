"""Approximate quantile constraint."""

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


class ApproxQuantileConstraint(Constraint):
    """Validates that an approximate quantile of *column* satisfies *assertion*."""

    def __init__(
        self, column: str, quantile: float, assertion: Assertion, *, hint: str = ""
    ) -> None:
        if not 0.0 <= quantile <= 1.0:
            raise ValueError(f"quantile must be in [0, 1], got {quantile}")
        self._column = column
        self._quantile = quantile
        self._assertion = assertion
        self._hint = hint

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        sql = (
            f"SELECT CAST(APPROX_PERCENTILE_CONT(\"{self._column}\", {self._quantile}) "
            f"AS DOUBLE) AS q FROM {table_name}"
        )
        rows = ctx.sql(sql).collect()
        raw = rows[0].column("q")[0].as_py()
        if raw is None:
            return ConstraintResult(
                status=ConstraintStatus.FAILURE,
                message=f"Column '{self._column}' produced NULL for quantile {self._quantile}",
                constraint_name=self.name(),
            )
        value = float(raw)
        passed = self._assertion.evaluate(value)
        return ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=value,
            message="" if passed else (
                f"ApproxQuantile({self._quantile}) of '{self._column}' is {value:.4f}, "
                f"expected {self._assertion}. {self._hint}".strip()
            ),
            constraint_name=self.name(),
        )

    def name(self) -> str:
        return f"ApproxQuantile({self._column}, {self._quantile})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(name=self.name(), column=self._column)
