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


class ApproxQuantileConstraint(Constraint):
    """Validates that an approximate quantile of *column* satisfies *assertion*."""

    def __init__(self, column: str, quantile: float, assertion: Assertion, *, hint: str = "") -> None:
        if not 0.0 <= quantile <= 1.0:
            raise ValueError(f"quantile must be in [0, 1], got {quantile}")
        self._column = column
        self._quantile = quantile
        self._assertion = assertion
        self._hint = hint

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        sql = (
            f'SELECT CAST(APPROX_PERCENTILE_CONT("{self._column}", {self._quantile}) '
            f"AS DOUBLE) AS q FROM {table_name}"
        )
        self.logger.debug("Executing SQL: %s", sql)
        rows = ctx.sql(sql).collect()
        raw = rows[0].column("q")[0].as_py()
        if raw is None:
            self.logger.warning("Column '%s' produced NULL for quantile %s", self._column, self._quantile)
            return ConstraintResult(
                status=ConstraintStatus.FAILURE,
                message=f"Column '{self._column}' produced NULL for quantile {self._quantile}",
                constraint_name=self.name(),
            )
        value = float(raw)
        self.logger.debug("Metric value: %s", value)
        passed = self._assertion.evaluate(value)
        result = ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=value,
            message=""
            if passed
            else (
                f"ApproxQuantile({self._quantile}) of '{self._column}' is {value:.4f}, "
                f"expected {self._assertion}. {self._hint}".strip()
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
        return f"ApproxQuantile({self._column}, {self._quantile})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(name=self.name(), column=self._column)
