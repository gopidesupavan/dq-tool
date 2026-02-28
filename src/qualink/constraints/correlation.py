from __future__ import annotations

import math
from typing import TYPE_CHECKING

from qualink.core.constraint import (
    Constraint,
    ConstraintMetadata,
    ConstraintResult,
    ConstraintStatus,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

    from qualink.constraints.assertion import Assertion


class CorrelationConstraint(Constraint):
    """Validates that the Pearson correlation of *column_a* and *column_b* satisfies *assertion*."""

    def __init__(
        self,
        column_a: str,
        column_b: str,
        assertion: Assertion,
        *,
        hint: str = "",
    ) -> None:
        self._col_a = column_a
        self._col_b = column_b
        self._assertion = assertion
        self._hint = hint

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        a, b = self._col_a, self._col_b
        sql = (
            f"SELECT "
            f'(COUNT(*) * SUM(CAST("{a}" AS DOUBLE) * CAST("{b}" AS DOUBLE)) '
            f' - SUM(CAST("{a}" AS DOUBLE)) * SUM(CAST("{b}" AS DOUBLE))) '
            f"/ GREATEST(SQRT("
            f'  (COUNT(*) * SUM(CAST("{a}" AS DOUBLE) * CAST("{a}" AS DOUBLE)) '
            f'   - POWER(SUM(CAST("{a}" AS DOUBLE)), 2)) * '
            f'  (COUNT(*) * SUM(CAST("{b}" AS DOUBLE) * CAST("{b}" AS DOUBLE)) '
            f'   - POWER(SUM(CAST("{b}" AS DOUBLE)), 2))'
            f"), 1e-15) AS corr "
            f'FROM {table_name} WHERE "{a}" IS NOT NULL AND "{b}" IS NOT NULL'
        )
        self.logger.debug("Executing SQL: %s", sql)
        rows = ctx.sql(sql).collect()
        raw = rows[0].column("corr")[0].as_py()
        value = float(raw) if raw is not None and not math.isnan(raw) else 0.0
        self.logger.debug("Metric value: %s", value)
        passed = self._assertion.evaluate(value)
        result = ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=value,
            message=""
            if passed
            else (
                f"Correlation({self._col_a}, {self._col_b}) is {value:.4f}, "
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
        return f"Correlation({self._col_a}, {self._col_b})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(name=self.name())
