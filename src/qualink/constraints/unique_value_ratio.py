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


class UniqueValueRatioConstraint(Constraint):
    """Validates that the unique-value ratio of *columns* satisfies *assertion*.

    UniqueValueRatio = (values appearing exactly once) / COUNT(DISTINCT values)
    """

    def __init__(self, columns: list[str], assertion: Assertion, *, hint: str = "") -> None:
        self._columns = list(columns)
        self._assertion = assertion
        self._hint = hint

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        cols = ", ".join(f'"{c}"' for c in self._columns)
        sql = (
            f"SELECT CAST(SUM(CASE WHEN cnt = 1 THEN 1 ELSE 0 END) AS DOUBLE) "
            f"/ CAST(GREATEST(COUNT(*), 1) AS DOUBLE) AS uvr "
            f"FROM (SELECT {cols}, COUNT(*) AS cnt FROM {table_name} GROUP BY {cols})"
        )
        self.logger.debug("Executing SQL: %s", sql)
        rows = ctx.sql(sql).collect()
        value: float = rows[0].column("uvr")[0].as_py()
        self.logger.debug("Metric value: %s", value)
        passed = self._assertion.evaluate(value)
        col_label = ", ".join(self._columns)
        result = ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=value,
            message=""
            if passed
            else (
                f"UniqueValueRatio of ({col_label}) is {value:.4f}, "
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
        return f"UniqueValueRatio({', '.join(self._columns)})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(
            name=self.name(),
            column=self._columns[0] if len(self._columns) == 1 else None,
        )
