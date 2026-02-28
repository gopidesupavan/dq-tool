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


class ColumnExistsConstraint(Constraint):
    """Validates that *column* exists in the table schema."""

    def __init__(self, column: str, *, hint: str = "") -> None:
        self._column = column
        self._hint = hint

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        sql = f"SELECT * FROM {table_name} LIMIT 0"
        self.logger.debug("Checking schema via: %s", sql)
        schema = ctx.sql(sql).schema()
        col_names = [schema.field(i).name for i in range(len(schema))]
        exists = self._column in col_names
        self.logger.debug("Available columns: %s", col_names)
        result = ConstraintResult(
            status=ConstraintStatus.SUCCESS if exists else ConstraintStatus.FAILURE,
            metric=1.0 if exists else 0.0,
            message=""
            if exists
            else (
                f"Column '{self._column}' does not exist in table '{table_name}'. "
                f"Available: {col_names}. {self._hint}".strip()
            ),
            constraint_name=self.name(),
        )
        if exists:
            self.logger.info("Constraint %s passed — column found", self.name())
        else:
            self.logger.info("Constraint %s failed — column '%s' not found", self.name(), self._column)
        return result

    def name(self) -> str:
        return f"ColumnExists({self._column})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(name=self.name(), column=self._column)
