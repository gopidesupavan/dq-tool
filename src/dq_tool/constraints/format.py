"""Format constraint: validates column values against patterns or known formats."""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext

from dq_tool.core.constraint import (
    Constraint,
    ConstraintMetadata,
    ConstraintResult,
    ConstraintStatus,
)

_BUILTIN_PATTERNS: dict[str, str] = {
    "email": r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$",
    "url": r"^https?://[^\s/$.?#].[^\s]*$",
    "phone": r"^\+?[0-9\s\-().]{7,20}$",
    "credit_card": r"^[0-9]{13,19}$",
    "ssn": r"^\d{3}-?\d{2}-?\d{4}$",
    "ipv4": r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$",
}


class FormatType(Enum):
    """Pre-defined format categories or a free-form regex."""

    REGEX = "regex"
    EMAIL = "email"
    URL = "url"
    PHONE = "phone"
    CREDIT_CARD = "credit_card"
    SSN = "ssn"
    IPV4 = "ipv4"


class FormatConstraint(Constraint):
    """Validates that at least *threshold* fraction of *column* values match a pattern."""

    def __init__(
        self,
        column: str,
        format_type: FormatType,
        *,
        pattern: str | None = None,
        threshold: float = 1.0,
    ) -> None:
        if format_type == FormatType.REGEX and not pattern:
            raise ValueError("A pattern is required when format_type is REGEX")
        self._column = column
        self._format_type = format_type
        self._pattern = pattern or _BUILTIN_PATTERNS.get(format_type.value, "")
        self._threshold = threshold

    async def evaluate(self, ctx: SessionContext, table_name: str) -> ConstraintResult:
        escaped = self._pattern.replace("'", "''")
        col_expr = f'CAST("{self._column}" AS VARCHAR)'
        sql = (
            f"SELECT CAST(SUM(CASE WHEN {col_expr} ~ '{escaped}' "
            f"THEN 1 ELSE 0 END) AS DOUBLE) "
            f"/ CAST(GREATEST(COUNT(\"{self._column}\"), 1) AS DOUBLE) AS compliance "
            f"FROM {table_name} WHERE \"{self._column}\" IS NOT NULL"
        )
        df = ctx.sql(sql)
        rows = df.collect()
        compliance: float = rows[0].column("compliance")[0].as_py()

        passed = compliance >= self._threshold
        return ConstraintResult(
            status=ConstraintStatus.SUCCESS if passed else ConstraintStatus.FAILURE,
            metric=compliance,
            message=(
                ""
                if passed
                else (
                    f"Format compliance of '{self._column}' ({self._format_type.value}) "
                    f"is {compliance:.4f}, expected >= {self._threshold}"
                )
            ),
            constraint_name=self.name(),
        )

    def name(self) -> str:
        return f"Format({self._column}, {self._format_type.value})"

    def metadata(self) -> ConstraintMetadata:
        return ConstraintMetadata(
            name=self.name(),
            description=(
                f"Format compliance of '{self._column}' "
                f"({self._format_type.value}) >= {self._threshold}"
            ),
            column=self._column,
        )
