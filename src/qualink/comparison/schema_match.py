"""Schema match check between two DataFusion tables."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from qualink.core.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from datafusion import SessionContext


@dataclass(frozen=True)
class SchemaMatchResult:
    matching_columns: list[str] = field(default_factory=list)
    only_in_a: list[str] = field(default_factory=list)
    only_in_b: list[str] = field(default_factory=list)
    type_mismatches: dict[str, tuple[str, str]] = field(default_factory=dict)

    @property
    def is_match(self) -> bool:
        return not self.only_in_a and not self.only_in_b and not self.type_mismatches


class SchemaMatch(LoggingMixin):
    """Compares schemas of two tables via DataFusion."""

    def __init__(self, table_a: str, table_b: str) -> None:
        self._table_a = table_a
        self._table_b = table_b

    async def run(self, ctx: SessionContext) -> SchemaMatchResult:
        self.logger.debug("Comparing schemas of '%s' and '%s'", self._table_a, self._table_b)
        sa = ctx.sql(f"SELECT * FROM {self._table_a} LIMIT 0").schema()
        sb = ctx.sql(f"SELECT * FROM {self._table_b} LIMIT 0").schema()

        cols_a = {sa.field(i).name: str(sa.field(i).type) for i in range(len(sa))}
        cols_b = {sb.field(i).name: str(sb.field(i).type) for i in range(len(sb))}

        matching, mismatches = [], {}
        for name in set(cols_a) & set(cols_b):
            if cols_a[name] == cols_b[name]:
                matching.append(name)
            else:
                mismatches[name] = (cols_a[name], cols_b[name])

        result = SchemaMatchResult(
            matching_columns=sorted(matching),
            only_in_a=sorted(set(cols_a) - set(cols_b)),
            only_in_b=sorted(set(cols_b) - set(cols_a)),
            type_mismatches=mismatches,
        )
        self.logger.info(
            "SchemaMatch result: matching=%d, only_in_a=%d, only_in_b=%d, mismatches=%d",
            len(result.matching_columns),
            len(result.only_in_a),
            len(result.only_in_b),
            len(result.type_mismatches),
        )
        return result
