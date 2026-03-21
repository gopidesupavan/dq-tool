from __future__ import annotations

from typing import TYPE_CHECKING

from qualink.datasources.prepared import SourceCapabilities

if TYPE_CHECKING:
    from datafusion import SessionContext


SUPPORTED_FILE_FORMATS = frozenset({"csv", "parquet", "json"})


def resolve_data_format(fmt: str | None, location: str, *, error_prefix: str) -> str:
    normalized = (fmt or "").lower()
    if normalized in SUPPORTED_FILE_FORMATS:
        return normalized
    if location.endswith(".csv"):
        return "csv"
    if location.endswith(".parquet") or location.endswith(".pq"):
        return "parquet"
    if location.endswith(".json") or location.endswith(".jsonl") or location.endswith(".ndjson"):
        return "json"
    raise ValueError(f"Cannot determine file format for {error_prefix}.")


def register_table_by_format(ctx: SessionContext, table_name: str, location: str, fmt: str) -> None:
    if fmt == "csv":
        ctx.register_csv(table_name, location)
        return
    if fmt == "parquet":
        ctx.register_parquet(table_name, location)
        return
    if fmt == "json":
        ctx.register_json(table_name, location)
        return
    raise ValueError(f"Unsupported format: {fmt}")


def native_capabilities(fmt: str) -> SourceCapabilities:
    is_parquet = fmt == "parquet"
    return SourceCapabilities(
        native_to_datafusion=True,
        supports_predicate_pushdown=is_parquet,
        supports_projection_pushdown=is_parquet,
        supports_parallel_scan=is_parquet,
    )
