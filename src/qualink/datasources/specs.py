from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from qualink.secrets import SecretResolver

SUPPORTED_OBJECT_STORE_SCHEMES = {
    "s3": "s3",
    "gs": "gcs",
    "gcs": "gcs",
    "az": "azure",
    "abfs": "azure",
    "abfss": "azure",
}


@dataclass(frozen=True)
class ConnectionSpec:
    """Named connection settings shared by one or more data sources."""

    name: str
    options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DataSourceSpec:
    """Normalized datasource definition used by the adapter layer."""

    name: str
    table_name: str
    connection: str | None = None
    table: str | None = None
    format: str | None = None
    path: str | None = None
    query: str | None = None
    options: dict[str, Any] = field(default_factory=dict)


def normalize_connection_specs(cfg: dict[str, Any]) -> dict[str, ConnectionSpec]:
    raw_connections = cfg.get("connections", {})
    if not isinstance(raw_connections, dict):
        raise ValueError("'connections' must be a mapping when provided.")

    resolver = SecretResolver()
    normalized: dict[str, ConnectionSpec] = {}
    for name, raw in raw_connections.items():
        if not isinstance(raw, dict):
            raise ValueError(f"Connection {name!r} must be a mapping.")
        normalized[name] = ConnectionSpec(
            name=name,
            options=resolver.resolve_options(dict(raw)),
        )
    return normalized


def normalize_data_source_specs(cfg: dict[str, Any]) -> list[DataSourceSpec]:
    raw_sources = cfg.get("data_sources")
    if raw_sources is None:
        raw_single = cfg.get("data_source")
        raw_sources = [] if raw_single is None else [raw_single]

    if not isinstance(raw_sources, list):
        raise ValueError("'data_sources' must be a list when provided.")

    normalized: list[DataSourceSpec] = []
    for index, raw in enumerate(raw_sources):
        if not isinstance(raw, dict):
            raise ValueError("Each data source must be a mapping.")

        source_name = str(raw.get("name", raw.get("table_name", f"source_{index}")))
        table_name = str(raw.get("table_name", source_name))
        path = _resolve_optional_string(raw, "path")
        query = _resolve_optional_string(raw, "query")
        table = _resolve_optional_string(raw, "table")
        connection = _resolve_optional_string(raw, "connection")
        if not path and not query and not table:
            raise ValueError(f"Data source {source_name!r} must define 'path', 'table', or 'query'.")
        if path and (table or query):
            raise ValueError(f"Data source {source_name!r} cannot mix 'path' with 'table' or 'query'.")
        if (table or query) and not connection:
            raise ValueError(
                f"Data source {source_name!r} requires 'connection' when using 'table' or 'query'."
            )

        normalized.append(
            DataSourceSpec(
                name=source_name,
                table_name=table_name,
                connection=connection,
                table=table,
                format=_resolve_format(raw, path),
                path=path,
                query=query,
                options=_extract_options(raw),
            )
        )

    return normalized


def infer_source_kind(source: DataSourceSpec, connection: ConnectionSpec | None = None) -> str:
    if connection is not None and connection.options.get("uri"):
        if source.table or source.query:
            return "adbc"
        raise ValueError(f"Data source {source.name!r} requires 'table' or 'query' for ADBC sources.")
    if source.table or source.query:
        raise ValueError(
            f"Data source {source.name!r} requires a supported 'connection' when using 'table' or 'query'."
        )
    if source.path is None:
        raise ValueError(f"Data source {source.name!r} must define 'path', 'table', or 'query'.")
    provider = infer_object_store_provider(source.path)
    if provider is not None:
        return "object_store"
    scheme = urlparse(source.path).scheme.lower()
    if scheme:
        return scheme
    return "file"


def infer_object_store_provider(path: str) -> str | None:
    scheme = urlparse(path).scheme.lower()
    if not scheme:
        return None
    return SUPPORTED_OBJECT_STORE_SCHEMES.get(scheme)


def infer_object_store_scheme(path: str) -> str | None:
    scheme = urlparse(path).scheme.lower()
    if not scheme or scheme not in SUPPORTED_OBJECT_STORE_SCHEMES:
        return None
    return scheme


def infer_object_store_bucket(path: str) -> str:
    parsed = urlparse(path)
    if not parsed.netloc:
        raise ValueError(f"Object store path must include a bucket/container: {path!r}")
    return parsed.netloc


def infer_object_store_key(path: str) -> str:
    parsed = urlparse(path)
    return parsed.path.lstrip("/")


def infer_data_format(path: str | None, explicit_format: str | None) -> str | None:
    if explicit_format is not None:
        return explicit_format.lower()
    if not path:
        return None
    lowered = path.lower()
    if lowered.endswith(".csv"):
        return "csv"
    if lowered.endswith(".parquet"):
        return "parquet"
    if lowered.endswith(".json") or lowered.endswith(".jsonl"):
        return "json"
    return None


def _resolve_format(raw: dict[str, Any], path: str | None) -> str | None:
    fmt = raw.get("format")
    if fmt is not None:
        return str(fmt).lower()
    return infer_data_format(path, None)


def _extract_options(raw: dict[str, Any]) -> dict[str, Any]:
    ignored = {"connection", "database", "format", "name", "path", "query", "table", "table_name"}
    return {key: value for key, value in raw.items() if key not in ignored}


def _resolve_optional_string(raw: dict[str, Any], key: str) -> str | None:
    value = raw.get(key)
    if value is None:
        return None
    return str(value)
