import pytest
from qualink.datasources import (
    ConnectionSpec,
    infer_object_store_bucket,
    infer_object_store_key,
    infer_object_store_provider,
    infer_source_kind,
    normalize_connection_specs,
    normalize_data_source_specs,
)


def test_normalize_connections() -> None:
    cfg = {
        "connections": {
            "warehouse": {
                "uri": "snowflake://user:pass@account/db/schema?warehouse=wh",
            }
        }
    }

    connections = normalize_connection_specs(cfg)

    assert "warehouse" in connections
    assert connections["warehouse"].options["uri"] == "snowflake://user:pass@account/db/schema?warehouse=wh"


def test_normalize_connections_resolves_inline_secret_refs() -> None:
    cfg = {
        "connections": {
            "warehouse": {
                "uri": {"from": "env", "key": "WAREHOUSE_URI"},
            }
        }
    }

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setenv("WAREHOUSE_URI", "snowflake://secret@account/db/schema?warehouse=wh")
        connections = normalize_connection_specs(cfg)

    assert connections["warehouse"].options["uri"] == "snowflake://secret@account/db/schema?warehouse=wh"


def test_normalize_data_sources_defaults_table_name_to_source_name() -> None:
    cfg = {"data_sources": [{"name": "users_source", "path": "users.csv"}]}

    sources = normalize_data_source_specs(cfg)

    assert sources[0].table_name == "users_source"


@pytest.mark.parametrize(
    ("connection", "raw", "expected_kind"),
    [
        (None, {"path": "users.csv", "table_name": "users"}, "file"),
        (None, {"path": "s3://bucket/users.parquet", "table_name": "users"}, "object_store"),
        (
            ConnectionSpec(name="sqlite_local", options={"uri": "sqlite:///tmp/users.db"}),
            {"connection": "sqlite_local", "query": "select 1", "table_name": "users"},
            "adbc",
        ),
        (
            ConnectionSpec(name="sqlite_local", options={"uri": "sqlite:///tmp/users.db"}),
            {"connection": "sqlite_local", "table": "users", "table_name": "users"},
            "adbc",
        ),
    ],
)
def test_normalize_data_sources(
    connection: ConnectionSpec | None,
    raw: dict[str, str],
    expected_kind: str,
) -> None:
    cfg = {"data_sources": [raw]}

    sources = normalize_data_source_specs(cfg)

    assert len(sources) == 1
    assert infer_source_kind(sources[0], connection) == expected_kind


@pytest.mark.parametrize(
    ("path", "provider", "bucket", "key"),
    [
        ("s3://bucket/path/users.csv", "s3", "bucket", "path/users.csv"),
        ("gs://bucket/path/users.csv", "gcs", "bucket", "path/users.csv"),
        ("abfss://container/path/users.csv", "azure", "container", "path/users.csv"),
    ],
)
def test_object_store_path_inference(path: str, provider: str, bucket: str, key: str) -> None:
    assert infer_object_store_provider(path) == provider
    assert infer_object_store_bucket(path) == bucket
    assert infer_object_store_key(path) == key


def test_data_source_requires_path_table_or_query() -> None:
    with pytest.raises(ValueError, match="must define 'path', 'table', or 'query'"):
        normalize_data_source_specs({"data_sources": [{"table_name": "users"}]})


def test_adbc_data_source_requires_connection() -> None:
    with pytest.raises(ValueError, match="requires 'connection' when using 'table' or 'query'"):
        normalize_data_source_specs({"data_sources": [{"table": "users", "table_name": "users"}]})
