from unittest.mock import MagicMock

import pytest
from qualink.datasources import ConnectionSpec, DataSourceSpec, default_source_adapter_registry


def test_registry_prepares_file_source() -> None:
    ctx = MagicMock()
    registry = default_source_adapter_registry()
    source = DataSourceSpec(
        name="users",
        table_name="users",
        path="users.csv",
        format="csv",
    )
    connection = ConnectionSpec(name="local")

    prepared = registry.prepare(ctx, source, connection)

    ctx.register_csv.assert_called_once_with("users", "users.csv")
    assert prepared.table_name == "users"
    assert prepared.capabilities.native_to_datafusion is True


def test_registry_rejects_unknown_source_type() -> None:
    ctx = MagicMock()
    registry = default_source_adapter_registry()
    source = DataSourceSpec(name="users", table_name="users", path="ftp://example.com/users.csv")

    with pytest.raises(ValueError, match="Unknown datasource kind"):
        registry.prepare(ctx, source, None)


def test_registry_rejects_unknown_connection_backed_source() -> None:
    ctx = MagicMock()
    registry = default_source_adapter_registry()
    source = DataSourceSpec(name="users", table_name="users", table="users", connection="warehouse")
    connection = ConnectionSpec(name="warehouse", options={"catalog": "unsupported"})

    with pytest.raises(ValueError, match="requires a supported 'connection'"):
        registry.prepare(ctx, source, connection)
