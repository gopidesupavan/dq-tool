from unittest.mock import MagicMock, patch

import pytest
from qualink.datasources import ConnectionSpec, DataSourceSpec
from qualink.datasources.adapters.adbc_table import AdbcTableAdapter


def test_validate_requires_connection() -> None:
    adapter = AdbcTableAdapter()
    source = DataSourceSpec(name="users", table_name="users", table="users")

    with pytest.raises(ValueError, match="requires a named connection"):
        adapter.validate(source, None)


def test_validate_requires_uri() -> None:
    adapter = AdbcTableAdapter()
    source = DataSourceSpec(name="users", table_name="users", table="users", connection="sqlite_local")
    connection = ConnectionSpec(name="sqlite_local")

    with pytest.raises(ValueError, match="requires 'uri'"):
        adapter.validate(source, connection)


def test_validate_rejects_unsupported_scheme() -> None:
    adapter = AdbcTableAdapter()
    source = DataSourceSpec(name="users", table_name="users", table="users", connection="warehouse")
    connection = ConnectionSpec(name="warehouse", options={"uri": "mysql://localhost/test"})

    with pytest.raises(ValueError, match="Unsupported ADBC URI scheme"):
        adapter.validate(source, connection)


@patch("qualink.datasources.adapters.adbc_table.import_module", side_effect=ImportError)
def test_missing_driver_package_raises_clear_error(mock_import_module) -> None:
    adapter = AdbcTableAdapter()
    source = DataSourceSpec(name="users", table_name="users", table="users", connection="sqlite_local")
    connection = ConnectionSpec(name="sqlite_local", options={"uri": "sqlite:///tmp/users.db"})

    with pytest.raises(ValueError, match="ADBC driver package 'adbc_driver_sqlite' is required"):
        adapter.prepare(MagicMock(), source, connection)

    mock_import_module.assert_called_once_with("adbc_driver_sqlite.dbapi")


def test_builds_select_star_query_for_table_source() -> None:
    adapter = AdbcTableAdapter()
    source = DataSourceSpec(name="users", table_name="users", table="users")

    assert adapter._build_table_query(source) == "SELECT * FROM users"


def test_sqlite_driver_uri_normalizes_absolute_path() -> None:
    adapter = AdbcTableAdapter()

    assert adapter._driver_uri("sqlite:////var/folders/example/users.db") == "/var/folders/example/users.db"
