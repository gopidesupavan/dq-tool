from __future__ import annotations

import sqlite3

import pytest
from datafusion import SessionContext
from qualink.config import run_yaml

pytest.importorskip("adbc_driver_sqlite.dbapi")


def _create_sqlite_db(path) -> None:
    connection = sqlite3.connect(path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE users (
                user_id INTEGER PRIMARY KEY,
                email TEXT NOT NULL,
                age INTEGER NOT NULL
            )
            """
        )
        cursor.executemany(
            "INSERT INTO users (user_id, email, age) VALUES (?, ?, ?)",
            [
                (1, "alice@example.com", 30),
                (2, "bob@example.com", 25),
                (3, "charlie@example.com", 35),
            ],
        )
        connection.commit()
    finally:
        connection.close()


@pytest.mark.asyncio()
async def test_run_yaml_with_sqlite_table_source(tmp_path) -> None:
    db_path = tmp_path / "users.db"
    _create_sqlite_db(db_path)

    yaml_config = f"""
suite:
  name: SQLite Table Source

connections:
  sqlite_local:
    uri: sqlite:///{db_path}

data_sources:
  - name: users_source
    connection: sqlite_local
    table: users
    table_name: users

checks:
  - name: Basic Checks
    level: error
    rules:
      - has_size:
          eq: 3
      - is_complete: email
"""

    result = await run_yaml(yaml_config, SessionContext())

    assert result.success is True


@pytest.mark.asyncio()
async def test_run_yaml_with_sqlite_query_source(tmp_path) -> None:
    db_path = tmp_path / "users.db"
    _create_sqlite_db(db_path)

    yaml_config = f"""
suite:
  name: SQLite Query Source

connections:
  sqlite_local:
    uri: sqlite:///{db_path}

data_sources:
  - name: users_source
    connection: sqlite_local
    query: |
      SELECT user_id, email, age
      FROM users
      WHERE age >= 30
    table_name: users_over_30

checks:
  - name: Filtered Checks
    level: error
    rules:
      - has_size:
          eq: 2
      - is_complete: email
"""

    result = await run_yaml(yaml_config, SessionContext())

    assert result.success is True
