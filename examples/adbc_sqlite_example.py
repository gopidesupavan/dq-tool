from __future__ import annotations

import asyncio
import sqlite3
import tempfile
from pathlib import Path

from qualink.config import run_yaml
from qualink.formatters import FormatterConfig, HumanFormatter


def _create_sqlite_db(path: Path) -> None:
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


async def main() -> None:
    try:
        __import__("adbc_driver_sqlite.dbapi")
    except ImportError as exc:
        raise SystemExit("Install the ADBC SQLite driver first: uv sync --group adbc") from exc

    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "users.db"
        _create_sqlite_db(db_path)

        yaml_config = f"""
suite:
  name: SQLite ADBC Example

connections:
  sqlite_local:
    uri: sqlite:///{db_path}

data_sources:
  - name: users_source
    connection: sqlite_local
    table: users
    table_name: users

checks:
  - name: User Checks
    level: error
    rules:
      - has_size:
          eq: 3
      - is_complete: email
      - is_unique: user_id
"""

        result = await run_yaml(yaml_config)
        print(HumanFormatter(FormatterConfig(show_passed=True)).format(result))


if __name__ == "__main__":
    asyncio.run(main())
