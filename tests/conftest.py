"""Shared fixtures for constraint integration tests.

Provides real CSV sample data registered in a DataFusion SessionContext
so that every constraint can be validated without mocking.
"""

from __future__ import annotations

import csv

import pytest
from datafusion import SessionContext


@pytest.fixture()
def sample_csv_dir(tmp_path):
    """Create sample CSV files and return the directory path."""

    # --- main sample table: users ---
    users_path = tmp_path / "users.csv"
    with open(users_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "email", "age", "score", "city"])
        writer.writerows(
            [
                [1, "Alice", "alice@example.com", 30, 85.5, "New York"],
                [2, "Bob", "bob@example.com", 25, 90.0, "London"],
                [
                    3,
                    "Charlie",
                    "",
                    35,
                    78.2,
                    "Paris",
                ],  # empty email (not null in CSV — treated as empty string)
                [4, "Diana", "diana@example.com", 28, 92.1, "New York"],
                [5, "Eve", "eve@example.com", 32, 88.0, "London"],
            ]
        )

    # --- table with nulls (uses a different approach — write with explicit None via pyarrow) ---
    # For CSV-based nulls, leave the field completely blank.
    users_nulls_path = tmp_path / "users_nulls.csv"
    with open(users_nulls_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "email", "age", "score"])
        writer.writerows(
            [
                [1, "Alice", "alice@test.com", 30, 85.5],
                [2, "Bob", "", 25, 90.0],  # email blank
                [3, "Charlie", "charlie@test.com", 35, 78.2],
                [4, "", "", "", ""],  # many blanks
                [5, "Eve", "eve@test.com", 32, 88.0],
            ]
        )

    # --- orders table (for referential integrity / row count match) ---
    orders_path = tmp_path / "orders.csv"
    with open(orders_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "user_id", "amount"])
        writer.writerows(
            [
                [101, 1, 250.00],
                [102, 2, 150.00],
                [103, 1, 300.00],
                [104, 3, 175.50],
                [105, 5, 420.00],
            ]
        )

    # --- orders with orphan keys ---
    orders_orphan_path = tmp_path / "orders_orphan.csv"
    with open(orders_orphan_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "user_id", "amount"])
        writer.writerows(
            [
                [201, 1, 100.00],
                [202, 2, 200.00],
                [203, 99, 300.00],  # user_id 99 doesn't exist in users
                [204, 100, 400.00],  # user_id 100 doesn't exist
            ]
        )

    # --- duplicate data table ---
    duplicates_path = tmp_path / "duplicates.csv"
    with open(duplicates_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "category", "value"])
        writer.writerows(
            [
                [1, "A", 10],
                [2, "B", 20],
                [3, "A", 10],  # duplicate category+value
                [4, "C", 30],
                [5, "B", 20],  # duplicate category+value
            ]
        )

    # --- table with correlated columns ---
    correlated_path = tmp_path / "correlated.csv"
    with open(correlated_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["x", "y", "z"])
        writer.writerows(
            [
                [1, 2, 10],
                [2, 4, 8],
                [3, 6, 6],
                [4, 8, 4],
                [5, 10, 2],
            ]
        )

    # --- users_b: same schema as users for schema match ---
    users_b_path = tmp_path / "users_b.csv"
    with open(users_b_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "email", "age", "score", "city"])
        writer.writerows(
            [
                [10, "Frank", "frank@example.com", 40, 70.0, "Berlin"],
                [11, "Grace", "grace@example.com", 22, 95.0, "Tokyo"],
            ]
        )

    return tmp_path


@pytest.fixture()
def df_ctx(sample_csv_dir) -> SessionContext:
    """Return a DataFusion SessionContext with all sample CSV tables registered."""
    ctx = SessionContext()

    csv_tables = {
        "users": "users.csv",
        "users_nulls": "users_nulls.csv",
        "orders": "orders.csv",
        "orders_orphan": "orders_orphan.csv",
        "duplicates": "duplicates.csv",
        "correlated": "correlated.csv",
        "users_b": "users_b.csv",
    }

    for table_name, filename in csv_tables.items():
        path = str(sample_csv_dir / filename)
        ctx.register_csv(table_name, path)

    return ctx
