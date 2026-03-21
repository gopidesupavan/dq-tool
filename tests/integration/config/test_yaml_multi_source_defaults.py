from __future__ import annotations

import pytest
from qualink.config import run_yaml


@pytest.mark.asyncio()
async def test_run_yaml_with_multiple_sources_uses_source_name_as_default_table_name(sample_csv_dir) -> None:
    yaml_config = f"""
suite:
  name: Multi Source Defaults

data_sources:
  - name: users_source
    path: {sample_csv_dir / "users.csv"}
    format: csv
  - name: users_b_source
    path: {sample_csv_dir / "users_b.csv"}
    format: csv

checks:
  - name: Schema Match
    level: error
    rules:
      - schema_match:
          table_a: users_source
          table_b: users_b_source
          eq: 1.0
"""

    result = await run_yaml(yaml_config)

    assert result.success is True
