from __future__ import annotations

import pytest
from qualink.checks.check import Check
from qualink.core import ValidationSuite
from qualink.core.level import Level


@pytest.mark.asyncio()
async def test_built_suite_runs_with_real_data(df_ctx) -> None:
    suite = (
        ValidationSuite.builder("Built Suite")
        .on_data(df_ctx, "users")
        .add_check(
            Check.builder("Critical").with_level(Level.ERROR).is_complete("id").is_unique("id").build()
        )
        .build()
    )

    result = await suite.run()

    assert result.success is True
    assert result.report.metrics.total_checks == 1
    assert result.report.metrics.passed == 2
