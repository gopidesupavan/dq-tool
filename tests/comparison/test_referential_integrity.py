from unittest.mock import MagicMock

import pytest
from datafusion import DataFrame, SessionContext
from qualink.comparison.referential_integrity import ReferentialIntegrity, ReferentialIntegrityResult


class TestReferentialIntegrityResult:
    def test_creation(self):
        result = ReferentialIntegrityResult(match_ratio=0.8, unmatched_count=2, total_count=10)
        assert result.match_ratio == 0.8
        assert result.unmatched_count == 2
        assert result.total_count == 10

    def test_is_valid_true(self):
        result = ReferentialIntegrityResult(match_ratio=1.0, unmatched_count=0, total_count=10)
        assert result.is_valid is True

    def test_is_valid_false(self):
        result = ReferentialIntegrityResult(match_ratio=0.9, unmatched_count=1, total_count=10)
        assert result.is_valid is False


class TestReferentialIntegrity:
    def test_creation(self):
        ri = ReferentialIntegrity("child_table", "child_col", "parent_table", "parent_col")
        assert ri._child_table == "child_table"
        assert ri._child_col == "child_col"
        assert ri._parent_table == "parent_table"
        assert ri._parent_col == "parent_col"

    @pytest.mark.asyncio()
    async def test_run_full_match(self):
        # Mock ctx.sql to return total=10, unmatched=0
        mock_ctx = MagicMock(spec=SessionContext)
        mock_result_total = MagicMock(spec=DataFrame)
        mock_result_total.collect.return_value = [MagicMock()]
        mock_result_total.collect.return_value[0].column.return_value = [MagicMock()]
        mock_result_total.collect.return_value[0].column.return_value[0].as_py.return_value = 10

        mock_result_unmatched = MagicMock(spec=DataFrame)
        mock_result_unmatched.collect.return_value = [MagicMock()]
        mock_result_unmatched.collect.return_value[0].column.return_value = [MagicMock()]
        mock_result_unmatched.collect.return_value[0].column.return_value[0].as_py.return_value = 0

        mock_ctx.sql.side_effect = [mock_result_total, mock_result_unmatched]

        ri = ReferentialIntegrity("child", "child_col", "parent", "parent_col")
        result = await ri.run(mock_ctx)

        assert result.match_ratio == 1.0
        assert result.unmatched_count == 0
        assert result.total_count == 10

    @pytest.mark.asyncio()
    async def test_run_partial_match(self):
        # total=10, unmatched=3
        mock_ctx = MagicMock(spec=SessionContext)
        mock_result_total = MagicMock(spec=DataFrame)
        mock_result_total.collect.return_value = [MagicMock()]
        mock_result_total.collect.return_value[0].column.return_value = [MagicMock()]
        mock_result_total.collect.return_value[0].column.return_value[0].as_py.return_value = 10

        mock_result_unmatched = MagicMock(spec=DataFrame)
        mock_result_unmatched.collect.return_value = [MagicMock()]
        mock_result_unmatched.collect.return_value[0].column.return_value = [MagicMock()]
        mock_result_unmatched.collect.return_value[0].column.return_value[0].as_py.return_value = 3

        mock_ctx.sql.side_effect = [mock_result_total, mock_result_unmatched]

        ri = ReferentialIntegrity("child", "child_col", "parent", "parent_col")
        result = await ri.run(mock_ctx)

        assert result.match_ratio == 0.7  # (10-3)/10
        assert result.unmatched_count == 3
        assert result.total_count == 10

    @pytest.mark.asyncio()
    async def test_run_no_data(self):
        # total=0
        mock_ctx = MagicMock(spec=SessionContext)
        mock_result_total = MagicMock(spec=DataFrame)
        mock_result_total.collect.return_value = [MagicMock()]
        mock_result_total.collect.return_value[0].column.return_value = [MagicMock()]
        mock_result_total.collect.return_value[0].column.return_value[0].as_py.return_value = 0

        mock_result_unmatched = MagicMock(spec=DataFrame)
        mock_result_unmatched.collect.return_value = [MagicMock()]
        mock_result_unmatched.collect.return_value[0].column.return_value = [MagicMock()]
        mock_result_unmatched.collect.return_value[0].column.return_value[0].as_py.return_value = 0

        mock_ctx.sql.side_effect = [mock_result_total, mock_result_unmatched]

        ri = ReferentialIntegrity("child", "child_col", "parent", "parent_col")
        result = await ri.run(mock_ctx)

        assert result.match_ratio == 0.0
        assert result.unmatched_count == 0
        assert result.total_count == 0
