from unittest.mock import MagicMock

import pytest
from datafusion import DataFrame, SessionContext
from qualink.comparison.row_count_match import RowCountMatch, RowCountMatchResult


class TestRowCountMatchResult:
    def test_creation(self):
        result = RowCountMatchResult(count_a=10, count_b=10, ratio=1.0)
        assert result.count_a == 10
        assert result.count_b == 10
        assert result.ratio == 1.0

    def test_is_match_true(self):
        result = RowCountMatchResult(count_a=10, count_b=10, ratio=1.0)
        assert result.is_match is True

    def test_is_match_false(self):
        result = RowCountMatchResult(count_a=10, count_b=8, ratio=0.8)
        assert result.is_match is False


class TestRowCountMatch:
    def test_creation(self):
        rcm = RowCountMatch("table_a", "table_b")
        assert rcm._table_a == "table_a"
        assert rcm._table_b == "table_b"

    @pytest.mark.asyncio()
    async def test_run_equal_counts(self):
        mock_ctx = MagicMock(spec=SessionContext)
        mock_result_a = MagicMock(spec=DataFrame)
        mock_result_a.collect.return_value = [MagicMock()]
        mock_result_a.collect.return_value[0].column.return_value = [MagicMock()]
        mock_result_a.collect.return_value[0].column.return_value[0].as_py.return_value = 100

        mock_result_b = MagicMock(spec=DataFrame)
        mock_result_b.collect.return_value = [MagicMock()]
        mock_result_b.collect.return_value[0].column.return_value = [MagicMock()]
        mock_result_b.collect.return_value[0].column.return_value[0].as_py.return_value = 100

        mock_ctx.sql.side_effect = [mock_result_a, mock_result_b]

        rcm = RowCountMatch("table_a", "table_b")
        result = await rcm.run(mock_ctx)

        assert result.count_a == 100
        assert result.count_b == 100
        assert result.ratio == 1.0

    @pytest.mark.asyncio()
    async def test_run_different_counts(self):
        mock_ctx = MagicMock(spec=SessionContext)
        mock_result_a = MagicMock(spec=DataFrame)
        mock_result_a.collect.return_value = [MagicMock()]
        mock_result_a.collect.return_value[0].column.return_value = [MagicMock()]
        mock_result_a.collect.return_value[0].column.return_value[0].as_py.return_value = 10

        mock_result_b = MagicMock(spec=DataFrame)
        mock_result_b.collect.return_value = [MagicMock()]
        mock_result_b.collect.return_value[0].column.return_value = [MagicMock()]
        mock_result_b.collect.return_value[0].column.return_value[0].as_py.return_value = 8

        mock_ctx.sql.side_effect = [mock_result_a, mock_result_b]

        rcm = RowCountMatch("table_a", "table_b")
        result = await rcm.run(mock_ctx)

        assert result.count_a == 10
        assert result.count_b == 8
        assert result.ratio == 0.8  # min(10,8)/max(10,8)

    @pytest.mark.asyncio()
    async def test_run_zero_counts(self):
        mock_ctx = MagicMock(spec=SessionContext)
        mock_result_a = MagicMock(spec=DataFrame)
        mock_result_a.collect.return_value = [MagicMock()]
        mock_result_a.collect.return_value[0].column.return_value = [MagicMock()]
        mock_result_a.collect.return_value[0].column.return_value[0].as_py.return_value = 0

        mock_result_b = MagicMock(spec=DataFrame)
        mock_result_b.collect.return_value = [MagicMock()]
        mock_result_b.collect.return_value[0].column.return_value = [MagicMock()]
        mock_result_b.collect.return_value[0].column.return_value[0].as_py.return_value = 0

        mock_ctx.sql.side_effect = [mock_result_a, mock_result_b]

        rcm = RowCountMatch("table_a", "table_b")
        result = await rcm.run(mock_ctx)

        assert result.count_a == 0
        assert result.count_b == 0
        assert result.ratio == 0.0
