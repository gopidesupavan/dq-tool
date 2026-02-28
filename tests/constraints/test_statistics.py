from unittest.mock import MagicMock

import pytest
from datafusion import DataFrame, SessionContext
from qualink.constraints.assertion import Assertion
from qualink.constraints.statistics import StatisticalConstraint, StatisticType
from qualink.core.constraint import ConstraintMetadata, ConstraintStatus


class TestStatisticalConstraint:
    def test_init(self) -> None:
        assertion = Assertion.greater_than(10.0)
        c = StatisticalConstraint("col", StatisticType.MEAN, assertion)
        assert c._column == "col"
        assert c._stat_type == StatisticType.MEAN
        assert c._assertion == assertion

    def test_name(self) -> None:
        assertion = Assertion.greater_than(10.0)
        c = StatisticalConstraint("test_col", StatisticType.MAX, assertion)
        assert c.name() == "MAX(test_col)"

    def test_metadata(self) -> None:
        assertion = Assertion.greater_than(10.0)
        c = StatisticalConstraint("col", StatisticType.SUM, assertion)
        meta = c.metadata()
        assert isinstance(meta, ConstraintMetadata)
        assert meta.name == "SUM(col)"
        assert "SUM of 'col' must satisfy > 10.0" in meta.description
        assert meta.column == "col"

    @pytest.mark.parametrize(
        "stat_type",
        [StatisticType.MIN, StatisticType.MAX, StatisticType.MEAN, StatisticType.SUM, StatisticType.STDDEV],
    )
    @pytest.mark.asyncio()
    async def test_evaluate_success(self, stat_type) -> None:
        mock_df = MagicMock(spec=DataFrame)
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 15.0
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock(spec=SessionContext)
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.greater_than(10.0)
        c = StatisticalConstraint("col", stat_type, assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 15.0
        assert result.message == ""

    @pytest.mark.asyncio()
    async def test_evaluate_failure(self) -> None:
        mock_df = MagicMock(spec=DataFrame)
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 5.0
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock(spec=SessionContext)
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.greater_than(10.0)
        c = StatisticalConstraint("col", StatisticType.MEAN, assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 5.0
        assert "MEAN('col') = 5.0, expected > 10.0" in result.message

    @pytest.mark.asyncio()
    async def test_evaluate_null_result(self) -> None:
        mock_df = MagicMock(spec=DataFrame)
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = None
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock(spec=SessionContext)
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.greater_than(10.0)
        c = StatisticalConstraint("col", StatisticType.MIN, assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric is None
