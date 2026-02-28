from unittest.mock import MagicMock

import pytest
from dq_tool.constraints.approx_count_distinct import ApproxCountDistinctConstraint
from dq_tool.constraints.assertion import Assertion
from dq_tool.core.constraint import ConstraintMetadata, ConstraintStatus


class TestApproxCountDistinctConstraint:
    def test_init(self) -> None:
        assertion = Assertion.greater_than(10.0)
        c = ApproxCountDistinctConstraint("col", assertion, hint="test hint")
        assert c._column == "col"
        assert c._assertion == assertion
        assert c._hint == "test hint"

    def test_name(self) -> None:
        assertion = Assertion.greater_than(5.0)
        c = ApproxCountDistinctConstraint("test_col", assertion)
        assert c.name() == "ApproxCountDistinct(test_col)"

    def test_metadata(self) -> None:
        assertion = Assertion.equal_to(100.0)
        c = ApproxCountDistinctConstraint("col", assertion)
        meta = c.metadata()
        assert isinstance(meta, ConstraintMetadata)
        assert meta.name == "ApproxCountDistinct(col)"
        assert meta.column == "col"

    @pytest.mark.asyncio()
    async def test_evaluate_success(self) -> None:
        mock_df = MagicMock()
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 15.0
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock()
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.greater_than(10.0)
        c = ApproxCountDistinctConstraint("col", assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 15.0
        assert result.message == ""
        assert result.constraint_name == "ApproxCountDistinct(col)"

    @pytest.mark.asyncio()
    async def test_evaluate_failure(self) -> None:
        mock_df = MagicMock()
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 5.0
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock()
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.greater_than(10.0)
        c = ApproxCountDistinctConstraint("col", assertion, hint="Check data quality")
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 5.0
        assert "ApproxCountDistinct of 'col' is 5" in result.message
        assert "expected > 10.0" in result.message
        assert "Check data quality" in result.message
