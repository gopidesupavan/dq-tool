from unittest.mock import MagicMock

import pytest
from dq_tool.constraints.assertion import Assertion
from dq_tool.constraints.max_length import MaxLengthConstraint
from dq_tool.core.constraint import ConstraintMetadata, ConstraintStatus


class TestMaxLengthConstraint:
    def test_init(self) -> None:
        assertion = Assertion.less_than_or_equal(100.0)
        c = MaxLengthConstraint("col", assertion, hint="check length")
        assert c._column == "col"
        assert c._assertion == assertion
        assert c._hint == "check length"

    def test_name(self) -> None:
        assertion = Assertion.less_than_or_equal(100.0)
        c = MaxLengthConstraint("test_col", assertion)
        assert c.name() == "MaxLength(test_col)"

    def test_metadata(self) -> None:
        assertion = Assertion.less_than_or_equal(100.0)
        c = MaxLengthConstraint("col", assertion)
        meta = c.metadata()
        assert isinstance(meta, ConstraintMetadata)
        assert meta.name == "MaxLength(col)"
        assert meta.column == "col"

    @pytest.mark.asyncio()
    async def test_evaluate_success(self) -> None:
        mock_df = MagicMock()
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 50.0
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock()
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.less_than_or_equal(100.0)
        c = MaxLengthConstraint("col", assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 50.0
        assert result.message == ""
        assert result.constraint_name == "MaxLength(col)"

    @pytest.mark.asyncio()
    async def test_evaluate_failure(self) -> None:
        mock_df = MagicMock()
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 150.0
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock()
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.less_than_or_equal(100.0)
        c = MaxLengthConstraint("col", assertion, hint="shorten strings")
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 150.0
        assert "MaxLength of 'col' is 150" in result.message
        assert "expected <= 100.0" in result.message
        assert "shorten strings" in result.message

    @pytest.mark.asyncio()
    async def test_evaluate_all_nulls(self) -> None:
        mock_df = MagicMock()
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = None
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock()
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.less_than_or_equal(100.0)
        c = MaxLengthConstraint("col", assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric is None
