from unittest.mock import MagicMock

import pytest
from datafusion import DataFrame, SessionContext
from qualink.constraints.assertion import Assertion
from qualink.constraints.min_length import MinLengthConstraint
from qualink.core.constraint import ConstraintMetadata, ConstraintStatus


class TestMinLengthConstraint:
    def test_init(self) -> None:
        assertion = Assertion.greater_than_or_equal(5.0)
        c = MinLengthConstraint("col", assertion, hint="check length")
        assert c._column == "col"
        assert c._assertion == assertion
        assert c._hint == "check length"

    def test_name(self) -> None:
        assertion = Assertion.greater_than_or_equal(5.0)
        c = MinLengthConstraint("test_col", assertion)
        assert c.name() == "MinLength(test_col)"

    def test_metadata(self) -> None:
        assertion = Assertion.greater_than_or_equal(5.0)
        c = MinLengthConstraint("col", assertion)
        meta = c.metadata()
        assert isinstance(meta, ConstraintMetadata)
        assert meta.name == "MinLength(col)"
        assert meta.column == "col"

    @pytest.mark.asyncio()
    async def test_evaluate_success(self) -> None:
        mock_df = MagicMock(spec=DataFrame)
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 10.0
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock(spec=SessionContext)
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.greater_than_or_equal(5.0)
        c = MinLengthConstraint("col", assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 10.0
        assert result.message == ""
        assert result.constraint_name == "MinLength(col)"

    @pytest.mark.asyncio()
    async def test_evaluate_failure(self) -> None:
        mock_df = MagicMock(spec=DataFrame)
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 3.0
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock(spec=SessionContext)
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.greater_than_or_equal(5.0)
        c = MinLengthConstraint("col", assertion, hint="lengthen strings")
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 3.0
        assert "MinLength of 'col' is 3" in result.message
        assert "expected >= 5.0" in result.message
        assert "lengthen strings" in result.message

    @pytest.mark.asyncio()
    async def test_evaluate_all_nulls(self) -> None:
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

        assertion = Assertion.greater_than_or_equal(5.0)
        c = MinLengthConstraint("col", assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric is None
