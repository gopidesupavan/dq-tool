from unittest.mock import MagicMock

import pytest
from datafusion import DataFrame, SessionContext
from qualink.constraints.approx_quantile import ApproxQuantileConstraint
from qualink.constraints.assertion import Assertion
from qualink.core.constraint import ConstraintMetadata, ConstraintStatus


class TestApproxQuantileConstraint:
    def test_init_valid(self) -> None:
        assertion = Assertion.greater_than(0.5)
        c = ApproxQuantileConstraint("col", 0.5, assertion, hint="test hint")
        assert c._column == "col"
        assert c._quantile == 0.5
        assert c._assertion == assertion
        assert c._hint == "test hint"

    def test_init_invalid_quantile_low(self) -> None:
        assertion = Assertion.greater_than(0.5)
        with pytest.raises(ValueError, match=r"quantile must be in \[0, 1\], got -0.1"):
            ApproxQuantileConstraint("col", -0.1, assertion)

    def test_init_invalid_quantile_high(self) -> None:
        assertion = Assertion.greater_than(0.5)
        with pytest.raises(ValueError, match=r"quantile must be in \[0, 1\], got 1.5"):
            ApproxQuantileConstraint("col", 1.5, assertion)

    def test_name(self) -> None:
        assertion = Assertion.greater_than(0.5)
        c = ApproxQuantileConstraint("test_col", 0.75, assertion)
        assert c.name() == "ApproxQuantile(test_col, 0.75)"

    def test_metadata(self) -> None:
        assertion = Assertion.equal_to(0.8)
        c = ApproxQuantileConstraint("col", 0.5, assertion)
        meta = c.metadata()
        assert isinstance(meta, ConstraintMetadata)
        assert meta.name == "ApproxQuantile(col, 0.5)"
        assert meta.column == "col"

    @pytest.mark.asyncio()
    async def test_evaluate_success(self) -> None:
        mock_df = MagicMock(spec=DataFrame)
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 0.6
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock(spec=SessionContext)
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.greater_than(0.5)
        c = ApproxQuantileConstraint("col", 0.5, assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 0.6
        assert result.message == ""
        assert result.constraint_name == "ApproxQuantile(col, 0.5)"

    @pytest.mark.asyncio()
    async def test_evaluate_failure(self) -> None:
        mock_df = MagicMock(spec=DataFrame)
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 0.3
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock(spec=SessionContext)
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.greater_than(0.5)
        c = ApproxQuantileConstraint("col", 0.5, assertion, hint="Check distribution")
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 0.3
        assert "ApproxQuantile(0.5) of 'col' is 0.3000" in result.message
        assert "expected > 0.5" in result.message
        assert "Check distribution" in result.message

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

        assertion = Assertion.greater_than(0.5)
        c = ApproxQuantileConstraint("col", 0.5, assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric is None
        assert "Column 'col' produced NULL for quantile 0.5" in result.message
