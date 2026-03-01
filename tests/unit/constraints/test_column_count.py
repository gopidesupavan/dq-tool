from unittest.mock import MagicMock

import pytest
from datafusion import DataFrame, SessionContext
from qualink.constraints.assertion import Assertion
from qualink.constraints.column_count import ColumnCountConstraint
from qualink.core.constraint import ConstraintMetadata, ConstraintStatus


class TestColumnCountConstraint:
    def test_init(self) -> None:
        assertion = Assertion.equal_to(5.0)
        c = ColumnCountConstraint(assertion)
        assert c._assertion == assertion

    def test_name(self) -> None:
        assertion = Assertion.greater_than(3.0)
        c = ColumnCountConstraint(assertion)
        assert c.name() == f"ColumnCount({assertion})"

    def test_metadata(self) -> None:
        assertion = Assertion.equal_to(10.0)
        c = ColumnCountConstraint(assertion)
        meta = c.metadata()
        assert isinstance(meta, ConstraintMetadata)
        assert meta.name == f"ColumnCount({assertion})"
        assert meta.description == f"Column count must satisfy {assertion}"

    @pytest.mark.asyncio()
    async def test_evaluate_success(self) -> None:
        mock_schema = MagicMock()
        mock_schema.__len__ = MagicMock(return_value=5)
        mock_df = MagicMock(spec=DataFrame)
        mock_df.schema.return_value = mock_schema

        mock_ctx = MagicMock(spec=SessionContext)
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.equal_to(5.0)
        c = ColumnCountConstraint(assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 5.0
        assert result.message == ""
        assert result.constraint_name == f"ColumnCount({assertion})"

    @pytest.mark.asyncio()
    async def test_evaluate_failure(self) -> None:
        mock_schema = MagicMock()
        mock_schema.__len__ = MagicMock(return_value=3)
        mock_df = MagicMock(spec=DataFrame)
        mock_df.schema.return_value = mock_schema

        mock_ctx = MagicMock(spec=SessionContext)
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.greater_than(4.0)
        c = ColumnCountConstraint(assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 3.0
