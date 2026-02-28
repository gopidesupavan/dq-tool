from unittest.mock import MagicMock

import pytest
from datafusion import DataFrame, SessionContext
from qualink.constraints.assertion import Assertion
from qualink.constraints.compliance import ComplianceConstraint
from qualink.core.constraint import ConstraintMetadata, ConstraintStatus


class TestComplianceConstraint:
    def test_init(self) -> None:
        assertion = Assertion.greater_than(0.8)
        c = ComplianceConstraint("test compliance", "age > 18", assertion, hint="check age")
        assert c._label == "test compliance"
        assert c._predicate == "age > 18"
        assert c._assertion == assertion
        assert c._hint == "check age"

    def test_name(self) -> None:
        assertion = Assertion.greater_than(0.8)
        c = ComplianceConstraint("label", "pred", assertion)
        assert c.name() == "Compliance(label)"

    def test_metadata(self) -> None:
        assertion = Assertion.greater_than(0.8)
        c = ComplianceConstraint("label", "pred", assertion)
        meta = c.metadata()
        assert isinstance(meta, ConstraintMetadata)
        assert meta.name == "Compliance(label)"
        assert meta.description == "pred"

    @pytest.mark.asyncio()
    async def test_evaluate_success(self) -> None:
        mock_df = MagicMock(spec=DataFrame)
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 0.9
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock(spec=SessionContext)
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.greater_than(0.8)
        c = ComplianceConstraint("test", "age > 18", assertion)
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 0.9
        assert result.message == ""
        assert result.constraint_name == "Compliance(test)"

    @pytest.mark.asyncio()
    async def test_evaluate_failure(self) -> None:
        mock_df = MagicMock(spec=DataFrame)
        mock_row = MagicMock()
        mock_column = MagicMock()
        mock_value = MagicMock()
        mock_value.as_py.return_value = 0.5
        mock_column.__getitem__.return_value = mock_value
        mock_row.column.return_value = mock_column
        mock_df.collect.return_value = [mock_row]

        mock_ctx = MagicMock(spec=SessionContext)
        mock_ctx.sql.return_value = mock_df

        assertion = Assertion.greater_than(0.8)
        c = ComplianceConstraint("test", "age > 18", assertion, hint="fix data")
        result = await c.evaluate(mock_ctx, "table")

        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 0.5
        assert "Compliance 'test' is 0.5000" in result.message
        assert "expected > 0.8" in result.message
