from unittest.mock import AsyncMock, MagicMock

import pytest
from dq_tool.checks.check import CheckResult
from dq_tool.core.constraint import ConstraintResult, ConstraintStatus
from dq_tool.core.level import Level
from dq_tool.core.result import CheckStatus
from dq_tool.core.suite import ValidationSuite, ValidationSuiteBuilder


class TestValidationSuite:
    def test_creation(self):
        suite = ValidationSuite("test")
        assert suite._name == "test"

    def test_builder_static_method(self):
        builder = ValidationSuite.builder("test")
        assert isinstance(builder, ValidationSuiteBuilder)
        assert builder._name == "test"

    def test_on_data(self):
        suite = ValidationSuite("test")
        mock_ctx = MagicMock()
        builder = suite.on_data(mock_ctx, "table")
        assert isinstance(builder, ValidationSuiteBuilder)
        assert builder._ctx == mock_ctx
        assert builder._table_name == "table"


class TestValidationSuiteBuilder:
    def test_creation(self):
        builder = ValidationSuiteBuilder("test")
        assert builder._name == "test"
        assert builder._description is None
        assert builder._ctx is None
        assert builder._table_name == "data"
        assert builder._checks == []

    def test_description(self):
        builder = ValidationSuiteBuilder("test").description("desc")
        assert builder._description == "desc"

    def test_with_name(self):
        builder = ValidationSuiteBuilder("test").with_name("new_name")
        assert builder._name == "new_name"

    def test_table_name(self):
        builder = ValidationSuiteBuilder("test").table_name("new_table")
        assert builder._table_name == "new_table"

    def test_on_data(self):
        mock_ctx = MagicMock()
        builder = ValidationSuiteBuilder("test").on_data(mock_ctx, "table")
        assert builder._ctx == mock_ctx
        assert builder._table_name == "table"

    def test_add_check(self):
        mock_check = MagicMock()
        builder = ValidationSuiteBuilder("test").add_check(mock_check)
        assert builder._checks == [mock_check]

    def test_add_checks(self):
        mock_checks = [MagicMock(), MagicMock()]
        builder = ValidationSuiteBuilder("test").add_checks(mock_checks)
        assert builder._checks == mock_checks

    @pytest.mark.asyncio()
    async def test_run_without_ctx_raises(self):
        builder = ValidationSuiteBuilder("test")
        with pytest.raises(RuntimeError, match="No data context set"):
            await builder.run()

    @pytest.mark.asyncio()
    async def test_run_success(self):
        # Mock check
        mock_check = MagicMock()
        mock_check.name = "test_check"
        mock_check.level = Level.INFO
        mock_result = CheckResult(
            check=mock_check,
            status=CheckStatus.SUCCESS,
            constraint_results=[ConstraintResult(status=ConstraintStatus.SUCCESS, constraint_name="con1")],
        )
        mock_check.run = AsyncMock(return_value=mock_result)

        mock_ctx = MagicMock()
        builder = ValidationSuiteBuilder("test").on_data(mock_ctx, "table").add_check(mock_check)

        result = await builder.run()

        assert result.success is True
        assert result.status == CheckStatus.SUCCESS
        assert result.report.suite_name == "test"
        assert result.report.metrics.total_checks == 1
        assert result.report.metrics.passed == 1

    @pytest.mark.asyncio()
    async def test_run_with_failure(self):
        # Mock check with failure
        mock_check = MagicMock()
        mock_check.name = "test_check"
        mock_check.level = Level.ERROR
        mock_result = CheckResult(
            check=mock_check,
            status=CheckStatus.ERROR,
            constraint_results=[
                ConstraintResult(status=ConstraintStatus.FAILURE, constraint_name="con1", message="fail")
            ],
        )
        mock_check.run = AsyncMock(return_value=mock_result)

        mock_ctx = MagicMock()
        builder = ValidationSuiteBuilder("test").on_data(mock_ctx, "table").add_check(mock_check)

        result = await builder.run()

        assert result.success is False
        assert result.status == CheckStatus.ERROR
        assert len(result.report.issues) == 1

    def test_build(self):
        builder = ValidationSuiteBuilder("test")
        suite = builder.build()
        assert isinstance(suite, ValidationSuite)
        assert suite._name == "test"
