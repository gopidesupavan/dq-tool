from unittest.mock import AsyncMock, MagicMock

import pytest
from dq_tool.checks.check import Check, CheckBuilder, CheckResult
from dq_tool.core.constraint import ConstraintResult, ConstraintStatus
from dq_tool.core.level import Level
from dq_tool.core.result import CheckStatus


class TestCheckResult:
    def test_creation(self):
        mock_check = MagicMock()
        results = [ConstraintResult(status=ConstraintStatus.SUCCESS)]
        cr = CheckResult(check=mock_check, status=CheckStatus.SUCCESS, constraint_results=results)
        assert cr.check == mock_check
        assert cr.status == CheckStatus.SUCCESS
        assert cr.constraint_results == results


class TestCheck:
    def test_creation(self):
        constraints = [MagicMock()]
        check = Check(_name="test", _level=Level.ERROR, _description="desc", _constraints=constraints)
        assert check.name == "test"
        assert check.level == Level.ERROR
        assert check.description == "desc"
        assert check.constraints == constraints

    def test_builder_static_method(self):
        builder = Check.builder("test")
        assert isinstance(builder, CheckBuilder)
        assert builder._name == "test"

    @pytest.mark.asyncio()
    async def test_run_success(self):
        mock_constraint = MagicMock()
        mock_constraint.evaluate = AsyncMock(
            return_value=ConstraintResult(status=ConstraintStatus.SUCCESS, constraint_name="con1")
        )
        mock_constraint.name = MagicMock(return_value="con1")

        check = Check(_name="test", _level=Level.INFO, _description="", _constraints=[mock_constraint])

        mock_ctx = MagicMock()
        result = await check.run(mock_ctx, "table")

        assert result.status == CheckStatus.SUCCESS
        assert len(result.constraint_results) == 1

    @pytest.mark.asyncio()
    async def test_run_failure(self):
        mock_constraint = MagicMock()
        mock_constraint.evaluate = AsyncMock(
            return_value=ConstraintResult(status=ConstraintStatus.FAILURE, constraint_name="con1")
        )
        mock_constraint.name = MagicMock(return_value="con1")

        check = Check(_name="test", _level=Level.ERROR, _description="", _constraints=[mock_constraint])

        mock_ctx = MagicMock()
        result = await check.run(mock_ctx, "table")

        assert result.status == CheckStatus.ERROR


class TestCheckBuilder:
    def test_creation(self):
        builder = CheckBuilder("test")
        assert builder._name == "test"
        assert builder._level == Level.ERROR
        assert builder._description == ""
        assert builder._constraints == []

    def test_with_level(self):
        builder = CheckBuilder("test").with_level(Level.WARNING)
        assert builder._level == Level.WARNING

    def test_with_description(self):
        builder = CheckBuilder("test").with_description("desc")
        assert builder._description == "desc"

    def test_add_constraint(self):
        mock_con = MagicMock()
        builder = CheckBuilder("test").add_constraint(mock_con)
        assert builder._constraints == [mock_con]

    def test_is_complete(self):
        # This method is implemented, but requires CompletenessConstraint
        # For now, just check it adds a constraint
        builder = CheckBuilder("test").is_complete("col")
        assert len(builder._constraints) == 1
        # The constraint is CompletenessConstraint, but since it's not imported here, assume it's added

    def test_build(self):
        builder = CheckBuilder("test").with_level(Level.INFO).with_description("desc")
        mock_con = MagicMock()
        builder.add_constraint(mock_con)
        check = builder.build()
        assert isinstance(check, Check)
        assert check.name == "test"
        assert check.level == Level.INFO
        assert check.description == "desc"
