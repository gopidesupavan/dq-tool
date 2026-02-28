from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from datafusion import SessionContext
from qualink.checks.check import CheckBuilder
from qualink.config.builder import _apply_rule, _build_check, build_suite_from_yaml, run_yaml
from qualink.core.level import Level
from qualink.core.result import ValidationResult
from qualink.core.suite import ValidationSuiteBuilder


class TestBuildSuiteFromYaml:
    @patch("qualink.config.builder.SessionContext")
    def test_build_suite_from_yaml(self, mock_ctx_class):
        mock_ctx = MagicMock(spec=SessionContext)
        mock_ctx_class.return_value = mock_ctx

        yaml_content = """
suite:
  name: Test Suite
data_source:
  type: csv
  path: test.csv
checks:
  - name: Test Check
    level: error
    rules:
      - is_complete: id
"""
        with patch("qualink.config.builder.load_yaml") as mock_load:
            mock_load.return_value = {
                "suite": {"name": "Test Suite"},
                "data_source": {"type": "csv", "path": "test.csv"},
                "checks": [{"name": "Test Check", "level": "error", "rules": [{"is_complete": "id"}]}],
            }
            builder = build_suite_from_yaml(yaml_content, None)  # Pass None to trigger registration
            assert builder is not None
            # Check that register_csv was called
            mock_ctx.register_csv.assert_called_with("data", "test.csv")


class TestRunYaml:
    @pytest.mark.asyncio()
    @patch("qualink.config.builder.build_suite_from_yaml")
    async def test_run_yaml(self, mock_build):
        mock_builder = MagicMock(spec=ValidationSuiteBuilder)
        mock_build.return_value = mock_builder
        mock_result = MagicMock(spec=ValidationResult)
        mock_builder.run = AsyncMock(return_value=mock_result)

        result = await run_yaml("yaml_string")
        mock_build.assert_called_with("yaml_string", None)
        mock_builder.run.assert_called_once()
        assert result == mock_result


class TestBuildCheck:
    def test_build_check(self):
        check_cfg = {
            "name": "Test Check",
            "level": "warning",
            "description": "Test desc",
            "rules": [{"is_complete": "col"}],
        }
        check = _build_check(check_cfg)
        assert check.name == "Test Check"
        assert check.level == Level.WARNING
        assert check.description == "Test desc"
        assert len(check.constraints) == 1


class TestApplyRule:
    """Tests for _apply_rule which now delegates directly to the constraint registry."""

    def test_apply_rule_column_only(self):
        from qualink.constraints.completeness import CompletenessConstraint

        cb = MagicMock(spec=CheckBuilder)
        rule = {"is_complete": "col"}
        _apply_rule(cb, rule)
        cb.add_constraint.assert_called_once()
        constraint = cb.add_constraint.call_args[0][0]
        assert isinstance(constraint, CompletenessConstraint)

    def test_apply_rule_column_assertion(self):
        from qualink.constraints.completeness import CompletenessConstraint

        cb = MagicMock(spec=CheckBuilder)
        rule = {"has_completeness": {"column": "col", "gte": 0.9}}
        _apply_rule(cb, rule)
        cb.add_constraint.assert_called_once()
        constraint = cb.add_constraint.call_args[0][0]
        assert isinstance(constraint, CompletenessConstraint)

    def test_apply_rule_columns_varargs(self):
        from qualink.constraints.uniqueness import UniquenessConstraint

        cb = MagicMock(spec=CheckBuilder)
        rule = {"is_unique": ["col1", "col2"]}
        _apply_rule(cb, rule)
        cb.add_constraint.assert_called_once()
        constraint = cb.add_constraint.call_args[0][0]
        assert isinstance(constraint, UniquenessConstraint)

    def test_apply_rule_assertion_only(self):
        from qualink.constraints.size import SizeConstraint

        cb = MagicMock(spec=CheckBuilder)
        rule = {"has_size": {"gt": 0}}
        _apply_rule(cb, rule)
        cb.add_constraint.assert_called_once()
        constraint = cb.add_constraint.call_args[0][0]
        assert isinstance(constraint, SizeConstraint)

    def test_apply_rule_two_column_assertion(self):
        from qualink.constraints.correlation import CorrelationConstraint

        cb = MagicMock(spec=CheckBuilder)
        rule = {"has_correlation": {"column_a": "col1", "column_b": "col2", "gt": 0.5}}
        _apply_rule(cb, rule)
        cb.add_constraint.assert_called_once()
        constraint = cb.add_constraint.call_args[0][0]
        assert isinstance(constraint, CorrelationConstraint)

    def test_apply_rule_has_pattern(self):
        from qualink.constraints.pattern_match import PatternMatchConstraint

        cb = MagicMock(spec=CheckBuilder)
        rule = {"has_pattern": {"column": "col", "pattern": "@", "eq": 1.0}}
        _apply_rule(cb, rule)
        cb.add_constraint.assert_called_once()
        constraint = cb.add_constraint.call_args[0][0]
        assert isinstance(constraint, PatternMatchConstraint)

    def test_apply_rule_has_approx_quantile(self):
        from qualink.constraints.approx_quantile import ApproxQuantileConstraint

        cb = MagicMock(spec=CheckBuilder)
        rule = {"has_approx_quantile": {"column": "col", "quantile": 0.5, "gt": 10}}
        _apply_rule(cb, rule)
        cb.add_constraint.assert_called_once()
        constraint = cb.add_constraint.call_args[0][0]
        assert isinstance(constraint, ApproxQuantileConstraint)

    def test_apply_rule_satisfies(self):
        from qualink.constraints.compliance import ComplianceConstraint

        cb = MagicMock(spec=CheckBuilder)
        rule = {"satisfies": {"predicate": "col > 0", "gt": 0}}
        _apply_rule(cb, rule)
        cb.add_constraint.assert_called_once()
        constraint = cb.add_constraint.call_args[0][0]
        assert isinstance(constraint, ComplianceConstraint)

    def test_apply_rule_custom_sql(self):
        from qualink.constraints.custom_sql import CustomSqlConstraint

        cb = MagicMock(spec=CheckBuilder)
        rule = {"custom_sql": "SELECT * FROM table"}
        _apply_rule(cb, rule)
        cb.add_constraint.assert_called_once()
        constraint = cb.add_constraint.call_args[0][0]
        assert isinstance(constraint, CustomSqlConstraint)

    def test_apply_rule_unknown(self):
        cb = MagicMock(spec=CheckBuilder)
        rule = {"unknown_rule": "value"}
        with pytest.raises(ValueError, match="Unknown constraint type"):
            _apply_rule(cb, rule)

    def test_apply_rule_schema_match(self):
        from qualink.constraints.schema_match import SchemaMatchConstraint

        cb = MagicMock(spec=CheckBuilder)
        rule = {"schema_match": {"table_a": "a", "table_b": "b", "eq": 1.0}}
        _apply_rule(cb, rule)
        cb.add_constraint.assert_called_once()
        constraint = cb.add_constraint.call_args[0][0]
        assert isinstance(constraint, SchemaMatchConstraint)
