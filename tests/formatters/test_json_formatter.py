import json

from dq_tool.core.level import Level
from dq_tool.core.result import (
    CheckStatus,
    ValidationIssue,
    ValidationMetrics,
    ValidationReport,
    ValidationResult,
)
from dq_tool.formatters.base import FormatterConfig
from dq_tool.formatters.json_formatter import JsonFormatter


class TestJsonFormatter:
    def test_format_basic(self):
        report = ValidationReport(
            suite_name="Test Suite",
            metrics=ValidationMetrics(total_checks=2, total_constraints=3, passed=2, failed=1),
        )
        result = ValidationResult(success=True, status=CheckStatus.SUCCESS, report=report)

        formatter = JsonFormatter()
        output = formatter.format(result)

        data = json.loads(output)
        assert data["suite"] == "Test Suite"
        assert data["success"] is True
        assert data["metrics"]["total_checks"] == 2
        assert data["metrics"]["total_constraints"] == 3
        assert data["metrics"]["passed"] == 2
        assert data["metrics"]["failed"] == 1
        assert data["metrics"]["pass_rate"] == 0.6667  # rounded to 4 decimals

    def test_format_with_issues(self):
        issue = ValidationIssue("check1", "con1", Level.WARNING, "warning msg", 0.8)
        report = ValidationReport(suite_name="Test", metrics=ValidationMetrics(), issues=[issue])
        result = ValidationResult(success=False, status=CheckStatus.WARNING, report=report)

        formatter = JsonFormatter()
        output = formatter.format(result)

        data = json.loads(output)
        assert "issues" in data
        assert len(data["issues"]) == 1
        assert data["issues"][0]["check"] == "check1"
        assert data["issues"][0]["constraint"] == "con1"
        assert data["issues"][0]["level"] == "warning"
        assert data["issues"][0]["message"] == "warning msg"
        assert data["issues"][0]["metric"] == 0.8

    def test_format_no_issues_when_config_disables(self):
        issue = ValidationIssue("c", "con", Level.INFO, "msg")
        report = ValidationReport("Test", issues=[issue])
        result = ValidationResult(success=True, status=CheckStatus.SUCCESS, report=report)

        formatter = JsonFormatter(FormatterConfig(show_issues=False))
        output = formatter.format(result)

        data = json.loads(output)
        assert "issues" not in data
