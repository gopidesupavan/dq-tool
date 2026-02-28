from dq_tool.core.constraint import ConstraintResult, ConstraintStatus
from dq_tool.core.level import Level
from dq_tool.core.result import (
    CheckStatus,
    ValidationIssue,
    ValidationMetrics,
    ValidationReport,
    ValidationResult,
)
from dq_tool.formatters.markdown_formatter import MarkdownFormatter


class TestMarkdownFormatter:
    def test_format_basic(self):
        report = ValidationReport(
            suite_name="Test Suite",
            metrics=ValidationMetrics(total_checks=1, total_constraints=2, passed=1, failed=1, skipped=0),
        )
        result = ValidationResult(success=False, status=CheckStatus.ERROR, report=report)

        formatter = MarkdownFormatter()
        output = formatter.format(result)

        assert "# Verification Report: Test Suite" in output
        assert "**Status:** FAIL" in output
        assert "| Total checks | 1 |" in output
        assert "| Passed | 1 |" in output
        assert "| Failed | 1 |" in output
        assert "| Pass rate | 50.0% |" in output

    def test_format_with_constraint_results(self):
        mock_results = [
            ConstraintResult(status=ConstraintStatus.SUCCESS, constraint_name="con1", metric=1.0),
            ConstraintResult(status=ConstraintStatus.FAILURE, constraint_name="con2", metric=0.5),
        ]
        report = ValidationReport(
            suite_name="Test", metrics=ValidationMetrics(), check_results={"check1": mock_results}
        )
        result = ValidationResult(success=False, status=CheckStatus.ERROR, report=report)

        formatter = MarkdownFormatter()
        output = formatter.format(result)

        assert "## Constraint Results" in output
        assert "| check1 | con1 | PASS | 1.0000 |" in output
        assert "| check1 | con2 | FAIL | 0.5000 |" in output

    def test_format_with_issues(self):
        issue = ValidationIssue("check1", "con1", Level.ERROR, "error msg")
        report = ValidationReport("Test", issues=[issue])
        result = ValidationResult(success=False, status=CheckStatus.ERROR, report=report)

        formatter = MarkdownFormatter()
        output = formatter.format(result)

        assert "## Issues" in output
        assert "- **[error]** check1 / con1: error msg" in output

    def test_format_no_metric(self):
        mock_results = [
            ConstraintResult(status=ConstraintStatus.SUCCESS, constraint_name="con1", metric=None)
        ]
        report = ValidationReport("Test", check_results={"check1": mock_results})
        result = ValidationResult(success=True, status=CheckStatus.SUCCESS, report=report)

        formatter = MarkdownFormatter()
        output = formatter.format(result)

        assert "| check1 | con1 | PASS | - |" in output
