from qualink.core.constraint import ConstraintResult, ConstraintStatus
from qualink.core.level import Level
from qualink.core.result import (
    CheckStatus,
    ValidationIssue,
    ValidationMetrics,
    ValidationReport,
    ValidationResult,
)
from qualink.formatters.markdown_formatter import MarkdownFormatter


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
        assert "Total checks" in output
        assert "Passed" in output
        assert "Failed" in output
        assert "50.0%" in output

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
        assert "con1" in output
        assert "con2" in output
        assert "PASS" in output
        assert "FAIL" in output

    def test_format_with_issues(self):
        issue = ValidationIssue("check1", "con1", Level.ERROR, "error msg")
        report = ValidationReport("Test", issues=[issue])
        result = ValidationResult(success=False, status=CheckStatus.ERROR, report=report)

        formatter = MarkdownFormatter()
        output = formatter.format(result)

        assert "## Issues" in output
        assert "**error**" in output
        assert "check1" in output
        assert "con1" in output
        assert "error msg" in output

    def test_format_no_metric(self):
        mock_results = [
            ConstraintResult(status=ConstraintStatus.SUCCESS, constraint_name="con1", metric=None)
        ]
        report = ValidationReport("Test", check_results={"check1": mock_results})
        result = ValidationResult(success=True, status=CheckStatus.SUCCESS, report=report)

        formatter = MarkdownFormatter()
        output = formatter.format(result)

        assert "con1" in output
        assert "PASS" in output
