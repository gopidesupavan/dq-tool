from qualink.core.constraint import ConstraintResult, ConstraintStatus
from qualink.core.level import Level
from qualink.core.result import (
    CheckStatus,
    ValidationIssue,
    ValidationMetrics,
    ValidationReport,
    ValidationResult,
)
from qualink.formatters.base import FormatterConfig
from qualink.formatters.human_formatter import HumanFormatter


class TestHumanFormatter:
    def test_format_success_no_issues(self):
        report = ValidationReport(
            suite_name="Test Suite", metrics=ValidationMetrics(total_checks=1, passed=1)
        )
        result = ValidationResult(success=True, status=CheckStatus.SUCCESS, report=report)

        formatter = HumanFormatter(FormatterConfig(colorize=False))
        output = formatter.format(result)

        assert "PASSED" in output
        assert "Test Suite" in output
        assert "Checks: 1" in output
        assert "Passed: 1" in output

    def test_format_failure_with_issues(self):
        issue = ValidationIssue("check1", "con1", Level.ERROR, "error message")
        report = ValidationReport(
            suite_name="Test Suite",
            metrics=ValidationMetrics(total_checks=1, failed=1, error_count=1),
            issues=[issue],
        )
        result = ValidationResult(success=False, status=CheckStatus.ERROR, report=report)

        formatter = HumanFormatter()
        output = formatter.format(result)

        assert "FAILED" in output
        assert "Issues:" in output
        assert "ERROR" in output
        assert "error message" in output

    def test_format_with_constraint_results(self):
        # Mock check results
        mock_check_results = {
            "check1": [
                ConstraintResult(status=ConstraintStatus.SUCCESS, constraint_name="con1"),
                ConstraintResult(status=ConstraintStatus.FAILURE, constraint_name="con2", message="fail"),
            ]
        }
        report = ValidationReport(
            suite_name="Test",
            metrics=ValidationMetrics(total_checks=1, passed=1, failed=1),
            check_results=mock_check_results,
        )
        result = ValidationResult(success=False, status=CheckStatus.ERROR, report=report)

        formatter = HumanFormatter(FormatterConfig(show_passed=True))
        output = formatter.format(result)

        assert "[PASS]" in output
        assert "[FAIL]" in output
        assert "fail" in output

    def test_colorize_enabled(self):
        formatter = HumanFormatter(FormatterConfig(colorize=True))
        colored = formatter._colour("test", formatter._GREEN)
        assert "\033[92m" in colored
        assert "\033[0m" in colored

    def test_colorize_disabled(self):
        formatter = HumanFormatter(FormatterConfig(colorize=False))
        colored = formatter._colour("test", formatter._GREEN)
        assert colored == "test"
        assert "\033[" not in colored

    def test_bold_enabled(self):
        formatter = HumanFormatter(FormatterConfig(colorize=True))
        bold = formatter._bold("test")
        assert "\033[1m" in bold

    def test_bold_disabled(self):
        formatter = HumanFormatter(FormatterConfig(colorize=False))
        bold = formatter._bold("test")
        assert bold == "test"

    def test_status_icon(self):
        formatter = HumanFormatter()
        assert "[PASS]" in formatter._status_icon(ConstraintStatus.SUCCESS)
        assert "[FAIL]" in formatter._status_icon(ConstraintStatus.FAILURE)
        assert "[SKIP]" in formatter._status_icon(ConstraintStatus.SKIPPED)
