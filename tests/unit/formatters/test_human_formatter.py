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
            suite_name="Test Suite",
            metrics=ValidationMetrics(total_checks=1, total_constraints=1, passed=1),
        )
        result = ValidationResult(success=True, status=CheckStatus.SUCCESS, report=report)

        formatter = HumanFormatter(FormatterConfig(colorize=False))
        output = formatter.format(result)

        assert "qualink" in output
        assert "Test Suite" in output
        assert "Validation result: PASS" in output
        assert "Summary" in output
        assert "Checks" in output
        assert "Passed" in output

    def test_format_failure_with_issues(self):
        issue = ValidationIssue("check1", "con1", Level.ERROR, "error message")
        report = ValidationReport(
            suite_name="Test Suite",
            metrics=ValidationMetrics(total_checks=1, failed=1, error_count=1),
            issues=[issue],
        )
        result = ValidationResult(success=False, status=CheckStatus.ERROR, report=report)

        formatter = HumanFormatter(FormatterConfig(colorize=False))
        output = formatter.format(result)

        assert "Validation result: FAIL" in output
        assert "Issues" in output
        assert "ERROR" in output
        assert "error message" in output

    def test_format_with_constraint_results(self):
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

        formatter = HumanFormatter(FormatterConfig(show_passed=True, colorize=False))
        output = formatter.format(result)

        assert "Checks" in output
        assert "check1" in output
        assert "PASS" in output
        assert "FAIL" in output
        assert "fail" in output

    def test_colorize_enabled_uses_ansi_output(self):
        report = ValidationReport(
            suite_name="Colored Suite",
            metrics=ValidationMetrics(total_checks=1, total_constraints=1, passed=1),
        )
        result = ValidationResult(success=True, status=CheckStatus.SUCCESS, report=report)

        formatter = HumanFormatter(FormatterConfig(colorize=True))
        output = formatter.format(result)

        assert "\033[" in output

    def test_colorize_disabled_uses_plain_output(self):
        report = ValidationReport(
            suite_name="Plain Suite",
            metrics=ValidationMetrics(total_checks=1, total_constraints=1, passed=1),
        )
        result = ValidationResult(success=True, status=CheckStatus.SUCCESS, report=report)

        formatter = HumanFormatter(FormatterConfig(colorize=False))
        output = formatter.format(result)

        assert "\033[" not in output

    def test_status_icon_markup(self):
        formatter = HumanFormatter()

        assert "green" in formatter._status_icon(ConstraintStatus.SUCCESS)
        assert "red" in formatter._status_icon(ConstraintStatus.FAILURE)
        assert "yellow" in formatter._status_icon(ConstraintStatus.SKIPPED)

    def test_format_includes_metric_values(self):
        report = ValidationReport(
            suite_name="Metric Suite",
            metrics=ValidationMetrics(total_checks=1, total_constraints=2, passed=1, failed=1),
            check_results={
                "quality": [
                    ConstraintResult(status=ConstraintStatus.SUCCESS, constraint_name="con1", metric=1.0),
                    ConstraintResult(
                        status=ConstraintStatus.FAILURE,
                        constraint_name="con2",
                        message="below threshold",
                        metric=0.5,
                    ),
                ]
            },
        )
        result = ValidationResult(success=False, status=CheckStatus.ERROR, report=report)

        formatter = HumanFormatter(FormatterConfig(colorize=False, show_passed=True))
        output = formatter.format(result)

        assert "1.0000" in output
        assert "0.5000" in output

    def test_issue_message_includes_description_and_extra(self):
        formatter = HumanFormatter()
        issue = ValidationIssue(
            "check1",
            "con1",
            Level.ERROR,
            "bad value",
            description="violates rule",
            metadata_extra={"column_type": "string"},
        )

        output = formatter._issue_message(issue)

        assert "bad value" in output
        assert "violates rule" in output
        assert "column_type=string" in output
