from qualink.core.level import Level
from qualink.core.result import (
    CheckStatus,
    ValidationIssue,
    ValidationMetrics,
    ValidationReport,
    ValidationResult,
)


class TestCheckStatus:
    def test_constants(self) -> None:
        assert CheckStatus.SUCCESS == "Success"
        assert CheckStatus.WARNING == "Warning"
        assert CheckStatus.ERROR == "Error"


class TestValidationIssue:
    def test_creation_minimal(self) -> None:
        issue = ValidationIssue(
            check_name="check1", constraint_name="constraint1", level=Level.ERROR, message="error msg"
        )
        assert issue.check_name == "check1"
        assert issue.constraint_name == "constraint1"
        assert issue.level == Level.ERROR
        assert issue.message == "error msg"
        assert issue.metric is None

    def test_creation_with_metric(self) -> None:
        issue = ValidationIssue(
            check_name="check1",
            constraint_name="constraint1",
            level=Level.WARNING,
            message="warning msg",
            metric=0.75,
        )
        assert issue.metric == 0.75


class TestValidationMetrics:
    def test_creation_default(self) -> None:
        metrics = ValidationMetrics()
        assert metrics.total_checks == 0
        assert metrics.total_constraints == 0
        assert metrics.passed == 0
        assert metrics.failed == 0
        assert metrics.skipped == 0
        assert metrics.error_count == 0
        assert metrics.warning_count == 0
        assert metrics.execution_time_ms == 0
        assert metrics.custom_metrics == {}
        assert metrics.pass_rate == 0.0
        assert metrics.success_rate() == 0.0

    def test_pass_rate_calculation(self) -> None:
        metrics = ValidationMetrics(passed=3, failed=1)
        assert metrics.pass_rate == 0.75

        metrics_zero = ValidationMetrics(passed=0, failed=0)
        assert metrics_zero.pass_rate == 0.0

    def test_success_rate_alias(self) -> None:
        metrics = ValidationMetrics(passed=2, failed=2)
        assert metrics.success_rate() == 0.5


class TestValidationReport:
    def test_creation(self) -> None:
        report = ValidationReport(suite_name="test_suite")
        assert report.suite_name == "test_suite"
        assert isinstance(report.metrics, ValidationMetrics)
        assert report.check_results == {}
        assert report.issues == []

    def test_add_issue(self) -> None:
        report = ValidationReport(suite_name="test")
        issue = ValidationIssue("c", "con", Level.INFO, "msg")
        report.add_issue(issue)
        assert len(report.issues) == 1
        assert report.issues[0] == issue


class TestValidationResult:
    def test_creation(self) -> None:
        report = ValidationReport("suite")
        result = ValidationResult(success=True, status=CheckStatus.SUCCESS, report=report)
        assert result.success is True
        assert result.status == CheckStatus.SUCCESS
        assert result.report == report

    def test_success_result_classmethod(self) -> None:
        report = ValidationReport("suite")
        result = ValidationResult.success_result(ValidationMetrics(), report)
        assert result.success is True
        assert result.status == CheckStatus.SUCCESS

    def test_failure_result_classmethod(self) -> None:
        report = ValidationReport("suite")
        result = ValidationResult.failure_result(report)
        assert result.success is False
        assert result.status == CheckStatus.ERROR

    def test_str_method(self) -> None:
        # Test successful result
        report = ValidationReport("suite", ValidationMetrics(total_checks=1, passed=1, execution_time_ms=42))
        result = ValidationResult(success=True, status=CheckStatus.SUCCESS, report=report)
        str_repr = str(result)
        assert "PASSED" in str_repr
        assert "suite" in str_repr
        assert "Execution time: 42 ms" in str_repr

        # Test failed result with issues
        issue = ValidationIssue("check", "con", Level.ERROR, "msg")
        report.issues.append(issue)
        result = ValidationResult(success=False, status=CheckStatus.ERROR, report=report)
        str_repr = str(result)
        assert "FAILED" in str_repr
        assert "Issues:" in str_repr
