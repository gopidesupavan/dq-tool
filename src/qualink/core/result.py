from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qualink.core.constraint import ConstraintResult
    from qualink.core.level import Level


class CheckStatus:
    """Outcome status after evaluating a check group."""

    SUCCESS = "Success"
    WARNING = "Warning"
    ERROR = "Error"


@dataclass
class ValidationIssue:
    """A single issue found during validation."""

    check_name: str
    constraint_name: str
    level: Level
    message: str
    metric: float | None = None


@dataclass
class ValidationMetrics:
    """Aggregate metrics for a validation run."""

    total_checks: int = 0
    total_constraints: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    error_count: int = 0
    warning_count: int = 0
    execution_time_ms: int = 0
    custom_metrics: dict[str, float] = field(default_factory=dict)

    @property
    def pass_rate(self) -> float:
        total = self.passed + self.failed
        return self.passed / total if total > 0 else 0.0

    def success_rate(self) -> float:
        return self.pass_rate


@dataclass
class ValidationReport:
    """Detailed report produced by a validation run."""

    suite_name: str
    metrics: ValidationMetrics = field(default_factory=ValidationMetrics)
    check_results: dict[str, list[ConstraintResult]] = field(default_factory=dict)
    issues: list[ValidationIssue] = field(default_factory=list)

    def add_issue(self, issue: ValidationIssue) -> None:
        self.issues.append(issue)


@dataclass
class ValidationResult:
    """Top-level outcome of running a ValidationSuite."""

    success: bool
    status: str  # one of CheckStatus constants
    report: ValidationReport

    @classmethod
    def success_result(cls, metrics: ValidationMetrics, report: ValidationReport) -> ValidationResult:
        return cls(success=True, status=CheckStatus.SUCCESS, report=report)

    @classmethod
    def failure_result(cls, report: ValidationReport) -> ValidationResult:
        return cls(success=False, status=CheckStatus.ERROR, report=report)

    def __str__(self) -> str:
        label = "PASSED" if self.success else "FAILED"
        m = self.report.metrics
        lines = [
            f"Validation {label}: {self.report.suite_name}",
            f"  Checks: {m.total_checks} | Constraints: {m.total_constraints}",
            f"  Passed: {m.passed} | Failed: {m.failed} | Skipped: {m.skipped}",
        ]
        if self.report.issues:
            lines.append("  Issues:")
            for issue in self.report.issues:
                lines.append(
                    f"    [{issue.level}] {issue.check_name} / {issue.constraint_name}: {issue.message}"
                )
        return "\n".join(lines)
