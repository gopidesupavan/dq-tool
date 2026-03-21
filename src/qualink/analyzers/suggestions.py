from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from qualink.analyzers.profiler import ColumnProfile


class SuggestionPriority(Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass(frozen=True)
class ConstraintSuggestion:
    constraint_type: str
    column: str
    params: dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.0
    rationale: str = ""
    priority: SuggestionPriority = SuggestionPriority.MEDIUM

    def to_yaml_rule(self) -> dict[str, Any]:
        if not self.params:
            return {self.constraint_type: self.column}
        payload = dict(self.params)
        payload.setdefault("column", self.column)
        return {self.constraint_type: payload}


class ConstraintSuggestionRule(ABC):
    @abstractmethod
    def apply(self, profile: ColumnProfile) -> list[ConstraintSuggestion]:
        """Return suggestions for a single column profile."""

    @abstractmethod
    def name(self) -> str:
        """Rule name."""


class SuggestionEngine:
    def __init__(self, confidence_threshold: float = 0.5, max_suggestions_per_column: int = 10) -> None:
        self._rules: list[ConstraintSuggestionRule] = []
        self._confidence_threshold = confidence_threshold
        self._max_suggestions_per_column = max_suggestions_per_column

    def add_rule(self, rule: ConstraintSuggestionRule) -> SuggestionEngine:
        self._rules.append(rule)
        return self

    def suggest(self, profile: ColumnProfile) -> list[ConstraintSuggestion]:
        suggestions = [suggestion for rule in self._rules for suggestion in rule.apply(profile)]
        filtered = [
            suggestion for suggestion in suggestions if suggestion.confidence >= self._confidence_threshold
        ]
        filtered.sort(key=lambda item: (-item.confidence, item.priority.value))
        return filtered[: self._max_suggestions_per_column]

    def suggest_batch(self, profiles: dict[str, ColumnProfile]) -> dict[str, list[ConstraintSuggestion]]:
        return {column: self.suggest(profile) for column, profile in profiles.items()}


class CompletenessRule(ConstraintSuggestionRule):
    def __init__(self, complete_threshold: float = 0.99, monitor_threshold: float = 0.9) -> None:
        self._complete_threshold = complete_threshold
        self._monitor_threshold = monitor_threshold

    def apply(self, profile: ColumnProfile) -> list[ConstraintSuggestion]:
        if profile.completeness >= self._complete_threshold:
            return [
                ConstraintSuggestion(
                    constraint_type="is_complete",
                    column=profile.column_name,
                    confidence=0.95,
                    rationale="Column is consistently complete in the profile baseline.",
                    priority=SuggestionPriority.CRITICAL,
                )
            ]
        if profile.completeness >= self._monitor_threshold:
            threshold = round(profile.completeness, 2)
            return [
                ConstraintSuggestion(
                    constraint_type="has_completeness",
                    column=profile.column_name,
                    params={"assertion": f">= {threshold}"},
                    confidence=0.8,
                    rationale=(
                        "Column is mostly complete and can be monitored with a retained baseline threshold."
                    ),
                    priority=SuggestionPriority.HIGH,
                )
            ]
        return []

    def name(self) -> str:
        return "completeness"


class UniquenessRule(ConstraintSuggestionRule):
    def __init__(self, unique_threshold: float = 0.99, monitor_threshold: float = 0.9) -> None:
        self._unique_threshold = unique_threshold
        self._monitor_threshold = monitor_threshold

    def apply(self, profile: ColumnProfile) -> list[ConstraintSuggestion]:
        if profile.uniqueness_ratio >= self._unique_threshold:
            return [
                ConstraintSuggestion(
                    constraint_type="is_unique",
                    column=profile.column_name,
                    confidence=0.92,
                    rationale="Profile shows near-perfect uniqueness, which is usually an identifier signal.",
                    priority=SuggestionPriority.CRITICAL,
                )
            ]
        if profile.uniqueness_ratio >= self._monitor_threshold:
            threshold = round(profile.uniqueness_ratio, 2)
            return [
                ConstraintSuggestion(
                    constraint_type="has_uniqueness",
                    column=profile.column_name,
                    params={"assertion": f">= {threshold}"},
                    confidence=0.78,
                    rationale="Profile shows high uniqueness, suitable for drift monitoring.",
                    priority=SuggestionPriority.HIGH,
                )
            ]
        return []

    def name(self) -> str:
        return "uniqueness"


class RangeRule(ConstraintSuggestionRule):
    def apply(self, profile: ColumnProfile) -> list[ConstraintSuggestion]:
        if profile.mean_value is None or profile.min_value is None or profile.max_value is None:
            return []
        return [
            ConstraintSuggestion(
                constraint_type="has_min",
                column=profile.column_name,
                params={"assertion": f">= {profile.min_value}"},
                confidence=0.7,
                rationale="Observed minimum is stable enough to retain as a lower bound baseline.",
                priority=SuggestionPriority.MEDIUM,
            ),
            ConstraintSuggestion(
                constraint_type="has_max",
                column=profile.column_name,
                params={"assertion": f"<= {profile.max_value}"},
                confidence=0.7,
                rationale="Observed maximum is stable enough to retain as an upper bound baseline.",
                priority=SuggestionPriority.MEDIUM,
            ),
            ConstraintSuggestion(
                constraint_type="has_mean",
                column=profile.column_name,
                params={"assertion": f"between {profile.mean_value * 0.95} {profile.mean_value * 1.05}"},
                confidence=0.6,
                rationale="Observed mean can be monitored within a small tolerance band.",
                priority=SuggestionPriority.LOW,
            ),
        ]

    def name(self) -> str:
        return "range"


class StringPatternRule(ConstraintSuggestionRule):
    def apply(self, profile: ColumnProfile) -> list[ConstraintSuggestion]:
        suggestions: list[ConstraintSuggestion] = []
        if profile.looks_like_email:
            suggestions.append(
                ConstraintSuggestion(
                    constraint_type="contains_email",
                    column=profile.column_name,
                    confidence=0.9,
                    rationale="Sample values strongly match an email address pattern.",
                    priority=SuggestionPriority.HIGH,
                )
            )
        if profile.looks_like_url:
            suggestions.append(
                ConstraintSuggestion(
                    constraint_type="contains_url",
                    column=profile.column_name,
                    confidence=0.9,
                    rationale="Sample values strongly match a URL pattern.",
                    priority=SuggestionPriority.HIGH,
                )
            )
        if profile.min_length is not None:
            suggestions.append(
                ConstraintSuggestion(
                    constraint_type="has_min_length",
                    column=profile.column_name,
                    params={"assertion": f">= {profile.min_length}"},
                    confidence=0.55,
                    rationale="Minimum observed string length can be retained as a loose lower bound.",
                    priority=SuggestionPriority.LOW,
                )
            )
        if profile.max_length is not None:
            suggestions.append(
                ConstraintSuggestion(
                    constraint_type="has_max_length",
                    column=profile.column_name,
                    params={"assertion": f"<= {profile.max_length}"},
                    confidence=0.55,
                    rationale="Maximum observed string length can be retained as a loose upper bound.",
                    priority=SuggestionPriority.LOW,
                )
            )
        return suggestions

    def name(self) -> str:
        return "string_patterns"
