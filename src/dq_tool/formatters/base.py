"""Abstract formatter interface and shared configuration."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dq_tool.core.result import ValidationResult


@dataclass(frozen=True)
class FormatterConfig:
    show_metrics: bool = True
    show_issues: bool = True
    show_passed: bool = False
    colorize: bool = True


class ResultFormatter(ABC):
    def __init__(self, config: FormatterConfig | None = None) -> None:
        self._config = config or FormatterConfig()

    @abstractmethod
    def format(self, result: ValidationResult) -> str: ...
