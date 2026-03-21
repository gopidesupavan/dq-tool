from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class OutputSpec:
    destination: str
    format: str = "human"
    show_passed: bool = False
    show_metrics: bool = True
    show_issues: bool = True
    colorize: bool = True


def normalize_output_specs(cfg: dict[str, Any]) -> list[OutputSpec]:
    raw_specs = cfg.get("outputs")
    if raw_specs is None:
        raw_single = cfg.get("output")
        raw_specs = [raw_single] if raw_single is not None else []

    normalized: list[OutputSpec] = []
    for index, raw in enumerate(raw_specs):
        if not isinstance(raw, dict):
            raise ValueError(f"Output entry at index {index} must be a mapping.")
        destination = _resolve_destination(raw, index)
        normalized.append(
            OutputSpec(
                destination=destination,
                format=str(raw.get("format", "human")).strip().lower(),
                show_passed=bool(raw.get("show_passed", False)),
                show_metrics=bool(raw.get("show_metrics", True)),
                show_issues=bool(raw.get("show_issues", True)),
                colorize=bool(raw.get("colorize", True)),
            )
        )
    return normalized


def _resolve_destination(raw: dict[str, Any], index: int) -> str:
    for key in ("destination", "path", "uri"):
        value = raw.get(key)
        if value:
            return str(value)
    raise ValueError(f"Output entry at index {index} must define destination, path, or uri.")
