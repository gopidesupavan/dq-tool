from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SourceCapabilities:
    native_to_datafusion: bool
    supports_predicate_pushdown: bool = False
    supports_projection_pushdown: bool = False
    supports_streaming: bool = False
    supports_parallel_scan: bool = False
    materialization_required: bool = False


@dataclass(frozen=True)
class PreparedSource:
    table_name: str
    capabilities: SourceCapabilities
    metadata: dict[str, str] = field(default_factory=dict)
