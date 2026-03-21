from __future__ import annotations

import json
from typing import Any

from qualink.secrets.backends import SecretBackendError, get_secret_backend

_MISSING = object()


class SecretResolver:
    """Resolves inline `from:` secret refs inside connection option dictionaries."""

    _SECRET_REF_KEYS = frozenset({"field", "from", "key", "required"})

    def __init__(self) -> None:
        self._cache: dict[tuple[str, str, str | None, tuple[tuple[str, str], ...]], str | object] = {}

    def resolve_options(self, options: dict[str, Any]) -> dict[str, Any]:
        resolved: dict[str, Any] = {}
        for key, value in options.items():
            resolved_value = self._resolve_value(value)
            if resolved_value is not _MISSING:
                resolved[key] = resolved_value
        return resolved

    def _resolve_value(self, value: Any) -> Any:
        if self._is_secret_ref(value):
            return self._resolve_secret_ref(value)
        if isinstance(value, dict):
            resolved: dict[str, Any] = {}
            for key, nested_value in value.items():
                result = self._resolve_value(nested_value)
                if result is not _MISSING:
                    resolved[key] = result
            return resolved
        if isinstance(value, list):
            resolved_items = []
            for item in value:
                result = self._resolve_value(item)
                if result is not _MISSING:
                    resolved_items.append(result)
            return resolved_items
        return value

    def _resolve_secret_ref(self, value: dict[str, Any]) -> Any:
        source = self._require_string(value, "from")
        key = self._require_string(value, "key")
        field = self._optional_string(value.get("field"))
        required = self._resolve_required_flag(value.get("required", True))
        options = {
            option_key: option_value
            for option_key, option_value in value.items()
            if option_key not in self._SECRET_REF_KEYS
        }
        cache_key = self._build_cache_key(source, key, field, options)
        if cache_key in self._cache:
            cached_value = self._cache[cache_key]
            return cached_value

        backend = get_secret_backend(source)
        secret_value = backend.fetch(key, options)
        if secret_value is None:
            if required:
                raise SecretBackendError(f"Secret source {source!r} did not return a value for key {key!r}.")
            self._cache[cache_key] = _MISSING
            return _MISSING

        resolved_value: Any = secret_value
        if field is not None:
            resolved_value = self._extract_secret_field(secret_value, field, source, key)

        self._cache[cache_key] = resolved_value
        return resolved_value

    def _is_secret_ref(self, value: Any) -> bool:
        return isinstance(value, dict) and "from" in value

    def _require_string(self, value: dict[str, Any], key: str) -> str:
        raw_value = value.get(key)
        if raw_value is None or str(raw_value).strip() == "":
            raise SecretBackendError(f"Inline secret refs must define {key!r}.")
        return str(raw_value)

    def _optional_string(self, value: Any) -> str | None:
        if value is None:
            return None
        return str(value)

    def _resolve_required_flag(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() not in {"0", "false", "no"}
        return bool(value)

    def _extract_secret_field(self, secret_value: str, field: str, source: str, key: str) -> str:
        try:
            parsed = json.loads(secret_value)
        except json.JSONDecodeError as exc:
            raise SecretBackendError(
                f"Secret source {source!r} returned a non-JSON value for key {key!r}, "
                f"but field {field!r} was requested."
            ) from exc

        current: Any = parsed
        for part in field.split("."):
            if not isinstance(current, dict) or part not in current:
                raise SecretBackendError(
                    f"Secret source {source!r} returned JSON for key {key!r}, "
                    f"but field {field!r} was not present."
                )
            current = current[part]

        if isinstance(current, str):
            return current
        return json.dumps(current) if isinstance(current, dict | list) else str(current)

    def _build_cache_key(
        self,
        source: str,
        key: str,
        field: str | None,
        options: dict[str, Any],
    ) -> tuple[str, str, str | None, tuple[tuple[str, str], ...]]:
        normalized_options = tuple(
            sorted((option_key, repr(option_value)) for option_key, option_value in options.items())
        )
        return (source, key, field, normalized_options)
