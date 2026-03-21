from __future__ import annotations

import base64
import os
from abc import ABC, abstractmethod
from importlib import import_module
from typing import Any


class SecretBackendError(ValueError):
    """Raised when a secret reference cannot be resolved."""


class SecretBackend(ABC):
    """Backend contract for resolving inline secret references."""

    @abstractmethod
    def fetch(self, key: str, options: dict[str, Any]) -> str | None:
        """Return a secret value for *key* or ``None`` when absent."""


class EnvSecretBackend(SecretBackend):
    def fetch(self, key: str, options: dict[str, Any]) -> str | None:
        del options
        return os.environ.get(key)


class AwsSsmSecretBackend(SecretBackend):
    def fetch(self, key: str, options: dict[str, Any]) -> str | None:
        boto3 = _load_optional_module("boto3", "aws_ssm")
        client = boto3.client("ssm", region_name=options.get("region"))
        response = client.get_parameter(Name=key, WithDecryption=True)
        parameter = response.get("Parameter", {})
        value = parameter.get("Value")
        return None if value is None else str(value)


class AwsSecretsManagerBackend(SecretBackend):
    def fetch(self, key: str, options: dict[str, Any]) -> str | None:
        boto3 = _load_optional_module("boto3", "aws_secretsmanager")
        client = boto3.client("secretsmanager", region_name=options.get("region"))
        response = client.get_secret_value(SecretId=key)
        if "SecretString" in response:
            return str(response["SecretString"])
        secret_binary = response.get("SecretBinary")
        if secret_binary is None:
            return None
        if isinstance(secret_binary, bytes):
            return secret_binary.decode("utf-8")
        decoded = base64.b64decode(secret_binary)
        return decoded.decode("utf-8")


class GcpSecretManagerBackend(SecretBackend):
    def fetch(self, key: str, options: dict[str, Any]) -> str | None:
        secretmanager = _load_optional_module("google.cloud.secretmanager", "gcp_secret_manager")
        project_id = options.get("project_id")
        if not project_id:
            raise SecretBackendError(
                "Secret backend 'gcp_secret_manager' requires 'project_id' on the inline secret ref."
            )
        version = str(options.get("version", "latest"))
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{key}/versions/{version}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("utf-8")


_SECRET_BACKENDS: dict[str, SecretBackend] = {
    "env": EnvSecretBackend(),
    "aws_ssm": AwsSsmSecretBackend(),
    "aws_secretsmanager": AwsSecretsManagerBackend(),
    "gcp_secret_manager": GcpSecretManagerBackend(),
}


def get_secret_backend(source: str) -> SecretBackend:
    backend = _SECRET_BACKENDS.get(source)
    if backend is None:
        supported = ", ".join(sorted(_SECRET_BACKENDS))
        raise SecretBackendError(f"Unsupported secret source {source!r}. Supported sources: {supported}.")
    return backend


def _load_optional_module(module_name: str, source: str) -> Any:
    try:
        return import_module(module_name)
    except ImportError as exc:  # pragma: no cover - exercised through tests with mocks
        raise SecretBackendError(
            f"Secret source {source!r} requires optional dependency {module_name!r}."
        ) from exc
