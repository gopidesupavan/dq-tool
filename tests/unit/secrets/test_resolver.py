from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import create_autospec, patch

import pytest
from qualink.secrets import SecretBackendError, SecretResolver


class SsmClient:
    def get_parameter(self, *, Name: str, WithDecryption: bool) -> dict[str, dict[str, str]]:
        raise NotImplementedError


class SecretsManagerClient:
    def get_secret_value(self, *, SecretId: str) -> dict[str, str | bytes]:
        raise NotImplementedError


class Boto3Module:
    def client(self, service_name: str, *, region_name: str | None = None) -> object:
        raise NotImplementedError


class GcpSecretManagerClient:
    def access_secret_version(self, *, request: dict[str, str]) -> object:
        raise NotImplementedError


def test_resolve_env_secret() -> None:
    resolver = SecretResolver()

    with patch.dict(os.environ, {"POSTGRES_URI": "postgresql://example"}, clear=True):
        resolved = resolver.resolve_options({"uri": {"from": "env", "key": "POSTGRES_URI"}})

    assert resolved == {"uri": "postgresql://example"}


def test_optional_missing_env_secret_is_omitted() -> None:
    resolver = SecretResolver()

    with patch.dict(os.environ, {}, clear=True):
        resolved = resolver.resolve_options(
            {"endpoint": {"from": "env", "key": "AWS_ENDPOINT_URL", "required": False}}
        )

    assert resolved == {}


def test_missing_required_env_secret_raises() -> None:
    resolver = SecretResolver()

    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(SecretBackendError, match="did not return a value"):
            resolver.resolve_options({"uri": {"from": "env", "key": "POSTGRES_URI"}})


def test_resolve_aws_ssm_secret() -> None:
    resolver = SecretResolver()
    ssm_client = create_autospec(SsmClient, instance=True)
    ssm_client.get_parameter.return_value = {"Parameter": {"Value": "postgresql://warehouse"}}
    boto3 = create_autospec(Boto3Module, instance=True)
    boto3.client.return_value = ssm_client

    with patch("qualink.secrets.backends.import_module", return_value=boto3):
        resolved = resolver.resolve_options(
            {
                "uri": {
                    "from": "aws_ssm",
                    "key": "/qualink/prod/postgres/uri",
                    "region": "us-east-1",
                }
            }
        )

    assert resolved == {"uri": "postgresql://warehouse"}
    boto3.client.assert_called_once_with("ssm", region_name="us-east-1")
    ssm_client.get_parameter.assert_called_once_with(Name="/qualink/prod/postgres/uri", WithDecryption=True)


def test_resolve_aws_secrets_manager_json_field() -> None:
    resolver = SecretResolver()
    secrets_client = create_autospec(SecretsManagerClient, instance=True)
    secrets_client.get_secret_value.return_value = {
        "SecretString": '{"uri": "snowflake://warehouse", "role": "analyst"}'
    }
    boto3 = create_autospec(Boto3Module, instance=True)
    boto3.client.return_value = secrets_client

    with patch("qualink.secrets.backends.import_module", return_value=boto3):
        resolved = resolver.resolve_options(
            {
                "uri": {
                    "from": "aws_secretsmanager",
                    "key": "qualink/prod/snowflake",
                    "field": "uri",
                    "region": "eu-west-1",
                }
            }
        )

    assert resolved == {"uri": "snowflake://warehouse"}
    boto3.client.assert_called_once_with("secretsmanager", region_name="eu-west-1")
    secrets_client.get_secret_value.assert_called_once_with(SecretId="qualink/prod/snowflake")


def test_resolve_gcp_secret_manager_secret() -> None:
    resolver = SecretResolver()
    gcp_client = create_autospec(GcpSecretManagerClient, instance=True)
    gcp_client.access_secret_version.return_value = SimpleNamespace(
        payload=SimpleNamespace(data=b"bigquery://warehouse")
    )
    secretmanager_module = SimpleNamespace(SecretManagerServiceClient=lambda: gcp_client)

    with patch("qualink.secrets.backends.import_module", return_value=secretmanager_module):
        resolved = resolver.resolve_options(
            {
                "uri": {
                    "from": "gcp_secret_manager",
                    "key": "qualink-bigquery-uri",
                    "project_id": "demo-project",
                }
            }
        )

    assert resolved == {"uri": "bigquery://warehouse"}
    gcp_client.access_secret_version.assert_called_once_with(
        request={"name": "projects/demo-project/secrets/qualink-bigquery-uri/versions/latest"}
    )


def test_secret_field_missing_raises() -> None:
    resolver = SecretResolver()
    secrets_client = create_autospec(SecretsManagerClient, instance=True)
    secrets_client.get_secret_value.return_value = {"SecretString": '{"uri": "snowflake://warehouse"}'}
    boto3 = create_autospec(Boto3Module, instance=True)
    boto3.client.return_value = secrets_client

    with patch("qualink.secrets.backends.import_module", return_value=boto3):
        with pytest.raises(SecretBackendError, match="field 'password' was not present"):
            resolver.resolve_options(
                {
                    "password": {
                        "from": "aws_secretsmanager",
                        "key": "qualink/prod/snowflake",
                        "field": "password",
                    }
                }
            )


@pytest.mark.parametrize(
    ("source", "module_name"),
    [
        ("aws_ssm", "boto3"),
        ("aws_secretsmanager", "boto3"),
        ("gcp_secret_manager", "google.cloud.secretmanager"),
    ],
)
def test_missing_optional_dependency_raises(source: str, module_name: str) -> None:
    resolver = SecretResolver()

    with patch("qualink.secrets.backends.import_module", side_effect=ImportError("missing")):
        with pytest.raises(SecretBackendError, match=module_name):
            resolver.resolve_options({"uri": {"from": source, "key": "demo", "project_id": "demo-project"}})


def test_unknown_secret_source_raises() -> None:
    resolver = SecretResolver()

    with pytest.raises(SecretBackendError, match="Unsupported secret source"):
        resolver.resolve_options({"uri": {"from": "azure_key_vault", "key": "secret"}})
