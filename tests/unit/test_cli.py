from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner
from qualink.cli import main

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
EXAMPLE_YAML = PROJECT_ROOT / "examples" / "showcase_all_rules.yaml"

runner = CliRunner()


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    """Run the CLI as a subprocess so we can inspect exit codes."""
    return subprocess.run(
        [sys.executable, "-m", "qualink", *args],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )


class TestCLIHelp:
    def test_help_flag_exits_zero(self):
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "qualinkctl" in result.output.lower() or "config" in result.output.lower()

    def test_no_args_exits_nonzero(self):
        result = runner.invoke(main, [])
        assert result.exit_code != 0


class TestCLIMissingFile:
    def test_missing_config_file(self):
        result = runner.invoke(main, ["nonexistent.yaml"])
        assert result.exit_code != 0
        assert "does not exist" in result.output.lower() or "error" in result.output.lower()


class TestCLIFormatOptions:
    """Verify that the --format flag is accepted for all choices."""

    @pytest.mark.parametrize("fmt", ["human", "json", "markdown"])
    def test_format_flag_accepted(self, fmt, tmp_path):
        """Smoke test: the format choices are accepted and run_yaml is invoked."""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("suite:\n  name: test\n")

        from qualink.core.result import (
            CheckStatus,
            ValidationMetrics,
            ValidationReport,
            ValidationResult,
        )

        fake_result = ValidationResult(
            success=True,
            status=CheckStatus.SUCCESS,
            report=ValidationReport(
                suite_name="test",
                metrics=ValidationMetrics(total_checks=1, total_constraints=1, passed=1),
            ),
        )

        with patch("qualink.cli.run_yaml", new_callable=AsyncMock, return_value=fake_result):
            result = runner.invoke(main, [str(yaml_file), "-f", fmt], catch_exceptions=False)

        assert result.exit_code == 0


class TestCLIMainFunction:
    """Test the main() entry point with mocked run_yaml."""

    def test_main_success(self, tmp_path):
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("suite:\n  name: test\n")

        from qualink.core.result import (
            CheckStatus,
            ValidationMetrics,
            ValidationReport,
            ValidationResult,
        )

        fake_result = ValidationResult(
            success=True,
            status=CheckStatus.SUCCESS,
            report=ValidationReport(
                suite_name="test",
                metrics=ValidationMetrics(total_checks=1, total_constraints=1, passed=1),
            ),
        )

        with patch("qualink.cli.run_yaml", new_callable=AsyncMock, return_value=fake_result):
            result = runner.invoke(main, [str(yaml_file)], catch_exceptions=False)

        assert result.exit_code == 0

    def test_main_failure(self, tmp_path):
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("suite:\n  name: test\n")

        from qualink.core.result import (
            CheckStatus,
            ValidationMetrics,
            ValidationReport,
            ValidationResult,
        )

        fake_result = ValidationResult(
            success=False,
            status=CheckStatus.ERROR,
            report=ValidationReport(
                suite_name="test",
                metrics=ValidationMetrics(total_checks=1, total_constraints=1, failed=1),
            ),
        )

        with patch("qualink.cli.run_yaml", new_callable=AsyncMock, return_value=fake_result):
            result = runner.invoke(main, [str(yaml_file)])

        assert result.exit_code == 1

    def test_main_output_to_file(self, tmp_path):
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("suite:\n  name: test\n")
        out_file = tmp_path / "result.json"

        from qualink.core.result import (
            CheckStatus,
            ValidationMetrics,
            ValidationReport,
            ValidationResult,
        )

        fake_result = ValidationResult(
            success=True,
            status=CheckStatus.SUCCESS,
            report=ValidationReport(
                suite_name="test",
                metrics=ValidationMetrics(total_checks=1, total_constraints=1, passed=1),
            ),
        )

        with patch("qualink.cli.run_yaml", new_callable=AsyncMock, return_value=fake_result):
            result = runner.invoke(
                main, [str(yaml_file), "-f", "json", "-o", str(out_file)], catch_exceptions=False
            )

        assert result.exit_code == 0
        assert out_file.exists()
        content = out_file.read_text()
        assert "test" in content

    def test_verbose_flag(self, tmp_path):
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("suite:\n  name: test\n")

        from qualink.core.result import (
            CheckStatus,
            ValidationMetrics,
            ValidationReport,
            ValidationResult,
        )

        fake_result = ValidationResult(
            success=True,
            status=CheckStatus.SUCCESS,
            report=ValidationReport(
                suite_name="test",
                metrics=ValidationMetrics(total_checks=1, total_constraints=1, passed=1),
            ),
        )

        with patch("qualink.cli.run_yaml", new_callable=AsyncMock, return_value=fake_result):
            result = runner.invoke(main, [str(yaml_file), "-v"], catch_exceptions=False)

        assert result.exit_code == 0

    def test_show_passed_flag(self, tmp_path):
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("suite:\n  name: test\n")

        from qualink.core.result import (
            CheckStatus,
            ValidationMetrics,
            ValidationReport,
            ValidationResult,
        )

        fake_result = ValidationResult(
            success=True,
            status=CheckStatus.SUCCESS,
            report=ValidationReport(
                suite_name="test",
                metrics=ValidationMetrics(total_checks=1, total_constraints=1, passed=1),
            ),
        )

        with patch("qualink.cli.run_yaml", new_callable=AsyncMock, return_value=fake_result):
            result = runner.invoke(main, [str(yaml_file), "--show-passed"], catch_exceptions=False)

        assert result.exit_code == 0

    def test_no_color_flag(self, tmp_path):
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("suite:\n  name: test\n")

        from qualink.core.result import (
            CheckStatus,
            ValidationMetrics,
            ValidationReport,
            ValidationResult,
        )

        fake_result = ValidationResult(
            success=True,
            status=CheckStatus.SUCCESS,
            report=ValidationReport(
                suite_name="test",
                metrics=ValidationMetrics(total_checks=1, total_constraints=1, passed=1),
            ),
        )

        with patch("qualink.cli.run_yaml", new_callable=AsyncMock, return_value=fake_result):
            result = runner.invoke(main, [str(yaml_file), "--no-color"], catch_exceptions=False)

        assert result.exit_code == 0
