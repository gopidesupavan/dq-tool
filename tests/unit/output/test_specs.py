from unittest.mock import MagicMock

import pytest
from qualink.core.result import CheckStatus, ValidationMetrics, ValidationReport, ValidationResult
from qualink.output import OutputService, normalize_output_specs


def test_normalize_output_specs_supports_output_and_outputs() -> None:
    specs = normalize_output_specs(
        {
            "output": {
                "path": "reports/result.json",
                "format": "json",
                "show_passed": True,
            }
        }
    )

    assert len(specs) == 1
    assert specs[0].destination == "reports/result.json"
    assert specs[0].format == "json"
    assert specs[0].show_passed is True


@pytest.mark.parametrize("key", ["destination", "path", "uri"])
def test_normalize_output_specs_accepts_destination_aliases(key: str) -> None:
    specs = normalize_output_specs({"outputs": [{key: "s3://bucket/report.md", "format": "markdown"}]})

    assert len(specs) == 1
    assert specs[0].destination == "s3://bucket/report.md"


def test_normalize_output_specs_rejects_missing_destination() -> None:
    with pytest.raises(ValueError, match="must define destination, path, or uri"):
        normalize_output_specs({"outputs": [{"format": "json"}]})


def test_output_service_rejects_unknown_format() -> None:
    result = ValidationResult(
        success=True,
        status=CheckStatus.SUCCESS,
        report=ValidationReport(
            suite_name="test",
            metrics=ValidationMetrics(total_checks=1, total_constraints=1, passed=1),
        ),
    )

    with pytest.raises(ValueError, match="Unsupported output format"):
        OutputService().render(result, normalize_output_specs({"output": {"path": "x", "format": "yaml"}})[0])


def test_output_service_logs_emit(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG", logger="qualink.output.service")
    result = ValidationResult(
        success=True,
        status=CheckStatus.SUCCESS,
        report=ValidationReport(
            suite_name="test",
            metrics=ValidationMetrics(total_checks=1, total_constraints=1, passed=1),
        ),
    )
    writer = MagicMock()
    spec = normalize_output_specs({"output": {"path": "reports/result.json", "format": "json"}})[0]

    OutputService(writer=writer).emit(result, spec)

    assert "Emitting validation output: format=json destination=reports/result.json" in caplog.text
    assert "Rendering validation result with formatter 'JsonFormatter'" in caplog.text
    assert "Rendered " in caplog.text


def test_output_service_emits_formatted_content() -> None:
    result = ValidationResult(
        success=True,
        status=CheckStatus.SUCCESS,
        report=ValidationReport(
            suite_name="test",
            metrics=ValidationMetrics(total_checks=1, total_constraints=1, passed=1),
        ),
    )
    writer = MagicMock()
    spec = normalize_output_specs({"output": {"path": "reports/result.json", "format": "json"}})[0]

    OutputService(writer=writer).emit(result, spec)

    writer.write_text.assert_called_once()
    assert writer.write_text.call_args[0][0] == "reports/result.json"
    assert '"suite": "test"' in writer.write_text.call_args[0][1]
