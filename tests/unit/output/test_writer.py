from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from qualink.output import ResultWriter, write_text_output
from qualink.output.writer import LocalFileResultSink, PyArrowFileSystemResultSink


def test_local_file_sink_writes_to_path(tmp_path: Path) -> None:
    target = tmp_path / "reports" / "result.json"

    write_text_output(target, '{"ok": true}')

    assert target.read_text(encoding="utf-8") == '{"ok": true}'


def test_local_file_sink_supports_plain_string_paths() -> None:
    sink = LocalFileResultSink()

    assert sink.supports("reports/result.json") is True
    assert sink.supports(Path("reports/result.json")) is True
    assert sink.supports("s3://bucket/result.json") is False


def test_local_file_sink_logs_write(caplog: pytest.LogCaptureFixture, tmp_path: Path) -> None:
    caplog.set_level("DEBUG", logger="qualink.output.writer")
    target = tmp_path / "reports" / "result.json"

    write_text_output(target, '{"ok": true}')

    assert "Writing validation output to local path" in caplog.text
    assert "Wrote 12 characters to local output path" in caplog.text


@patch("qualink.output.writer._resolve_filesystem_from_uri")
def test_pyarrow_sink_writes_remote_uri(mock_from_uri) -> None:
    mock_filesystem = MagicMock()
    mock_stream = MagicMock()
    mock_filesystem.open_output_stream.return_value.__enter__.return_value = mock_stream
    mock_from_uri.return_value = (mock_filesystem, "reports/result.json")

    write_text_output("s3://bucket/reports/result.json", "hello")

    mock_from_uri.assert_called_once_with("s3://bucket/reports/result.json")
    mock_filesystem.create_dir.assert_called_once_with("reports", recursive=True)
    mock_stream.write.assert_called_once_with(b"hello")


@patch("qualink.output.writer._resolve_filesystem_from_uri")
def test_pyarrow_sink_logs_remote_write(
    mock_from_uri,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("DEBUG", logger="qualink.output.writer")
    mock_filesystem = MagicMock()
    mock_stream = MagicMock()
    mock_filesystem.open_output_stream.return_value.__enter__.return_value = mock_stream
    mock_from_uri.return_value = (mock_filesystem, "reports/result.json")

    write_text_output("s3://bucket/reports/result.json", "hello")

    assert "Writing validation output to filesystem URI" in caplog.text
    assert (
        "Resolved filesystem URI 's3://bucket/reports/result.json' to path 'reports/result.json'"
        in caplog.text
    )
    assert "Wrote 5 characters to filesystem URI: s3://bucket/reports/result.json" in caplog.text


def test_result_writer_rejects_unknown_scheme() -> None:
    writer = ResultWriter(sinks=[PyArrowFileSystemResultSink()])

    with pytest.raises(ValueError, match="Unsupported output destination"):
        writer.write_text("ftp://example.com/result.json", "nope")
