from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from urllib.parse import urlparse

import pyarrow.fs as pafs

from qualink.core.logging_mixin import get_logger

_logger = get_logger("output.writer")


class ResultSink(ABC):
    @abstractmethod
    def kind(self) -> str:
        """Return the sink kind handled by this implementation."""

    @abstractmethod
    def supports(self, destination: str | Path) -> bool:
        """Return True when this sink can write to the destination."""

    @abstractmethod
    def write_text(self, destination: str | Path, content: str) -> None:
        """Write text content to the destination."""


class LocalFileResultSink(ResultSink):
    def kind(self) -> str:
        return "filesystem"

    def supports(self, destination: str | Path) -> bool:
        if isinstance(destination, Path):
            return True
        return "://" not in destination

    def write_text(self, destination: str | Path, content: str) -> None:
        path = destination if isinstance(destination, Path) else Path(destination)
        _logger.info("Writing validation output to local path: %s", path)
        _logger.debug("Ensuring local output directory exists: %s", path.parent)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        _logger.debug("Wrote %d characters to local output path: %s", len(content), path)


class PyArrowFileSystemResultSink(ResultSink):
    _SUPPORTED_SCHEMES = frozenset({"s3", "gs", "gcs", "az", "abfs", "abfss", "file"})

    def kind(self) -> str:
        return "filesystem"

    def supports(self, destination: str | Path) -> bool:
        if isinstance(destination, Path):
            return False
        scheme = urlparse(destination).scheme.lower()
        return scheme in self._SUPPORTED_SCHEMES

    def write_text(self, destination: str | Path, content: str) -> None:
        if not isinstance(destination, str):
            raise TypeError("Remote filesystem destinations must be string URIs.")

        _logger.info("Writing validation output to filesystem URI: %s", destination)
        filesystem, path = _resolve_filesystem_from_uri(destination)
        _logger.debug("Resolved filesystem URI '%s' to path '%s'", destination, path)
        directory = str(Path(path).parent)
        if directory not in {"", "."}:
            _logger.debug("Ensuring remote output directory exists: %s", directory)
            filesystem.create_dir(directory, recursive=True)
        with filesystem.open_output_stream(path) as output_stream:
            output_stream.write(content.encode("utf-8"))
        _logger.debug("Wrote %d characters to filesystem URI: %s", len(content), destination)


class ResultWriter:
    def __init__(self, sinks: list[ResultSink] | None = None) -> None:
        self._sinks = sinks or [LocalFileResultSink(), PyArrowFileSystemResultSink()]

    def write_text(self, destination: str | Path, content: str) -> None:
        for sink in self._sinks:
            if sink.supports(destination):
                _logger.debug(
                    "Selected result sink '%s' for destination '%s'",
                    type(sink).__name__,
                    destination,
                )
                sink.write_text(destination, content)
                return
        _logger.error("No result sink supports destination: %r", destination)
        raise ValueError(f"Unsupported output destination: {destination!r}")


def write_text_output(destination: str | Path, content: str) -> None:
    ResultWriter().write_text(destination, content)


def _resolve_filesystem_from_uri(destination: str) -> tuple[pafs.FileSystem, str]:
    _logger.debug("Resolving filesystem from URI: %s", destination)
    return pafs.FileSystem.from_uri(destination)
