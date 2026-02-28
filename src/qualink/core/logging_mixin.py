from __future__ import annotations

import functools
import logging


def configure_logging(
    level: int = logging.DEBUG,
    fmt: str = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt: str = "%Y-%m-%d %H:%M:%S",
) -> None:
    """Attach a ``StreamHandler`` to the ``qualink`` root logger.

    Safe to call more than once — handlers are not duplicated.
    """
    root = logging.getLogger("qualink")
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
        root.addHandler(handler)
    root.setLevel(level)


class LoggingMixin:
    """Mixin that provides a per-class ``self.logger`` cached property.

    The logger name follows the pattern ``<module>.<qualname>`` so that
    users can fine-tune levels per package, e.g.::

        logging.getLogger("qualink.constraints").setLevel(logging.DEBUG)
    """

    @functools.cached_property
    def logger(self) -> logging.Logger:
        cls = type(self)
        return logging.getLogger(f"{cls.__module__}.{cls.__qualname__}")


def get_logger(name: str) -> logging.Logger:
    """Return a logger under the ``qualink`` namespace."""
    return logging.getLogger(f"qualink.{name}")
