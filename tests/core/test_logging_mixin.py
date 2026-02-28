from __future__ import annotations

import logging

from qualink.core.logging_mixin import LoggingMixin, configure_logging, get_logger


class _DummyClass(LoggingMixin):
    """Test class that uses LoggingMixin."""

    pass


class _ChildClass(_DummyClass):
    """Subclass to verify logger naming propagation."""

    pass


class TestLoggingMixin:
    def test_logger_property_returns_logger(self):
        obj = _DummyClass()
        assert isinstance(obj.logger, logging.Logger)

    def test_logger_name_includes_module_and_class(self):
        obj = _DummyClass()
        expected = f"{_DummyClass.__module__}.{_DummyClass.__qualname__}"
        assert obj.logger.name == expected

    def test_logger_is_cached(self):
        obj = _DummyClass()
        assert obj.logger is obj.logger

    def test_child_class_has_own_logger_name(self):
        parent = _DummyClass()
        child = _ChildClass()
        assert parent.logger.name != child.logger.name
        assert "ChildClass" in child.logger.name

    def test_different_instances_same_class_same_logger_name(self):
        a = _DummyClass()
        b = _DummyClass()
        assert a.logger.name == b.logger.name


class TestGetLogger:
    def test_returns_namespaced_logger(self):
        logger = get_logger("my_module")
        assert logger.name == "qualink.my_module"
        assert isinstance(logger, logging.Logger)


class TestConfigureLogging:
    def test_adds_handler_to_root_logger(self):
        root = logging.getLogger("qualink")
        original_handlers = list(root.handlers)
        # Remove any existing handlers to test fresh
        root.handlers.clear()

        configure_logging(level=logging.DEBUG)
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0], logging.StreamHandler)
        assert root.level == logging.DEBUG

        # Restore
        root.handlers.clear()
        root.handlers.extend(original_handlers)

    def test_does_not_duplicate_handlers(self):
        root = logging.getLogger("qualink")
        original_handlers = list(root.handlers)
        root.handlers.clear()

        configure_logging(level=logging.INFO)
        configure_logging(level=logging.DEBUG)
        assert len(root.handlers) == 1

        # Restore
        root.handlers.clear()
        root.handlers.extend(original_handlers)


class TestConstraintInheritsLogger:
    """Verify that Constraint subclasses automatically get a logger."""

    def test_constraint_subclass_has_logger(self):
        from qualink.constraints.assertion import Assertion
        from qualink.constraints.completeness import CompletenessConstraint

        c = CompletenessConstraint("col", Assertion.equal_to(1.0))
        assert isinstance(c.logger, logging.Logger)
        assert "CompletenessConstraint" in c.logger.name
