from qualink.core.level import Level


class TestLevel:
    def test_enum_values(self) -> None:
        assert Level.INFO == 0
        assert Level.WARNING == 1
        assert Level.ERROR == 2

    def test_as_str(self) -> None:
        assert Level.INFO.as_str() == "info"
        assert Level.WARNING.as_str() == "warning"
        assert Level.ERROR.as_str() == "error"

    def test_str_method(self) -> None:
        assert str(Level.INFO) == "info"
        assert str(Level.WARNING) == "warning"
        assert str(Level.ERROR) == "error"

    def test_is_at_least(self) -> None:
        assert Level.ERROR.is_at_least(Level.ERROR) is True
        assert Level.ERROR.is_at_least(Level.WARNING) is True
        assert Level.ERROR.is_at_least(Level.INFO) is True
        assert Level.WARNING.is_at_least(Level.WARNING) is True
        assert Level.WARNING.is_at_least(Level.INFO) is True
        assert Level.WARNING.is_at_least(Level.ERROR) is False
        assert Level.INFO.is_at_least(Level.INFO) is True
        assert Level.INFO.is_at_least(Level.WARNING) is False
        assert Level.INFO.is_at_least(Level.ERROR) is False
