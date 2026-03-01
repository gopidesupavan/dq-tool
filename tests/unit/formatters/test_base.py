from qualink.formatters.base import FormatterConfig, ResultFormatter


class TestFormatterConfig:
    def test_creation_default(self):
        config = FormatterConfig()
        assert config.show_metrics is True
        assert config.show_issues is True
        assert config.show_passed is False
        assert config.colorize is True

    def test_creation_custom(self):
        config = FormatterConfig(show_metrics=False, show_issues=False, show_passed=True, colorize=False)
        assert config.show_metrics is False
        assert config.show_issues is False
        assert config.show_passed is True
        assert config.colorize is False


class TestResultFormatter:
    def test_init_default_config(self):
        class ConcreteFormatter(ResultFormatter):
            def format(self, result):
                return ""

        formatter = ConcreteFormatter()
        assert isinstance(formatter._config, FormatterConfig)

    def test_init_custom_config(self):
        config = FormatterConfig(colorize=False)

        class ConcreteFormatter(ResultFormatter):
            def format(self, result):
                return ""

        formatter = ConcreteFormatter(config)
        assert formatter._config == config
