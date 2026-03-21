from unittest.mock import MagicMock, patch

import pytest
from qualink.config.parser import load_yaml, parse_assertion
from qualink.constraints.assertion import Assertion


class TestParseAssertion:
    def test_parse_shorthand_greater_than(self):
        assertion = parse_assertion("> 5")
        assert isinstance(assertion, Assertion)
        # Assuming Assertion has _op and _value, but since it's private, check by testing
        # For now, just check it's an Assertion

    def test_parse_shorthand_greater_than_or_equal(self):
        assertion = parse_assertion(">= 0.95")
        assert isinstance(assertion, Assertion)

    def test_parse_shorthand_less_than(self):
        assertion = parse_assertion("< 10")
        assert isinstance(assertion, Assertion)

    def test_parse_shorthand_less_than_or_equal(self):
        assertion = parse_assertion("<= 100")
        assert isinstance(assertion, Assertion)

    def test_parse_shorthand_equal_to(self):
        assertion = parse_assertion("== 3")
        assert isinstance(assertion, Assertion)

    def test_parse_shorthand_between(self):
        assertion = parse_assertion("between 1 10")
        assert isinstance(assertion, Assertion)

    def test_parse_shorthand_invalid(self):
        with pytest.raises(ValueError, match="Invalid assertion shorthand"):
            parse_assertion("invalid")

    def test_parse_dict_greater_than(self):
        assertion = parse_assertion({"operator": "greater_than", "value": 5})
        assert isinstance(assertion, Assertion)

    def test_parse_dict_between(self):
        assertion = parse_assertion({"operator": "between", "lower": 1, "upper": 10})
        assert isinstance(assertion, Assertion)

    def test_parse_dict_invalid_operator(self):
        with pytest.raises(ValueError, match="Unknown assertion operator"):
            parse_assertion({"operator": "invalid", "value": 5})

    def test_parse_invalid_type(self):
        with pytest.raises(ValueError, match="Cannot parse assertion"):
            parse_assertion(123)  # type: ignore[arg-type]


class TestLoadYaml:
    def test_load_yaml_from_path(self, tmp_path):
        yaml_content = """
suite:
  name: Test Suite
data_source:
  path: data.csv
checks:
  - name: Test Check
    rules:
      - is_complete: id
"""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(yaml_content)
        data = load_yaml(yaml_file)
        assert data["suite"]["name"] == "Test Suite"

    def test_load_yaml_from_string(self):
        yaml_string = """
suite:
  name: Test Suite
"""
        data = load_yaml(yaml_string)
        assert data["suite"]["name"] == "Test Suite"

    def test_load_yaml_from_short_string_path(self, tmp_path, monkeypatch):
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("suite:\n  name: Test")
        monkeypatch.chdir(tmp_path)

        data = load_yaml("test.yaml")

        assert data["suite"]["name"] == "Test"

    def test_load_yaml_from_file_uri(self, tmp_path):
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("suite:\n  name: File URI Test")

        data = load_yaml(yaml_file.resolve().as_uri())

        assert data["suite"]["name"] == "File URI Test"

    def test_load_yaml_from_object_store_uri(self):
        mock_filesystem = MagicMock()
        mock_stream = MagicMock()
        mock_stream.read.return_value = b"suite:\n  name: Object Store Test\n"
        mock_filesystem.open_input_stream.return_value.__enter__.return_value = mock_stream

        with patch("qualink.config.parser.pafs") as mock_pafs:
            mock_pafs.FileSystem.from_uri.return_value = (mock_filesystem, "cfg.yaml")
            data = load_yaml("s3://bucket/config.yaml")

        assert data["suite"]["name"] == "Object Store Test"
        mock_filesystem.open_input_stream.assert_called_once_with("cfg.yaml")

    def test_load_yaml_missing_yaml_path_raises(self):
        with pytest.raises(FileNotFoundError, match="YAML config file not found"):
            load_yaml("missing.yaml")
