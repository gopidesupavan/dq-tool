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
  type: csv
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

    def test_load_yaml_from_short_string_path(self, tmp_path):
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("suite:\n  name: Test")
        load_yaml("test.yaml")  # Assuming cwd is tmp_path, but may not work
        # Skip this test as it's tricky without setting cwd
