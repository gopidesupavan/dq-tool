from qualink.analyzers import (
    ColumnProfile,
    CompletenessRule,
    RangeRule,
    StringPatternRule,
    SuggestionEngine,
    UniquenessRule,
)


def test_suggestion_engine_emits_completeness_and_uniqueness_rules() -> None:
    profile = ColumnProfile(
        column_name="user_id",
        data_type="int64",
        row_count=100,
        null_count=0,
        completeness=1.0,
        distinct_count=100,
        uniqueness_ratio=1.0,
    )

    suggestions = SuggestionEngine().add_rule(CompletenessRule()).add_rule(UniquenessRule()).suggest(profile)

    assert [suggestion.constraint_type for suggestion in suggestions] == ["is_complete", "is_unique"]


def test_uniqueness_rule_monitoring_suggestion_uses_assertion_payload() -> None:
    profile = ColumnProfile(
        column_name="user_id",
        data_type="int64",
        row_count=100,
        null_count=0,
        completeness=1.0,
        distinct_count=95,
        uniqueness_ratio=0.95,
    )

    suggestions = SuggestionEngine().add_rule(UniquenessRule()).suggest(profile)

    assert suggestions[0].constraint_type == "has_uniqueness"
    assert suggestions[0].params == {"assertion": ">= 0.95"}


def test_suggestion_engine_emits_numeric_monitoring_rules() -> None:
    profile = ColumnProfile(
        column_name="amount",
        data_type="float64",
        row_count=100,
        null_count=2,
        completeness=0.98,
        distinct_count=95,
        uniqueness_ratio=0.95,
        min_value=1.0,
        max_value=10.0,
        mean_value=5.0,
    )

    suggestions = SuggestionEngine().add_rule(RangeRule()).suggest(profile)

    assert {suggestion.constraint_type for suggestion in suggestions} == {"has_max", "has_mean", "has_min"}


def test_string_pattern_rule_emits_email_and_length_constraints() -> None:
    profile = ColumnProfile(
        column_name="email",
        data_type="string",
        row_count=10,
        null_count=0,
        completeness=1.0,
        distinct_count=10,
        uniqueness_ratio=1.0,
        min_length=13,
        max_length=18,
        sample_values=["a@example.com", "b@example.com"],
    )

    suggestions = SuggestionEngine().add_rule(StringPatternRule()).suggest(profile)

    assert {suggestion.constraint_type for suggestion in suggestions} == {
        "contains_email",
        "has_max_length",
        "has_min_length",
    }
