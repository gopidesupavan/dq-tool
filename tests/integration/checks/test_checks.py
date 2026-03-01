"""Integration tests for Check and CheckBuilder using real CSV data and DataFusion.

Exercises the full Check.run() pipeline — builder → build → run — with a real
DataFusion SessionContext.  No mocking.  The ``df_ctx`` fixture from the
top-level conftest provides sample tables (users, orders, duplicates, …).
"""

from __future__ import annotations

import pytest
from qualink.checks.check import Check, CheckResult
from qualink.constraints.assertion import Assertion
from qualink.core.constraint import ConstraintStatus
from qualink.core.level import Level
from qualink.core.result import CheckStatus


class TestChecksIntegration:
    """Integration tests for Check and CheckBuilder using real CSV data and DataFusion."""

    # -- Check.run() — direct construction ----------------------------------

    @pytest.mark.asyncio()
    async def test_single_passing_constraint(self, df_ctx):
        """A check with one completeness constraint that passes."""
        from qualink.constraints.completeness import CompletenessConstraint

        check = Check(
            _name="completeness_check",
            _level=Level.ERROR,
            _description="name must be complete",
            _constraints=[CompletenessConstraint("name", Assertion.equal_to(1.0))],
        )
        result = await check.run(df_ctx, "users")

        assert isinstance(result, CheckResult)
        assert result.status == CheckStatus.SUCCESS
        assert len(result.constraint_results) == 1
        assert result.constraint_results[0].status == ConstraintStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_single_failing_constraint_error_level(self, df_ctx):
        """A failing constraint at ERROR level → CheckStatus.ERROR."""
        from qualink.constraints.size import SizeConstraint

        check = Check(
            _name="size_check",
            _level=Level.ERROR,
            _description="expect > 100 rows",
            _constraints=[SizeConstraint(Assertion.greater_than(100.0))],
        )
        result = await check.run(df_ctx, "users")

        assert result.status == CheckStatus.ERROR
        assert result.constraint_results[0].status == ConstraintStatus.FAILURE

    @pytest.mark.asyncio()
    async def test_single_failing_constraint_warning_level(self, df_ctx):
        """A failing constraint at WARNING level → CheckStatus.WARNING."""
        from qualink.constraints.size import SizeConstraint

        check = Check(
            _name="size_warn",
            _level=Level.WARNING,
            _description="soft expectation",
            _constraints=[SizeConstraint(Assertion.greater_than(100.0))],
        )
        result = await check.run(df_ctx, "users")

        assert result.status == CheckStatus.WARNING

    @pytest.mark.asyncio()
    async def test_multiple_constraints_all_pass(self, df_ctx):
        """Multiple constraints that all pass → SUCCESS."""
        from qualink.constraints.column_exists import ColumnExistsConstraint
        from qualink.constraints.completeness import CompletenessConstraint
        from qualink.constraints.size import SizeConstraint

        check = Check(
            _name="multi_pass",
            _level=Level.ERROR,
            _description="all must pass",
            _constraints=[
                SizeConstraint(Assertion.equal_to(5.0)),
                ColumnExistsConstraint("email"),
                CompletenessConstraint("name", Assertion.equal_to(1.0)),
            ],
        )
        result = await check.run(df_ctx, "users")

        assert result.status == CheckStatus.SUCCESS
        assert len(result.constraint_results) == 3
        assert all(r.status == ConstraintStatus.SUCCESS for r in result.constraint_results)

    @pytest.mark.asyncio()
    async def test_mixed_pass_and_fail(self, df_ctx):
        """One passes, one fails → overall check fails."""
        from qualink.constraints.column_exists import ColumnExistsConstraint

        check = Check(
            _name="mixed_check",
            _level=Level.ERROR,
            _description="mixed results",
            _constraints=[
                ColumnExistsConstraint("name"),  # passes
                ColumnExistsConstraint("nonexistent"),  # fails
            ],
        )
        result = await check.run(df_ctx, "users")

        assert result.status == CheckStatus.ERROR
        statuses = [r.status for r in result.constraint_results]
        assert ConstraintStatus.SUCCESS in statuses
        assert ConstraintStatus.FAILURE in statuses

    @pytest.mark.asyncio()
    async def test_check_result_carries_check_reference(self, df_ctx):
        """CheckResult.check points back to the original Check."""
        from qualink.constraints.size import SizeConstraint

        check = Check(
            _name="ref_check",
            _level=Level.INFO,
            _description="",
            _constraints=[SizeConstraint(Assertion.greater_than(0.0))],
        )
        result = await check.run(df_ctx, "users")

        assert result.check is check
        assert result.check.name == "ref_check"

    # -- CheckBuilder — fluent API ------------------------------------------

    @pytest.mark.asyncio()
    async def test_is_complete(self, df_ctx):
        """builder.is_complete() adds a CompletenessConstraint == 1.0."""
        check = Check.builder("completeness").is_complete("name").build()
        result = await check.run(df_ctx, "users")

        assert result.status == CheckStatus.SUCCESS
        assert result.constraint_results[0].metric == 1.0

    @pytest.mark.asyncio()
    async def test_has_completeness(self, df_ctx):
        """builder.has_completeness() with a custom assertion."""
        check = (
            Check.builder("partial_completeness")
            .has_completeness("name", Assertion.greater_than(0.5))
            .build()
        )
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_has_column_pass(self, df_ctx):
        check = Check.builder("col_check").has_column("email").build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_has_column_fail(self, df_ctx):
        check = Check.builder("col_check").has_column("missing_col").build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.ERROR

    @pytest.mark.asyncio()
    async def test_is_unique(self, df_ctx):
        check = Check.builder("unique_id").is_unique("id").build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_is_unique_fails(self, df_ctx):
        check = Check.builder("unique_city").is_unique("city").build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.ERROR

    @pytest.mark.asyncio()
    async def test_is_primary_key(self, df_ctx):
        check = Check.builder("pk_check").is_primary_key("id").build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_has_distinctness(self, df_ctx):
        check = Check.builder("distinct_check").has_distinctness(["id"], Assertion.equal_to(1.0)).build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_has_unique_value_ratio(self, df_ctx):
        check = Check.builder("uvr_check").has_unique_value_ratio(["id"], Assertion.equal_to(1.0)).build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_satisfies_with_assertion(self, df_ctx):
        """satisfies() with assertion → ComplianceConstraint."""
        check = (
            Check.builder("age_positive")
            .satisfies('"age" > 0', "positive_ages", Assertion.equal_to(1.0))
            .build()
        )
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS
        assert result.constraint_results[0].metric == 1.0

    @pytest.mark.asyncio()
    async def test_satisfies_without_assertion(self, df_ctx):
        """satisfies() without assertion → CustomSqlConstraint (all rows must match)."""
        check = Check.builder("score_check").satisfies('"score" > 0', "all_scores_positive").build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_satisfies_fails(self, df_ctx):
        check = (
            Check.builder("impossible")
            .satisfies('"age" > 100', "age_over_100", Assertion.equal_to(1.0))
            .build()
        )
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.ERROR
        assert result.constraint_results[0].metric == 0.0

    @pytest.mark.asyncio()
    async def test_has_pattern_with_assertion(self, df_ctx):
        check = (
            Check.builder("name_pattern").has_pattern("name", r"^[A-Za-z]", Assertion.equal_to(1.0)).build()
        )
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_has_pattern_without_assertion(self, df_ctx):
        """has_pattern() without assertion → FormatConstraint with REGEX type."""
        check = Check.builder("name_regex").has_pattern("name", r"^[A-Za-z]").build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_contains_email(self, df_ctx):
        check = Check.builder("email_format").contains_email("email").build()
        result = await check.run(df_ctx, "users")
        # Row 3 has empty string — may not pass at 100% threshold
        assert result.constraint_results[0].metric is not None

    @pytest.mark.asyncio()
    async def test_has_min(self, df_ctx):
        check = Check.builder("min_age").has_min("age", Assertion.equal_to(25.0)).build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS
        assert result.constraint_results[0].metric == 25.0

    @pytest.mark.asyncio()
    async def test_has_max(self, df_ctx):
        check = Check.builder("max_age").has_max("age", Assertion.equal_to(35.0)).build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS
        assert result.constraint_results[0].metric == 35.0

    @pytest.mark.asyncio()
    async def test_has_mean(self, df_ctx):
        check = Check.builder("mean_age").has_mean("age", Assertion.equal_to(30.0)).build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_has_sum(self, df_ctx):
        check = Check.builder("sum_age").has_sum("age", Assertion.equal_to(150.0)).build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_has_standard_deviation(self, df_ctx):
        check = Check.builder("stddev_age").has_standard_deviation("age", Assertion.greater_than(0.0)).build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS
        assert result.constraint_results[0].metric is not None
        assert result.constraint_results[0].metric > 0.0

    @pytest.mark.asyncio()
    async def test_has_min_length(self, df_ctx):
        check = Check.builder("min_len").has_min_length("name", Assertion.greater_than_or_equal(3.0)).build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS
        assert result.constraint_results[0].metric == 3.0

    @pytest.mark.asyncio()
    async def test_has_max_length(self, df_ctx):
        check = Check.builder("max_len").has_max_length("name", Assertion.less_than_or_equal(10.0)).build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS
        assert result.constraint_results[0].metric == 7.0

    @pytest.mark.asyncio()
    async def test_has_approx_count_distinct(self, df_ctx):
        check = (
            Check.builder("approx_distinct")
            .has_approx_count_distinct("city", Assertion.between(2.0, 4.0))
            .build()
        )
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_has_approx_quantile(self, df_ctx):
        check = (
            Check.builder("median_age").has_approx_quantile("age", 0.5, Assertion.between(28.0, 32.0)).build()
        )
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_has_size(self, df_ctx):
        check = Check.builder("size_check").has_size(Assertion.equal_to(5.0)).build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS
        assert result.constraint_results[0].metric == 5.0

    @pytest.mark.asyncio()
    async def test_has_column_count(self, df_ctx):
        check = Check.builder("col_count").has_column_count(Assertion.equal_to(6.0)).build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS
        assert result.constraint_results[0].metric == 6.0

    @pytest.mark.asyncio()
    async def test_custom_sql(self, df_ctx):
        check = Check.builder("custom").custom_sql('"score" > 0', hint="positive_scores").build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_has_correlation(self, df_ctx):
        check = Check.builder("corr_check").has_correlation("x", "y", Assertion.greater_than(0.99)).build()
        result = await check.run(df_ctx, "correlated")
        assert result.status == CheckStatus.SUCCESS
        assert result.constraint_results[0].metric == pytest.approx(1.0, abs=0.01)

    @pytest.mark.asyncio()
    async def test_referential_integrity_pass(self, df_ctx):
        check = (
            Check.builder("ri_check")
            .referential_integrity("orders", "user_id", "users", "id", Assertion.equal_to(1.0))
            .build()
        )
        result = await check.run(df_ctx, "orders")
        assert result.status == CheckStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_referential_integrity_fail(self, df_ctx):
        check = (
            Check.builder("ri_fail")
            .referential_integrity("orders_orphan", "user_id", "users", "id", Assertion.equal_to(1.0))
            .build()
        )
        result = await check.run(df_ctx, "orders_orphan")
        assert result.status == CheckStatus.ERROR
        assert result.constraint_results[0].metric is not None
        assert result.constraint_results[0].metric < 1.0

    @pytest.mark.asyncio()
    async def test_row_count_match_pass(self, df_ctx):
        check = Check.builder("rcm_check").row_count_match("users", "orders", Assertion.equal_to(1.0)).build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_row_count_match_fail(self, df_ctx):
        check = Check.builder("rcm_fail").row_count_match("users", "users_b", Assertion.equal_to(1.0)).build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.ERROR

    @pytest.mark.asyncio()
    async def test_schema_match_pass(self, df_ctx):
        check = (
            Check.builder("schema_check").schema_match("users", "users_b", Assertion.equal_to(1.0)).build()
        )
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_schema_match_fail(self, df_ctx):
        check = Check.builder("schema_fail").schema_match("users", "orders", Assertion.equal_to(1.0)).build()
        result = await check.run(df_ctx, "users")
        assert result.status == CheckStatus.ERROR

    # -- CheckBuilder — chaining multiple builder methods -------------------

    @pytest.mark.asyncio()
    async def test_full_data_quality_check_passes(self, df_ctx):
        """A comprehensive check combining many builder methods — all pass."""
        check = (
            Check.builder("full_dq")
            .with_level(Level.ERROR)
            .with_description("Comprehensive data quality check on users")
            .has_size(Assertion.equal_to(5.0))
            .has_column_count(Assertion.equal_to(6.0))
            .has_column("id")
            .has_column("name")
            .has_column("email")
            .is_unique("id")
            .is_complete("name")
            .has_min("age", Assertion.greater_than_or_equal(0.0))
            .has_max("age", Assertion.less_than_or_equal(150.0))
            .has_mean("age", Assertion.between(20.0, 40.0))
            .build()
        )
        result = await check.run(df_ctx, "users")

        assert result.status == CheckStatus.SUCCESS
        assert len(result.constraint_results) == 10
        assert all(r.status == ConstraintStatus.SUCCESS for r in result.constraint_results)
        assert result.check.name == "full_dq"
        assert result.check.level == Level.ERROR
        assert result.check.description == "Comprehensive data quality check on users"

    @pytest.mark.asyncio()
    async def test_chained_check_with_some_failures(self, df_ctx):
        """A check where some constraints pass and some fail."""
        check = (
            Check.builder("partial_dq")
            .with_level(Level.WARNING)
            .has_size(Assertion.equal_to(5.0))  # passes
            .has_column("id")  # passes
            .is_unique("city")  # fails — city has dupes
            .has_max("age", Assertion.less_than(30.0))  # fails — max age is 35
            .build()
        )
        result = await check.run(df_ctx, "users")

        assert result.status == CheckStatus.WARNING
        assert len(result.constraint_results) == 4
        passed = [r for r in result.constraint_results if r.status == ConstraintStatus.SUCCESS]
        failed = [r for r in result.constraint_results if r.status == ConstraintStatus.FAILURE]
        assert len(passed) == 2
        assert len(failed) == 2

    @pytest.mark.asyncio()
    async def test_builder_level_and_description(self, df_ctx):
        """with_level() and with_description() carry through to the built Check."""
        check = (
            Check.builder("info_check")
            .with_level(Level.INFO)
            .with_description("Just an info check")
            .has_size(Assertion.greater_than(0.0))
            .build()
        )
        result = await check.run(df_ctx, "users")

        assert result.check.level == Level.INFO
        assert result.check.description == "Just an info check"
        assert result.status == CheckStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_builder_produces_correct_constraint_count(self, df_ctx):
        """Each builder method adds exactly one constraint."""
        check = (
            Check.builder("count_test")
            .is_complete("name")
            .has_column("id")
            .is_unique("id")
            .has_size(Assertion.greater_than(0.0))
            .has_min("age", Assertion.greater_than(0.0))
            .build()
        )
        assert len(check.constraints) == 5

        result = await check.run(df_ctx, "users")
        assert len(result.constraint_results) == 5

    @pytest.mark.asyncio()
    async def test_cross_table_check(self, df_ctx):
        """A check that validates relationships across multiple tables."""
        check = (
            Check.builder("cross_table")
            .with_level(Level.ERROR)
            .referential_integrity("orders", "user_id", "users", "id", Assertion.equal_to(1.0))
            .row_count_match("users", "orders", Assertion.equal_to(1.0))
            .schema_match("users", "users_b", Assertion.equal_to(1.0))
            .build()
        )
        result = await check.run(df_ctx, "orders")

        assert result.status == CheckStatus.SUCCESS
        assert len(result.constraint_results) == 3
        assert all(r.status == ConstraintStatus.SUCCESS for r in result.constraint_results)
