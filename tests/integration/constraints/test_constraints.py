from __future__ import annotations

import pytest
from qualink.constraints.approx_count_distinct import ApproxCountDistinctConstraint
from qualink.constraints.approx_quantile import ApproxQuantileConstraint
from qualink.constraints.assertion import Assertion
from qualink.constraints.column_count import ColumnCountConstraint
from qualink.constraints.column_exists import ColumnExistsConstraint
from qualink.constraints.completeness import CompletenessConstraint
from qualink.constraints.compliance import ComplianceConstraint
from qualink.constraints.correlation import CorrelationConstraint
from qualink.constraints.custom_sql import CustomSqlConstraint
from qualink.constraints.distinctness import DistinctnessConstraint
from qualink.constraints.format import FormatConstraint, FormatType
from qualink.constraints.max_length import MaxLengthConstraint
from qualink.constraints.min_length import MinLengthConstraint
from qualink.constraints.pattern_match import PatternMatchConstraint
from qualink.constraints.referential_integrity import ReferentialIntegrityConstraint
from qualink.constraints.row_count_match import RowCountMatchConstraint
from qualink.constraints.schema_match import SchemaMatchConstraint
from qualink.constraints.size import SizeConstraint
from qualink.constraints.statistics import StatisticalConstraint, StatisticType
from qualink.constraints.unique_value_ratio import UniqueValueRatioConstraint
from qualink.constraints.uniqueness import UniquenessConstraint
from qualink.core.constraint import ConstraintStatus


class TestConstraintsIntegration:
    """Integration tests for every constraint using real CSV data and DataFusion."""

    @pytest.mark.asyncio()
    async def test_full_completeness_passes(self, df_ctx):
        """All rows in 'users' have a non-null 'name' → completeness == 1.0."""
        c = CompletenessConstraint("name", Assertion.equal_to(1.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 1.0

    @pytest.mark.asyncio()
    async def test_partial_completeness_fails(self, df_ctx):
        """'users_nulls' has blank emails → completeness < 1.0."""
        c = CompletenessConstraint("email", Assertion.equal_to(1.0))
        result = await c.evaluate(df_ctx, "users_nulls")
        assert result.metric is not None

    @pytest.mark.asyncio()
    async def test_row_count_passes(self, df_ctx):
        """'users' has exactly 5 rows."""
        c = SizeConstraint(Assertion.equal_to(5.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 5.0

    @pytest.mark.asyncio()
    async def test_row_count_fails(self, df_ctx):
        c = SizeConstraint(Assertion.greater_than(100.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.FAILURE

    @pytest.mark.asyncio()
    async def test_column_count_passes(self, df_ctx):
        """'users' has 6 columns: id, name, email, age, score, city."""
        c = ColumnCountConstraint(Assertion.equal_to(6.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 6.0

    @pytest.mark.asyncio()
    async def test_column_count_fails(self, df_ctx):
        c = ColumnCountConstraint(Assertion.equal_to(10.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.FAILURE

    @pytest.mark.asyncio()
    async def test_existing_column_passes(self, df_ctx):
        c = ColumnExistsConstraint("email")
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 1.0

    @pytest.mark.asyncio()
    async def test_missing_column_fails(self, df_ctx):
        c = ColumnExistsConstraint("nonexistent")
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 0.0
        assert "nonexistent" in result.message

    @pytest.mark.asyncio()
    async def test_all_ages_positive(self, df_ctx):
        """All users have age > 0."""
        c = ComplianceConstraint("positive_age", '"age" > 0', Assertion.equal_to(1.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 1.0

    @pytest.mark.asyncio()
    async def test_some_ages_above_30(self, df_ctx):
        """Not all users are above 30 → compliance < 1."""
        c = ComplianceConstraint("age_above_30", '"age" > 30', Assertion.equal_to(1.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.FAILURE
        assert result.metric is not None
        assert result.metric < 1.0

    @pytest.mark.asyncio()
    async def test_unique_id_passes(self, df_ctx):
        """'id' is unique across all rows in 'users'."""
        c = UniquenessConstraint(["id"], threshold=1.0)
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 1.0

    @pytest.mark.asyncio()
    async def test_non_unique_city_fails(self, df_ctx):
        """'city' has duplicates (New York, London appear twice)."""
        c = UniquenessConstraint(["city"], threshold=1.0)
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.FAILURE
        assert result.metric is not None
        assert result.metric < 1.0

    @pytest.mark.asyncio()
    async def test_all_distinct_id(self, df_ctx):
        """'id' in 'users' is fully distinct: 5 distinct / 5 total = 1.0."""
        c = DistinctnessConstraint(["id"], Assertion.equal_to(1.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 1.0

    @pytest.mark.asyncio()
    async def test_low_distinctness_fails(self, df_ctx):
        """'category' in 'duplicates' has 3 distinct / 5 rows = 0.6."""
        c = DistinctnessConstraint(["category"], Assertion.greater_than(0.8))
        result = await c.evaluate(df_ctx, "duplicates")
        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == pytest.approx(0.6)

    @pytest.mark.asyncio()
    async def test_all_unique_ids(self, df_ctx):
        """All ids in 'users' appear exactly once → UVR = 1.0."""
        c = UniqueValueRatioConstraint(["id"], Assertion.equal_to(1.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 1.0

    @pytest.mark.asyncio()
    async def test_duplicated_category_value(self, df_ctx):
        """'category' in 'duplicates': A appears 2, B 2, C 1 → 1/3 unique groups."""
        c = UniqueValueRatioConstraint(["category"], Assertion.greater_than(0.5))
        result = await c.evaluate(df_ctx, "duplicates")
        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == pytest.approx(1 / 3, abs=0.01)

    @pytest.mark.asyncio()
    async def test_max_age(self, df_ctx):
        """Max age in 'users' is 35."""
        c = StatisticalConstraint("age", StatisticType.MAX, Assertion.equal_to(35.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 35.0

    @pytest.mark.asyncio()
    async def test_min_age(self, df_ctx):
        """Min age in 'users' is 25."""
        c = StatisticalConstraint("age", StatisticType.MIN, Assertion.equal_to(25.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 25.0

    @pytest.mark.asyncio()
    async def test_mean_age(self, df_ctx):
        """Mean age = (30+25+35+28+32)/5 = 30.0."""
        c = StatisticalConstraint("age", StatisticType.MEAN, Assertion.equal_to(30.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 30.0

    @pytest.mark.asyncio()
    async def test_sum_age(self, df_ctx):
        """Sum age = 30+25+35+28+32 = 150."""
        c = StatisticalConstraint("age", StatisticType.SUM, Assertion.equal_to(150.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 150.0

    @pytest.mark.asyncio()
    async def test_sum_too_high_fails(self, df_ctx):
        c = StatisticalConstraint("age", StatisticType.SUM, Assertion.greater_than(1000.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.FAILURE

    @pytest.mark.asyncio()
    async def test_max_length_name_passes(self, df_ctx):
        """Longest name in 'users' is 'Charlie' (7 chars) → max_length <= 10."""
        c = MaxLengthConstraint("name", Assertion.less_than_or_equal(10.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 7.0

    @pytest.mark.asyncio()
    async def test_max_length_too_short_fails(self, df_ctx):
        c = MaxLengthConstraint("name", Assertion.less_than(5.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.FAILURE

    @pytest.mark.asyncio()
    async def test_min_length_name_passes(self, df_ctx):
        """Shortest name in 'users' is 'Bob' or 'Eve' (3 chars) → min_length >= 3."""
        c = MinLengthConstraint("name", Assertion.greater_than_or_equal(3.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 3.0

    @pytest.mark.asyncio()
    async def test_min_length_too_long_fails(self, df_ctx):
        c = MinLengthConstraint("name", Assertion.greater_than(5.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.FAILURE

    @pytest.mark.asyncio()
    async def test_email_pattern_passes(self, df_ctx):
        """All emails in 'users' match a basic email pattern."""
        c = PatternMatchConstraint(
            "email",
            r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$",
            Assertion.equal_to(1.0),
        )
        result = await c.evaluate(df_ctx, "users")
        assert result.metric is not None

    @pytest.mark.asyncio()
    async def test_name_starts_with_letter(self, df_ctx):
        """All names start with a letter."""
        c = PatternMatchConstraint("name", r"^[A-Za-z]", Assertion.equal_to(1.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 1.0

    @pytest.mark.asyncio()
    async def test_email_format_type(self, df_ctx):
        """Use built-in EMAIL format on 'users' table."""
        c = FormatConstraint("email", FormatType.EMAIL, threshold=0.5)
        result = await c.evaluate(df_ctx, "users")
        assert result.metric is not None
        assert result.metric > 0.0

    @pytest.mark.asyncio()
    async def test_ipv4_format_fails_on_names(self, df_ctx):
        """Names are not IPv4 addresses."""
        c = FormatConstraint("name", FormatType.IPV4, threshold=1.0)
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 0.0

    @pytest.mark.asyncio()
    async def test_all_scores_positive(self, df_ctx):
        c = CustomSqlConstraint('"score" > 0', hint="all_scores_positive")
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 1.0

    @pytest.mark.asyncio()
    async def test_impossible_condition_fails(self, df_ctx):
        c = CustomSqlConstraint('"age" > 100', hint="impossible_age")
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 0.0

    @pytest.mark.asyncio()
    async def test_distinct_ids(self, df_ctx):
        """5 distinct ids in 'users'."""
        c = ApproxCountDistinctConstraint("id", Assertion.greater_than_or_equal(5.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric is not None
        assert result.metric >= 5.0

    @pytest.mark.asyncio()
    async def test_few_cities(self, df_ctx):
        """3 distinct cities (New York, London, Paris) — approx should be ~3."""
        c = ApproxCountDistinctConstraint("city", Assertion.between(2.0, 4.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_median_age(self, df_ctx):
        """Median (p50) of ages [25,28,30,32,35] = 30."""
        c = ApproxQuantileConstraint("age", 0.5, Assertion.between(28.0, 32.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_90th_percentile_score(self, df_ctx):
        """p90 of scores [78.2, 85.5, 88.0, 90.0, 92.1] should be high."""
        c = ApproxQuantileConstraint("score", 0.9, Assertion.greater_than(85.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS

    @pytest.mark.asyncio()
    async def test_perfect_positive_correlation(self, df_ctx):
        """x and y in 'correlated' are perfectly positively correlated (y=2x)."""
        c = CorrelationConstraint("x", "y", Assertion.greater_than(0.99))
        result = await c.evaluate(df_ctx, "correlated")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == pytest.approx(1.0, abs=0.01)

    @pytest.mark.asyncio()
    async def test_perfect_negative_correlation(self, df_ctx):
        """x and z in 'correlated' are perfectly negatively correlated (z = 12-2x)."""
        c = CorrelationConstraint("x", "z", Assertion.less_than(-0.99))
        result = await c.evaluate(df_ctx, "correlated")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == pytest.approx(-1.0, abs=0.01)

    @pytest.mark.asyncio()
    async def test_valid_references_pass(self, df_ctx):
        """All user_ids in 'orders' exist in 'users'."""
        c = ReferentialIntegrityConstraint(
            child_table="orders",
            child_column="user_id",
            parent_table="users",
            parent_column="id",
            assertion=Assertion.equal_to(1.0),
        )
        result = await c.evaluate(df_ctx, "orders")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 1.0

    @pytest.mark.asyncio()
    async def test_orphan_keys_fail(self, df_ctx):
        """'orders_orphan' has user_ids 99 & 100 not in 'users'."""
        c = ReferentialIntegrityConstraint(
            child_table="orders_orphan",
            child_column="user_id",
            parent_table="users",
            parent_column="id",
            assertion=Assertion.equal_to(1.0),
        )
        result = await c.evaluate(df_ctx, "orders_orphan")
        assert result.status == ConstraintStatus.FAILURE
        assert result.metric is not None
        assert result.metric < 1.0

    @pytest.mark.asyncio()
    async def test_same_row_count(self, df_ctx):
        """'users' and 'orders' both have 5 rows → ratio == 1.0."""
        c = RowCountMatchConstraint("users", "orders", Assertion.equal_to(1.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 1.0

    @pytest.mark.asyncio()
    async def test_different_row_count(self, df_ctx):
        """'users' (5 rows) vs 'users_b' (2 rows) → ratio = 2/5 = 0.4."""
        c = RowCountMatchConstraint("users", "users_b", Assertion.equal_to(1.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == pytest.approx(0.4)

    @pytest.mark.asyncio()
    async def test_matching_schemas(self, df_ctx):
        """'users' and 'users_b' have the same columns."""
        c = SchemaMatchConstraint("users", "users_b", Assertion.equal_to(1.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.SUCCESS
        assert result.metric == 1.0

    @pytest.mark.asyncio()
    async def test_different_schemas(self, df_ctx):
        """'users' and 'orders' have completely different schemas."""
        c = SchemaMatchConstraint("users", "orders", Assertion.equal_to(1.0))
        result = await c.evaluate(df_ctx, "users")
        assert result.status == ConstraintStatus.FAILURE
        assert result.metric == 0.0
