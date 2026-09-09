"""Tests for qraft.pricing: resolver, the cached-token formula, coverage flags."""

from decimal import Decimal

import pytest

from qraft import pricing
from qraft.pricing import CostSummary, Price, StaticTablePricing, cost

TABLE = {
    "currency": "USD",
    "revision": "2026-09",
    "models": {
        # 0.05 / 0.40 / 0.005 per million.
        "mock-nano": {"input": 0.05, "output": 0.40, "cached_input": 0.005},
        "mock-split": {
            "input": 1.0,
            "output": 2.0,
            "cached_input": 0.1,
            "cached_is_subset": False,
        },
    },
}


@pytest.fixture(autouse=True)
def _fresh_resolver():
    pricing.reset_resolver()
    yield
    pricing.reset_resolver()


@pytest.fixture
def priced(settings):
    settings.QRAFT_PRICING = TABLE
    pricing.reset_resolver()
    return TABLE


class TestResolver:
    def test_absent_setting_means_no_resolver_and_no_cost(self, settings):
        settings.QRAFT_PRICING = None
        pricing.reset_resolver()

        assert pricing.get_resolver() is None
        assert pricing.usage_entry({"model": "mock-nano", "input_tokens": 1000}) == {
            "model": "mock-nano",
            "provider": None,
            "input_tokens": 1000,
            "output_tokens": None,
            "cached_input_tokens": None,
            "estimated_cost": None,
            "currency": None,
            "pricing_revision": None,
            "cost_source": "none",
        }

    def test_table_prices_a_known_model_and_declines_an_unknown_one(self, priced):
        resolver = StaticTablePricing()

        price = resolver.price("mock-nano")
        assert (price.input, price.output, price.cached_input) == (
            Decimal("0.05"),
            Decimal("0.40"),
            Decimal("0.005"),
        )
        assert (price.currency, price.revision) == ("USD", "2026-09")
        assert resolver.price("who-knows") is None

    def test_a_call_with_no_model_records_no_entry(self, priced):
        assert pricing.usage_entry({"input_tokens": 10}) is None


class TestFormula:
    def test_cached_tokens_are_a_subset_of_input_by_default(self):
        price = Price(
            input=Decimal("0.05"), output=Decimal("0.40"), cached_input=Decimal("0.005")
        )
        # 900 uncached @ 0.05 + 100 cached @ 0.005 + 500 out @ 0.40, per million.
        assert price.amount(1000, 500, 100) == Decimal("0.0002455")

    def test_a_provider_that_reports_them_separately_prices_them_on_top(self):
        price = Price(
            input=Decimal("1.0"),
            output=Decimal("2.0"),
            cached_input=Decimal("0.1"),
            cached_is_subset=False,
        )
        # All 1000 input at full rate, plus 100 cached at the cached rate.
        assert price.amount(1000, 0, 100) == Decimal("0.00101")

    def test_cached_exceeding_input_never_goes_negative(self):
        price = Price(
            input=Decimal("1.0"), output=Decimal("0"), cached_input=Decimal("0")
        )
        assert price.amount(10, 0, 999) == Decimal(0)

    def test_a_price_without_a_cached_rate_bills_cached_at_the_input_rate(self):
        price = Price(input=Decimal("2.0"), output=Decimal("0"))
        assert price.amount(1000, 0, 400) == Decimal("0.002")


class TestEntries:
    def test_the_resolver_prices_an_entry_and_stamps_its_revision(self, priced):
        entry = pricing.usage_entry(
            {
                "model": "mock-nano",
                "provider": "mockprovider",
                "input_tokens": 1000,
                "output_tokens": 500,
                "cached_input_tokens": 100,
            }
        )
        assert entry["cost_source"] == "resolver"
        assert entry["estimated_cost"] == "0.0002455"
        assert (entry["currency"], entry["pricing_revision"]) == ("USD", "2026-09")
        assert entry["provider"] == "mockprovider"

    def test_caller_supplied_cost_wins_over_the_table(self, priced):
        entry = pricing.usage_entry(
            {
                "model": "mock-nano",
                "input_tokens": 1000,
                "cost": Decimal("9.99"),
                "currency": "EUR",
            }
        )
        assert (entry["cost_source"], entry["estimated_cost"]) == ("caller", "9.99")
        assert entry["currency"] == "EUR"


class TestCostSummary:
    def _entry(self, **overrides):
        entry = {
            "model": "mock-nano",
            "provider": None,
            "input_tokens": 1000,
            "output_tokens": 0,
            "cached_input_tokens": 0,
            "estimated_cost": "0.00005",
            "currency": "USD",
            "pricing_revision": "2026-09",
            "cost_source": "resolver",
        }
        entry.update(overrides)
        return entry

    def test_complete_coverage_sums_as_decimal_and_reports_estimated(self, priced):
        summary = cost({"entries": [self._entry(), self._entry()]})

        assert isinstance(summary, CostSummary)
        assert summary.amount == Decimal("0.0001")
        assert (summary.currency, summary.coverage, summary.estimated) == (
            "USD",
            "complete",
            True,
        )

    def test_caller_only_entries_are_not_estimated(self, priced):
        summary = cost(
            {"entries": [self._entry(cost_source="caller", estimated_cost="1.25")]}
        )
        assert (summary.amount, summary.estimated) == (Decimal("1.25"), False)

    def test_an_unpriceable_entry_makes_coverage_partial(self, priced):
        summary = cost(
            {
                "entries": [
                    self._entry(),
                    self._entry(
                        model="who-knows", estimated_cost=None, cost_source="none"
                    ),
                ]
            }
        )
        assert summary.coverage == "partial"

    def test_an_entry_left_unpriced_is_priced_at_read_time(self, priced):
        """A table that did not know the model when the entry was written."""
        unpriced = self._entry(estimated_cost=None, cost_source="none")

        summary = cost({"entries": [unpriced]})
        assert summary.amount == Decimal("0.00005")
        assert (summary.coverage, summary.estimated) == ("complete", True)

    def test_no_entries_and_no_cost_is_zero_with_no_coverage(self, priced):
        summary = cost({})
        assert (summary.amount, summary.coverage, summary.currency) == (
            Decimal(0),
            "none",
            None,
        )

    def test_a_pre_entries_usage_dict_reports_its_own_cost(self, priced):
        summary = cost({"cost": "0.42", "currency": "USD", "input_tokens": 10})
        assert (summary.amount, summary.coverage, summary.estimated) == (
            Decimal("0.42"),
            "complete",
            False,
        )

    def test_as_dict_is_json_safe(self, priced):
        import json

        payload = cost({"entries": [self._entry()]}).as_dict()
        assert json.loads(json.dumps(payload)) == payload
        assert payload["amount"] == "0.00005"


@pytest.mark.django_db
class TestRecordedUsage:
    def test_entries_price_two_models_separately_and_aggregate_as_decimal(
        self, priced, qraft_task, qraft_task_attempt
    ):
        from qraft.context import _current_q2_task_id, aggregate_usage, record_usage

        _current_q2_task_id.set(qraft_task_attempt.q2_task_id)
        record_usage(model="mock-nano", input_tokens=1_000_000, output_tokens=0)
        record_usage(model="mock-split", input_tokens=1_000_000, output_tokens=0)

        qraft_task_attempt.refresh_from_db()
        entries = qraft_task_attempt.usage["entries"]
        # Summing the totals would bill one model's tokens at the other's rate;
        # the entries are what keep them apart.
        assert [Decimal(e["estimated_cost"]) for e in entries] == [
            Decimal("0.05"),
            Decimal("1"),
        ]
        assert qraft_task_attempt.usage["input_tokens"] == 2_000_000

        totals = aggregate_usage(qraft_task)
        summary = totals["cost_summary"]
        assert Decimal(summary["amount"]) == Decimal("1.05")
        assert (summary["currency"], summary["coverage"], summary["estimated"]) == (
            "USD",
            "complete",
            True,
        )

    def test_no_pricing_configured_leaves_the_aggregate_uncosted(
        self, settings, qraft_task, qraft_task_attempt
    ):
        from qraft.context import _current_q2_task_id, aggregate_usage, record_usage

        settings.QRAFT_PRICING = None
        pricing.reset_resolver()
        _current_q2_task_id.set(qraft_task_attempt.q2_task_id)
        record_usage(model="mock-nano", input_tokens=1_000_000)

        totals = aggregate_usage(qraft_task)
        assert totals["cost_summary"]["coverage"] == "none"
        assert Decimal(totals["cost_summary"]["amount"]) == Decimal(0)
