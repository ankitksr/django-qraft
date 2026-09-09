"""
Cost from usage: an optional resolver that turns recorded tokens into money.

No `QRAFT_PRICING` setting means no resolver, and every cost path answers
"unknown". With one, `record_usage()` prices each increment as it is written
and `cost()` sums the entries, re-pricing at read time what an older or
unknown table left unpriced. A configured price is an estimate of what the
provider will bill, never proof of it; the field names say so.
"""

from dataclasses import dataclass
from decimal import Decimal
from functools import lru_cache
from typing import Protocol

from django.conf import settings
from django.utils.module_loading import import_string

MILLION = Decimal(1_000_000)
DEFAULT_RESOLVER = "qraft.pricing.StaticTablePricing"

TOKEN_FIELDS = ("input_tokens", "output_tokens", "cached_input_tokens")


@dataclass(frozen=True)
class Price:
    """Per-million-token rates for one model."""

    input: Decimal
    output: Decimal
    cached_input: Decimal | None = None
    currency: str = "USD"
    revision: str | None = None
    # OpenAI reports cached tokens inside input tokens; a provider that
    # reports them separately sets this False and they are priced on top.
    cached_is_subset: bool = True

    def amount(self, input_tokens, output_tokens, cached_input_tokens) -> Decimal:
        input_tokens = Decimal(input_tokens or 0)
        output_tokens = Decimal(output_tokens or 0)
        cached = Decimal(cached_input_tokens or 0)
        cached_rate = self.cached_input if self.cached_input is not None else self.input
        uncached = input_tokens - cached if self.cached_is_subset else input_tokens
        total = (
            max(uncached, Decimal(0)) * self.input
            + cached * cached_rate
            + output_tokens * self.output
        )
        return total / MILLION


class PricingResolver(Protocol):
    def price(self, model: str, provider: str | None = None) -> Price | None: ...


class StaticTablePricing:
    """Prices from the `models` table in `QRAFT_PRICING`."""

    def __init__(self, config: dict | None = None):
        config = config if config is not None else _config() or {}
        self.currency = config.get("currency", "USD")
        self.revision = config.get("revision")
        self.models = config.get("models", {})

    def price(self, model: str, provider: str | None = None) -> Price | None:
        entry = self.models.get(model)
        if entry is None:
            return None
        cached = entry.get("cached_input")
        return Price(
            input=Decimal(str(entry["input"])),
            output=Decimal(str(entry["output"])),
            cached_input=Decimal(str(cached)) if cached is not None else None,
            currency=entry.get("currency", self.currency),
            revision=entry.get("revision", self.revision),
            cached_is_subset=entry.get("cached_is_subset", True),
        )


def _config() -> dict | None:
    config = getattr(settings, "QRAFT_PRICING", None)
    return config if isinstance(config, dict) else None


@lru_cache(maxsize=1)
def get_resolver() -> PricingResolver | None:
    """The configured resolver, or None when `QRAFT_PRICING` is absent."""
    config = _config()
    if config is None:
        return None
    resolver = import_string(config.get("resolver", DEFAULT_RESOLVER))
    return resolver(config) if resolver is StaticTablePricing else resolver()


def reset_resolver() -> None:
    get_resolver.cache_clear()


def _decimal(value) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (ArithmeticError, ValueError):
        return None


def usage_entry(fields: dict) -> dict | None:
    """
    One per-increment entry for a `record_usage()` call that names a model.

    Caller-supplied `cost` always wins for its entry; otherwise the resolver
    prices it, or the entry is left unpriced with `cost_source="none"`.
    """
    model = fields.get("model")
    if not model:
        return None
    provider = fields.get("provider")
    entry = {
        "model": model,
        "provider": provider,
        **{name: fields.get(name) for name in TOKEN_FIELDS},
        "estimated_cost": None,
        "currency": None,
        "pricing_revision": None,
        "cost_source": "none",
    }
    caller_cost = _decimal(fields.get("cost"))
    if caller_cost is not None:
        entry.update(
            estimated_cost=str(caller_cost),
            currency=fields.get("currency"),
            cost_source="caller",
        )
        return entry
    price = _lookup(model, provider)
    if price is not None:
        entry.update(
            estimated_cost=str(
                price.amount(
                    entry["input_tokens"],
                    entry["output_tokens"],
                    entry["cached_input_tokens"],
                )
            ),
            currency=price.currency,
            pricing_revision=price.revision,
            cost_source="resolver",
        )
    return entry


def _lookup(model, provider) -> Price | None:
    resolver = get_resolver()
    if resolver is None:
        return None
    return resolver.price(model, provider=provider)


@dataclass
class CostSummary:
    amount: Decimal
    currency: str | None
    # complete: every entry with tokens has a cost; partial: some; none: none
    coverage: str
    # False only when every costed entry came from the caller
    estimated: bool

    def as_dict(self) -> dict:
        return {
            "amount": str(self.amount),
            "currency": self.currency,
            "coverage": self.coverage,
            "estimated": self.estimated,
        }


def cost(usage: dict | None) -> CostSummary:
    """
    Sum the cost of a usage dict's entries.

    Entries recorded before a resolver existed, or under a model the table did
    not know, are priced at read time when the current table can, and marked
    estimated. A usage dict with no entries but a top-level `cost` (the 1.3
    convention) is reported as that amount, from the caller.
    """
    usage = usage or {}
    entries = usage.get("entries") or []
    if not entries:
        legacy = _decimal(usage.get("cost"))
        if legacy is None:
            return CostSummary(Decimal(0), None, "none", True)
        return CostSummary(legacy, usage.get("currency"), "complete", False)

    amount = Decimal(0)
    currency = None
    priced = unpriced = 0
    estimated = False
    for entry in entries:
        entry_cost = _decimal(entry.get("estimated_cost"))
        source = entry.get("cost_source", "none")
        if entry_cost is None:
            price = _lookup(entry.get("model"), entry.get("provider"))
            if price is not None:
                entry_cost = price.amount(
                    entry.get("input_tokens"),
                    entry.get("output_tokens"),
                    entry.get("cached_input_tokens"),
                )
                source = "resolver"
                currency = currency or price.currency
        if entry_cost is None:
            if any(entry.get(name) for name in TOKEN_FIELDS):
                unpriced += 1
            continue
        priced += 1
        amount += entry_cost
        currency = currency or entry.get("currency")
        estimated = estimated or source != "caller"

    if priced and not unpriced:
        coverage = "complete"
    elif priced:
        coverage = "partial"
    else:
        coverage = "none"
    return CostSummary(amount, currency, coverage, estimated or not priced)
