from __future__ import annotations

from dataclasses import dataclass


KOREAN_EQUITY_MARKETS = frozenset({"KOSPI", "KOSDAQ"})


def normalize_stock_code(value: str | None) -> str:
    if value is None:
        return ""

    code = value.strip().upper()
    if code.startswith("A") and len(code) == 7 and code[1:].isdigit():
        code = code[1:]

    if code.isdigit():
        return code.zfill(6)

    return code


@dataclass(frozen=True)
class KrxListing:
    base_date: str
    short_code: str
    isin_code: str
    market: str
    item_name: str
    corporation_registration_number: str
    corporation_name: str

    @property
    def normalized_short_code(self) -> str:
        return normalize_stock_code(self.short_code)

    @property
    def is_supported_market(self) -> bool:
        return self.market in KOREAN_EQUITY_MARKETS


@dataclass(frozen=True)
class DartCompany:
    corp_code: str
    corp_name: str
    stock_code: str
    modify_date: str

    @property
    def normalized_stock_code(self) -> str:
        return normalize_stock_code(self.stock_code)

    @property
    def is_listed(self) -> bool:
        return bool(self.normalized_stock_code)


@dataclass(frozen=True)
class MatchedCompany:
    corp_code: str
    corp_name: str
    stock_code: str
    market: str
    item_name: str
    isin_code: str
    corporation_registration_number: str
    corporation_name: str


@dataclass(frozen=True)
class AmbiguousMatch:
    listing: KrxListing
    candidates: tuple[DartCompany, ...]


@dataclass(frozen=True)
class MatchResult:
    matched: tuple[MatchedCompany, ...]
    unmatched_listings: tuple[KrxListing, ...]
    ambiguous_matches: tuple[AmbiguousMatch, ...]

    @property
    def total_listings(self) -> int:
        return (
            len(self.matched)
            + len(self.unmatched_listings)
            + len(self.ambiguous_matches)
        )
