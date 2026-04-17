from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation


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


@dataclass(frozen=True)
class FinancialStatementRow:
    corp_code: str
    corp_name: str
    stock_code: str
    business_year: str
    report_code: str
    fs_div: str
    fs_name: str
    statement_div: str
    statement_name: str
    account_id: str
    account_name: str
    current_term_name: str
    current_amount: Decimal | None
    previous_term_name: str
    previous_amount: Decimal | None
    before_previous_term_name: str
    before_previous_amount: Decimal | None


def parse_decimal_amount(value: str | None) -> Decimal | None:
    if value is None:
        return None

    normalized = value.strip().replace(",", "")
    if not normalized or normalized == "-":
        return None

    try:
        return Decimal(normalized)
    except InvalidOperation:
        return None
