from __future__ import annotations

from collections import defaultdict

from .models import (
    AmbiguousMatch,
    DartCompany,
    KrxListing,
    MatchedCompany,
    MatchResult,
)


def match_listings_to_dart(
    listings: list[KrxListing],
    dart_companies: list[DartCompany],
) -> MatchResult:
    dart_by_stock_code: dict[str, list[DartCompany]] = defaultdict(list)
    for company in dart_companies:
        if company.is_listed:
            dart_by_stock_code[company.normalized_stock_code].append(company)

    matched: list[MatchedCompany] = []
    unmatched: list[KrxListing] = []
    ambiguous: list[AmbiguousMatch] = []

    for listing in listings:
        candidates = dart_by_stock_code.get(listing.normalized_short_code, [])
        if not candidates:
            unmatched.append(listing)
            continue

        if len(candidates) > 1:
            ambiguous.append(AmbiguousMatch(listing=listing, candidates=tuple(candidates)))
            continue

        company = candidates[0]
        matched.append(
            MatchedCompany(
                corp_code=company.corp_code,
                corp_name=company.corp_name,
                stock_code=company.normalized_stock_code,
                market=listing.market,
                item_name=listing.item_name,
                isin_code=listing.isin_code,
                corporation_registration_number=(
                    listing.corporation_registration_number
                ),
                corporation_name=listing.corporation_name,
            )
        )

    return MatchResult(
        matched=tuple(matched),
        unmatched_listings=tuple(unmatched),
        ambiguous_matches=tuple(ambiguous),
    )
