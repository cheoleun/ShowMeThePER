from show_me_the_per.matching import match_listings_to_dart
from show_me_the_per.models import DartCompany, KrxListing
import unittest


def _listing(short_code: str, market: str = "KOSPI") -> KrxListing:
    return KrxListing(
        base_date="20260415",
        short_code=short_code,
        isin_code=f"KR7{short_code}000",
        market=market,
        item_name=f"Item {short_code}",
        corporation_registration_number=f"CRNO{short_code}",
        corporation_name=f"Corp {short_code}",
    )


def _company(corp_code: str, stock_code: str) -> DartCompany:
    return DartCompany(
        corp_code=corp_code,
        corp_name=f"Dart {stock_code}",
        stock_code=stock_code,
        modify_date="20240101",
    )


class MatchingTests(unittest.TestCase):
    def test_match_listings_to_dart_matches_by_stock_code(self) -> None:
        result = match_listings_to_dart(
            listings=[_listing("005930")],
            dart_companies=[_company("00126380", "005930")],
        )

        self.assertEqual(result.total_listings, 1)
        self.assertEqual(len(result.matched), 1)
        self.assertEqual(result.matched[0].corp_code, "00126380")
        self.assertEqual(result.matched[0].stock_code, "005930")

    def test_match_listings_to_dart_tracks_unmatched_and_ambiguous(self) -> None:
        result = match_listings_to_dart(
            listings=[_listing("000001"), _listing("000002")],
            dart_companies=[
                _company("00000001", "000002"),
                _company("00000002", "000002"),
            ],
        )

        self.assertEqual(len(result.matched), 0)
        self.assertEqual(len(result.unmatched_listings), 1)
        self.assertEqual(result.unmatched_listings[0].short_code, "000001")
        self.assertEqual(len(result.ambiguous_matches), 1)
        self.assertEqual(result.ambiguous_matches[0].listing.short_code, "000002")


if __name__ == "__main__":
    unittest.main()
