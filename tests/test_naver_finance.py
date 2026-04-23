from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path
import unittest

from show_me_the_per.naver_finance import parse_naver_finance_snapshot


class NaverFinanceTests(unittest.TestCase):
    def test_parse_snapshot_from_main_page_fixture(self) -> None:
        fixture = (
            Path(__file__).parent
            / "fixtures"
            / "naver_finance_main_005930.html"
        ).read_text(encoding="utf-8")

        snapshot = parse_naver_finance_snapshot(
            fixture,
            stock_code="005930",
            fetched_at=datetime(2026, 4, 22, 7, 30, 0),
        )

        self.assertEqual(snapshot.corp_name, "삼성전자")
        self.assertEqual(snapshot.market, "KOSPI")
        self.assertEqual(snapshot.base_date, "20260422")
        self.assertEqual(snapshot.close_price, Decimal("84500"))
        self.assertEqual(snapshot.market_cap, Decimal("504448000000000"))
        self.assertEqual(snapshot.per, Decimal("33.14"))
        self.assertEqual(snapshot.pbr, Decimal("3.25"))
        self.assertEqual(snapshot.roe, Decimal("10.85"))
        self.assertEqual(snapshot.eps, Decimal("6564"))


if __name__ == "__main__":
    unittest.main()
