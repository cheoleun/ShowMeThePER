from __future__ import annotations

import os
from decimal import Decimal
from unittest import TestCase
from unittest.mock import patch

from fastapi.testclient import TestClient

from show_me_the_per.models import FinancialStatementRow
from show_me_the_per.web import create_app, default_end_year


class WebTests(TestCase):
    def test_home_renders_browser_form(self) -> None:
        client = TestClient(create_app(FakeOpenDartClient))

        response = client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn("OpenDART 고유번호", response.text)
        self.assertIn("조회", response.text)
        self.assertIn(str(default_end_year()), response.text)

    def test_analysis_requires_api_key(self) -> None:
        client = TestClient(create_app(FakeOpenDartClient))

        with patch.dict(os.environ, {}, clear=True):
            response = client.get(
                "/analysis",
                params={
                    "corp_code": "00126380",
                    "recent_years": "2",
                    "end_year": "2025",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("OPENDART_API_KEY", response.text)

    def test_analysis_collects_and_renders_financials(self) -> None:
        client = TestClient(create_app(FakeOpenDartClient))

        with patch.dict(os.environ, {"OPENDART_API_KEY": "test-key"}):
            response = client.get(
                "/analysis",
                params={
                    "corp_code": "00126380",
                    "recent_years": "2",
                    "end_year": "2025",
                    "fs_div": "CFS",
                    "threshold_percent": "20",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("Samsung Electronics", response.text)
        self.assertIn("연간 금액", response.text)
        self.assertIn("분기 금액", response.text)
        self.assertIn("성장률 필터 결과", response.text)
        self.assertIn("성장률 차트", response.text)
        self.assertIn("<svg", response.text)
        self.assertIn("25.00%", response.text)

    def test_analysis_validation_errors_stay_in_browser(self) -> None:
        client = TestClient(create_app(FakeOpenDartClient))

        response = client.get(
            "/analysis",
            params={
                "corp_code": "",
                "recent_years": "abc",
                "end_year": "2025",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("OpenDART 고유번호를 입력해 주세요.", response.text)


class FakeOpenDartClient:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def fetch_major_accounts(
        self,
        corp_codes: list[str],
        business_year: str,
        report_code: str,
        fs_div: str | None = None,
        batch_size: int = 100,
    ) -> list[FinancialStatementRow]:
        year = int(business_year)
        annual_amounts = {
            2023: Decimal("100"),
            2024: Decimal("120"),
            2025: Decimal("150"),
        }
        quarter_amounts = {
            2024: {
                "11013": Decimal("20"),
                "11012": Decimal("50"),
                "11014": Decimal("80"),
                "11011": Decimal("120"),
            },
            2025: {
                "11013": Decimal("30"),
                "11012": Decimal("65"),
                "11014": Decimal("100"),
                "11011": Decimal("150"),
            },
        }
        current_amount = quarter_amounts.get(year, {}).get(
            report_code,
            annual_amounts.get(year, Decimal("0")),
        )
        return [
            FinancialStatementRow(
                corp_code=corp_codes[0],
                corp_name="Samsung Electronics",
                stock_code="005930",
                business_year=business_year,
                report_code=report_code,
                fs_div=fs_div or "CFS",
                fs_name="Consolidated financial statements",
                statement_div="IS",
                statement_name="Income statement",
                account_id="ifrs-full_Revenue",
                account_name="Revenue",
                current_term_name="Current",
                current_amount=current_amount,
                previous_term_name="Previous",
                previous_amount=annual_amounts.get(year - 1),
                before_previous_term_name="Before previous",
                before_previous_amount=annual_amounts.get(year - 2),
            )
        ]


if __name__ == "__main__":
    import unittest

    unittest.main()
