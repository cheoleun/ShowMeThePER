from __future__ import annotations

import os
from decimal import Decimal
from unittest import TestCase
from unittest.mock import patch

from fastapi.testclient import TestClient

from show_me_the_per.models import DartCompany, FinancialStatementRow
from show_me_the_per.web import (
    create_app,
    default_end_year,
    render_metric_amount_chart,
    resolve_company_query,
)


class WebTests(TestCase):
    def test_home_renders_v2_analysis_toolbar(self) -> None:
        client = TestClient(create_app(FakeOpenDartClient))

        response = client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn("재무정보", response.text)
        self.assertIn("VS 기업비교", response.text)
        self.assertIn('id="analysis-form"', response.text)
        self.assertIn("loading-indicator", response.text)
        self.assertIn(str(default_end_year()), response.text)

    def test_analysis_requires_api_key(self) -> None:
        client = TestClient(create_app(FakeOpenDartClient))

        with patch.dict(os.environ, {}, clear=True):
            response = client.get(
                "/analysis",
                params={
                    "company_query": "Samsung Electronics",
                    "recent_years": "3",
                    "end_year": "2025",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("OPENDART_API_KEY", response.text)

    def test_analysis_collects_and_renders_v2_dashboard(self) -> None:
        client = TestClient(create_app(FakeOpenDartClient))

        with patch.dict(os.environ, {"OPENDART_API_KEY": "test-key"}):
            response = client.get(
                "/analysis",
                params={
                    "company_query": "Samsung Electronics",
                    "recent_years": "5",
                    "end_year": "2025",
                    "fs_div": "CFS",
                    "threshold_percent": "20",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("Samsung Electronics", response.text)
        self.assertIn("최근 구간 요약", response.text)
        self.assertIn("대표 차트", response.text)
        self.assertIn("이 기업으로 비교 시작", response.text)
        self.assertIn("성장률 필터 결과", response.text)
        self.assertIn("분기", response.text)
        self.assertIn("4분기 누적", response.text)
        self.assertIn("연간", response.text)
        self.assertIn("YoY 성장률", response.text)
        self.assertIn("QoQ 성장률", response.text)
        self.assertIn("<svg", response.text)

    def test_compare_empty_page_renders_form(self) -> None:
        client = TestClient(create_app(FakeOpenDartClient))

        response = client.get(
            "/compare",
            params={
                "primary_company_query": "Samsung Electronics",
                "recent_years": "5",
                "end_year": "2025",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("VS 기업비교", response.text)
        self.assertIn('id="compare-form"', response.text)
        self.assertIn("비교할 두 기업을 모두 입력해 주세요.", response.text)

    def test_compare_collects_and_renders_dashboard(self) -> None:
        client = TestClient(create_app(FakeOpenDartClient))

        with patch.dict(os.environ, {"OPENDART_API_KEY": "test-key"}):
            response = client.get(
                "/compare",
                params={
                    "primary_company_query": "Samsung Electronics",
                    "secondary_company_query": "Vinatac",
                    "recent_years": "5",
                    "end_year": "2025",
                    "fs_div": "CFS",
                    "threshold_percent": "20",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("Samsung Electronics", response.text)
        self.assertIn("Vinatac", response.text)
        self.assertIn("대표 비교 차트", response.text)
        self.assertIn("최근 값 비교", response.text)
        self.assertIn("같은 구간, 같은 지표를 한 축에서 비교합니다.", response.text)
        self.assertIn("<svg", response.text)

    def test_analysis_validation_errors_stay_in_browser(self) -> None:
        client = TestClient(create_app(FakeOpenDartClient))

        response = client.get(
            "/analysis",
            params={
                "company_query": "",
                "recent_years": "abc",
                "end_year": "2025",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("기업 이름을 입력해 주세요.", response.text)

    def test_analysis_company_lookup_failure_stays_in_browser(self) -> None:
        client = TestClient(create_app(FailingCompanyListClient))

        with patch.dict(os.environ, {"OPENDART_API_KEY": "test-key"}):
            response = client.get(
                "/analysis",
                params={
                    "company_query": "Samsung Electronics",
                    "recent_years": "2",
                    "end_year": "2025",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("기업 목록을 가져오는 중 오류가 발생했습니다.", response.text)
        self.assertIn("temporary company lookup failure", response.text)

    def test_analysis_financial_collection_failure_stays_in_browser(self) -> None:
        client = TestClient(create_app(FailingFinancialClient))

        with patch.dict(os.environ, {"OPENDART_API_KEY": "test-key"}):
            response = client.get(
                "/analysis",
                params={
                    "company_query": "Samsung Electronics",
                    "recent_years": "2",
                    "end_year": "2025",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("수집 오류", response.text)
        self.assertIn("temporary financial fetch failure", response.text)

    def test_resolve_company_query_accepts_name_stock_code_and_corp_code(self) -> None:
        companies = FakeOpenDartClient("test-key").fetch_companies()

        by_name = resolve_company_query(companies, "Samsung Electronics")
        by_stock_code = resolve_company_query(companies, "005930")
        by_corp_code = resolve_company_query(companies, "00126380")

        self.assertEqual(by_name.corp_code, "00126380")
        self.assertEqual(by_stock_code.corp_code, "00126380")
        self.assertEqual(by_corp_code.stock_code, "005930")

    def test_amount_chart_has_axes_tooltip_and_compact_amount_labels(self) -> None:
        chart = render_metric_amount_chart(
            "revenue",
            [
                {
                    "period": "2025Q2",
                    "values": {
                        "revenue": {
                            "amount": "1500000",
                            "growth_rate": "25.00",
                        }
                    },
                },
                {
                    "period": "2025Q1",
                    "values": {
                        "revenue": {
                            "amount": "1200000",
                            "growth_rate": "-5.00",
                        }
                    },
                },
            ],
            growth_label="YoY 성장률",
            include_qoq=True,
        )

        self.assertIn("rgb(62, 230, 165)", chart)
        self.assertIn("rgb(240, 81, 81)", chart)
        self.assertIn('data-axis="amount-left"', chart)
        self.assertIn('data-axis="growth-right"', chart)
        self.assertIn("<title>2025Q1", chart)
        self.assertIn("금액: 1,200,000", chart)
        self.assertIn(">1.2M</text>", chart)
        self.assertIn(">1.5M</text>", chart)
        self.assertIn("<polyline", chart)


class FakeOpenDartClient:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def fetch_companies(self) -> list[DartCompany]:
        return [
            DartCompany(
                corp_code="00126380",
                corp_name="Samsung Electronics",
                stock_code="005930",
                modify_date="20260419",
            ),
            DartCompany(
                corp_code="00888888",
                corp_name="Vinatac",
                stock_code="126340",
                modify_date="20260419",
            ),
        ]

    def fetch_major_accounts(
        self,
        corp_codes: list[str],
        business_year: str,
        report_code: str,
        fs_div: str | None = None,
        batch_size: int = 100,
    ) -> list[FinancialStatementRow]:
        corp_code = corp_codes[0]
        year = int(business_year)
        multiplier = Decimal("1") if corp_code == "00126380" else Decimal("0.35")
        annual_amounts = {
            2021: Decimal("70"),
            2022: Decimal("85"),
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
        ) * multiplier
        return [
            FinancialStatementRow(
                corp_code=corp_code,
                corp_name="Samsung Electronics"
                if corp_code == "00126380"
                else "Vinatac",
                stock_code="005930" if corp_code == "00126380" else "126340",
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


class FailingCompanyListClient(FakeOpenDartClient):
    def fetch_companies(self) -> list[DartCompany]:
        raise RuntimeError("temporary company lookup failure")


class FailingFinancialClient(FakeOpenDartClient):
    def fetch_major_accounts(
        self,
        corp_codes: list[str],
        business_year: str,
        report_code: str,
        fs_div: str | None = None,
        batch_size: int = 100,
    ) -> list[FinancialStatementRow]:
        raise RuntimeError("temporary financial fetch failure")


if __name__ == "__main__":
    import unittest

    unittest.main()
