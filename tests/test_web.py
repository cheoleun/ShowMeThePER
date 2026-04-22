from __future__ import annotations

import os
import tempfile
from decimal import Decimal
from unittest import TestCase
from unittest.mock import patch

from fastapi.testclient import TestClient

from show_me_the_per.krx import KrxStockPriceSnapshot
from show_me_the_per.models import DartCompany, FinancialStatementRow
from show_me_the_per.web import (
    create_app,
    default_end_year,
    render_compare_metric_chart,
    render_metric_amount_chart,
    resolve_company_query,
)


class WebTests(TestCase):
    def test_home_renders_v2_analysis_toolbar(self) -> None:
        client = TestClient(create_app(FakeOpenDartClient))

        response = client.get("/")

        self.assertEqual(response.status_code, 200)
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

        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {
                    "OPENDART_API_KEY": "test-key",
                    "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
                },
            ):
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
        self.assertIn("<svg", response.text)
        self.assertIn("/compare?primary_company_query=Samsung+Electronics", response.text)
        self.assertIn("OpenDART 신규 수집", response.text)

    def test_analysis_top_tabs_keep_company_context(self) -> None:
        client = TestClient(create_app(FakeOpenDartClient))

        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {
                    "OPENDART_API_KEY": "test-key",
                    "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
                },
            ):
                response = client.get(
                    "/analysis",
                    params={
                        "company_query": "Vinatac",
                        "recent_years": "10",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                        "tab": "overview",
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertIn(
            'class="top-tab is-active" href="/analysis?company_query=Vinatac&amp;recent_years=10&amp;end_year=2025&amp;fs_div=CFS&amp;threshold_percent=20&amp;tab=overview#overview-summary"',
            response.text,
        )
        self.assertIn(
            '/analysis?company_query=Vinatac&amp;recent_years=10&amp;end_year=2025&amp;fs_div=CFS&amp;threshold_percent=20&amp;tab=financials#financials-details',
            response.text,
        )
        self.assertIn(
            '/analysis?company_query=Vinatac&amp;recent_years=10&amp;end_year=2025&amp;fs_div=CFS&amp;threshold_percent=20&amp;tab=growth#growth-details',
            response.text,
        )
        self.assertIn(
            '/compare?primary_company_query=Vinatac&amp;recent_years=10&amp;end_year=2025&amp;fs_div=CFS&amp;threshold_percent=20',
            response.text,
        )

    def test_growth_tab_opens_growth_details_without_losing_data(self) -> None:
        client = TestClient(create_app(FakeOpenDartClient))

        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {
                    "OPENDART_API_KEY": "test-key",
                    "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
                },
            ):
                response = client.get(
                    "/analysis",
                    params={
                        "company_query": "Samsung Electronics",
                        "recent_years": "5",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                        "tab": "growth",
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertIn("Samsung Electronics", response.text)
        self.assertIn('<details class="panel" open>', response.text)

    def test_analysis_renders_eps_and_market_summary(self) -> None:
        client = TestClient(create_app(FakeOpenDartClient, FakeKrxStockPriceClient))

        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {
                    "OPENDART_API_KEY": "test-key",
                    "KRX_SERVICE_KEY": "krx-test-key",
                    "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
                },
            ):
                response = client.get(
                    "/analysis",
                    params={
                        "company_query": "Vinatac",
                        "recent_years": "5",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertIn("EPS", response.text)
        self.assertIn("KOSDAQ", response.text)
        self.assertIn("시가총액", response.text)
        self.assertIn("전일 종가", response.text)

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
        self.assertIn('id="compare-form"', response.text)

    def test_compare_collects_and_renders_dashboard(self) -> None:
        client = TestClient(create_app(FakeOpenDartClient, FakeKrxStockPriceClient))

        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {
                    "OPENDART_API_KEY": "test-key",
                    "KRX_SERVICE_KEY": "krx-test-key",
                    "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
                },
            ):
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
        self.assertIn("<svg", response.text)
        self.assertIn("KOSPI", response.text)
        self.assertIn("KOSDAQ", response.text)
        self.assertIn("시가총액", response.text)
        self.assertIn("전일 종가", response.text)
        self.assertNotIn("Samsung Electronics (005930)", response.text)
        self.assertNotIn("Vinatac (126340)", response.text)

    def test_compare_top_tabs_keep_both_companies(self) -> None:
        client = TestClient(create_app(FakeOpenDartClient))

        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {
                    "OPENDART_API_KEY": "test-key",
                    "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
                },
            ):
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
        self.assertIn(
            '/compare?primary_company_query=Samsung+Electronics&amp;secondary_company_query=Vinatac&amp;recent_years=5&amp;end_year=2025&amp;fs_div=CFS&amp;threshold_percent=20',
            response.text,
        )
        self.assertIn(
            '/analysis?company_query=Samsung+Electronics&amp;recent_years=5&amp;end_year=2025&amp;fs_div=CFS&amp;threshold_percent=20&amp;tab=financials#financials-details',
            response.text,
        )

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
        self.assertIn('class="notice error"', response.text)
        self.assertIn('id="analysis-form"', response.text)
        self.assertNotIn("<svg", response.text)

    def test_analysis_company_lookup_failure_stays_in_browser(self) -> None:
        client = TestClient(create_app(FailingCompanyListClient))

        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {
                    "OPENDART_API_KEY": "test-key",
                    "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
                },
            ):
                response = client.get(
                    "/analysis",
                    params={
                        "company_query": "Samsung Electronics",
                        "recent_years": "2",
                        "end_year": "2025",
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertIn("temporary company lookup failure", response.text)

    def test_analysis_financial_collection_failure_stays_in_browser(self) -> None:
        client = TestClient(create_app(FailingFinancialClient))

        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {
                    "OPENDART_API_KEY": "test-key",
                    "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
                },
            ):
                response = client.get(
                    "/analysis",
                    params={
                        "company_query": "Samsung Electronics",
                        "recent_years": "2",
                        "end_year": "2025",
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertIn("temporary financial fetch failure", response.text)

    def test_resolve_company_query_accepts_name_stock_code_and_corp_code(self) -> None:
        companies = FakeOpenDartClient("test-key").fetch_companies()

        by_name = resolve_company_query(companies, "Samsung Electronics")
        by_stock_code = resolve_company_query(companies, "005930")
        by_corp_code = resolve_company_query(companies, "00126380")

        self.assertEqual(by_name.corp_code, "00126380")
        self.assertEqual(by_stock_code.corp_code, "00126380")
        self.assertEqual(by_corp_code.stock_code, "005930")

    def test_quarterly_amount_chart_uses_full_ten_year_window_and_pastel_colors(
        self,
    ) -> None:
        chart = render_metric_amount_chart(
            "revenue",
            _build_quarterly_rows(2016, 2025),
            period_key="quarterly",
            growth_label="YoY growth",
            include_qoq=True,
        )

        self.assertIn("#a5b4fc", chart)
        self.assertIn("#fdba74", chart)
        self.assertIn("#86efac", chart)
        self.assertIn("#fde68a", chart)
        self.assertIn("#c4b5fd", chart)
        self.assertIn("#f9a8d4", chart)
        self.assertIn('stroke-width="1.6"', chart)
        self.assertIn('r="2.25"', chart)
        self.assertIn('data-axis="amount-left"', chart)
        self.assertIn('data-axis="growth-right"', chart)
        self.assertIn("<title>2025Q4", chart)
        self.assertIn("212,000", chart)
        self.assertIn(">2016</text>", chart)
        self.assertIn(">2025</text>", chart)
        self.assertNotIn(">212K</text>", chart)
        self.assertIn("<polyline", chart)

    def test_annual_amount_chart_uses_pastel_bar_color(self) -> None:
        chart = render_metric_amount_chart(
            "revenue",
            _build_annual_rows(2016, 2025),
            period_key="annual",
            growth_label="YoY growth",
            include_qoq=False,
        )

        self.assertIn(">2016</text>", chart)
        self.assertIn(">2025</text>", chart)
        self.assertIn("#bfdbfe", chart)
        self.assertIn("#c4b5fd", chart)

    def test_compare_quarterly_chart_renders_vertical_two_panel_layout(self) -> None:
        chart = render_compare_metric_chart(
            "revenue",
            _build_quarterly_rows(2016, 2025),
            _build_quarterly_rows(2016, 2025, multiplier=Decimal("0.7")),
            period_key="quarterly",
            primary_name="Samsung Electronics",
            secondary_name="Vinatac",
            title="Quarterly comparison",
        )

        self.assertIn("compare-chart-stack", chart)
        self.assertIn("compare-panel-title", chart)
        self.assertIn("Samsung Electronics", chart)
        self.assertIn("Vinatac", chart)
        self.assertGreaterEqual(chart.count("<svg"), 2)

    def test_analysis_uses_db_cache_when_switching_from_ten_years_to_five_years(
        self,
    ) -> None:
        CountingOpenDartClient.reset()
        client = TestClient(create_app(CountingOpenDartClient))

        with tempfile.TemporaryDirectory() as directory:
            env = {
                "OPENDART_API_KEY": "test-key",
                "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
            }
            with patch.dict(os.environ, env):
                first = client.get(
                    "/analysis",
                    params={
                        "company_query": "Samsung Electronics",
                        "recent_years": "10",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                    },
                )
                first_company_calls = CountingOpenDartClient.company_fetch_count
                first_financial_calls = len(CountingOpenDartClient.major_account_calls)

                second = client.get(
                    "/analysis",
                    params={
                        "company_query": "Samsung Electronics",
                        "recent_years": "5",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                    },
                )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(CountingOpenDartClient.company_fetch_count, first_company_calls)
        self.assertEqual(len(CountingOpenDartClient.major_account_calls), first_financial_calls)
        self.assertIn("DB 캐시 사용", second.text)

    def test_analysis_fetches_only_missing_latest_year_from_db_cache(self) -> None:
        CountingOpenDartClient.reset()
        client = TestClient(create_app(CountingOpenDartClient))

        with tempfile.TemporaryDirectory() as directory:
            env = {
                "OPENDART_API_KEY": "test-key",
                "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
            }
            with patch.dict(os.environ, env):
                first = client.get(
                    "/analysis",
                    params={
                        "company_query": "Samsung Electronics",
                        "recent_years": "2",
                        "end_year": "2024",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                    },
                )
                self.assertEqual(first.status_code, 200)
                CountingOpenDartClient.major_account_calls = []

                second = client.get(
                    "/analysis",
                    params={
                        "company_query": "Samsung Electronics",
                        "recent_years": "3",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                    },
                )

        self.assertEqual(second.status_code, 200)
        fetched_years = {
            business_year
            for business_year, _, _ in CountingOpenDartClient.major_account_calls
        }
        self.assertEqual(fetched_years, {"2025"})
        self.assertIn("최신 1개 연도 갱신", second.text)


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
            2015: Decimal("60"),
            2016: Decimal("65"),
            2017: Decimal("70"),
            2018: Decimal("78"),
            2019: Decimal("88"),
            2020: Decimal("95"),
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
        rows = [
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
        if report_code == "11011":
            eps_amounts = {
                2015: Decimal("120"),
                2016: Decimal("135"),
                2017: Decimal("148"),
                2018: Decimal("166"),
                2019: Decimal("182"),
                2020: Decimal("194"),
                2021: Decimal("143"),
                2022: Decimal("168"),
                2023: Decimal("205"),
                2024: Decimal("248"),
                2025: Decimal("310"),
            }
            rows.append(
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
                    account_id="ifrs-full_BasicEarningsLossPerShare",
                    account_name="Basic earnings per share",
                    current_term_name="Current",
                    current_amount=eps_amounts.get(year) * multiplier,
                    previous_term_name="Previous",
                    previous_amount=eps_amounts.get(year - 1),
                    before_previous_term_name="Before previous",
                    before_previous_amount=eps_amounts.get(year - 2),
                )
            )
        return rows


class FakeKrxStockPriceClient:
    def __init__(self, service_key: str) -> None:
        self.service_key = service_key

    def fetch_stock_price(
        self,
        stock_code: str,
        *,
        base_date: str,
    ) -> KrxStockPriceSnapshot:
        if stock_code == "126340":
            return KrxStockPriceSnapshot(
                base_date=base_date,
                stock_code="126340",
                item_name="Vinatac",
                market="KOSDAQ",
                close_price=Decimal("37100"),
                market_cap=Decimal("561080217600"),
                listed_stock_count=Decimal("15123456"),
            )
        return KrxStockPriceSnapshot(
            base_date=base_date,
            stock_code="005930",
            item_name="Samsung Electronics",
            market="KOSPI",
            close_price=Decimal("84500"),
            market_cap=Decimal("504448025000000"),
            listed_stock_count=Decimal("5969782550"),
        )


class CountingOpenDartClient(FakeOpenDartClient):
    company_fetch_count = 0
    major_account_calls: list[tuple[str, str, str]] = []

    @classmethod
    def reset(cls) -> None:
        cls.company_fetch_count = 0
        cls.major_account_calls = []

    def fetch_companies(self) -> list[DartCompany]:
        type(self).company_fetch_count += 1
        return super().fetch_companies()

    def fetch_major_accounts(
        self,
        corp_codes: list[str],
        business_year: str,
        report_code: str,
        fs_div: str | None = None,
        batch_size: int = 100,
    ) -> list[FinancialStatementRow]:
        type(self).major_account_calls.append((business_year, report_code, corp_codes[0]))
        return super().fetch_major_accounts(
            corp_codes,
            business_year,
            report_code,
            fs_div=fs_div,
            batch_size=batch_size,
        )


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


def _build_quarterly_rows(
    start_year: int,
    end_year: int,
    *,
    multiplier: Decimal = Decimal("1"),
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    base = Decimal("100000")
    for year in range(end_year, start_year - 1, -1):
        for quarter in range(4, 0, -1):
            amount = (
                base
                + Decimal(year - start_year) * Decimal("8000")
                + Decimal(quarter) * Decimal("10000")
            ) * multiplier
            growth = Decimal(year - start_year + quarter)
            rows.append(
                {
                    "period": f"{year}Q{quarter}",
                    "fiscal_year": year,
                    "fiscal_quarter": quarter,
                    "values": {
                        "revenue": {
                            "amount": str(amount),
                            "growth_rate": str(growth),
                            "qoq_growth_rate": str(growth - Decimal("1.5")),
                        }
                    },
                }
            )
    return rows


def _build_annual_rows(start_year: int, end_year: int) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for year in range(end_year, start_year - 1, -1):
        amount = Decimal("1000000") + Decimal(year - start_year) * Decimal("150000")
        rows.append(
            {
                "period": str(year),
                "fiscal_year": year,
                "fiscal_quarter": None,
                "values": {
                    "revenue": {
                        "amount": str(amount),
                        "growth_rate": str(Decimal(year - start_year + 3)),
                    }
                },
            }
        )
    return rows
