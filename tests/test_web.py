from __future__ import annotations

import os
import tempfile
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from fastapi.testclient import TestClient

from show_me_the_per.krx import KrxApiError, KrxStockPriceSnapshot
from show_me_the_per.models import DartCompany, FinancialStatementRow, KrxListing
from show_me_the_per.storage import (
    create_refresh_job,
    read_company_master_entries,
    read_financial_period_values_from_database,
    read_refresh_job,
    read_refresh_job_items,
    store_opendart_api_key,
    store_company_master_entries,
    summarize_database,
)
from show_me_the_per.web import (
    _format_won,
    _web_cache_database_path,
    create_app,
    default_end_year,
    render_compare_metric_chart,
    render_metric_amount_chart,
    resolve_company_query,
)


class WebTests(TestCase):
    def test_format_won_keeps_integer_prices(self) -> None:
        self.assertEqual(_format_won("1342000"), "1,342,000원")
        self.assertEqual(_format_won("37100"), "37,100원")
        self.assertEqual(_format_won("1000"), "1,000원")
        self.assertEqual(_format_won("0"), "0원")
        self.assertEqual(_format_won(None), "-")

    def test_default_web_cache_path_uses_user_local_cache_directory(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            path = _web_cache_database_path("CFS")

        normalized = str(path).replace("\\", "/")
        self.assertTrue(normalized.endswith("/show-me-the-per-cfs.sqlite3"))
        self.assertNotIn("/data/web-cache/", normalized)

    def test_web_cache_path_respects_environment_override(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {"SHOW_ME_THE_PER_WEB_CACHE_DIR": directory},
                clear=True,
            ):
                path = _web_cache_database_path("CFS")

        self.assertEqual(
            path,
            Path(directory) / "show-me-the-per-cfs.sqlite3",
        )

    def test_home_renders_v2_analysis_toolbar(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

        response = client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn('id="analysis-form"', response.text)
        self.assertIn("loading-indicator", response.text)
        self.assertIn(str(default_end_year()), response.text)

    def test_analysis_requires_api_key(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

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
        self.assertIn("OpenDART 키를 먼저 설정해 주세요.", response.text)

    def test_analysis_collects_and_renders_v2_dashboard(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

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
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

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
            'class="top-tab is-active" href="/analysis?company_query=Vinatac&amp;recent_years=10&amp;end_year=2025&amp;fs_div=CFS&amp;threshold_percent=20&amp;tab=financials#financials-details"',
            response.text,
        )
        self.assertIn(
            '/analysis?company_query=Vinatac&amp;recent_years=10&amp;end_year=2025&amp;fs_div=CFS&amp;threshold_percent=20&amp;tab=financials#financials-details',
            response.text,
        )
        self.assertIn(
            '/compare?primary_company_query=Vinatac&amp;recent_years=10&amp;end_year=2025&amp;fs_div=CFS&amp;threshold_percent=20',
            response.text,
        )
        self.assertIn('/ranking?', response.text)
        self.assertIn('/db-update?', response.text)
        self.assertIn(">기업필터</a>", response.text)
        self.assertIn(">DB 업데이트</a>", response.text)
        self.assertIn('threshold_percent=20', response.text)
        self.assertNotIn("데이터/API 설정", response.text)
        self.assertNotIn("OpenDART 키 관리", response.text)
        self.assertNotIn(">요약</a>", response.text)
        self.assertNotIn("tab=overview#overview-summary", response.text)
        self.assertNotIn("tab=growth#growth-details", response.text)

    def test_analysis_refresh_link_keeps_current_query_state(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

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
                        "recent_years": "10",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                        "tab": "growth",
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertIn(
            '/analysis?company_query=Samsung+Electronics&amp;recent_years=10&amp;end_year=2025&amp;fs_div=CFS&amp;threshold_percent=20&amp;tab=financials&amp;refresh=1#financials-details',
            response.text,
        )

    def test_growth_tab_url_normalizes_to_financials_without_breaking_page(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

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
        self.assertIn('class="top-tab is-active"', response.text)
        self.assertIn("tab=financials#financials-details", response.text)
        self.assertIn('class="growth-detail-chart amount-chart"', response.text)
        self.assertIn('class="growth-table"', response.text)
        self.assertIn('stroke="#c4b5fd"', response.text)
        self.assertIn('stroke="#ddd6fe"', response.text)

    def test_analysis_renders_eps_and_market_summary(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

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
        self.assertIn('data-metric-toggle="eps"', response.text)
        self.assertIn('data-metric-periods="annual"', response.text)
        self.assertIn("시가총액", response.text)
        self.assertIn("전일 종가", response.text)
        self.assertIn("전일 종가 37,100원", response.text)

    def test_analysis_uses_valuation_snapshot_when_krx_key_is_missing(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

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
                        "recent_years": "5",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertIn("KOSDAQ", response.text)
        self.assertIn("EPS", response.text)
        self.assertNotIn(">PER ", response.text)
        self.assertNotIn(">PBR ", response.text)
        self.assertNotIn(">ROE ", response.text)

    def test_analysis_falls_back_to_valuation_snapshot_when_krx_lookup_fails(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                MissingKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

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
        self.assertIn("KOSDAQ", response.text)
        self.assertIn("EPS", response.text)
        self.assertNotIn(">PER ", response.text)
        self.assertNotIn(">PBR ", response.text)
        self.assertNotIn(">ROE ", response.text)

    def test_compare_empty_page_renders_form(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

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
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

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
        self.assertIn("EPS", response.text)
        self.assertIn('data-metric-toggle="eps"', response.text)
        self.assertIn('data-metric-periods="annual"', response.text)
        self.assertNotIn("Samsung Electronics (005930)", response.text)
        self.assertNotIn("Vinatac (126340)", response.text)

    def test_dashboard_metric_script_resets_eps_outside_annual_period(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

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
        self.assertIn('data-metric-toggle="eps" data-metric-periods="annual"', response.text)
        self.assertIn("button.hidden = !allowed;", response.text)
        self.assertIn("allowedMetricButtons[0]", response.text)

    def test_compare_top_tabs_keep_both_companies(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

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
        self.assertIn("/db-update?", response.text)
        self.assertIn(">기업필터</a>", response.text)
        self.assertIn(">DB 업데이트</a>", response.text)
        self.assertNotIn("데이터/API 설정", response.text)
        self.assertNotIn("OpenDART 키 관리", response.text)
        self.assertNotIn(">요약</a>", response.text)
        self.assertNotIn(">성장률</a>", response.text)

    def test_analysis_validation_errors_stay_in_browser(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

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
        client = TestClient(
            create_app(
                FailingCompanyListClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

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
        client = TestClient(
            create_app(
                FailingFinancialClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

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
        client = TestClient(
            create_app(
                CountingOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

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
        client = TestClient(
            create_app(
                CountingOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

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
                        "recent_years": "2",
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

    def test_refresh_replaces_corrupted_company_cache(self) -> None:
        seed_client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )
        refresh_client = TestClient(
            create_app(
                CorrectedSamsungOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            env = {
                "OPENDART_API_KEY": "test-key",
                "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
            }
            with patch.dict(os.environ, env):
                seeded = seed_client.get(
                    "/analysis",
                    params={
                        "company_query": "Samsung Electronics",
                        "recent_years": "10",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                    },
                )
                database_path = Path(directory) / "show-me-the-per-cfs.sqlite3"
                self.assertEqual(_read_annual_amount(database_path, "00126380", 2019), Decimal("88"))

                refreshed = refresh_client.get(
                    "/analysis",
                    params={
                        "company_query": "Samsung Electronics",
                        "recent_years": "10",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                        "refresh": "1",
                    },
                )
                self.assertEqual(
                    _read_annual_amount(database_path, "00126380", 2019),
                    Decimal("230400881000000"),
                )

                self.assertEqual(seeded.status_code, 200)
                self.assertEqual(refreshed.status_code, 200)
                self.assertIn("OpenDART 강제 재수집", refreshed.text)

    def test_refresh_failure_keeps_existing_company_cache(self) -> None:
        seed_client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )
        failing_refresh_client = TestClient(
            create_app(
                FailingFinancialClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            env = {
                "OPENDART_API_KEY": "test-key",
                "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
            }
            with patch.dict(os.environ, env):
                seeded = seed_client.get(
                    "/analysis",
                    params={
                        "company_query": "Samsung Electronics",
                        "recent_years": "10",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                    },
                )
                database_path = Path(directory) / "show-me-the-per-cfs.sqlite3"
                before_amount = _read_annual_amount(database_path, "00126380", 2019)
                failed = failing_refresh_client.get(
                    "/analysis",
                    params={
                        "company_query": "Samsung Electronics",
                        "recent_years": "10",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                        "refresh": "1",
                    },
                )
                self.assertEqual(seeded.status_code, 200)
                self.assertEqual(failed.status_code, 200)
                self.assertIn("temporary financial fetch failure", failed.text)
                self.assertEqual(before_amount, Decimal("88"))
                self.assertEqual(
                    _read_annual_amount(database_path, "00126380", 2019),
                    Decimal("88"),
                )

    def test_ranking_page_filters_from_db_cache(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

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
                        "recent_years": "3",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                    },
                )
                second = client.get(
                    "/analysis",
                    params={
                        "company_query": "Vinatac",
                        "recent_years": "2",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                    },
                )

            self.assertEqual(first.status_code, 200)
            self.assertEqual(second.status_code, 200)

            with patch.dict(
                os.environ,
                {"SHOW_ME_THE_PER_WEB_CACHE_DIR": directory},
                clear=True,
            ):
                response = client.get(
                    "/ranking",
                    params={
                        "recent_years": "2",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertIn("/ranking?", response.text)
        self.assertIn("현재 조건을 통과한 기업이 없습니다", response.text)
        self.assertNotIn('name="max_per"', response.text)
        self.assertNotIn('name="max_pbr"', response.text)
        self.assertNotIn('name="min_roe"', response.text)
        self.assertNotIn("<th>PER</th>", response.text)
        self.assertNotIn("<th>PBR</th>", response.text)
        self.assertNotIn("<th>ROE</th>", response.text)

    def test_ranking_page_limits_rendered_rows_for_browser_safety(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            env = {
                "OPENDART_API_KEY": "test-key",
                "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
            }
            with patch.dict(os.environ, env):
                client.get(
                    "/analysis",
                    params={
                        "company_query": "Samsung Electronics",
                        "recent_years": "3",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                    },
                )
                client.get(
                    "/analysis",
                    params={
                        "company_query": "Vinatac",
                        "recent_years": "3",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                    },
                )

            with patch.dict(
                os.environ,
                {"SHOW_ME_THE_PER_WEB_CACHE_DIR": directory},
                clear=True,
            ):
                response = client.get(
                    "/ranking",
                    params={
                        "display_limit": "1",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertIn("현재 조건을 통과한 기업이 없습니다", response.text)
        self.assertIn('name="display_limit"', response.text)
        self.assertNotIn("Vinatac", response.text)
        self.assertNotIn("미통과", response.text)
        self.assertNotIn("<th>통과</th>", response.text)

    def test_ranking_page_renders_growth_condition_matrix_without_update_panel(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {"SHOW_ME_THE_PER_WEB_CACHE_DIR": directory},
                clear=True,
            ):
                response = client.get("/ranking")

        self.assertEqual(response.status_code, 200)
        self.assertIn('value="annual_yoy:revenue"', response.text)
        self.assertIn('value="quarterly_qoq:net_income"', response.text)
        self.assertIn('name="growth_condition_key"', response.text)
        self.assertIn('name="growth_period__annual_yoy__revenue"', response.text)
        self.assertIn('value="3"', response.text)
        self.assertIn('name="growth_period__quarterly_qoq__net_income"', response.text)
        self.assertIn('value="12"', response.text)
        self.assertIn('name="display_limit"', response.text)
        self.assertIn(">100개</option>", response.text)
        self.assertIn("기업필터", response.text)
        self.assertIn(">DB 업데이트</a>", response.text)
        self.assertNotIn("데이터/API 설정", response.text)
        self.assertNotIn("OpenDART 키 관리", response.text)
        self.assertNotIn('id="shared-settings-panel"', response.text)
        self.assertIn("DB 업데이트는 별도 페이지에서 관리합니다.", response.text)
        self.assertIn('class="ranking-action-row"', response.text)
        self.assertIn("전체 최소 성장률", response.text)
        self.assertNotIn("KRX 연결 점검 실행", response.text)
        self.assertNotIn("회사 목록 동기화", response.text)
        self.assertNotIn("전체 DB 초기화", response.text)
        self.assertNotIn("krx-diagnostic-result", response.text)
        self.assertNotIn(">요약</a>", response.text)
        self.assertNotIn('class="top-tab">성장률</a>', response.text)

    def test_db_update_page_renders_operational_panel(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {"SHOW_ME_THE_PER_WEB_CACHE_DIR": directory},
                clear=True,
            ):
                response = client.get("/db-update")

        self.assertEqual(response.status_code, 200)
        self.assertIn("DB 업데이트", response.text)
        self.assertIn("KRX 연결 점검 실행", response.text)
        self.assertIn("회사 목록 동기화", response.text)
        self.assertIn("전체 DB 초기화", response.text)
        self.assertIn("상장사 대상 목록 생성", response.text)
        self.assertIn("OpenDART 키 변경은 데이터/API 설정에서 관리합니다.", response.text)
        self.assertIn("데이터/API 설정", response.text)
        self.assertIn("OpenDART 키 관리", response.text)
        self.assertIn("krx-diagnostic-result", response.text)
        self.assertIn('id="refresh-status-panel"', response.text)
        self.assertIn("DB 업데이트 대기 중", response.text)
        self.assertIn("window.confirm(", response.text)

    def test_ranking_company_master_sync_returns_korean_message_on_403(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
                ForbiddenKrxListingClient,
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {
                    "OPENDART_API_KEY": "test-key",
                    "KRX_SERVICE_KEY": "krx-test-key",
                    "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
                },
            ):
                response = client.post(
                    "/ranking/company-master/sync",
                    params={"fs_div": "CFS"},
                    json={},
                )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["status"], "error")
        self.assertIn("KRX 회사 목록 조회가 403으로 거부되었습니다.", response.json()["message"])
        self.assertIn("KRX_SERVICE_KEY", response.json()["message"])

    def test_ranking_page_supports_repeated_growth_conditions(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {
                    "OPENDART_API_KEY": "test-key",
                    "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
                },
            ):
                client.get(
                    "/analysis",
                    params={
                        "company_query": "Samsung Electronics",
                        "recent_years": "3",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                    },
                )
                client.get(
                    "/analysis",
                    params={
                        "company_query": "Vinatac",
                        "recent_years": "3",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                    },
                )

            with patch.dict(
                os.environ,
                {"SHOW_ME_THE_PER_WEB_CACHE_DIR": directory},
                clear=True,
            ):
                response = client.get(
                    "/ranking",
                    params=[
                        ("end_year", "2025"),
                        ("fs_div", "CFS"),
                        ("threshold_percent", "20"),
                        ("sort_by", "overall_minimum_growth_rate"),
                        ("growth_condition", "annual_yoy:revenue"),
                        ("growth_condition", "quarterly_yoy:operating_income"),
                    ],
                )

        self.assertEqual(response.status_code, 200)
        self.assertIn('value="annual_yoy:revenue" checked', response.text)
        self.assertIn('value="quarterly_yoy:operating_income" checked', response.text)
        self.assertIn("현재 조건을 통과한 기업이 없습니다", response.text)
        self.assertIn("growth_condition=annual_yoy%3Arevenue%3A3", str(response.url))
        self.assertIn(
            "growth_condition=quarterly_yoy%3Aoperating_income%3A12",
            str(response.url),
        )

    def test_ranking_page_uses_condition_specific_period_inputs(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {
                    "OPENDART_API_KEY": "test-key",
                    "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
                },
            ):
                client.get(
                    "/analysis",
                    params={
                        "company_query": "Samsung Electronics",
                        "recent_years": "3",
                        "end_year": "2025",
                        "fs_div": "CFS",
                        "threshold_percent": "20",
                    },
                )

            with patch.dict(
                os.environ,
                {"SHOW_ME_THE_PER_WEB_CACHE_DIR": directory},
                clear=True,
            ):
                response = client.get(
                    "/ranking",
                    params=[
                        ("end_year", "2025"),
                        ("fs_div", "CFS"),
                        ("threshold_percent", "20"),
                        ("growth_condition_key", "annual_yoy:revenue"),
                        ("growth_period__annual_yoy__revenue", "1"),
                    ],
                )

        self.assertEqual(response.status_code, 200)
        self.assertIn("최근 1년", response.text)
        self.assertIn("조건에 맞는 기업", response.text)
        self.assertNotIn("성장률 랭킹", response.text)
        self.assertIn("growth_condition=annual_yoy%3Arevenue%3A1", str(response.url))

    def test_ranking_update_job_requires_company_master_sync(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
                FakeKrxListingClient,
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {
                    "OPENDART_API_KEY": "test-key",
                    "KRX_SERVICE_KEY": "krx-test-key",
                    "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
                },
            ):
                response = client.post(
                    "/ranking/update-jobs",
                    params={"fs_div": "CFS"},
                    json={
                        "scope": "ALL",
                        "fs_div": "CFS",
                        "year_from": 2024,
                        "year_to": 2025,
                        "batch_size": 25,
                    },
                )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["status"], "requires_company_sync")

    def test_ranking_company_master_sync_and_job_lifecycle(self) -> None:
        RecordingKrxListingClient.called_base_dates = []
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
                RecordingKrxListingClient,
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "show-me-the-per-cfs.sqlite3"
            with patch.dict(
                os.environ,
                {
                    "OPENDART_API_KEY": "test-key",
                    "KRX_SERVICE_KEY": "krx-test-key",
                    "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
                },
            ):
                sync_response = client.post(
                    "/ranking/company-master/sync",
                    params={"fs_div": "CFS"},
                    json={},
                )
                create_response = client.post(
                    "/ranking/update-jobs",
                    params={"fs_div": "CFS"},
                    json={
                        "scope": "ALL",
                        "fs_div": "CFS",
                        "year_from": 2024,
                        "year_to": 2025,
                        "batch_size": 1,
                    },
                )

                job_id = int(create_response.json()["job"]["id"])
                status_response = client.get(
                    f"/ranking/update-jobs/{job_id}",
                    params={"fs_div": "CFS"},
                )
                batch_response = client.post(
                    f"/ranking/update-jobs/{job_id}/run-next-batch",
                    params={"fs_div": "CFS"},
                )
                pause_response = client.post(
                    f"/ranking/update-jobs/{job_id}/pause",
                    params={"fs_div": "CFS"},
                )
                resume_response = client.post(
                    f"/ranking/update-jobs/{job_id}/resume",
                    params={"fs_div": "CFS"},
                )

            entries = read_company_master_entries(database_path)
            job = read_refresh_job(database_path, job_id=job_id)
            success_items = read_refresh_job_items(
                database_path,
                job_id=job_id,
                statuses=["success"],
            )

        self.assertEqual(sync_response.status_code, 200)
        self.assertEqual(sync_response.json()["company_master_status"]["count"], 2)
        self.assertTrue(sync_response.json()["krx_base_date"])
        self.assertEqual(sync_response.json()["krx_listing_count"], 2)
        self.assertEqual(sync_response.json()["dart_company_count"], 2)
        self.assertEqual(
            sync_response.json()["krx_base_date"],
            RecordingKrxListingClient.called_base_dates[0],
        )
        self.assertEqual(len(entries), 2)
        self.assertEqual(create_response.status_code, 200)
        self.assertEqual(create_response.json()["job"]["total_companies"], 2)
        self.assertEqual(create_response.json()["job"]["estimated_remaining_batches"], 1)
        self.assertEqual(create_response.json()["job"]["next_pending_corp_name"], "Samsung Electronics")
        self.assertEqual(status_response.status_code, 200)
        self.assertEqual(status_response.json()["job"]["status"], "running")
        self.assertEqual(batch_response.status_code, 200)
        self.assertEqual(batch_response.json()["job"]["completed_companies"], 2)
        self.assertEqual(batch_response.json()["job"]["remaining_companies"], 0)
        self.assertEqual(batch_response.json()["job"]["estimated_remaining_batches"], 0)
        self.assertEqual(len(success_items), 2)
        self.assertEqual(pause_response.status_code, 200)
        self.assertEqual(pause_response.json()["job"]["status"], "paused")
        self.assertEqual(resume_response.status_code, 200)
        self.assertEqual(resume_response.json()["job"]["status"], "running")
        self.assertIsNotNone(job)
        self.assertEqual(job["pending_companies"], 0)

    def test_ranking_company_master_sync_skips_same_seoul_day(self) -> None:
        client = TestClient(
            create_app(
                FailingCompanyListClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
                FakeKrxListingClient,
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "show-me-the-per-cfs.sqlite3"
            store_company_master_entries(
                database_path,
                [
                    {
                        "corp_code": "00126380",
                        "corp_name": "Samsung Electronics",
                        "stock_code": "005930",
                        "market": "KOSPI",
                        "item_name": "Samsung Electronics",
                        "modify_date": "20260422",
                        "matched_at": "2026-04-22T16:30:00Z",
                    }
                ],
            )
            with patch.dict(
                os.environ,
                {"SHOW_ME_THE_PER_WEB_CACHE_DIR": directory},
                clear=True,
            ):
                response = client.post(
                    "/ranking/company-master/sync",
                    params={"fs_div": "CFS"},
                    json={},
                )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["skipped_today"])
        self.assertIn("오늘 이미 동기화됨", response.json()["message"])

    def test_ranking_run_next_batch_marks_no_data_as_skipped(self) -> None:
        client = TestClient(
            create_app(
                NoDataOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
                FakeKrxListingClient,
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "show-me-the-per-cfs.sqlite3"
            with patch.dict(
                os.environ,
                {
                    "OPENDART_API_KEY": "test-key",
                    "KRX_SERVICE_KEY": "krx-test-key",
                    "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
                },
            ):
                client.post("/ranking/company-master/sync", params={"fs_div": "CFS"}, json={})
                create_response = client.post(
                    "/ranking/update-jobs",
                    params={"fs_div": "CFS"},
                    json={
                        "scope": "ALL",
                        "fs_div": "CFS",
                        "year_from": 2024,
                        "year_to": 2025,
                        "batch_size": 25,
                    },
                )
                job_id = int(create_response.json()["job"]["id"])
                batch_response = client.post(
                    f"/ranking/update-jobs/{job_id}/run-next-batch",
                    params={"fs_div": "CFS"},
                )
            skipped_items = read_refresh_job_items(
                database_path,
                job_id=job_id,
                statuses=["skipped"],
            )

        self.assertEqual(batch_response.status_code, 200)
        self.assertEqual(batch_response.json()["job"]["skipped_companies"], 2)
        self.assertEqual(batch_response.json()["job"]["failed_companies"], 0)
        self.assertEqual(len(skipped_items), 2)
        self.assertEqual(skipped_items[0]["last_error"], "데이터 없음")

    def test_ranking_run_next_batch_blocks_on_opendart_rate_limit(self) -> None:
        client = TestClient(
            create_app(
                RateLimitedOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
                FakeKrxListingClient,
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "show-me-the-per-cfs.sqlite3"
            with patch.dict(
                os.environ,
                {
                    "OPENDART_API_KEY": "test-key",
                    "KRX_SERVICE_KEY": "krx-test-key",
                    "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
                },
            ):
                client.post("/ranking/company-master/sync", params={"fs_div": "CFS"}, json={})
                create_response = client.post(
                    "/ranking/update-jobs",
                    params={"fs_div": "CFS"},
                    json={
                        "scope": "ALL",
                        "fs_div": "CFS",
                        "year_from": 2024,
                        "year_to": 2025,
                        "batch_size": 1,
                    },
                )
                job_id = int(create_response.json()["job"]["id"])
                batch_response = client.post(
                    f"/ranking/update-jobs/{job_id}/run-next-batch",
                    params={"fs_div": "CFS"},
                )
            job = read_refresh_job(database_path, job_id=job_id)
            pending_items = read_refresh_job_items(
                database_path,
                job_id=job_id,
                statuses=["pending"],
            )

        self.assertEqual(batch_response.status_code, 200)
        self.assertEqual(batch_response.json()["job"]["status"], "blocked")
        self.assertIn("요청 제한 초과 (020)", batch_response.json()["job"]["last_error"])
        self.assertIsNotNone(job)
        self.assertEqual(job["status"], "blocked")
        self.assertEqual(job["failed_companies"], 0)
        self.assertEqual(job["pending_companies"], 2)
        self.assertEqual(len(pending_items), 2)

    def test_ranking_company_master_sync_prefers_active_local_opendart_key(self) -> None:
        RecordingApiKeyOpenDartClient.used_api_keys = []
        client = TestClient(
            create_app(
                RecordingApiKeyOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
                FakeKrxListingClient,
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            settings_database_path = Path(directory) / "show-me-the-per-settings.sqlite3"
            store_opendart_api_key(
                settings_database_path,
                label="로컬 키",
                api_key="local-key",
            )
            with patch.dict(
                os.environ,
                {
                    "OPENDART_API_KEY": "env-key",
                    "KRX_SERVICE_KEY": "krx-test-key",
                    "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
                },
            ):
                response = client.post(
                    "/ranking/company-master/sync",
                    params={"fs_div": "CFS"},
                    json={},
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(RecordingApiKeyOpenDartClient.used_api_keys, ["local-key"])

    def test_ranking_opendart_key_endpoints_manage_local_keys(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
                FakeKrxListingClient,
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {"SHOW_ME_THE_PER_WEB_CACHE_DIR": directory},
                clear=True,
            ):
                add_first = client.post(
                    "/ranking/opendart-keys",
                    json={"label": "키1", "api_key": "first-key"},
                )
                add_second = client.post(
                    "/ranking/opendart-keys",
                    json={"label": "키2", "api_key": "second-key"},
                )
                second_row = next(
                    item for item in add_second.json()["keys"] if item["label"] == "키2"
                )
                second_id = int(second_row["id"])
                activate_second = client.post(
                    f"/ranking/opendart-keys/{second_id}/activate",
                    json={},
                )
                delete_second = client.request(
                    "DELETE",
                    f"/ranking/opendart-keys/{second_id}",
                )

        self.assertEqual(add_first.status_code, 200)
        self.assertEqual(len(add_first.json()["keys"]), 1)
        self.assertTrue(add_first.json()["keys"][0]["is_active"])
        self.assertEqual(add_second.status_code, 200)
        self.assertEqual(len(add_second.json()["keys"]), 2)
        self.assertNotIn("second-key", str(add_second.json()["keys"]))
        self.assertEqual(activate_second.status_code, 200)
        self.assertIn("활성 OpenDART 키를 변경했습니다.", activate_second.json()["message"])
        activated_second_row = next(
            item for item in activate_second.json()["keys"] if item["label"] == "키2"
        )
        self.assertTrue(activated_second_row["is_active"])
        self.assertEqual(delete_second.status_code, 200)
        self.assertEqual(len(delete_second.json()["keys"]), 1)

    def test_ranking_retry_failed_update_job_requeues_failed_items(self) -> None:
        client = TestClient(
            create_app(
                FailingFinancialClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
                FakeKrxListingClient,
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "show-me-the-per-cfs.sqlite3"
            with patch.dict(
                os.environ,
                {
                    "OPENDART_API_KEY": "test-key",
                    "KRX_SERVICE_KEY": "krx-test-key",
                    "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
                },
            ):
                client.post(
                    "/ranking/company-master/sync",
                    params={"fs_div": "CFS"},
                    json={},
                )
                create_response = client.post(
                    "/ranking/update-jobs",
                    params={"fs_div": "CFS"},
                    json={
                        "scope": "ALL",
                        "fs_div": "CFS",
                        "year_from": 2024,
                        "year_to": 2025,
                        "batch_size": 1,
                    },
                )
                job_id = int(create_response.json()["job"]["id"])
                batch_response = client.post(
                    f"/ranking/update-jobs/{job_id}/run-next-batch",
                    params={"fs_div": "CFS"},
                )
                failed_items = read_refresh_job_items(
                    database_path,
                    job_id=job_id,
                    statuses=["failed"],
                )
                retry_response = client.post(
                    f"/ranking/update-jobs/{job_id}/retry-failed",
                    params={"fs_div": "CFS"},
                )

            pending_items = read_refresh_job_items(
                database_path,
                job_id=job_id,
                statuses=["pending"],
            )

        self.assertEqual(batch_response.status_code, 200)
        self.assertEqual(batch_response.json()["job"]["failed_companies"], 2)
        self.assertEqual(len(failed_items), 2)
        self.assertEqual(retry_response.status_code, 200)
        self.assertEqual(retry_response.json()["job"]["status"], "running")
        self.assertEqual(len(pending_items), 2)

    def test_ranking_reset_databases_clears_cache_and_reports_summary(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
                FakeKrxListingClient,
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            cfs_database_path = Path(directory) / "show-me-the-per-cfs.sqlite3"
            ofs_database_path = Path(directory) / "show-me-the-per-ofs.sqlite3"
            for database_path in (cfs_database_path, ofs_database_path):
                store_company_master_entries(
                    database_path,
                    [
                        {
                            "corp_code": "00126380",
                            "corp_name": "Samsung Electronics",
                            "stock_code": "005930",
                            "market": "KOSPI",
                            "item_name": "Samsung Electronics",
                            "modify_date": "20260422",
                        }
                    ],
                )
                create_refresh_job(
                    database_path,
                    scope="ALL",
                    fs_div="CFS",
                    year_from=2024,
                    year_to=2025,
                    batch_size=25,
                    companies=[
                        {
                            "corp_code": "00126380",
                            "corp_name": "Samsung Electronics",
                            "stock_code": "005930",
                            "market": "KOSPI",
                        }
                    ],
                )

            with patch.dict(
                os.environ,
                {"SHOW_ME_THE_PER_WEB_CACHE_DIR": directory},
                clear=True,
            ):
                response = client.post("/ranking/reset-databases", json={})

            cfs_summary = summarize_database(cfs_database_path)
            ofs_summary = summarize_database(ofs_database_path)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ok")
        self.assertEqual(response.json()["summary"]["cleared"], 2)
        self.assertEqual(response.json()["summary"]["skipped"], 1)
        self.assertIn("전체 DB 캐시 초기화 완료", response.json()["message"])
        self.assertTrue(all(count == 0 for count in cfs_summary.values()))
        self.assertTrue(all(count == 0 for count in ofs_summary.values()))

    def test_ranking_krx_diagnostics_returns_probe_summary(self) -> None:
        client = TestClient(
            create_app(
                FakeOpenDartClient,
                FakeKrxStockPriceClient,
                FakeNaverFinanceClient,
                FakeKrxListingClient,
            )
        )

        fake_diagnostics = {
            "service_key_present": True,
            "service_key_length": 24,
            "service_key_masked": "abcd********wxyz",
            "probes": [
                {
                    "name": "company_list",
                    "status_code": 401,
                    "result_code": "99",
                    "result_message": "AUTH ERROR",
                    "response_preview": '{"error":"bad key"}',
                },
                {
                    "name": "stock_price",
                    "status_code": 200,
                    "result_code": "00",
                    "result_message": "NORMAL SERVICE.",
                    "response_preview": '{"response":{"header":{"resultCode":"00"}}}',
                },
            ],
        }

        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {
                    "KRX_SERVICE_KEY": "krx-test-key",
                    "SHOW_ME_THE_PER_WEB_CACHE_DIR": directory,
                },
                clear=True,
            ):
                with patch("show_me_the_per.web.diagnose_krx_service", return_value=fake_diagnostics):
                    response = client.post("/ranking/krx-diagnostics", json={})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ok")
        self.assertIn("회사목록 401", response.json()["message"])
        self.assertIn("시세 200", response.json()["message"])
        self.assertIn("abcd********wxyz", response.json()["message"])
        self.assertIn("preview", response.json()["message"])


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


class CorrectedSamsungOpenDartClient(FakeOpenDartClient):
    def fetch_major_accounts(
        self,
        corp_codes: list[str],
        business_year: str,
        report_code: str,
        fs_div: str | None = None,
        batch_size: int = 100,
    ) -> list[FinancialStatementRow]:
        corp_code = corp_codes[0]
        if corp_code != "00126380":
            return super().fetch_major_accounts(
                corp_codes,
                business_year,
                report_code,
                fs_div=fs_div,
                batch_size=batch_size,
            )

        year = int(business_year)
        annual_amounts = {
            2015: Decimal("200653482000000"),
            2016: Decimal("201866745000000"),
            2017: Decimal("239575376000000"),
            2018: Decimal("243771415000000"),
            2019: Decimal("230400881000000"),
            2020: Decimal("236806988000000"),
            2021: Decimal("279604799000000"),
            2022: Decimal("302231360000000"),
            2023: Decimal("258935494000000"),
            2024: Decimal("300870903000000"),
            2025: Decimal("333605938000000"),
        }
        quarter_amounts = {
            2024: {
                "11013": Decimal("71915600000000"),
                "11012": Decimal("155000000000000"),
                "11014": Decimal("228000000000000"),
                "11011": Decimal("300870903000000"),
            },
            2025: {
                "11013": Decimal("79000000000000"),
                "11012": Decimal("166000000000000"),
                "11014": Decimal("248000000000000"),
                "11011": Decimal("333605938000000"),
            },
        }
        current_amount = quarter_amounts.get(year, {}).get(
            report_code,
            annual_amounts.get(year, Decimal("0")),
        )
        rows = [
            FinancialStatementRow(
                corp_code="00126380",
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
        if report_code == "11011":
            eps_amounts = {
                2015: Decimal("2500"),
                2016: Decimal("2750"),
                2017: Decimal("3200"),
                2018: Decimal("3600"),
                2019: Decimal("3166"),
                2020: Decimal("3841"),
                2021: Decimal("3991"),
                2022: Decimal("5777"),
                2023: Decimal("8057"),
                2024: Decimal("9300"),
                2025: Decimal("10150"),
            }
            rows.append(
                FinancialStatementRow(
                    corp_code="00126380",
                    corp_name="Samsung Electronics",
                    stock_code="005930",
                    business_year=business_year,
                    report_code=report_code,
                    fs_div=fs_div or "CFS",
                    fs_name="Consolidated financial statements",
                    statement_div="IS",
                    statement_name="Income statement",
                    account_id="ifrs-full_BasicEarningsLossPerShare",
                    account_name="Basic earnings per share",
                    current_term_name="Current",
                    current_amount=eps_amounts.get(year),
                    previous_term_name="Previous",
                    previous_amount=eps_amounts.get(year - 1),
                    before_previous_term_name="Before previous",
                    before_previous_amount=eps_amounts.get(year - 2),
                )
            )
        return rows


class FakeKrxListingClient:
    def __init__(self, service_key: str) -> None:
        self.service_key = service_key

    def fetch_listings(
        self,
        base_date: str | None = None,
        page_size: int = 1000,
        max_pages: int | None = None,
    ) -> list[KrxListing]:
        return [
            KrxListing(
                base_date="20260422",
                short_code="005930",
                isin_code="KR7005930003",
                market="KOSPI",
                item_name="Samsung Electronics",
                corporation_registration_number="1301110006246",
                corporation_name="Samsung Electronics",
            ),
            KrxListing(
                base_date="20260422",
                short_code="126340",
                isin_code="KR7126340004",
                market="KOSDAQ",
                item_name="Vinatac",
                corporation_registration_number="1101112345678",
                corporation_name="Vinatac",
            ),
        ]


class RecordingKrxListingClient(FakeKrxListingClient):
    called_base_dates: list[str | None] = []

    def fetch_listings(
        self,
        base_date: str | None = None,
        page_size: int = 1000,
        max_pages: int | None = None,
    ) -> list[KrxListing]:
        type(self).called_base_dates.append(base_date)
        return super().fetch_listings(
            base_date=base_date,
            page_size=page_size,
            max_pages=max_pages,
        )


class ForbiddenKrxListingClient:
    def __init__(self, service_key: str) -> None:
        self.service_key = service_key

    def fetch_listings(
        self,
        base_date: str | None = None,
        page_size: int = 1000,
        max_pages: int | None = None,
    ) -> list[KrxListing]:
        raise KrxApiError(
            "KRX 회사 목록 조회가 403으로 거부되었습니다. "
            "KRX_SERVICE_KEY 값이 올바른지, 공공데이터포털 활용신청/승인이 완료됐는지, "
            "또는 해당 API 접근이 일시적으로 제한된 것은 아닌지 확인해 주세요.",
            status_code=403,
        )


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


class FakeNaverFinanceClient:
    def fetch_snapshot(self, stock_code: str) -> object:
        if stock_code == "126340":
            return type(
                "Snapshot",
                (),
                {
                    "corp_name": "Vinatac",
                    "market": "KOSDAQ",
                    "close_price": Decimal("37100"),
                    "market_cap": Decimal("561080217600"),
                    "per": Decimal("9.1"),
                    "pbr": Decimal("1.2"),
                    "roe": Decimal("21.4"),
                    "eps": Decimal("4100"),
                    "base_date": "20260422",
                    "source": "naver_finance",
                    "fetched_at": "2026-04-22T07:30:00Z",
                },
            )()
        return type(
            "Snapshot",
            (),
            {
                "corp_name": "Samsung Electronics",
                "market": "KOSPI",
                "close_price": Decimal("84500"),
                "market_cap": Decimal("504448025000000"),
                "per": Decimal("8.4"),
                "pbr": Decimal("1.1"),
                "roe": Decimal("23.0"),
                "eps": Decimal("6564"),
                "base_date": "20260422",
                "source": "naver_finance",
                "fetched_at": "2026-04-22T07:30:00Z",
            },
        )()


class MissingKrxStockPriceClient:
    def __init__(self, service_key: str) -> None:
        self.service_key = service_key

    def fetch_stock_price(
        self,
        stock_code: str,
        *,
        base_date: str,
    ) -> KrxStockPriceSnapshot:
        raise LookupError(f"missing stock price for {stock_code} on {base_date}")


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


class NoDataOpenDartClient(FakeOpenDartClient):
    def fetch_major_accounts(
        self,
        corp_codes: list[str],
        business_year: str,
        report_code: str,
        fs_div: str | None = None,
        batch_size: int = 100,
    ) -> list[FinancialStatementRow]:
        return []


class RateLimitedOpenDartClient(FakeOpenDartClient):
    def fetch_major_accounts(
        self,
        corp_codes: list[str],
        business_year: str,
        report_code: str,
        fs_div: str | None = None,
        batch_size: int = 100,
    ) -> list[FinancialStatementRow]:
        raise ValueError("OpenDART major account request failed: 020 요청 제한 초과")


class RecordingApiKeyOpenDartClient(FakeOpenDartClient):
    used_api_keys: list[str] = []

    def fetch_companies(self) -> list[DartCompany]:
        type(self).used_api_keys.append(self.api_key)
        return super().fetch_companies()


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


def _read_annual_amount(
    database_path: Path,
    corp_code: str,
    fiscal_year: int,
) -> Decimal:
    values = read_financial_period_values_from_database(
        database_path,
        corp_code=corp_code,
        metric="revenue",
        period_type="annual",
    )
    for value in values:
        if value.fiscal_year == fiscal_year:
            return value.amount
    raise AssertionError(f"annual revenue not found for {corp_code} {fiscal_year}")


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
