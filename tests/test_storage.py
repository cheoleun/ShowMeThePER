from __future__ import annotations

import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
import sqlite3

from show_me_the_per.krx import KrxStockPriceSnapshot
from show_me_the_per.models import FinancialStatementRow
from show_me_the_per.pipeline import (
    CollectionError,
    build_analysis_artifacts,
    write_analysis_outputs,
)
from show_me_the_per.storage import (
    build_database_company_screening_payload,
    build_database_growth_ranking_payload,
    create_refresh_job,
    read_dart_companies_from_database,
    read_company_master_entries,
    read_company_master_status,
    read_financial_statement_rows_from_database,
    read_latest_equity_price_snapshot,
    read_latest_valuation_snapshot,
    read_financial_period_values_from_database,
    read_refresh_job,
    read_refresh_job_items,
    read_growth_filter_results_from_database,
    read_growth_points_from_database,
    reset_database_cache,
    record_refresh_job_item_result,
    retry_failed_refresh_job_items,
    store_analysis_artifacts,
    store_analysis_directory,
    store_company_master_entries,
    store_equity_price_snapshot,
    store_valuation_snapshot,
    summarize_database,
)
from show_me_the_per.rankings import ValuationSnapshot


class StorageTests(unittest.TestCase):
    def test_store_analysis_artifacts_writes_tables_idempotently(self) -> None:
        artifacts = build_analysis_artifacts(
            [
                financial_row(
                    "00126380",
                    "ifrs-full_Revenue",
                    "Revenue",
                    "130",
                    previous_amount=Decimal("100"),
                )
            ],
            expected_corp_codes=["00126380"],
            expected_business_years=["2025"],
            expected_report_codes=["11011"],
            recent_annual_periods=1,
            recent_quarterly_periods=1,
        )

        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "analysis.sqlite3"
            first_summary = store_analysis_artifacts(database_path, artifacts)
            second_summary = store_analysis_artifacts(database_path, artifacts)
            values = read_financial_period_values_from_database(database_path)
            growth_points = read_growth_points_from_database(database_path)

        self.assertEqual(first_summary, second_summary)
        self.assertEqual(second_summary["financial_statement_rows"], 1)
        self.assertEqual(second_summary["financial_period_values"], 2)
        self.assertEqual(second_summary["growth_points"], 2)
        self.assertEqual(second_summary["growth_filter_results"], 1)
        self.assertEqual(len(values), 2)
        self.assertEqual(len(growth_points), 2)

    def test_read_financial_statement_rows_from_database_filters_years(self) -> None:
        artifacts = build_analysis_artifacts(
            [
                financial_row(
                    "00126380",
                    "ifrs-full_Revenue",
                    "Revenue",
                    "130",
                    business_year="2024",
                    previous_amount=Decimal("100"),
                ),
                financial_row(
                    "00126380",
                    "ifrs-full_Revenue",
                    "Revenue",
                    "150",
                    business_year="2025",
                    previous_amount=Decimal("130"),
                ),
            ],
            expected_corp_codes=["00126380"],
            expected_business_years=["2024", "2025"],
            expected_report_codes=["11011"],
            recent_annual_periods=1,
            recent_quarterly_periods=1,
        )

        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "analysis.sqlite3"
            store_analysis_artifacts(database_path, artifacts)
            rows = read_financial_statement_rows_from_database(
                database_path,
                corp_code="00126380",
                business_years=["2025"],
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].business_year, "2025")
        self.assertEqual(rows[0].current_amount, Decimal("150"))

    def test_read_dart_companies_from_database_returns_unique_companies(self) -> None:
        artifacts = build_analysis_artifacts(
            [
                financial_row(
                    "00126380",
                    "ifrs-full_Revenue",
                    "Revenue",
                    "130",
                    stock_code="005930",
                ),
                financial_row(
                    "00434003",
                    "ifrs-full_Revenue",
                    "Revenue",
                    "150",
                    stock_code="000660",
                ),
            ],
            expected_corp_codes=["00126380", "00434003"],
            expected_business_years=["2025"],
            expected_report_codes=["11011"],
            recent_annual_periods=1,
            recent_quarterly_periods=1,
        )

        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "analysis.sqlite3"
            store_analysis_artifacts(database_path, artifacts)
            companies = read_dart_companies_from_database(database_path)

        self.assertEqual(len(companies), 2)
        self.assertEqual(companies[0].corp_name, "Samsung Electronics")
        self.assertEqual({company.stock_code for company in companies}, {"005930", "000660"})

    def test_store_company_master_entries_and_status(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "analysis.sqlite3"
            summary = store_company_master_entries(
                database_path,
                [
                    {
                        "corp_code": "00126380",
                        "corp_name": "Samsung Electronics",
                        "stock_code": "005930",
                        "market": "KOSPI",
                        "item_name": "Samsung Electronics",
                        "modify_date": "20260422",
                    },
                    {
                        "corp_code": "00888888",
                        "corp_name": "Vinatac",
                        "stock_code": "126340",
                        "market": "KOSDAQ",
                        "item_name": "Vinatac",
                        "modify_date": "20260422",
                    },
                ],
            )
            entries = read_company_master_entries(database_path, market="KOSDAQ")
            status = read_company_master_status(database_path)

        self.assertEqual(summary["count"], 2)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["corp_code"], "00888888")
        self.assertEqual(status["count"], 2)
        self.assertFalse(status["is_stale"])

    def test_create_refresh_job_and_retry_failed_items(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "analysis.sqlite3"
            job = create_refresh_job(
                database_path,
                scope="ALL",
                fs_div="CFS",
                year_from=2016,
                year_to=2025,
                batch_size=25,
                companies=[
                    {
                        "corp_code": "00126380",
                        "corp_name": "Samsung Electronics",
                        "stock_code": "005930",
                        "market": "KOSPI",
                    },
                    {
                        "corp_code": "00888888",
                        "corp_name": "Vinatac",
                        "stock_code": "126340",
                        "market": "KOSDAQ",
                    },
                ],
            )
            pending_items = read_refresh_job_items(
                database_path,
                job_id=int(job["id"]),
                statuses=["pending"],
                limit=1,
            )
            record_refresh_job_item_result(
                database_path,
                job_id=int(job["id"]),
                corp_code="00126380",
                corp_name="Samsung Electronics",
                status="failed",
                last_error="temporary failure",
            )
            retried = retry_failed_refresh_job_items(
                database_path,
                job_id=int(job["id"]),
            )
            refreshed = read_refresh_job(database_path, job_id=int(job["id"]))

        self.assertEqual(len(pending_items), 1)
        self.assertEqual(pending_items[0]["corp_code"], "00126380")
        self.assertEqual(retried["status"], "running")
        self.assertEqual(refreshed["pending_companies"], 2)
        self.assertEqual(refreshed["estimated_remaining_batches"], 1)
        self.assertEqual(refreshed["next_pending_corp_name"], "Samsung Electronics")

    def test_store_and_read_latest_equity_price_snapshot(self) -> None:
        snapshot_old = KrxStockPriceSnapshot(
            base_date="20260418",
            stock_code="126340",
            item_name="Vinatac",
            market="KOSDAQ",
            close_price=Decimal("36000"),
            market_cap=Decimal("544444416000"),
            listed_stock_count=Decimal("15123456"),
        )
        snapshot_new = KrxStockPriceSnapshot(
            base_date="20260421",
            stock_code="126340",
            item_name="Vinatac",
            market="KOSDAQ",
            close_price=Decimal("37100"),
            market_cap=Decimal("561080217600"),
            listed_stock_count=Decimal("15123456"),
        )

        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "analysis.sqlite3"
            store_equity_price_snapshot(database_path, snapshot_old)
            store_equity_price_snapshot(database_path, snapshot_new)
            latest = read_latest_equity_price_snapshot(
                database_path,
                stock_code="126340",
            )

        self.assertIsNotNone(latest)
        self.assertEqual(latest.base_date, "20260421")
        self.assertEqual(latest.market, "KOSDAQ")
        self.assertEqual(latest.close_price, Decimal("37100"))

    def test_reset_database_cache_clears_all_cache_tables_and_keeps_schema_version(self) -> None:
        artifacts = build_analysis_artifacts(
            [
                financial_row(
                    "00126380",
                    "ifrs-full_Revenue",
                    "Revenue",
                    "130",
                    previous_amount=Decimal("100"),
                )
            ],
            expected_corp_codes=["00126380"],
            expected_business_years=["2025"],
            expected_report_codes=["11011"],
            recent_annual_periods=1,
            recent_quarterly_periods=1,
        )
        snapshot = KrxStockPriceSnapshot(
            base_date="20260421",
            stock_code="126340",
            item_name="Vinatac",
            market="KOSDAQ",
            close_price=Decimal("37100"),
            market_cap=Decimal("561080217600"),
            listed_stock_count=Decimal("15123456"),
        )
        valuation = ValuationSnapshot(
            stock_code="126340",
            corp_code="00888888",
            corp_name="Vinatac",
            market="KOSDAQ",
            close_price=Decimal("37100"),
            market_cap=Decimal("561080217600"),
            per=Decimal("9.1"),
            pbr=Decimal("1.2"),
            roe=Decimal("21.4"),
            eps=Decimal("4100"),
            base_date="20260422",
            source="naver_finance",
            fetched_at="2026-04-22T07:30:00Z",
        )

        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "analysis.sqlite3"
            store_analysis_artifacts(database_path, artifacts)
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
            store_equity_price_snapshot(database_path, snapshot)
            store_valuation_snapshot(database_path, valuation)

            result = reset_database_cache(database_path)
            summary = summarize_database(database_path)

            connection = sqlite3.connect(database_path)
            try:
                schema_version = connection.execute(
                    "SELECT version FROM schema_version"
                ).fetchone()[0]
            finally:
                connection.close()

        self.assertEqual(result["status"], "cleared")
        self.assertGreater(int(result["cleared_rows"]), 0)
        self.assertEqual(schema_version, 3)
        self.assertTrue(all(count == 0 for count in summary.values()))

    def test_reset_database_cache_skips_missing_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "missing.sqlite3"
            result = reset_database_cache(database_path)

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["cleared_rows"], 0)

    def test_store_analysis_directory_loads_written_json_outputs(self) -> None:
        artifacts = build_analysis_artifacts(
            [
                financial_row(
                    "00126380",
                    "ifrs-full_Revenue",
                    "Revenue",
                    "130",
                    previous_amount=Decimal("100"),
                )
            ],
            expected_corp_codes=["00126380"],
            expected_business_years=["2025"],
            expected_report_codes=["11011"],
            recent_annual_periods=1,
            recent_quarterly_periods=1,
        )

        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            output_dir = base / "analysis"
            database_path = base / "analysis.sqlite3"
            write_analysis_outputs(output_dir, artifacts)

            summary = store_analysis_directory(database_path, output_dir)

        self.assertEqual(summary["financial_statement_rows"], 1)
        self.assertEqual(summary["financial_period_values"], 2)
        self.assertEqual(summary["growth_filter_results"], 1)

    def test_store_analysis_artifacts_writes_collection_errors(self) -> None:
        artifacts = build_analysis_artifacts(
            [],
            collection_errors=[
                CollectionError(
                    corp_codes=("00126380",),
                    business_year="2025",
                    report_code="11013",
                    fs_div="CFS",
                    error_type="ValueError",
                    message="temporary failure",
                )
            ],
            expected_corp_codes=["00126380"],
            expected_business_years=["2025"],
            expected_report_codes=["11013"],
            recent_annual_periods=1,
            recent_quarterly_periods=1,
        )

        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "analysis.sqlite3"
            summary = store_analysis_artifacts(database_path, artifacts)

        self.assertEqual(summary["collection_errors"], 1)

    def test_database_summary_initializes_empty_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "empty.sqlite3"
            summary = summarize_database(database_path)

        self.assertEqual(
            summary,
            {
                "financial_statement_rows": 0,
                "financial_period_values": 0,
                "growth_points": 0,
                "growth_filter_results": 0,
                "collection_errors": 0,
                "equity_price_snapshots": 0,
                "valuation_snapshots": 0,
                "company_master_entries": 0,
                "refresh_jobs": 0,
                "refresh_job_items": 0,
            },
        )

    def test_read_growth_points_from_database_filters_by_series(self) -> None:
        artifacts = build_analysis_artifacts(
            [
                financial_row(
                    "00126380",
                    "ifrs-full_Revenue",
                    "Revenue",
                    "144",
                    previous_amount=Decimal("120"),
                    before_previous_amount=Decimal("100"),
                )
            ],
            expected_corp_codes=["00126380"],
            expected_business_years=["2025"],
            expected_report_codes=["11011"],
            recent_annual_periods=1,
            recent_quarterly_periods=1,
        )

        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "analysis.sqlite3"
            store_analysis_artifacts(database_path, artifacts)
            annual_points = read_growth_points_from_database(
                database_path,
                corp_code="00126380",
                metric="revenue",
                series_type="annual_yoy",
            )

        self.assertEqual(len(annual_points), 3)
        self.assertEqual(annual_points[-1].growth_rate, Decimal("20.0"))

    def test_read_growth_filter_results_from_database_filters_passed_results(
        self,
    ) -> None:
        artifacts = build_analysis_artifacts(
            [
                financial_row(
                    "00126380",
                    "ifrs-full_Revenue",
                    "Revenue",
                    "130",
                    previous_amount=Decimal("100"),
                ),
                financial_row(
                    "00434003",
                    "ifrs-full_Revenue",
                    "Revenue",
                    "105",
                    previous_amount=Decimal("100"),
                ),
            ],
            expected_corp_codes=["00126380", "00434003"],
            expected_business_years=["2025"],
            expected_report_codes=["11011"],
            threshold_percent=Decimal("20"),
            recent_annual_periods=1,
            recent_quarterly_periods=1,
        )

        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "analysis.sqlite3"
            store_analysis_artifacts(database_path, artifacts)
            passed_results = read_growth_filter_results_from_database(
                database_path,
                metric="revenue",
                series_type="annual_yoy",
                passed=True,
            )

        self.assertEqual(len(passed_results), 1)
        self.assertEqual(passed_results[0]["corp_code"], "00126380")

    def test_build_database_growth_ranking_payload_ranks_stored_filter_results(
        self,
    ) -> None:
        artifacts = build_analysis_artifacts(
            [
                financial_row(
                    "00126380",
                    "ifrs-full_Revenue",
                    "Revenue",
                    "130",
                    previous_amount=Decimal("100"),
                    stock_code="005930",
                ),
                financial_row(
                    "00434003",
                    "ifrs-full_Revenue",
                    "Revenue",
                    "150",
                    previous_amount=Decimal("100"),
                    stock_code="000660",
                ),
            ],
            expected_corp_codes=["00126380", "00434003"],
            expected_business_years=["2025"],
            expected_report_codes=["11011"],
            threshold_percent=Decimal("20"),
            recent_annual_periods=1,
            recent_quarterly_periods=1,
        )

        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "analysis.sqlite3"
            store_analysis_artifacts(database_path, artifacts)
            payload = build_database_growth_ranking_payload(
                database_path,
                growth_metric="revenue",
                growth_series_type="annual_yoy",
                limit=1,
            )

        self.assertEqual(payload["summary"]["filter_results"], 2)
        self.assertEqual(payload["summary"]["growth_rankings"], 1)
        self.assertEqual(payload["growth_rankings"][0]["corp_code"], "00434003")
        self.assertEqual(payload["growth_rankings"][0]["stock_code"], "000660")

    def test_store_and_read_latest_valuation_snapshot(self) -> None:
        snapshot_old = ValuationSnapshot(
            corp_code="00888888",
            corp_name="Vinatac",
            stock_code="126340",
            per=Decimal("12.1"),
            pbr=Decimal("1.8"),
            roe=Decimal("14.2"),
            eps=Decimal("2345"),
            close_price=Decimal("36000"),
            market_cap=Decimal("544444416000"),
            market="KOSDAQ",
            base_date="20260421",
            source="naver_finance",
            fetched_at="2026-04-21T07:30:00Z",
        )
        snapshot_new = ValuationSnapshot(
            corp_code="00888888",
            corp_name="Vinatac",
            stock_code="126340",
            per=Decimal("11.4"),
            pbr=Decimal("1.7"),
            roe=Decimal("15.0"),
            eps=Decimal("2450"),
            close_price=Decimal("37100"),
            market_cap=Decimal("561080217600"),
            market="KOSDAQ",
            base_date="20260422",
            source="naver_finance",
            fetched_at="2026-04-22T07:30:00Z",
        )

        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "analysis.sqlite3"
            store_valuation_snapshot(database_path, snapshot_old)
            store_valuation_snapshot(database_path, snapshot_new)
            latest = read_latest_valuation_snapshot(
                database_path,
                stock_code="126340",
            )

        self.assertIsNotNone(latest)
        self.assertEqual(latest.base_date, "20260422")
        self.assertEqual(latest.per, Decimal("11.4"))
        self.assertEqual(latest.market_cap, Decimal("561080217600"))

    def test_build_database_company_screening_payload_combines_growth_and_valuation(
        self,
    ) -> None:
        artifacts = build_analysis_artifacts(
            [
                financial_row(
                    "00126380",
                    "ifrs-full_Revenue",
                    "Revenue",
                    "150",
                    business_year="2025",
                    previous_amount=Decimal("120"),
                    before_previous_amount=Decimal("100"),
                    stock_code="005930",
                ),
                financial_row(
                    "00126380",
                    "ifrs-full_Revenue",
                    "Revenue",
                    "120",
                    business_year="2024",
                    previous_amount=Decimal("100"),
                    before_previous_amount=Decimal("80"),
                    stock_code="005930",
                ),
                financial_row(
                    "00126380",
                    "ifrs-full_Revenue",
                    "Revenue",
                    "80",
                    business_year="2022",
                    previous_amount=Decimal("60"),
                    before_previous_amount=Decimal("50"),
                    stock_code="005930",
                ),
                financial_row(
                    "00126380",
                    "ifrs-full_Revenue",
                    "Revenue",
                    "100",
                    business_year="2023",
                    previous_amount=Decimal("80"),
                    before_previous_amount=Decimal("70"),
                    stock_code="005930",
                ),
            ],
            expected_corp_codes=["00126380"],
            expected_business_years=["2022", "2023", "2024", "2025"],
            expected_report_codes=["11011"],
            recent_annual_periods=3,
            recent_quarterly_periods=12,
        )

        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "analysis.sqlite3"
            store_analysis_artifacts(database_path, artifacts)
            store_valuation_snapshot(
                database_path,
                ValuationSnapshot(
                    corp_code="00126380",
                    corp_name="Samsung Electronics",
                    stock_code="005930",
                    per=Decimal("8"),
                    pbr=Decimal("0.9"),
                    roe=Decimal("22"),
                    eps=Decimal("6564"),
                    close_price=Decimal("84500"),
                    market_cap=Decimal("504448025000000"),
                    market="KOSPI",
                    base_date="20260422",
                    source="naver_finance",
                    fetched_at="2026-04-22T07:30:00Z",
                ),
            )
            payload = build_database_company_screening_payload(
                database_path,
                start_year=2023,
                end_year=2025,
                fs_div="CFS",
                growth_metric="revenue",
                growth_series_type="annual_yoy",
                threshold_percent=Decimal("20"),
                recent_annual_periods=3,
                recent_quarterly_periods=12,
                max_per=Decimal("10"),
                min_roe=Decimal("20"),
                sort_by="market_cap",
            )

        self.assertEqual(payload["summary"]["screening_rows"], 1)
        self.assertEqual(payload["screening_rows"][0]["corp_code"], "00126380")
        self.assertEqual(payload["screening_rows"][0]["per"], "8")


def financial_row(
    corp_code: str,
    account_id: str,
    account_name: str,
    current_amount: str,
    *,
    previous_amount: Decimal | None = None,
    before_previous_amount: Decimal | None = None,
    business_year: str = "2025",
    report_code: str = "11011",
    stock_code: str = "005930",
) -> FinancialStatementRow:
    return FinancialStatementRow(
        corp_code=corp_code,
        corp_name="Samsung Electronics",
        stock_code=stock_code,
        business_year=business_year,
        report_code=report_code,
        fs_div="CFS",
        fs_name="Consolidated financial statements",
        statement_div="IS",
        statement_name="Income statement",
        account_id=account_id,
        account_name=account_name,
        current_term_name="Current",
        current_amount=Decimal(current_amount),
        previous_term_name="Previous",
        previous_amount=previous_amount,
        before_previous_term_name="Before previous",
        before_previous_amount=before_previous_amount,
    )


if __name__ == "__main__":
    unittest.main()
