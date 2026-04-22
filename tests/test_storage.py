from __future__ import annotations

import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from show_me_the_per.models import FinancialStatementRow
from show_me_the_per.pipeline import (
    CollectionError,
    build_analysis_artifacts,
    write_analysis_outputs,
)
from show_me_the_per.storage import (
    build_database_growth_ranking_payload,
    read_dart_companies_from_database,
    read_financial_statement_rows_from_database,
    read_financial_period_values_from_database,
    read_growth_filter_results_from_database,
    read_growth_points_from_database,
    store_analysis_artifacts,
    store_analysis_directory,
    summarize_database,
)


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
