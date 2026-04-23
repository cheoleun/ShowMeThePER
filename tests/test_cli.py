from __future__ import annotations

import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from show_me_the_per import cli
from show_me_the_per.cli import main, parse_corp_code_args
from show_me_the_per.models import DartCompany, FinancialStatementRow
from show_me_the_per.storage import summarize_database


class CliTests(unittest.TestCase):
    def test_parse_corp_code_args_accepts_repeated_and_comma_separated_values(
        self,
    ) -> None:
        self.assertEqual(
            parse_corp_code_args(["00126380, 00434003", "12345678"]),
            ["00126380", "00434003", "12345678"],
        )

    def test_growth_metrics_command_writes_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            input_path = Path(directory) / "values.json"
            output_path = Path(directory) / "growth.json"
            input_path.write_text(
                json.dumps(
                    {
                        "values": [
                            {
                                "corp_code": "00126380",
                                "metric": "revenue",
                                "period_type": "annual",
                                "fiscal_year": 2023,
                                "amount": "100",
                            },
                            {
                                "corp_code": "00126380",
                                "metric": "revenue",
                                "period_type": "annual",
                                "fiscal_year": 2024,
                                "amount": "130",
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )

            main(
                [
                    "growth-metrics",
                    "--input",
                    str(input_path),
                    "--output",
                    str(output_path),
                    "--recent-annual-periods",
                    "1",
                ]
            )

            payload = json.loads(output_path.read_text("utf-8"))

        self.assertEqual(payload["summary"]["growth_points"], 2)
        self.assertTrue(payload["filter"]["results"][0]["passed"])

    def test_financial_period_values_command_writes_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            input_path = Path(directory) / "financials.json"
            output_path = Path(directory) / "values.json"
            input_path.write_text(
                json.dumps(
                    {
                        "rows": [
                            {
                                "corp_code": "00126380",
                                "corp_name": "Samsung Electronics",
                                "stock_code": "005930",
                                "business_year": "2025",
                                "report_code": "11011",
                                "fs_div": "CFS",
                                "fs_name": "Consolidated financial statements",
                                "statement_div": "IS",
                                "statement_name": "Income statement",
                                "account_id": "ifrs-full_Revenue",
                                "account_name": "Revenue",
                                "current_term_name": "Current",
                                "current_amount": "130",
                                "previous_term_name": "Previous",
                                "previous_amount": "100",
                                "before_previous_term_name": "Before previous",
                                "before_previous_amount": None,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            main(
                [
                    "financial-period-values",
                    "--input",
                    str(input_path),
                    "--output",
                    str(output_path),
                ]
            )

            payload = json.loads(output_path.read_text("utf-8"))

        self.assertEqual(payload["summary"]["values"], 2)
        self.assertEqual(payload["values"][0]["metric"], "revenue")

    def test_rank_companies_command_writes_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            growth_path = Path(directory) / "growth.json"
            valuation_path = Path(directory) / "valuation.json"
            output_path = Path(directory) / "rankings.json"
            growth_path.write_text(
                json.dumps(
                    {
                        "growth_points": [
                            {
                                "corp_code": "00126380",
                                "metric": "revenue",
                                "series_type": "annual_yoy",
                                "fiscal_year": 2025,
                                "fiscal_quarter": None,
                                "amount": "100",
                                "base_amount": "80",
                                "growth_rate": "25",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            valuation_path.write_text(
                json.dumps(
                    {
                        "companies": [
                            {
                                "corp_code": "00126380",
                                "corp_name": "Samsung Electronics",
                                "stock_code": "005930",
                                "per": "8",
                                "pbr": "0.9",
                                "roe": "22",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            main(
                [
                    "rank-companies",
                    "--growth-input",
                    str(growth_path),
                    "--valuation-input",
                    str(valuation_path),
                    "--output",
                    str(output_path),
                    "--growth-condition",
                    "annual_yoy:revenue:1",
                    "--max-per",
                    "10",
                    "--min-roe",
                    "20",
                ]
            )

            payload = json.loads(output_path.read_text("utf-8"))

        self.assertEqual(payload["growth_rankings"][0]["corp_code"], "00126380")
        self.assertEqual(payload["valuation_rankings"][0]["rank_value"], "22")

    def test_rank_companies_command_accepts_repeated_growth_conditions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            growth_path = Path(directory) / "growth.json"
            output_path = Path(directory) / "rankings.json"
            growth_path.write_text(
                json.dumps(
                    {
                        "growth_points": [
                            {
                                "corp_code": "00126380",
                                "metric": "revenue",
                                "series_type": "annual_yoy",
                                "fiscal_year": 2025,
                                "fiscal_quarter": None,
                                "amount": "100",
                                "base_amount": "80",
                                "growth_rate": "25",
                            },
                            {
                                "corp_code": "00126380",
                                "metric": "operating_income",
                                "series_type": "quarterly_yoy",
                                "fiscal_year": 2025,
                                "fiscal_quarter": 1,
                                "amount": "100",
                                "base_amount": "80",
                                "growth_rate": "21",
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )

            main(
                [
                    "rank-companies",
                    "--growth-input",
                    str(growth_path),
                    "--output",
                    str(output_path),
                    "--growth-condition",
                    "annual_yoy:revenue:1",
                    "--growth-condition",
                    "quarterly_yoy:operating_income:1",
                ]
            )

            payload = json.loads(output_path.read_text("utf-8"))

        self.assertEqual(
            payload["filters"]["growth_conditions"],
            [
                {
                    "metric": "revenue",
                    "series_type": "annual_yoy",
                    "recent_periods": 1,
                },
                {
                    "metric": "operating_income",
                    "series_type": "quarterly_yoy",
                    "recent_periods": 1,
                },
            ],
        )
        self.assertEqual(
            payload["screening_rows"][0]["matched_growth_condition_count"],
            2,
        )

    def test_collect_analysis_command_writes_pipeline_outputs(self) -> None:
        original_client = cli.OpenDartClient
        cli.OpenDartClient = FakeOpenDartClient
        try:
            with tempfile.TemporaryDirectory() as directory:
                output_dir = Path(directory) / "analysis"
                database_path = Path(directory) / "analysis.sqlite3"

                main(
                    [
                        "collect-analysis",
                        "--opendart-api-key",
                        "test-key",
                        "--corp-code",
                        "00126380",
                        "--year-from",
                        "2025",
                        "--year-to",
                        "2025",
                        "--report-code",
                        "11011",
                        "--output-dir",
                        str(output_dir),
                        "--database",
                        str(database_path),
                        "--recent-annual-periods",
                        "1",
                        "--recent-quarterly-periods",
                        "1",
                    ]
                )

                coverage = json.loads(
                    (output_dir / "coverage-report.json").read_text("utf-8")
                )
                growth = json.loads(
                    (output_dir / "growth-metrics.json").read_text("utf-8")
                )
                errors = json.loads(
                    (output_dir / "collection-errors.json").read_text("utf-8")
                )
                database_summary = summarize_database(database_path)
        finally:
            cli.OpenDartClient = original_client

        self.assertEqual(coverage["summary"]["corp_codes"], 1)
        self.assertEqual(growth["summary"]["growth_points"], 3)
        self.assertEqual(errors["summary"]["errors"], 0)
        self.assertEqual(database_summary["financial_statement_rows"], 1)
        self.assertEqual(database_summary["financial_period_values"], 3)
        self.assertEqual(database_summary["valuation_snapshots"], 0)

    def test_analysis_to_db_command_stores_existing_outputs(self) -> None:
        original_client = cli.OpenDartClient
        cli.OpenDartClient = FakeOpenDartClient
        try:
            with tempfile.TemporaryDirectory() as directory:
                output_dir = Path(directory) / "analysis"
                database_path = Path(directory) / "analysis.sqlite3"
                summary_path = Path(directory) / "summary.json"

                main(
                    [
                        "collect-analysis",
                        "--opendart-api-key",
                        "test-key",
                        "--corp-code",
                        "00126380",
                        "--business-year",
                        "2025",
                        "--report-code",
                        "11011",
                        "--output-dir",
                        str(output_dir),
                        "--recent-annual-periods",
                        "1",
                        "--recent-quarterly-periods",
                        "1",
                    ]
                )
                main(
                    [
                        "analysis-to-db",
                        "--input-dir",
                        str(output_dir),
                        "--database",
                        str(database_path),
                        "--summary-output",
                        str(summary_path),
                    ]
                )
                summary = json.loads(summary_path.read_text("utf-8"))
        finally:
            cli.OpenDartClient = original_client

        self.assertEqual(summary["financial_statement_rows"], 1)
        self.assertEqual(summary["growth_filter_results"], 1)

    def test_rank_growth_from_db_command_writes_output(self) -> None:
        original_client = cli.OpenDartClient
        cli.OpenDartClient = FakeOpenDartClient
        try:
            with tempfile.TemporaryDirectory() as directory:
                output_dir = Path(directory) / "analysis"
                database_path = Path(directory) / "analysis.sqlite3"
                ranking_path = Path(directory) / "db-growth-ranking.json"

                main(
                    [
                        "collect-analysis",
                        "--opendart-api-key",
                        "test-key",
                        "--corp-code",
                        "00126380",
                        "--business-year",
                        "2025",
                        "--report-code",
                        "11011",
                        "--output-dir",
                        str(output_dir),
                        "--database",
                        str(database_path),
                        "--recent-annual-periods",
                        "1",
                        "--recent-quarterly-periods",
                        "1",
                    ]
                )
                main(
                    [
                        "rank-growth-from-db",
                        "--database",
                        str(database_path),
                        "--growth-condition",
                        "annual_yoy:revenue:1",
                        "--growth-metric",
                        "revenue",
                        "--growth-series-type",
                        "annual_yoy",
                        "--output",
                        str(ranking_path),
                    ]
                )
                payload = json.loads(ranking_path.read_text("utf-8"))
        finally:
            cli.OpenDartClient = original_client

        self.assertEqual(payload["summary"]["growth_rankings"], 1)
        self.assertEqual(payload["growth_rankings"][0]["corp_code"], "00126380")

    def test_company_growth_report_command_writes_html(self) -> None:
        original_client = cli.OpenDartClient
        cli.OpenDartClient = FakeOpenDartClient
        try:
            with tempfile.TemporaryDirectory() as directory:
                output_dir = Path(directory) / "analysis"
                database_path = Path(directory) / "analysis.sqlite3"
                report_path = Path(directory) / "report.html"

                main(
                    [
                        "collect-analysis",
                        "--opendart-api-key",
                        "test-key",
                        "--corp-code",
                        "00126380",
                        "--business-year",
                        "2025",
                        "--report-code",
                        "11011",
                        "--output-dir",
                        str(output_dir),
                        "--database",
                        str(database_path),
                        "--recent-annual-periods",
                        "1",
                        "--recent-quarterly-periods",
                        "1",
                    ]
                )
                main(
                    [
                        "company-growth-report",
                        "--database",
                        str(database_path),
                        "--corp-code",
                        "00126380",
                        "--recent-years",
                        "3",
                        "--output",
                        str(report_path),
                    ]
                )
                html = report_path.read_text("utf-8")
        finally:
            cli.OpenDartClient = original_client

        self.assertIn("<html", html)
        self.assertIn("005930", html)

    def test_growth_ranking_report_command_writes_html(self) -> None:
        original_client = cli.OpenDartClient
        cli.OpenDartClient = FakeOpenDartClient
        try:
            with tempfile.TemporaryDirectory() as directory:
                output_dir = Path(directory) / "analysis"
                database_path = Path(directory) / "analysis.sqlite3"
                report_path = Path(directory) / "ranking.html"

                main(
                    [
                        "collect-analysis",
                        "--opendart-api-key",
                        "test-key",
                        "--corp-code",
                        "00126380",
                        "--business-year",
                        "2025",
                        "--report-code",
                        "11011",
                        "--output-dir",
                        str(output_dir),
                        "--database",
                        str(database_path),
                        "--recent-annual-periods",
                        "1",
                        "--recent-quarterly-periods",
                        "1",
                    ]
                )
                main(
                    [
                        "growth-ranking-report",
                        "--database",
                        str(database_path),
                        "--growth-condition",
                        "annual_yoy:revenue:1",
                        "--growth-metric",
                        "revenue",
                        "--growth-series-type",
                        "annual_yoy",
                        "--output",
                        str(report_path),
                    ]
                )
                html = report_path.read_text("utf-8")
        finally:
            cli.OpenDartClient = original_client

        self.assertIn("<html", html)
        self.assertIn("성장률 랭킹 리포트", html)
        self.assertIn("005930", html)

    def test_collect_analysis_command_records_partial_failures(self) -> None:
        original_client = cli.OpenDartClient
        cli.OpenDartClient = PartiallyFailingOpenDartClient
        try:
            with tempfile.TemporaryDirectory() as directory:
                output_dir = Path(directory) / "analysis"

                main(
                    [
                        "collect-analysis",
                        "--opendart-api-key",
                        "test-key",
                        "--corp-code",
                        "00126380",
                        "--business-year",
                        "2025",
                        "--report-code",
                        "11013,11011",
                        "--output-dir",
                        str(output_dir),
                        "--recent-annual-periods",
                        "1",
                        "--recent-quarterly-periods",
                        "1",
                    ]
                )

                coverage = json.loads(
                    (output_dir / "coverage-report.json").read_text("utf-8")
                )
                errors = json.loads(
                    (output_dir / "collection-errors.json").read_text("utf-8")
                )
        finally:
            cli.OpenDartClient = original_client

        self.assertEqual(coverage["summary"]["collection_errors"], 1)
        self.assertEqual(errors["errors"][0]["report_code"], "11013")

    def test_refresh_valuations_and_rank_companies_from_database(self) -> None:
        original_open_dart_client = cli.OpenDartClient
        original_naver_client = cli.NaverFinanceClient
        cli.OpenDartClient = FakeOpenDartClient
        cli.NaverFinanceClient = FakeNaverFinanceClient
        try:
            with tempfile.TemporaryDirectory() as directory:
                output_dir = Path(directory) / "analysis"
                database_path = Path(directory) / "analysis.sqlite3"
                summary_path = Path(directory) / "valuations.json"
                ranking_path = Path(directory) / "screening.json"

                main(
                    [
                        "collect-analysis",
                        "--opendart-api-key",
                        "test-key",
                        "--corp-code",
                        "00126380",
                        "--year-from",
                        "2023",
                        "--year-to",
                        "2025",
                        "--report-code",
                        "11011",
                        "--output-dir",
                        str(output_dir),
                        "--database",
                        str(database_path),
                        "--recent-annual-periods",
                        "3",
                        "--recent-quarterly-periods",
                        "12",
                    ]
                )
                main(
                    [
                        "refresh-valuations",
                        "--database",
                        str(database_path),
                        "--output",
                        str(summary_path),
                    ]
                )
                main(
                    [
                        "rank-companies",
                        "--database",
                        str(database_path),
                        "--output",
                        str(ranking_path),
                        "--growth-condition",
                        "annual_yoy:revenue:2",
                        "--recent-years",
                        "2",
                        "--end-year",
                        "2025",
                        "--max-per",
                        "10",
                        "--min-roe",
                        "20",
                    ]
                )

                refresh_summary = json.loads(summary_path.read_text("utf-8"))
                ranking_payload = json.loads(ranking_path.read_text("utf-8"))
        finally:
            cli.OpenDartClient = original_open_dart_client
            cli.NaverFinanceClient = original_naver_client

        self.assertEqual(refresh_summary["summary"]["companies"], 1)
        self.assertEqual(ranking_payload["summary"]["screening_rows"], 1)
        self.assertEqual(ranking_payload["screening_rows"][0]["corp_code"], "00126380")


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
            2024: Decimal("130"),
            2025: Decimal("170"),
        }
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
                current_amount=annual_amounts.get(year, Decimal("170")),
                previous_term_name="Previous",
                previous_amount=annual_amounts.get(year - 1),
                before_previous_term_name="Before previous",
                before_previous_amount=annual_amounts.get(year - 2),
            )
        ]


class PartiallyFailingOpenDartClient(FakeOpenDartClient):
    def fetch_major_accounts(
        self,
        corp_codes: list[str],
        business_year: str,
        report_code: str,
        fs_div: str | None = None,
        batch_size: int = 100,
    ) -> list[FinancialStatementRow]:
        if report_code == "11013":
            raise ValueError("temporary OpenDART failure")
        return super().fetch_major_accounts(
            corp_codes,
            business_year,
            report_code,
            fs_div,
            batch_size,
        )


class FakeNaverFinanceClient:
    def fetch_snapshot(self, stock_code: str) -> object:
        return type(
            "Snapshot",
            (),
            {
                "corp_name": "Samsung Electronics",
                "market": "KOSPI",
                "close_price": Decimal("84500"),
                "market_cap": Decimal("504448025000000"),
                "per": Decimal("8"),
                "pbr": Decimal("0.9"),
                "roe": Decimal("22"),
                "eps": Decimal("6564"),
                "base_date": "20260422",
                "source": "naver_finance",
                "fetched_at": "2026-04-22T07:30:00Z",
            },
        )()


if __name__ == "__main__":
    unittest.main()
