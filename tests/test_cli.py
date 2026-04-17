from __future__ import annotations

import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from show_me_the_per import cli
from show_me_the_per.cli import main, parse_corp_code_args
from show_me_the_per.models import FinancialStatementRow


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
                        "filter": {
                            "results": [
                                {
                                    "corp_code": "00126380",
                                    "metric": "revenue",
                                    "series_type": "annual_yoy",
                                    "recent_periods": 3,
                                    "minimum_growth_rate": "25",
                                    "passed": True,
                                }
                            ]
                        }
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
                    "--max-per",
                    "10",
                    "--min-roe",
                    "20",
                ]
            )

            payload = json.loads(output_path.read_text("utf-8"))

        self.assertEqual(payload["growth_rankings"][0]["corp_code"], "00126380")
        self.assertEqual(payload["valuation_rankings"][0]["rank_value"], "22")

    def test_collect_analysis_command_writes_pipeline_outputs(self) -> None:
        original_client = cli.OpenDartClient
        cli.OpenDartClient = FakeOpenDartClient
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
                        "--year-from",
                        "2025",
                        "--year-to",
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

                coverage = json.loads(
                    (output_dir / "coverage-report.json").read_text("utf-8")
                )
                growth = json.loads(
                    (output_dir / "growth-metrics.json").read_text("utf-8")
                )
        finally:
            cli.OpenDartClient = original_client

        self.assertEqual(coverage["summary"]["corp_codes"], 1)
        self.assertEqual(growth["summary"]["growth_points"], 2)


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
                current_amount=Decimal("130"),
                previous_term_name="Previous",
                previous_amount=Decimal("100"),
                before_previous_term_name="Before previous",
                before_previous_amount=None,
            )
        ]


if __name__ == "__main__":
    unittest.main()
