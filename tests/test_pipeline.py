from __future__ import annotations

import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from show_me_the_per.models import FinancialStatementRow
from show_me_the_per.pipeline import (
    build_analysis_artifacts,
    build_coverage_report,
    collect_financial_statement_rows,
    parse_business_years,
    read_corp_codes_from_file,
    resolve_corp_codes,
    write_analysis_outputs,
)


class FinancialCollectionPipelineTests(unittest.TestCase):
    def test_collect_financial_statement_rows_fetches_each_year_and_report(
        self,
    ) -> None:
        client = FakeMajorAccountClient()

        rows = collect_financial_statement_rows(
            client,
            corp_codes=["00126380", "00126380", "00434003"],
            business_years=["2024", "2025"],
            report_codes=["11013", "11011"],
            fs_div="CFS",
            batch_size=50,
        )

        self.assertEqual(len(rows), 4)
        self.assertEqual(
            client.calls,
            [
                (["00126380", "00434003"], "2024", "11013", "CFS", 50),
                (["00126380", "00434003"], "2024", "11011", "CFS", 50),
                (["00126380", "00434003"], "2025", "11013", "CFS", 50),
                (["00126380", "00434003"], "2025", "11011", "CFS", 50),
            ],
        )

    def test_build_coverage_report_marks_missing_metric_entries(self) -> None:
        rows = [
            financial_row("00126380", "ifrs-full_Revenue", "Revenue", "130"),
            financial_row(
                "00126380",
                "dart_OperatingIncomeLoss",
                "Operating income",
                "30",
            ),
        ]
        artifacts = build_analysis_artifacts(
            rows,
            expected_corp_codes=["00126380", "00000000"],
            expected_business_years=["2025"],
            expected_report_codes=["11011"],
            recent_annual_periods=1,
            recent_quarterly_periods=1,
        )

        report = artifacts.coverage_report

        self.assertEqual(report["summary"]["corp_codes"], 2)
        self.assertEqual(report["summary"]["missing_metric_entries"], 4)
        samsung = report["companies"][0]
        empty_company = report["companies"][1]
        self.assertEqual(samsung["missing_metrics"], ["net_income"])
        self.assertEqual(
            empty_company["missing_metrics"],
            ["revenue", "operating_income", "net_income"],
        )

    def test_build_coverage_report_includes_growth_filter_results(self) -> None:
        rows = [
            financial_row(
                "00126380",
                "ifrs-full_Revenue",
                "Revenue",
                "144",
                previous_amount=Decimal("120"),
                before_previous_amount=Decimal("100"),
            )
        ]
        artifacts = build_analysis_artifacts(
            rows,
            expected_corp_codes=["00126380"],
            expected_business_years=["2025"],
            expected_report_codes=["11011"],
            recent_annual_periods=1,
            recent_quarterly_periods=1,
        )

        revenue = artifacts.coverage_report["companies"][0]["metrics"][0]

        self.assertEqual(revenue["metric"], "revenue")
        self.assertEqual(revenue["annual_years"], [2023, 2024, 2025])
        self.assertTrue(revenue["growth_filter_results"])

    def test_write_analysis_outputs_writes_all_artifacts(self) -> None:
        rows = [
            financial_row(
                "00126380",
                "ifrs-full_Revenue",
                "Revenue",
                "130",
                previous_amount=Decimal("100"),
            )
        ]
        artifacts = build_analysis_artifacts(
            rows,
            expected_corp_codes=["00126380"],
            expected_business_years=["2025"],
            expected_report_codes=["11011"],
            recent_annual_periods=1,
            recent_quarterly_periods=1,
        )

        with tempfile.TemporaryDirectory() as directory:
            paths = write_analysis_outputs(Path(directory), artifacts)

            payloads = {
                name: json.loads(path.read_text("utf-8"))
                for name, path in paths.items()
            }

        self.assertEqual(payloads["financial_statements"]["summary"]["rows"], 1)
        self.assertEqual(payloads["financial_period_values"]["summary"]["values"], 2)
        self.assertEqual(payloads["growth_metrics"]["summary"]["growth_points"], 2)
        self.assertEqual(payloads["coverage_report"]["summary"]["corp_codes"], 1)

    def test_read_corp_codes_from_company_master_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "company-master.json"
            path.write_text(
                json.dumps(
                    {
                        "matched": [
                            {"corp_code": "00126380"},
                            {"corp_code": "00434003"},
                            {"corp_code": "00126380"},
                        ]
                    }
                ),
                encoding="utf-8",
            )

            corp_codes = read_corp_codes_from_file(path)

        self.assertEqual(corp_codes, ["00126380", "00434003"])

    def test_resolve_corp_codes_combines_inline_and_file_values(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "corp-codes.txt"
            path.write_text("00434003\n12345678", encoding="utf-8")

            corp_codes = resolve_corp_codes(["00126380,00434003"], path)

        self.assertEqual(corp_codes, ["00126380", "00434003", "12345678"])

    def test_parse_business_years_accepts_inline_values_and_range(self) -> None:
        self.assertEqual(
            parse_business_years(["2023,2024"], year_from="2024", year_to="2025"),
            ["2023", "2024", "2025"],
        )

    def test_parse_business_years_rejects_partial_range(self) -> None:
        with self.assertRaises(ValueError):
            parse_business_years([], year_from="2024")


class CoverageReportTests(unittest.TestCase):
    def test_build_coverage_report_can_be_used_without_growth_payload(self) -> None:
        report = build_coverage_report(
            rows=[],
            period_values=[],
            expected_corp_codes=["00126380"],
            expected_business_years=["2025"],
            expected_report_codes=["11011"],
        )

        self.assertEqual(report["summary"]["growth_points"], 0)
        self.assertEqual(report["companies"][0]["raw_rows"], 0)


class FakeMajorAccountClient:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], str, str, str | None, int]] = []

    def fetch_major_accounts(
        self,
        corp_codes: list[str],
        business_year: str,
        report_code: str,
        fs_div: str | None = None,
        batch_size: int = 100,
    ) -> list[FinancialStatementRow]:
        self.calls.append(
            (corp_codes, business_year, report_code, fs_div, batch_size)
        )
        return [
            financial_row(
                corp_codes[0],
                "ifrs-full_Revenue",
                "Revenue",
                "100",
                business_year=business_year,
                report_code=report_code,
            )
        ]


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
) -> FinancialStatementRow:
    return FinancialStatementRow(
        corp_code=corp_code,
        corp_name="Samsung Electronics",
        stock_code="005930",
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
