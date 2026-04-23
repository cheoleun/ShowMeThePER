from __future__ import annotations

import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from show_me_the_per.models import FinancialStatementRow
from show_me_the_per.pipeline import build_analysis_artifacts
from show_me_the_per.reports import (
    build_company_growth_report_payload,
    build_growth_ranking_report_payload,
    render_company_growth_report_html,
    render_growth_ranking_report_html,
    write_company_growth_report_html,
    write_growth_ranking_report_html,
)
from show_me_the_per.storage import store_analysis_artifacts


class ReportTests(unittest.TestCase):
    def test_build_company_growth_report_payload_groups_metrics_and_series(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = build_database(Path(directory))

            payload = build_company_growth_report_payload(
                database_path,
                corp_code="00126380",
                recent_years=3,
            )

        self.assertEqual(payload["company"]["corp_code"], "00126380")
        self.assertEqual(payload["company"]["stock_code"], "005930")
        self.assertEqual(payload["summary"]["growth_points"], 3)
        self.assertEqual(payload["metrics"][0]["metric"], "revenue")
        self.assertEqual(
            payload["metrics"][0]["series"][0]["series_type"],
            "annual_yoy",
        )

    def test_render_company_growth_report_html_contains_table_and_svg(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = build_database(Path(directory))
            payload = build_company_growth_report_payload(
                database_path,
                corp_code="00126380",
                recent_years=3,
            )

            html = render_company_growth_report_html(payload)

        self.assertIn("<table>", html)
        self.assertIn("<svg", html)
        self.assertIn("성장률 필터 결과", html)
        self.assertIn("005930", html)

    def test_write_company_growth_report_html_writes_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            database_path = build_database(base)
            payload = build_company_growth_report_payload(
                database_path,
                corp_code="00126380",
                recent_years=3,
            )
            output_path = base / "report.html"

            write_company_growth_report_html(output_path, payload)

            html = output_path.read_text(encoding="utf-8")

        self.assertIn("성장률 리포트", html)
        self.assertIn("20.00%", html)

    def test_build_growth_ranking_report_payload_includes_labeled_rankings(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = build_ranking_database(Path(directory))

            payload = build_growth_ranking_report_payload(
                database_path,
                growth_conditions=["annual_yoy:revenue:1"],
                growth_metric="revenue",
                growth_series_type="annual_yoy",
                limit=1,
            )

        self.assertEqual(payload["display"]["metric_label"], "매출")
        self.assertEqual(payload["summary"]["growth_rankings"], 1)
        self.assertEqual(payload["growth_rankings"][0]["corp_code"], "00434003")
        self.assertEqual(payload["growth_rankings"][0]["series_label"], "연간 YoY")

    def test_render_growth_ranking_report_html_contains_table_and_svg(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = build_ranking_database(Path(directory))
            payload = build_growth_ranking_report_payload(
                database_path,
                growth_conditions=["annual_yoy:revenue:1"],
                growth_metric="revenue",
                growth_series_type="annual_yoy",
            )

            html = render_growth_ranking_report_html(payload)

        self.assertIn("성장률 랭킹 리포트", html)
        self.assertIn("<table>", html)
        self.assertIn("<svg", html)
        self.assertIn("000660", html)
        self.assertIn("50.00%", html)

    def test_write_growth_ranking_report_html_writes_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            database_path = build_ranking_database(base)
            payload = build_growth_ranking_report_payload(
                database_path,
                growth_conditions=["annual_yoy:revenue:1"],
                growth_metric="revenue",
                growth_series_type="annual_yoy",
            )
            output_path = base / "ranking.html"

            write_growth_ranking_report_html(output_path, payload)

            html = output_path.read_text(encoding="utf-8")

        self.assertIn("랭킹 목록", html)
        self.assertIn("SK Hynix", html)


def build_database(base: Path) -> Path:
    database_path = base / "analysis.sqlite3"
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
        threshold_percent=Decimal("20"),
        recent_annual_periods=1,
        recent_quarterly_periods=1,
    )
    store_analysis_artifacts(database_path, artifacts)
    return database_path


def build_ranking_database(base: Path) -> Path:
    database_path = base / "analysis.sqlite3"
    artifacts = build_analysis_artifacts(
        [
            financial_row(
                "00126380",
                "ifrs-full_Revenue",
                "Revenue",
                "130",
                previous_amount=Decimal("100"),
                stock_code="005930",
                corp_name="Samsung Electronics",
            ),
            financial_row(
                "00434003",
                "ifrs-full_Revenue",
                "Revenue",
                "150",
                previous_amount=Decimal("100"),
                stock_code="000660",
                corp_name="SK Hynix",
            ),
        ],
        expected_corp_codes=["00126380", "00434003"],
        expected_business_years=["2025"],
        expected_report_codes=["11011"],
        threshold_percent=Decimal("20"),
        recent_annual_periods=1,
        recent_quarterly_periods=1,
    )
    store_analysis_artifacts(database_path, artifacts)
    return database_path


def financial_row(
    corp_code: str,
    account_id: str,
    account_name: str,
    current_amount: str,
    *,
    previous_amount: Decimal | None = None,
    before_previous_amount: Decimal | None = None,
    stock_code: str = "005930",
    corp_name: str = "Samsung Electronics",
) -> FinancialStatementRow:
    return FinancialStatementRow(
        corp_code=corp_code,
        corp_name=corp_name,
        stock_code=stock_code,
        business_year="2025",
        report_code="11011",
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
