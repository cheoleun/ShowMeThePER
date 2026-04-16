import json
import tempfile
import unittest
from pathlib import Path

from show_me_the_per.company_master import (
    build_match_summary,
    write_company_master_outputs,
)
from show_me_the_per.matching import match_listings_to_dart
from show_me_the_per.models import DartCompany, KrxListing


class CompanyMasterTests(unittest.TestCase):
    def test_build_match_summary_reports_counts_and_rate(self) -> None:
        result = match_listings_to_dart(
            listings=[_listing("005930"), _listing("000001")],
            dart_companies=[_company("00126380", "005930")],
        )

        summary = build_match_summary(result)

        self.assertEqual(summary["total_listings"], 2)
        self.assertEqual(summary["matched"], 1)
        self.assertEqual(summary["unmatched_listings"], 1)
        self.assertEqual(summary["ambiguous_matches"], 0)
        self.assertEqual(summary["match_rate"], 0.5)

    def test_write_company_master_outputs_writes_requested_files(self) -> None:
        result = match_listings_to_dart(
            listings=[_listing("005930"), _listing("000001")],
            dart_companies=[_company("00126380", "005930")],
        )

        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            write_company_master_outputs(
                result=result,
                output_json=base / "company-master.json",
                matched_csv=base / "matched.csv",
                unmatched_csv=base / "unmatched.csv",
                ambiguous_json=base / "ambiguous.json",
                report_markdown=base / "report.md",
            )

            payload = json.loads((base / "company-master.json").read_text("utf-8"))
            self.assertEqual(payload["summary"]["matched"], 1)
            self.assertIn("00126380", (base / "matched.csv").read_text("utf-8"))
            self.assertIn("000001", (base / "unmatched.csv").read_text("utf-8"))
            self.assertEqual(
                json.loads((base / "ambiguous.json").read_text("utf-8")),
                [],
            )
            self.assertIn(
                "기업 마스터 매칭 리포트",
                (base / "report.md").read_text("utf-8"),
            )


def _listing(short_code: str) -> KrxListing:
    return KrxListing(
        base_date="20260415",
        short_code=short_code,
        isin_code=f"KR7{short_code}000",
        market="KOSPI",
        item_name=f"Item {short_code}",
        corporation_registration_number=f"CRNO{short_code}",
        corporation_name=f"Corp {short_code}",
    )


def _company(corp_code: str, stock_code: str) -> DartCompany:
    return DartCompany(
        corp_code=corp_code,
        corp_name=f"Dart {stock_code}",
        stock_code=stock_code,
        modify_date="20240101",
    )


if __name__ == "__main__":
    unittest.main()
