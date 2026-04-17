from io import BytesIO
import json
from zipfile import ZipFile
import unittest

from decimal import Decimal

from show_me_the_per import opendart
from show_me_the_per.opendart import (
    OpenDartClient,
    chunked,
    parse_corp_code_xml,
    parse_corp_code_zip,
    parse_major_accounts_payload,
)


CORP_CODE_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<result>
  <list>
    <corp_code>00126380</corp_code>
    <corp_name>Samsung Electronics</corp_name>
    <stock_code>005930</stock_code>
    <modify_date>20240101</modify_date>
  </list>
  <list>
    <corp_code>00434003</corp_code>
    <corp_name>Private Company</corp_name>
    <stock_code></stock_code>
    <modify_date>20240101</modify_date>
  </list>
</result>
"""


class OpenDartParserTests(unittest.TestCase):
    def test_parse_corp_code_xml_extracts_companies(self) -> None:
        companies = parse_corp_code_xml(CORP_CODE_XML)

        self.assertEqual(len(companies), 2)
        self.assertEqual(companies[0].corp_code, "00126380")
        self.assertEqual(companies[0].normalized_stock_code, "005930")
        self.assertFalse(companies[1].is_listed)

    def test_parse_corp_code_zip_extracts_first_xml_file(self) -> None:
        archive_content = BytesIO()
        with ZipFile(archive_content, "w") as archive:
            archive.writestr("CORPCODE.xml", CORP_CODE_XML)

        companies = parse_corp_code_zip(archive_content.getvalue())

        self.assertEqual(
            [company.corp_code for company in companies],
            [
                "00126380",
                "00434003",
            ],
        )

    def test_parse_major_accounts_payload_extracts_amounts(self) -> None:
        payload = {
            "status": "000",
            "message": "정상",
            "list": [
                {
                    "corp_code": "00126380",
                    "corp_name": "Samsung Electronics",
                    "stock_code": "005930",
                    "bsns_year": "2025",
                    "reprt_code": "11011",
                    "fs_div": "CFS",
                    "fs_nm": "Consolidated financial statements",
                    "sj_div": "IS",
                    "sj_nm": "Income statement",
                    "account_id": "ifrs-full_Revenue",
                    "account_nm": "Revenue",
                    "thstrm_nm": "Current",
                    "thstrm_amount": "1,234",
                    "frmtrm_nm": "Previous",
                    "frmtrm_amount": "-",
                    "bfefrmtrm_nm": "Before previous",
                    "bfefrmtrm_amount": "900",
                }
            ],
        }

        rows = parse_major_accounts_payload(payload)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].current_amount, Decimal("1234"))
        self.assertIsNone(rows[0].previous_amount)
        self.assertEqual(rows[0].before_previous_amount, Decimal("900"))

    def test_parse_major_accounts_payload_returns_empty_for_no_data(self) -> None:
        rows = parse_major_accounts_payload({"status": "013", "message": "No data"})

        self.assertEqual(rows, [])

    def test_fetch_major_accounts_filters_rows_by_requested_fs_div(self) -> None:
        original_urlopen = opendart.urlopen
        payload = {
            "status": "000",
            "list": [
                major_account_item("CFS", "100"),
                major_account_item("OFS", "80"),
            ],
        }
        opendart.urlopen = lambda url, timeout: FakeResponse(
            json.dumps(payload).encode("utf-8")
        )
        try:
            rows = OpenDartClient("test-key").fetch_major_accounts(
                ["00126380"],
                business_year="2025",
                report_code="11011",
                fs_div="CFS",
            )
        finally:
            opendart.urlopen = original_urlopen

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].fs_div, "CFS")
        self.assertEqual(rows[0].current_amount, Decimal("100"))

    def test_chunked_splits_values(self) -> None:
        self.assertEqual(chunked(["a", "b", "c"], 2), [["a", "b"], ["c"]])


class FakeResponse:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None

    def read(self) -> bytes:
        return self.content


def major_account_item(fs_div: str, amount: str) -> dict[str, str]:
    return {
        "corp_code": "00126380",
        "corp_name": "Samsung Electronics",
        "stock_code": "005930",
        "bsns_year": "2025",
        "reprt_code": "11011",
        "fs_div": fs_div,
        "fs_nm": "Financial statements",
        "sj_div": "IS",
        "sj_nm": "Income statement",
        "account_id": "ifrs-full_Revenue",
        "account_nm": "Revenue",
        "thstrm_nm": "Current",
        "thstrm_amount": amount,
        "frmtrm_nm": "Previous",
        "frmtrm_amount": "90",
        "bfefrmtrm_nm": "Before previous",
        "bfefrmtrm_amount": "80",
    }


if __name__ == "__main__":
    unittest.main()
