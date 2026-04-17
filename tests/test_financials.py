import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from show_me_the_per.financials import write_financial_statement_rows
from show_me_the_per.models import FinancialStatementRow


class FinancialOutputTests(unittest.TestCase):
    def test_write_financial_statement_rows_serializes_decimal_amounts(self) -> None:
        row = FinancialStatementRow(
            corp_code="00126380",
            corp_name="Samsung Electronics",
            stock_code="005930",
            business_year="2025",
            report_code="11011",
            fs_div="CFS",
            fs_name="Consolidated financial statements",
            statement_div="IS",
            statement_name="Income statement",
            account_id="ifrs-full_Revenue",
            account_name="Revenue",
            current_term_name="Current",
            current_amount=Decimal("1000"),
            previous_term_name="Previous",
            previous_amount=Decimal("900"),
            before_previous_term_name="Before previous",
            before_previous_amount=None,
        )

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "financials.json"
            write_financial_statement_rows(output, [row])

            payload = json.loads(output.read_text("utf-8"))

        self.assertEqual(payload["summary"]["rows"], 1)
        self.assertEqual(payload["rows"][0]["current_amount"], "1000")
        self.assertIsNone(payload["rows"][0]["before_previous_amount"])


if __name__ == "__main__":
    unittest.main()
