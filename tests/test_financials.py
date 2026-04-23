from __future__ import annotations

import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from show_me_the_per.financials import (
    build_annual_period_values_from_rows,
    build_period_values_from_rows,
    build_quarterly_period_values_from_rows,
    map_financial_account_to_metric,
    read_financial_statement_rows,
    write_financial_period_values,
    write_financial_statement_rows,
)
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

    def test_read_financial_statement_rows_parses_decimal_amounts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "financials.json"
            path.write_text(
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
                                "current_amount": "1,000",
                                "previous_term_name": "Previous",
                                "previous_amount": "-",
                                "before_previous_term_name": "Before previous",
                                "before_previous_amount": None,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            rows = read_financial_statement_rows(path)

        self.assertEqual(rows[0].current_amount, Decimal("1000"))
        self.assertIsNone(rows[0].previous_amount)

    def test_build_annual_period_values_from_rows_maps_key_accounts(self) -> None:
        rows = [
            financial_row(
                account_id="ifrs-full_Revenue",
                account_name="Revenue",
                current_amount=Decimal("150"),
                previous_amount=Decimal("120"),
                before_previous_amount=Decimal("100"),
            ),
            financial_row(
                account_id="dart_OperatingIncomeLoss",
                account_name="영업이익",
                current_amount=Decimal("30"),
                previous_amount=None,
                before_previous_amount=None,
            ),
            financial_row(
                account_id="unknown",
                account_name="Other",
                current_amount=Decimal("999"),
                previous_amount=None,
                before_previous_amount=None,
            ),
        ]

        values = build_annual_period_values_from_rows(rows)

        self.assertEqual(len(values), 4)
        self.assertEqual(
            [(value.metric, value.fiscal_year, value.amount) for value in values],
            [
                ("revenue", 2025, Decimal("150")),
                ("revenue", 2024, Decimal("120")),
                ("revenue", 2023, Decimal("100")),
                ("operating_income", 2025, Decimal("30")),
            ],
        )

    def test_build_annual_period_values_prefers_latest_restated_amount(self) -> None:
        rows = [
            financial_row(
                report_code="11011",
                business_year="2024",
                account_id="ifrs-full_Revenue",
                account_name="Revenue",
                current_amount=Decimal("100"),
                previous_amount=None,
                before_previous_amount=None,
            ),
            financial_row(
                report_code="11011",
                business_year="2025",
                account_id="ifrs-full_Revenue",
                account_name="Revenue",
                current_amount=Decimal("130"),
                previous_amount=Decimal("105"),
                before_previous_amount=None,
            ),
        ]

        values = build_annual_period_values_from_rows(rows)

        value_2024 = [value for value in values if value.fiscal_year == 2024][0]
        self.assertEqual(value_2024.amount, Decimal("105"))

    def test_build_quarterly_period_values_from_period_reports(self) -> None:
        rows = [
            financial_row(
                report_code="11013",
                account_id="ifrs-full_Revenue",
                account_name="Revenue",
                current_amount=Decimal("100"),
                previous_amount=None,
                before_previous_amount=None,
            ),
            financial_row(
                report_code="11012",
                account_id="ifrs-full_Revenue",
                account_name="Revenue",
                current_amount=Decimal("150"),
                previous_amount=None,
                before_previous_amount=None,
            ),
            financial_row(
                report_code="11014",
                account_id="ifrs-full_Revenue",
                account_name="Revenue",
                current_amount=Decimal("200"),
                previous_amount=None,
                before_previous_amount=None,
            ),
            financial_row(
                report_code="11011",
                account_id="ifrs-full_Revenue",
                account_name="Revenue",
                current_amount=Decimal("700"),
                previous_amount=None,
                before_previous_amount=None,
            ),
        ]

        values = build_quarterly_period_values_from_rows(rows)

        self.assertEqual(
            [(value.fiscal_quarter, value.amount) for value in values],
            [
                (1, Decimal("100")),
                (2, Decimal("150")),
                (3, Decimal("200")),
                (4, Decimal("250")),
            ],
        )

    def test_build_quarterly_period_values_prefers_consolidated_rows(self) -> None:
        rows = [
            financial_row(
                report_code="11013",
                account_id="ifrs-full_Revenue",
                account_name="Revenue",
                current_amount=Decimal("80"),
                previous_amount=None,
                before_previous_amount=None,
                fs_div="CFS",
            ),
            financial_row(
                report_code="11013",
                account_id="ifrs-full_Revenue",
                account_name="Revenue",
                current_amount=Decimal("55"),
                previous_amount=None,
                before_previous_amount=None,
                fs_div="OFS",
            ),
        ]

        values = build_quarterly_period_values_from_rows(rows)

        self.assertEqual(values[0].amount, Decimal("80"))

    def test_build_period_values_includes_annual_and_quarterly_values(self) -> None:
        rows = [
            financial_row(
                report_code="11013",
                account_id="ifrs-full_Revenue",
                account_name="Revenue",
                current_amount=Decimal("100"),
                previous_amount=None,
                before_previous_amount=None,
            ),
            financial_row(
                report_code="11011",
                account_id="ifrs-full_Revenue",
                account_name="Revenue",
                current_amount=Decimal("700"),
                previous_amount=Decimal("500"),
                before_previous_amount=None,
            ),
        ]

        values = build_period_values_from_rows(rows)

        self.assertEqual(
            sorted({value.period_type for value in values}),
            ["annual", "quarter"],
        )

    def test_build_period_values_keeps_eps_annual_only(self) -> None:
        rows = [
            financial_row(
                report_code="11013",
                account_id="ifrs-full_BasicEarningsLossPerShare",
                account_name="Basic earnings per share",
                current_amount=Decimal("80"),
                previous_amount=None,
                before_previous_amount=None,
            ),
            financial_row(
                report_code="11011",
                account_id="ifrs-full_BasicEarningsLossPerShare",
                account_name="Basic earnings per share",
                current_amount=Decimal("310"),
                previous_amount=Decimal("250"),
                before_previous_amount=Decimal("200"),
            ),
        ]

        values = build_period_values_from_rows(rows)

        self.assertEqual(
            [(value.metric, value.period_type, value.fiscal_year) for value in values],
            [
                ("eps", "annual", 2025),
                ("eps", "annual", 2024),
                ("eps", "annual", 2023),
            ],
        )

    def test_map_financial_account_to_metric_uses_name_fallback(self) -> None:
        self.assertEqual(
            map_financial_account_to_metric("", "당기순이익"),
            "net_income",
        )

    def test_write_financial_period_values_serializes_values(self) -> None:
        values = build_annual_period_values_from_rows(
            [
                financial_row(
                    account_id="ifrs-full_ProfitLoss",
                    account_name="Profit",
                    current_amount=Decimal("20"),
                    previous_amount=None,
                    before_previous_amount=None,
                )
            ]
        )

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "values.json"
            write_financial_period_values(path, values)

            payload = json.loads(path.read_text("utf-8"))

        self.assertEqual(payload["summary"]["values"], 1)
        self.assertEqual(payload["values"][0]["metric"], "net_income")
        self.assertEqual(payload["values"][0]["amount"], "20")


def financial_row(
    *,
    account_id: str,
    account_name: str,
    current_amount: Decimal | None,
    previous_amount: Decimal | None,
    before_previous_amount: Decimal | None,
    report_code: str = "11011",
    business_year: str = "2025",
    fs_div: str = "CFS",
) -> FinancialStatementRow:
    return FinancialStatementRow(
        corp_code="00126380",
        corp_name="Samsung Electronics",
        stock_code="005930",
        business_year=business_year,
        report_code=report_code,
        fs_div=fs_div,
        fs_name="Consolidated financial statements",
        statement_div="IS",
        statement_name="Income statement",
        account_id=account_id,
        account_name=account_name,
        current_term_name="Current",
        current_amount=current_amount,
        previous_term_name="Previous",
        previous_amount=previous_amount,
        before_previous_term_name="Before previous",
        before_previous_amount=before_previous_amount,
    )


if __name__ == "__main__":
    unittest.main()
