import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from show_me_the_per.growth import (
    ANNUAL_YOY,
    QUARTERLY_QOQ,
    QUARTERLY_YOY,
    TRAILING_FOUR_QUARTER_YOY,
    build_growth_metrics_payload,
    calculate_annual_yoy_growth,
    calculate_quarterly_qoq_growth,
    calculate_quarterly_yoy_growth,
    calculate_trailing_four_quarter_yoy_growth,
    passes_recent_growth_threshold,
    read_financial_period_values,
    write_growth_metrics_payload,
)
from show_me_the_per.models import FinancialPeriodValue


class GrowthCalculationTests(unittest.TestCase):
    def test_calculate_annual_yoy_growth_compares_previous_year(self) -> None:
        points = calculate_annual_yoy_growth(
            [
                annual_value(2022, "100"),
                annual_value(2023, "120"),
                annual_value(2024, "150"),
            ]
        )

        self.assertEqual(points[0].series_type, ANNUAL_YOY)
        self.assertIsNone(points[0].growth_rate)
        self.assertEqual(points[1].growth_rate, Decimal("20.0"))
        self.assertEqual(points[2].growth_rate, Decimal("25.00"))

    def test_calculate_quarterly_yoy_growth_compares_same_quarter_last_year(
        self,
    ) -> None:
        points = calculate_quarterly_yoy_growth(
            [
                quarter_value(2024, 1, "100"),
                quarter_value(2024, 2, "110"),
                quarter_value(2025, 1, "125"),
                quarter_value(2025, 2, "132"),
            ]
        )

        q1_2025 = [point for point in points if point.period_label == "2025Q1"][0]

        self.assertEqual(q1_2025.series_type, QUARTERLY_YOY)
        self.assertEqual(q1_2025.base_amount, Decimal("100"))
        self.assertEqual(q1_2025.growth_rate, Decimal("25.00"))

    def test_calculate_trailing_four_quarter_yoy_growth_compares_ttm_windows(
        self,
    ) -> None:
        points = calculate_trailing_four_quarter_yoy_growth(
            [
                quarter_value(2024, 1, "100"),
                quarter_value(2024, 2, "100"),
                quarter_value(2024, 3, "100"),
                quarter_value(2024, 4, "100"),
                quarter_value(2025, 1, "125"),
                quarter_value(2025, 2, "125"),
                quarter_value(2025, 3, "125"),
                quarter_value(2025, 4, "125"),
            ]
        )

        q4_2025 = [point for point in points if point.period_label == "2025Q4"][0]

        self.assertEqual(q4_2025.series_type, TRAILING_FOUR_QUARTER_YOY)
        self.assertEqual(q4_2025.amount, Decimal("500"))
        self.assertEqual(q4_2025.base_amount, Decimal("400"))
        self.assertEqual(q4_2025.growth_rate, Decimal("25.00"))

    def test_calculate_quarterly_qoq_growth_compares_previous_quarter(self) -> None:
        points = calculate_quarterly_qoq_growth(
            [
                quarter_value(2024, 3, "100"),
                quarter_value(2024, 4, "120"),
                quarter_value(2025, 1, "144"),
            ]
        )

        q1_2025 = [point for point in points if point.period_label == "2025Q1"][0]

        self.assertEqual(q1_2025.series_type, QUARTERLY_QOQ)
        self.assertEqual(q1_2025.base_amount, Decimal("120"))
        self.assertEqual(q1_2025.growth_rate, Decimal("20.0"))

    def test_calculate_quarterly_qoq_growth_requires_positive_previous_value(self) -> None:
        points = calculate_quarterly_qoq_growth(
            [
                quarter_value(2024, 4, "0"),
                quarter_value(2025, 1, "144"),
            ]
        )

        q1_2025 = [point for point in points if point.period_label == "2025Q1"][0]

        self.assertIsNone(q1_2025.growth_rate)

    def test_recent_threshold_requires_all_points_to_pass_not_average(self) -> None:
        points = calculate_annual_yoy_growth(
            [
                annual_value(2020, "100"),
                annual_value(2021, "130"),
                annual_value(2022, "169"),
                annual_value(2023, "186"),
            ]
        )

        self.assertFalse(
            passes_recent_growth_threshold(
                points,
                threshold_percent=Decimal("20"),
                recent_periods=3,
            )
        )

    def test_recent_threshold_fails_when_growth_cannot_be_calculated(self) -> None:
        points = calculate_annual_yoy_growth(
            [
                annual_value(2021, "0"),
                annual_value(2022, "120"),
                annual_value(2023, "150"),
            ]
        )

        self.assertFalse(
            passes_recent_growth_threshold(
                points,
                threshold_percent=Decimal("20"),
                recent_periods=2,
            )
        )

    def test_growth_payload_includes_filter_results(self) -> None:
        payload = build_growth_metrics_payload(
            [
                annual_value(2021, "100"),
                annual_value(2022, "120"),
                annual_value(2023, "144"),
                annual_value(2024, "173"),
            ],
            threshold_percent=Decimal("20"),
            recent_annual_periods=3,
            recent_quarterly_periods=12,
        )

        annual_result = [
            result
            for result in payload["filter"]["results"]
            if result["series_type"] == ANNUAL_YOY
        ][0]

        self.assertTrue(annual_result["passed"])
        self.assertEqual(annual_result["minimum_growth_rate"], "20.0")


class GrowthJsonTests(unittest.TestCase):
    def test_read_and_write_growth_metrics_payload(self) -> None:
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
                                "amount": "125",
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )

            values = read_financial_period_values(input_path)
            write_growth_metrics_payload(
                output_path,
                values,
                threshold_percent=Decimal("20"),
                recent_annual_periods=1,
            )

            payload = json.loads(output_path.read_text("utf-8"))

        self.assertEqual(payload["summary"]["values"], 2)
        self.assertEqual(payload["growth_points"][1]["growth_rate"], "25.00")
        self.assertTrue(payload["filter"]["results"][0]["passed"])


def annual_value(year: int, amount: str) -> FinancialPeriodValue:
    return FinancialPeriodValue(
        corp_code="00126380",
        metric="revenue",
        period_type="annual",
        fiscal_year=year,
        amount=Decimal(amount),
    )


def quarter_value(year: int, quarter: int, amount: str) -> FinancialPeriodValue:
    return FinancialPeriodValue(
        corp_code="00126380",
        metric="revenue",
        period_type="quarter",
        fiscal_year=year,
        fiscal_quarter=quarter,
        amount=Decimal(amount),
    )


if __name__ == "__main__":
    unittest.main()
