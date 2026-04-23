from __future__ import annotations

import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from show_me_the_per.rankings import (
    ValuationSnapshot,
    build_screening_rows,
    build_ranking_payload,
    filter_valuation_snapshots,
    normalize_growth_conditions,
    parse_growth_condition,
    rank_growth_filter_results,
    rank_valuation_snapshots,
    read_valuation_snapshots,
    write_ranking_payload,
)


class RankingTests(unittest.TestCase):
    def test_rank_growth_filter_results_uses_minimum_growth_descending(self) -> None:
        rankings = rank_growth_filter_results(
            [
                growth_result("00126380", "revenue", "annual_yoy", "25", True),
                growth_result("00434003", "revenue", "annual_yoy", "35", True),
                growth_result("00000000", "revenue", "annual_yoy", "99", False),
            ],
            metric="revenue",
            series_type="annual_yoy",
        )

        self.assertEqual(
            [(item["rank"], item["corp_code"], item["minimum_growth_rate"]) for item in rankings],
            [(1, "00434003", "35"), (2, "00126380", "25")],
        )

    def test_filter_valuation_snapshots_applies_per_pbr_roe_bounds(self) -> None:
        snapshots = [
            valuation("00126380", per="8", pbr="0.9", roe="22"),
            valuation("00434003", per="15", pbr="0.8", roe="30"),
            valuation("00000000", per="7", pbr="1.5", roe="25"),
        ]

        filtered = filter_valuation_snapshots(
            snapshots,
            max_per=Decimal("10"),
            max_pbr=Decimal("1"),
            min_roe=Decimal("20"),
        )

        self.assertEqual([snapshot.corp_code for snapshot in filtered], ["00126380"])

    def test_rank_valuation_snapshots_sorts_roe_desc_and_per_asc(self) -> None:
        snapshots = [
            valuation("00126380", per="8", pbr="0.9", roe="22"),
            valuation("00434003", per="5", pbr="0.8", roe="30"),
        ]

        roe_rankings = rank_valuation_snapshots(snapshots, rank_by="roe")
        per_rankings = rank_valuation_snapshots(snapshots, rank_by="per")

        self.assertEqual(roe_rankings[0]["corp_code"], "00434003")
        self.assertEqual(per_rankings[0]["corp_code"], "00434003")

    def test_build_ranking_payload_combines_growth_and_valuation_rankings(self) -> None:
        payload = build_ranking_payload(
            {
                "growth_points": [
                    growth_point(
                        "00126380",
                        "revenue",
                        "annual_yoy",
                        "2025",
                        growth_rate="25",
                    )
                ]
            },
            [valuation("00126380", per="8", pbr="0.9", roe="22")],
            growth_conditions=[{"metric": "revenue", "series_type": "annual_yoy", "recent_periods": 1}],
            max_per=Decimal("10"),
            min_roe=Decimal("20"),
        )

        self.assertEqual(payload["summary"]["growth_rankings"], 1)
        self.assertEqual(payload["summary"]["valuation_rankings"], 1)
        self.assertEqual(payload["summary"]["screening_rows"], 1)

    def test_build_screening_rows_applies_market_and_multiple_growth_conditions(self) -> None:
        rows = build_screening_rows(
            [
                growth_point("00126380", "revenue", "annual_yoy", "2025", growth_rate="25"),
                growth_point("00126380", "operating_income", "quarterly_yoy", "2025", quarter=4, growth_rate="21"),
                growth_point("00434003", "revenue", "annual_yoy", "2025", growth_rate="30"),
                growth_point("00434003", "operating_income", "quarterly_yoy", "2025", quarter=4, growth_rate="19"),
            ],
            [
                valuation(
                    "00126380",
                    per="8",
                    pbr="0.9",
                    roe="22",
                    market_cap="504448025000000",
                    market="KOSPI",
                ),
                valuation(
                    "00434003",
                    per="6",
                    pbr="0.8",
                    roe="18",
                    market_cap="160000000000000",
                    market="KOSPI",
                ),
            ],
            growth_conditions=[
                "annual_yoy:revenue:1",
                "quarterly_yoy:operating_income:1",
            ],
            include_failed_growth=True,
            market="KOSPI",
            sort_by="overall_minimum_growth_rate",
            threshold_percent=Decimal("20"),
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["corp_code"], "00126380")
        self.assertEqual(rows[0]["matched_growth_condition_count"], 2)
        self.assertEqual(rows[0]["overall_minimum_growth_rate"], "21")
        self.assertTrue(rows[0]["passed"])
        self.assertFalse(rows[1]["passed"])
        self.assertEqual(len(rows[0]["growth_checks"]), 2)

    def test_normalize_growth_conditions_uses_default_when_empty(self) -> None:
        conditions = normalize_growth_conditions()

        self.assertEqual(
            conditions,
            [{"metric": "revenue", "series_type": "annual_yoy", "recent_periods": 3}],
        )

    def test_parse_growth_condition_supports_explicit_recent_periods(self) -> None:
        condition = parse_growth_condition("quarterly_qoq:net_income:8")

        self.assertEqual(
            condition,
            {
                "metric": "net_income",
                "series_type": "quarterly_qoq",
                "recent_periods": 8,
            },
        )

    def test_parse_growth_condition_uses_default_period_when_omitted(self) -> None:
        condition = parse_growth_condition("annual_yoy:revenue")

        self.assertEqual(condition["recent_periods"], 3)

    def test_read_and_write_ranking_payload(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            valuation_path = base / "valuations.json"
            output_path = base / "rankings.json"
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

            snapshots = read_valuation_snapshots(valuation_path)
            write_ranking_payload(
                output_path,
                {
                    "growth_points": [
                        growth_point(
                            "00126380",
                            "revenue",
                            "annual_yoy",
                            "2025",
                            growth_rate="25",
                        )
                    ]
                },
                snapshots,
                growth_conditions=["annual_yoy:revenue:1"],
            )
            payload = json.loads(output_path.read_text("utf-8"))

        self.assertEqual(payload["valuation_rankings"][0]["rank_value"], "22")


def growth_result(
    corp_code: str,
    metric: str,
    series_type: str,
    minimum_growth_rate: str,
    passed: bool,
) -> dict[str, object]:
    return {
        "corp_code": corp_code,
        "metric": metric,
        "series_type": series_type,
        "recent_periods": 3,
        "minimum_growth_rate": minimum_growth_rate,
        "passed": passed,
    }


def growth_point(
    corp_code: str,
    metric: str,
    series_type: str,
    fiscal_year: str,
    *,
    quarter: int | None = None,
    growth_rate: str,
) -> dict[str, object]:
    return {
        "corp_code": corp_code,
        "metric": metric,
        "series_type": series_type,
        "fiscal_year": int(fiscal_year),
        "fiscal_quarter": quarter,
        "amount": "100",
        "base_amount": "80",
        "growth_rate": growth_rate,
    }


def valuation(
    corp_code: str,
    *,
    per: str,
    pbr: str,
    roe: str,
    market_cap: str | None = None,
    market: str | None = None,
) -> ValuationSnapshot:
    return ValuationSnapshot(
        corp_code=corp_code,
        corp_name=f"Corp {corp_code}",
        stock_code=corp_code[-6:],
        per=Decimal(per),
        pbr=Decimal(pbr),
        roe=Decimal(roe),
        market_cap=None if market_cap is None else Decimal(market_cap),
        market=market,
    )


if __name__ == "__main__":
    unittest.main()
