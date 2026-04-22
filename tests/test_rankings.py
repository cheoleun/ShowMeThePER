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
                "filter": {
                    "results": [
                        growth_result("00126380", "revenue", "annual_yoy", "25", True)
                    ]
                }
            },
            [valuation("00126380", per="8", pbr="0.9", roe="22")],
            max_per=Decimal("10"),
            min_roe=Decimal("20"),
        )

        self.assertEqual(payload["summary"]["growth_rankings"], 1)
        self.assertEqual(payload["summary"]["valuation_rankings"], 1)
        self.assertEqual(payload["summary"]["screening_rows"], 1)

    def test_build_screening_rows_applies_market_and_sorting(self) -> None:
        rows = build_screening_rows(
            [
                growth_result("00126380", "revenue", "annual_yoy", "25", True),
                growth_result("00434003", "revenue", "annual_yoy", "30", True),
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
            market="KOSPI",
            min_roe=Decimal("20"),
            sort_by="market_cap",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["corp_code"], "00126380")

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
                    "filter": {
                        "results": [
                            growth_result("00126380", "revenue", "annual_yoy", "25", True)
                        ]
                    }
                },
                snapshots,
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
