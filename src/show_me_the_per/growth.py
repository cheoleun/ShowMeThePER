from __future__ import annotations

import json
from dataclasses import asdict
from decimal import Decimal, InvalidOperation
from itertools import groupby
from pathlib import Path
from typing import Iterable

from .models import FinancialPeriodValue, GrowthPoint


ANNUAL_YOY = "annual_yoy"
QUARTERLY_YOY = "quarterly_yoy"
TRAILING_FOUR_QUARTER_YOY = "trailing_four_quarter_yoy"
QUARTERLY_QOQ = "quarterly_qoq"
FILTERABLE_METRICS = frozenset({"revenue", "operating_income", "net_income"})


def calculate_annual_yoy_growth(
    values: Iterable[FinancialPeriodValue],
) -> list[GrowthPoint]:
    annual_values = sorted(
        (value for value in values if value.period_type == "annual"),
        key=lambda value: (value.corp_code, value.metric, value.fiscal_year),
    )
    points: list[GrowthPoint] = []

    for _, grouped_values in groupby(
        annual_values,
        key=lambda value: (value.corp_code, value.metric),
    ):
        by_year = {value.fiscal_year: value for value in grouped_values}
        for year in sorted(by_year):
            current = by_year[year]
            previous = by_year.get(year - 1)
            points.append(
                GrowthPoint(
                    corp_code=current.corp_code,
                    metric=current.metric,
                    series_type=ANNUAL_YOY,
                    fiscal_year=current.fiscal_year,
                    fiscal_quarter=None,
                    amount=current.amount,
                    base_amount=None if previous is None else previous.amount,
                    growth_rate=_calculate_growth_rate(
                        current.amount,
                        None if previous is None else previous.amount,
                    ),
                )
            )

    return points


def calculate_quarterly_yoy_growth(
    values: Iterable[FinancialPeriodValue],
) -> list[GrowthPoint]:
    quarterly_values = _quarterly_values_by_identity(values)
    points: list[GrowthPoint] = []

    for _, grouped_values in quarterly_values:
        by_period = {
            _quarter_index(value.fiscal_year, value.fiscal_quarter): value
            for value in grouped_values
            if value.fiscal_quarter is not None
        }
        for period_index in sorted(by_period):
            current = by_period[period_index]
            previous = by_period.get(period_index - 4)
            points.append(
                GrowthPoint(
                    corp_code=current.corp_code,
                    metric=current.metric,
                    series_type=QUARTERLY_YOY,
                    fiscal_year=current.fiscal_year,
                    fiscal_quarter=current.fiscal_quarter,
                    amount=current.amount,
                    base_amount=None if previous is None else previous.amount,
                    growth_rate=_calculate_growth_rate(
                        current.amount,
                        None if previous is None else previous.amount,
                    ),
                )
            )

    return points


def calculate_trailing_four_quarter_yoy_growth(
    values: Iterable[FinancialPeriodValue],
) -> list[GrowthPoint]:
    quarterly_values = _quarterly_values_by_identity(values)
    points: list[GrowthPoint] = []

    for _, grouped_values in quarterly_values:
        by_period = {
            _quarter_index(value.fiscal_year, value.fiscal_quarter): value
            for value in grouped_values
            if value.fiscal_quarter is not None
        }
        for period_index in sorted(by_period):
            current_window = _sum_quarter_window(by_period, period_index)
            previous_window = _sum_quarter_window(by_period, period_index - 4)
            current = by_period[period_index]
            if current_window is None:
                continue

            points.append(
                GrowthPoint(
                    corp_code=current.corp_code,
                    metric=current.metric,
                    series_type=TRAILING_FOUR_QUARTER_YOY,
                    fiscal_year=current.fiscal_year,
                    fiscal_quarter=current.fiscal_quarter,
                    amount=current_window,
                    base_amount=previous_window,
                    growth_rate=_calculate_growth_rate(
                        current_window,
                        previous_window,
                    ),
                )
            )

    return points


def calculate_quarterly_qoq_growth(
    values: Iterable[FinancialPeriodValue],
) -> list[GrowthPoint]:
    quarterly_values = _quarterly_values_by_identity(values)
    points: list[GrowthPoint] = []

    for _, grouped_values in quarterly_values:
        by_period = {
            _quarter_index(value.fiscal_year, value.fiscal_quarter): value
            for value in grouped_values
            if value.fiscal_quarter is not None
        }
        for period_index in sorted(by_period):
            current = by_period[period_index]
            previous = by_period.get(period_index - 1)
            points.append(
                GrowthPoint(
                    corp_code=current.corp_code,
                    metric=current.metric,
                    series_type=QUARTERLY_QOQ,
                    fiscal_year=current.fiscal_year,
                    fiscal_quarter=current.fiscal_quarter,
                    amount=current.amount,
                    base_amount=None if previous is None else previous.amount,
                    growth_rate=_calculate_growth_rate(
                        current.amount,
                        None if previous is None else previous.amount,
                    ),
                )
            )

    return points


def build_default_growth_points(
    values: Iterable[FinancialPeriodValue],
) -> list[GrowthPoint]:
    copied_values = list(values)
    return [
        *calculate_annual_yoy_growth(copied_values),
        *calculate_quarterly_yoy_growth(copied_values),
        *calculate_trailing_four_quarter_yoy_growth(copied_values),
        *calculate_quarterly_qoq_growth(copied_values),
    ]


def passes_recent_growth_threshold(
    points: Iterable[GrowthPoint],
    *,
    threshold_percent: Decimal,
    recent_periods: int,
) -> bool:
    recent_points = _recent_points(points, recent_periods)
    if len(recent_points) < recent_periods:
        return False

    return all(
        point.growth_rate is not None
        and point.growth_rate >= threshold_percent
        for point in recent_points
    )


def minimum_recent_growth_rate(
    points: Iterable[GrowthPoint],
    *,
    recent_periods: int,
) -> Decimal | None:
    recent_points = _recent_points(points, recent_periods)
    rates = [
        point.growth_rate
        for point in recent_points
        if point.growth_rate is not None
    ]
    if len(rates) < recent_periods:
        return None

    return min(rates)


def build_growth_metrics_payload(
    values: Iterable[FinancialPeriodValue],
    *,
    threshold_percent: Decimal = Decimal("20"),
    recent_annual_periods: int = 3,
    recent_quarterly_periods: int = 12,
) -> dict[str, object]:
    copied_values = list(values)
    points = build_default_growth_points(copied_values)
    return {
        "summary": {
            "values": len(copied_values),
            "growth_points": len(points),
            "corp_codes": sorted({value.corp_code for value in copied_values}),
            "metrics": sorted({value.metric for value in copied_values}),
        },
        "filter": {
            "threshold_percent": str(threshold_percent),
            "recent_annual_periods": recent_annual_periods,
            "recent_quarterly_periods": recent_quarterly_periods,
            "results": _build_filter_results(
                points,
                threshold_percent=threshold_percent,
                recent_annual_periods=recent_annual_periods,
                recent_quarterly_periods=recent_quarterly_periods,
            ),
        },
        "growth_points": [_json_ready(asdict(point)) for point in points],
    }


def write_growth_metrics_payload(
    path: Path,
    values: Iterable[FinancialPeriodValue],
    *,
    threshold_percent: Decimal = Decimal("20"),
    recent_annual_periods: int = 3,
    recent_quarterly_periods: int = 12,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            build_growth_metrics_payload(
                values,
                threshold_percent=threshold_percent,
                recent_annual_periods=recent_annual_periods,
                recent_quarterly_periods=recent_quarterly_periods,
            ),
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def read_financial_period_values(path: Path) -> list[FinancialPeriodValue]:
    payload = json.loads(path.read_text("utf-8"))
    raw_values = payload["values"] if isinstance(payload, dict) else payload
    return [_parse_period_value(item) for item in raw_values]


def _parse_period_value(item: dict[str, object]) -> FinancialPeriodValue:
    amount = _parse_decimal(item.get("amount"))
    if amount is None:
        raise ValueError("financial period value amount is required")

    fiscal_quarter = item.get("fiscal_quarter")
    return FinancialPeriodValue(
        corp_code=str(item["corp_code"]),
        metric=str(item["metric"]),
        period_type=str(item["period_type"]),
        fiscal_year=int(item["fiscal_year"]),
        fiscal_quarter=None if fiscal_quarter is None else int(fiscal_quarter),
        amount=amount,
    )


def _build_filter_results(
    points: list[GrowthPoint],
    *,
    threshold_percent: Decimal,
    recent_annual_periods: int,
    recent_quarterly_periods: int,
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    sorted_points = sorted(
        points,
        key=lambda point: (point.corp_code, point.metric, point.series_type),
    )

    for (corp_code, metric, series_type), grouped_points in groupby(
        sorted_points,
        key=lambda point: (point.corp_code, point.metric, point.series_type),
    ):
        if metric not in FILTERABLE_METRICS:
            continue
        grouped = list(grouped_points)
        recent_periods = (
            recent_annual_periods
            if series_type == ANNUAL_YOY
            else recent_quarterly_periods
        )
        minimum_rate = minimum_recent_growth_rate(
            grouped,
            recent_periods=recent_periods,
        )
        results.append(
            {
                "corp_code": corp_code,
                "metric": metric,
                "series_type": series_type,
                "recent_periods": recent_periods,
                "minimum_growth_rate": (
                    None if minimum_rate is None else str(minimum_rate)
                ),
                "passed": passes_recent_growth_threshold(
                    grouped,
                    threshold_percent=threshold_percent,
                    recent_periods=recent_periods,
                ),
            }
        )

    return results


def _quarterly_values_by_identity(
    values: Iterable[FinancialPeriodValue],
) -> Iterable[tuple[tuple[str, str], Iterable[FinancialPeriodValue]]]:
    quarterly_values = sorted(
        (
            value
            for value in values
            if value.period_type == "quarter" and value.fiscal_quarter is not None
        ),
        key=lambda value: (
            value.corp_code,
            value.metric,
            value.fiscal_year,
            value.fiscal_quarter or 0,
        ),
    )
    return groupby(quarterly_values, key=lambda value: (value.corp_code, value.metric))


def _sum_quarter_window(
    by_period: dict[int, FinancialPeriodValue],
    period_index: int,
) -> Decimal | None:
    amounts: list[Decimal] = []
    for index in range(period_index - 3, period_index + 1):
        value = by_period.get(index)
        if value is None:
            return None
        amounts.append(value.amount)

    return sum(amounts, Decimal("0"))


def _calculate_growth_rate(
    current_amount: Decimal,
    previous_amount: Decimal | None,
) -> Decimal | None:
    if previous_amount is None or previous_amount <= 0:
        return None

    return (current_amount - previous_amount) / previous_amount * Decimal("100")


def _recent_points(
    points: Iterable[GrowthPoint],
    recent_periods: int,
) -> list[GrowthPoint]:
    return sorted(points, key=_point_period_index, reverse=True)[:recent_periods]


def _point_period_index(point: GrowthPoint) -> int:
    if point.fiscal_quarter is None:
        return point.fiscal_year
    return _quarter_index(point.fiscal_year, point.fiscal_quarter)


def _quarter_index(year: int, quarter: int | None) -> int:
    if quarter is None:
        raise ValueError("quarter is required for quarterly growth calculation")
    return year * 4 + quarter - 1


def _parse_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value).replace(",", ""))
    except InvalidOperation:
        return None


def _json_ready(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value
