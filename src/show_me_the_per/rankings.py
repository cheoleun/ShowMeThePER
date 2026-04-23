from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable

from .growth import (
    ANNUAL_YOY,
    QUARTERLY_QOQ,
    QUARTERLY_YOY,
    TRAILING_FOUR_QUARTER_YOY,
)


VALID_GROWTH_METRICS = ("revenue", "operating_income", "net_income")
VALID_GROWTH_SERIES_TYPES = (
    ANNUAL_YOY,
    QUARTERLY_YOY,
    QUARTERLY_QOQ,
    TRAILING_FOUR_QUARTER_YOY,
)
VALID_SCREENING_SORT_KEYS = {"market_cap", "overall_minimum_growth_rate"}
VALID_VALUATION_SORT_KEYS = VALID_SCREENING_SORT_KEYS

DEFAULT_SCREENING_GROWTH_METRIC = "revenue"
DEFAULT_SCREENING_GROWTH_SERIES_TYPE = ANNUAL_YOY
DEFAULT_SCREENING_GROWTH_CONDITIONS = (
    {
        "metric": DEFAULT_SCREENING_GROWTH_METRIC,
        "series_type": DEFAULT_SCREENING_GROWTH_SERIES_TYPE,
    },
)

GROWTH_SERIES_LABELS = {
    ANNUAL_YOY: "연간 YoY",
    QUARTERLY_YOY: "분기 YoY",
    QUARTERLY_QOQ: "분기 QoQ",
    TRAILING_FOUR_QUARTER_YOY: "최근 4분기 누적 YoY",
}
GROWTH_METRIC_LABELS = {
    "revenue": "매출",
    "operating_income": "영업이익",
    "net_income": "순이익",
}


@dataclass(frozen=True)
class ValuationSnapshot:
    corp_code: str
    corp_name: str
    stock_code: str
    per: Decimal | None = None
    pbr: Decimal | None = None
    roe: Decimal | None = None
    eps: Decimal | None = None
    close_price: Decimal | None = None
    market_cap: Decimal | None = None
    market: str | None = None
    base_date: str = ""
    source: str = ""
    fetched_at: str = ""


def read_growth_metrics_payload(path: Path) -> dict[str, object]:
    return json.loads(path.read_text("utf-8"))


def read_valuation_snapshots(path: Path) -> list[ValuationSnapshot]:
    payload = json.loads(path.read_text("utf-8"))
    raw_values = payload["companies"] if isinstance(payload, dict) else payload
    return [_parse_valuation_snapshot(item) for item in raw_values]


def parse_growth_condition(value: str) -> dict[str, str]:
    text = str(value or "").strip()
    if not text:
        raise ValueError("growth condition is required")

    series_type, separator, metric = text.partition(":")
    if separator != ":":
        raise ValueError(
            "growth condition must use the form '<series_type>:<metric>'"
        )
    normalized = {
        "series_type": series_type.strip(),
        "metric": metric.strip(),
    }
    return _validate_growth_condition(normalized)


def normalize_growth_conditions(
    growth_conditions: Iterable[dict[str, object] | str] | None = None,
    *,
    growth_metric: str | None = None,
    growth_series_type: str | None = None,
) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    raw_conditions: list[dict[str, object] | str] = list(growth_conditions or [])
    if not raw_conditions:
        raw_conditions = [
            {
                "metric": growth_metric or DEFAULT_SCREENING_GROWTH_METRIC,
                "series_type": growth_series_type or DEFAULT_SCREENING_GROWTH_SERIES_TYPE,
            }
        ]

    for item in raw_conditions:
        if isinstance(item, str):
            condition = parse_growth_condition(item)
        elif isinstance(item, dict):
            condition = _validate_growth_condition(
                {
                    "metric": str(item.get("metric", "")).strip(),
                    "series_type": str(item.get("series_type", "")).strip(),
                }
            )
        else:
            raise ValueError("growth condition must be a string or dict")

        key = (condition["series_type"], condition["metric"])
        if key in seen:
            continue
        seen.add(key)
        normalized.append(condition)

    if not normalized:
        return [dict(item) for item in DEFAULT_SCREENING_GROWTH_CONDITIONS]
    return normalized


def build_ranking_payload(
    growth_metrics_payload: dict[str, object],
    valuation_snapshots: Iterable[ValuationSnapshot] = (),
    *,
    growth_conditions: Iterable[dict[str, object] | str] | None = None,
    growth_metric: str | None = None,
    growth_series_type: str | None = None,
    include_failed_growth: bool = False,
    max_per: Decimal | None = None,
    max_pbr: Decimal | None = None,
    min_roe: Decimal | None = None,
    rank_valuation_by: str = "roe",
) -> dict[str, object]:
    valuation_list = list(valuation_snapshots)
    normalized_conditions = normalize_growth_conditions(
        growth_conditions,
        growth_metric=growth_metric,
        growth_series_type=growth_series_type,
    )
    primary_condition = normalized_conditions[0]
    growth_rankings = rank_growth_filter_results(
        _growth_filter_results(growth_metrics_payload),
        metric=primary_condition["metric"],
        series_type=primary_condition["series_type"],
        include_failed=include_failed_growth,
    )
    filtered_valuations = filter_valuation_snapshots(
        valuation_list,
        max_per=max_per,
        max_pbr=max_pbr,
        min_roe=min_roe,
    )
    valuation_rankings = rank_valuation_snapshots(
        filtered_valuations,
        rank_by=rank_valuation_by,
    )
    screening_rows = build_screening_rows(
        _growth_filter_results(growth_metrics_payload),
        valuation_list,
        growth_conditions=normalized_conditions,
        include_failed_growth=include_failed_growth,
        sort_by="market_cap",
    )

    return {
        "summary": {
            "growth_rankings": len(growth_rankings),
            "valuation_rankings": len(valuation_rankings),
            "valuation_inputs": len(valuation_list),
            "screening_rows": len(screening_rows),
        },
        "filters": {
            "growth_conditions": normalized_conditions,
            "growth_metric": primary_condition["metric"],
            "growth_series_type": primary_condition["series_type"],
            "include_failed_growth": include_failed_growth,
            "max_per": _decimal_to_string(max_per),
            "max_pbr": _decimal_to_string(max_pbr),
            "min_roe": _decimal_to_string(min_roe),
            "rank_valuation_by": rank_valuation_by,
        },
        "growth_rankings": growth_rankings,
        "valuation_rankings": valuation_rankings,
        "screening_rows": screening_rows,
    }


def write_ranking_payload(
    path: Path,
    growth_metrics_payload: dict[str, object],
    valuation_snapshots: Iterable[ValuationSnapshot] = (),
    *,
    growth_conditions: Iterable[dict[str, object] | str] | None = None,
    growth_metric: str | None = None,
    growth_series_type: str | None = None,
    include_failed_growth: bool = False,
    max_per: Decimal | None = None,
    max_pbr: Decimal | None = None,
    min_roe: Decimal | None = None,
    rank_valuation_by: str = "roe",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            build_ranking_payload(
                growth_metrics_payload,
                valuation_snapshots,
                growth_conditions=growth_conditions,
                growth_metric=growth_metric,
                growth_series_type=growth_series_type,
                include_failed_growth=include_failed_growth,
                max_per=max_per,
                max_pbr=max_pbr,
                min_roe=min_roe,
                rank_valuation_by=rank_valuation_by,
            ),
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def rank_growth_filter_results(
    filter_results: Iterable[dict[str, object]],
    *,
    metric: str | None = None,
    series_type: str | None = None,
    include_failed: bool = False,
) -> list[dict[str, object]]:
    candidates: list[tuple[Decimal, dict[str, object]]] = []
    for result in filter_results:
        if metric is not None and result.get("metric") != metric:
            continue
        if series_type is not None and result.get("series_type") != series_type:
            continue
        if not include_failed and result.get("passed") is not True:
            continue

        minimum_rate = _parse_decimal(result.get("minimum_growth_rate"))
        if minimum_rate is None:
            continue
        candidates.append((minimum_rate, result))

    return [
        {
            "rank": rank,
            "corp_code": str(result.get("corp_code", "")),
            "metric": str(result.get("metric", "")),
            "series_type": str(result.get("series_type", "")),
            "recent_periods": result.get("recent_periods"),
            "minimum_growth_rate": str(minimum_rate),
            "passed": result.get("passed") is True,
        }
        for rank, (minimum_rate, result) in enumerate(
            sorted(candidates, key=lambda item: item[0], reverse=True),
            start=1,
        )
    ]


def filter_valuation_snapshots(
    snapshots: Iterable[ValuationSnapshot],
    *,
    max_per: Decimal | None = None,
    max_pbr: Decimal | None = None,
    min_roe: Decimal | None = None,
) -> list[ValuationSnapshot]:
    filtered: list[ValuationSnapshot] = []
    for snapshot in snapshots:
        if max_per is not None and (
            snapshot.per is None or snapshot.per > max_per
        ):
            continue
        if max_pbr is not None and (
            snapshot.pbr is None or snapshot.pbr > max_pbr
        ):
            continue
        if min_roe is not None and (
            snapshot.roe is None or snapshot.roe < min_roe
        ):
            continue
        filtered.append(snapshot)

    return filtered


def rank_valuation_snapshots(
    snapshots: Iterable[ValuationSnapshot],
    *,
    rank_by: str = "roe",
) -> list[dict[str, object]]:
    if rank_by not in {"per", "pbr", "roe"}:
        raise ValueError("rank_by must be one of per, pbr, roe.")

    candidates: list[ValuationSnapshot] = [
        snapshot
        for snapshot in snapshots
        if getattr(snapshot, rank_by) is not None
    ]
    descending = rank_by == "roe"

    return [
        {
            "rank": rank,
            **_json_ready(asdict(snapshot)),
            "rank_by": rank_by,
            "rank_value": str(getattr(snapshot, rank_by)),
        }
        for rank, snapshot in enumerate(
            sorted(
                candidates,
                key=lambda snapshot: getattr(snapshot, rank_by) or Decimal("0"),
                reverse=descending,
            ),
            start=1,
        )
    ]


def build_screening_rows(
    filter_results: Iterable[dict[str, object]],
    valuation_snapshots: Iterable[ValuationSnapshot],
    *,
    company_index: dict[str, dict[str, str]] | None = None,
    price_index: dict[str, dict[str, object]] | None = None,
    growth_conditions: Iterable[dict[str, object] | str] | None = None,
    growth_metric: str | None = None,
    growth_series_type: str | None = None,
    include_failed_growth: bool = False,
    max_per: Decimal | None = None,
    max_pbr: Decimal | None = None,
    min_roe: Decimal | None = None,
    market: str | None = None,
    sort_by: str = "market_cap",
) -> list[dict[str, object]]:
    normalized_conditions = normalize_growth_conditions(
        growth_conditions,
        growth_metric=growth_metric,
        growth_series_type=growth_series_type,
    )
    if sort_by not in VALID_SCREENING_SORT_KEYS:
        raise ValueError(
            "sort_by must be one of market_cap, overall_minimum_growth_rate."
        )

    company_profiles = company_index or {}
    latest_valuations = _latest_snapshots_by_corp_code(valuation_snapshots)
    normalized_market = (market or "").strip().upper()
    grouped_results = _group_filter_results_by_company(filter_results)
    rows: list[dict[str, object]] = []

    for corp_code in sorted(grouped_results):
        checks = _build_growth_checks(
            grouped_results.get(corp_code, {}),
            growth_conditions=normalized_conditions,
        )
        passed = bool(checks) and all(check["passed"] is True for check in checks)
        if not include_failed_growth and not passed:
            continue

        valuation = latest_valuations.get(corp_code)
        price_profile = _dict((price_index or {}).get(corp_code))
        profile = company_profiles.get(corp_code, {})
        overall_minimum = _overall_minimum_growth_rate(checks)
        matched_count = sum(check["passed"] is True for check in checks)
        market_value = (
            str(price_profile.get("market", "") or "")
            or (valuation.market if valuation else "")
        )

        if normalized_market and market_value.upper() != normalized_market:
            continue

        row = {
            "corp_code": corp_code,
            "corp_name": (
                (valuation.corp_name if valuation is not None else "")
                or profile.get("corp_name", "")
            ),
            "stock_code": (
                (valuation.stock_code if valuation is not None else "")
                or profile.get("stock_code", "")
            ),
            "market": market_value,
            "close_price": _decimal_to_string(
                _coalesce_decimal(
                    _parse_decimal(price_profile.get("close_price")),
                    valuation.close_price if valuation else None,
                )
            ),
            "market_cap": _decimal_to_string(
                _coalesce_decimal(
                    _parse_decimal(price_profile.get("market_cap")),
                    valuation.market_cap if valuation else None,
                )
            ),
            "per": _decimal_to_string(valuation.per if valuation else None),
            "pbr": _decimal_to_string(valuation.pbr if valuation else None),
            "roe": _decimal_to_string(valuation.roe if valuation else None),
            "eps": _decimal_to_string(valuation.eps if valuation else None),
            "base_date": (
                str(price_profile.get("base_date", "") or "")
                or (valuation.base_date if valuation else "")
            ),
            "source": (
                str(price_profile.get("source", "") or "")
                or (valuation.source if valuation else "")
            ),
            "fetched_at": valuation.fetched_at if valuation else "",
            "growth_checks": checks,
            "matched_growth_condition_count": matched_count,
            "total_growth_condition_count": len(checks),
            "overall_minimum_growth_rate": _decimal_to_string(overall_minimum),
            "minimum_growth_rate": _decimal_to_string(overall_minimum),
            "passed": passed,
        }
        if len(normalized_conditions) == 1:
            row["metric"] = normalized_conditions[0]["metric"]
            row["series_type"] = normalized_conditions[0]["series_type"]
        else:
            row["metric"] = ""
            row["series_type"] = ""
        rows.append(row)

    rows.sort(key=lambda row: _screening_sort_key(row, sort_by))
    for index, row in enumerate(rows, start=1):
        row["growth_rank"] = index
    return rows


def _build_growth_checks(
    result_index: dict[tuple[str, str], dict[str, object]],
    *,
    growth_conditions: list[dict[str, str]],
) -> list[dict[str, object]]:
    checks: list[dict[str, object]] = []
    for condition in growth_conditions:
        series_type = condition["series_type"]
        metric = condition["metric"]
        result = result_index.get((series_type, metric))
        minimum_growth_rate = None if result is None else result.get("minimum_growth_rate")
        check = {
            "metric": metric,
            "metric_label": GROWTH_METRIC_LABELS.get(metric, metric),
            "series_type": series_type,
            "series_label": GROWTH_SERIES_LABELS.get(series_type, series_type),
            "recent_periods": (
                0 if result is None else int(result.get("recent_periods", 0) or 0)
            ),
            "minimum_growth_rate": None
            if minimum_growth_rate in {None, ""}
            else str(minimum_growth_rate),
            "passed": result.get("passed") is True if result is not None else False,
        }
        checks.append(check)
    return checks


def _group_filter_results_by_company(
    filter_results: Iterable[dict[str, object]],
) -> dict[str, dict[tuple[str, str], dict[str, object]]]:
    grouped: dict[str, dict[tuple[str, str], dict[str, object]]] = {}
    for result in filter_results:
        corp_code = str(result.get("corp_code", "")).strip()
        metric = str(result.get("metric", "")).strip()
        series_type = str(result.get("series_type", "")).strip()
        if not corp_code or not metric or not series_type:
            continue
        grouped.setdefault(corp_code, {})[(series_type, metric)] = result
    return grouped


def _overall_minimum_growth_rate(
    growth_checks: Iterable[dict[str, object]],
) -> Decimal | None:
    rates = [
        _parse_decimal(check.get("minimum_growth_rate"))
        for check in growth_checks
    ]
    valid_rates = [rate for rate in rates if rate is not None]
    if not valid_rates:
        return None
    return min(valid_rates)


def _screening_sort_key(
    row: dict[str, object],
    sort_by: str,
) -> tuple[int, int, Decimal, str, str]:
    passed_rank = 0 if row.get("passed") is True else 1
    value = _parse_decimal(row.get(sort_by))
    missing_rank = 0 if value is not None else 1
    if value is None:
        normalized_value = Decimal("0")
    elif sort_by in {"market_cap", "overall_minimum_growth_rate"}:
        normalized_value = -value
    else:
        normalized_value = value
    return (
        passed_rank,
        missing_rank,
        normalized_value,
        str(row.get("corp_name", "")),
        str(row.get("corp_code", "")),
    )


def _latest_snapshots_by_corp_code(
    snapshots: Iterable[ValuationSnapshot],
) -> dict[str, ValuationSnapshot]:
    latest: dict[str, ValuationSnapshot] = {}
    for snapshot in snapshots:
        current = latest.get(snapshot.corp_code)
        if current is None or _snapshot_sort_key(snapshot) > _snapshot_sort_key(current):
            latest[snapshot.corp_code] = snapshot
    return latest


def _snapshot_sort_key(snapshot: ValuationSnapshot) -> tuple[str, str]:
    return (snapshot.base_date, snapshot.fetched_at)


def _coalesce_decimal(*values: Decimal | None) -> Decimal | None:
    for value in values:
        if value is not None:
            return value
    return None


def _validate_growth_condition(condition: dict[str, str]) -> dict[str, str]:
    metric = condition.get("metric", "").strip()
    series_type = condition.get("series_type", "").strip()
    if metric not in VALID_GROWTH_METRICS:
        raise ValueError(
            f"unsupported growth metric: {metric or '-'}"
        )
    if series_type not in VALID_GROWTH_SERIES_TYPES:
        raise ValueError(
            f"unsupported growth series type: {series_type or '-'}"
        )
    return {
        "metric": metric,
        "series_type": series_type,
    }


def _dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _growth_filter_results(payload: dict[str, object]) -> list[dict[str, object]]:
    filters = payload.get("filter", {})
    if not isinstance(filters, dict):
        return []
    results = filters.get("results", [])
    if not isinstance(results, list):
        return []
    return [result for result in results if isinstance(result, dict)]


def _parse_valuation_snapshot(item: dict[str, object]) -> ValuationSnapshot:
    return ValuationSnapshot(
        corp_code=str(item.get("corp_code", "")).strip(),
        corp_name=str(item.get("corp_name", "")).strip(),
        stock_code=str(item.get("stock_code", "")).strip(),
        per=_parse_decimal(item.get("per")),
        pbr=_parse_decimal(item.get("pbr")),
        roe=_parse_decimal(item.get("roe")),
        eps=_parse_decimal(item.get("eps")),
        close_price=_parse_decimal(item.get("close_price")),
        market_cap=_parse_decimal(item.get("market_cap")),
        market=_optional_text(item.get("market")),
        base_date=str(item.get("base_date", "")).strip(),
        source=str(item.get("source", "")).strip(),
        fetched_at=str(item.get("fetched_at", "")).strip(),
    )


def _parse_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if not text or text.upper() in {"-", "N/A"}:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _decimal_to_string(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _json_ready(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value
