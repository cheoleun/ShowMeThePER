from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable


VALID_VALUATION_SORT_KEYS = {"market_cap", "per", "pbr", "roe"}
DEFAULT_SCREENING_GROWTH_METRIC = "revenue"
DEFAULT_SCREENING_GROWTH_SERIES_TYPE = "annual_yoy"


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


def build_ranking_payload(
    growth_metrics_payload: dict[str, object],
    valuation_snapshots: Iterable[ValuationSnapshot] = (),
    *,
    growth_metric: str | None = None,
    growth_series_type: str | None = None,
    include_failed_growth: bool = False,
    max_per: Decimal | None = None,
    max_pbr: Decimal | None = None,
    min_roe: Decimal | None = None,
    rank_valuation_by: str = "roe",
) -> dict[str, object]:
    valuation_list = list(valuation_snapshots)
    growth_rankings = rank_growth_filter_results(
        _growth_filter_results(growth_metrics_payload),
        metric=growth_metric,
        series_type=growth_series_type,
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
        growth_metric=growth_metric,
        growth_series_type=growth_series_type,
        include_failed_growth=include_failed_growth,
        max_per=max_per,
        max_pbr=max_pbr,
        min_roe=min_roe,
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
            "growth_metric": growth_metric,
            "growth_series_type": growth_series_type,
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
    growth_metric: str | None = None,
    growth_series_type: str | None = None,
    include_failed_growth: bool = False,
    max_per: Decimal | None = None,
    max_pbr: Decimal | None = None,
    min_roe: Decimal | None = None,
    market: str | None = None,
    sort_by: str = "market_cap",
) -> list[dict[str, object]]:
    if sort_by not in VALID_VALUATION_SORT_KEYS:
        raise ValueError(
            "sort_by must be one of market_cap, per, pbr, roe."
        )

    company_profiles = company_index or {}
    latest_valuations = _latest_snapshots_by_corp_code(valuation_snapshots)
    normalized_market = (market or "").strip().upper()
    rows: list[dict[str, object]] = []

    grouped_results = _group_filter_results_by_company(
        filter_results,
        metric=growth_metric,
        series_type=growth_series_type,
    )

    growth_ranked = sorted(
        grouped_results.items(),
        key=lambda item: _parse_decimal(item[1]["minimum_growth_rate"]) or Decimal("-999999"),
        reverse=True,
    )

    for growth_rank, (corp_code, summary) in enumerate(growth_ranked, start=1):
        if not include_failed_growth and summary["passed"] is not True:
            continue
        valuation = latest_valuations.get(corp_code)
        price_profile = _dict((price_index or {}).get(corp_code))
        profile = company_profiles.get(corp_code, {})

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
            "metric": summary.get("metric"),
            "series_type": summary.get("series_type"),
            "recent_periods": summary.get("recent_periods"),
            "minimum_growth_rate": summary.get("minimum_growth_rate"),
            "growth_rank": growth_rank,
            "passed": summary.get("passed") is True,
            "per": _decimal_to_string(valuation.per if valuation else None),
            "pbr": _decimal_to_string(valuation.pbr if valuation else None),
            "roe": _decimal_to_string(valuation.roe if valuation else None),
            "eps": _decimal_to_string(valuation.eps if valuation else None),
            "market": (
                str(price_profile.get("market", "") or "")
                or (valuation.market if valuation else "")
            ),
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
            "base_date": (
                str(price_profile.get("base_date", "") or "")
                or (valuation.base_date if valuation else "")
            ),
            "source": (
                str(price_profile.get("source", "") or "")
                or (valuation.source if valuation else "")
            ),
            "fetched_at": valuation.fetched_at if valuation else "",
        }

        if normalized_market and str(row["market"]).upper() != normalized_market:
            continue
        if max_per is not None and _parse_decimal(row["per"]) is None:
            continue
        if max_pbr is not None and _parse_decimal(row["pbr"]) is None:
            continue
        if min_roe is not None and _parse_decimal(row["roe"]) is None:
            continue
        if max_per is not None and (_parse_decimal(row["per"]) or Decimal("0")) > max_per:
            continue
        if max_pbr is not None and (_parse_decimal(row["pbr"]) or Decimal("0")) > max_pbr:
            continue
        if min_roe is not None and (_parse_decimal(row["roe"]) or Decimal("0")) < min_roe:
            continue
        rows.append(row)

    return sorted(
        rows,
        key=lambda row: _screening_sort_key(row, sort_by),
    )


def _group_filter_results_by_company(
    filter_results: Iterable[dict[str, object]],
    *,
    metric: str | None,
    series_type: str | None,
) -> dict[str, dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = {}
    for result in filter_results:
        if metric is not None and result.get("metric") != metric:
            continue
        if series_type is not None and result.get("series_type") != series_type:
            continue
        corp_code = str(result.get("corp_code", ""))
        if not corp_code:
            continue
        grouped.setdefault(corp_code, []).append(result)

    summary: dict[str, dict[str, object]] = {}
    for corp_code, results in grouped.items():
        minimum_rates = [
            _parse_decimal(result.get("minimum_growth_rate"))
            for result in results
        ]
        valid_rates = [rate for rate in minimum_rates if rate is not None]
        summary[corp_code] = {
            "metric": metric,
            "series_type": series_type,
            "recent_periods": max(
                int(result.get("recent_periods", 0) or 0)
                for result in results
            ),
            "minimum_growth_rate": (
                None if not valid_rates else str(min(valid_rates))
            ),
            "passed": all(result.get("passed") is True for result in results),
        }
    return summary


def _screening_sort_key(row: dict[str, object], sort_by: str) -> tuple[int, Decimal]:
    value = _parse_decimal(row.get(sort_by))
    if value is None:
        return (1, Decimal("0"))
    if sort_by in {"per", "pbr"}:
        return (0, value)
    return (0, -value)


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
