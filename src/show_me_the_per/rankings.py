from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class ValuationSnapshot:
    corp_code: str
    corp_name: str
    stock_code: str
    per: Decimal | None = None
    pbr: Decimal | None = None
    roe: Decimal | None = None


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
    valuation_rankings = rank_valuation_snapshots(
        filter_valuation_snapshots(
            valuation_list,
            max_per=max_per,
            max_pbr=max_pbr,
            min_roe=min_roe,
        ),
        rank_by=rank_valuation_by,
    )

    return {
        "summary": {
            "growth_rankings": len(growth_rankings),
            "valuation_rankings": len(valuation_rankings),
            "valuation_inputs": len(valuation_list),
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
    )


def _parse_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if not text or text == "-":
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _decimal_to_string(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _json_ready(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value
