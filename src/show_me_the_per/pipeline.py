from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Protocol

from .financials import (
    CORE_METRICS,
    METRIC_ORDER,
    REPORT_CODE_ANNUAL,
    REPORT_CODE_HALF,
    REPORT_CODE_Q1,
    REPORT_CODE_Q3,
    REPORT_CODE_QUARTERS,
    build_period_values_from_rows,
    write_financial_period_values,
    write_financial_statement_rows,
)
from .growth import build_growth_metrics_payload
from .models import FinancialPeriodValue, FinancialStatementRow


DEFAULT_REPORT_CODES = (
    REPORT_CODE_Q1,
    REPORT_CODE_HALF,
    REPORT_CODE_Q3,
    REPORT_CODE_ANNUAL,
)


class MajorAccountClient(Protocol):
    def fetch_major_accounts(
        self,
        corp_codes: list[str],
        business_year: str,
        report_code: str,
        fs_div: str | None = None,
        batch_size: int = 100,
    ) -> list[FinancialStatementRow]:
        ...


@dataclass(frozen=True)
class CollectionError:
    corp_codes: tuple[str, ...]
    business_year: str
    report_code: str
    fs_div: str | None
    error_type: str
    message: str


@dataclass(frozen=True)
class CollectionRun:
    rows: list[FinancialStatementRow]
    errors: list[CollectionError] = field(default_factory=list)


@dataclass(frozen=True)
class AnalysisArtifacts:
    financial_statement_rows: list[FinancialStatementRow]
    financial_period_values: list[FinancialPeriodValue]
    growth_metrics: dict[str, object]
    coverage_report: dict[str, object]
    collection_errors: list[CollectionError] = field(default_factory=list)


def collect_financial_statement_rows(
    client: MajorAccountClient,
    *,
    corp_codes: Iterable[str],
    business_years: Iterable[str | int],
    report_codes: Iterable[str] = DEFAULT_REPORT_CODES,
    fs_div: str | None = None,
    batch_size: int = 100,
) -> list[FinancialStatementRow]:
    return collect_financial_statement_run(
        client,
        corp_codes=corp_codes,
        business_years=business_years,
        report_codes=report_codes,
        fs_div=fs_div,
        batch_size=batch_size,
        continue_on_error=False,
    ).rows


def collect_financial_statement_run(
    client: MajorAccountClient,
    *,
    corp_codes: Iterable[str],
    business_years: Iterable[str | int],
    report_codes: Iterable[str] = DEFAULT_REPORT_CODES,
    fs_div: str | None = None,
    batch_size: int = 100,
    continue_on_error: bool = True,
) -> CollectionRun:
    unique_corp_codes = _dedupe_strings(str(code) for code in corp_codes)
    if not unique_corp_codes:
        raise ValueError("at least one corp code is required")

    unique_business_years = _dedupe_strings(str(year) for year in business_years)
    if not unique_business_years:
        raise ValueError("at least one business year is required")

    unique_report_codes = _dedupe_strings(report_codes)
    if not unique_report_codes:
        raise ValueError("at least one report code is required")

    rows: list[FinancialStatementRow] = []
    errors: list[CollectionError] = []
    for business_year in unique_business_years:
        for report_code in unique_report_codes:
            try:
                rows.extend(
                    client.fetch_major_accounts(
                        corp_codes=unique_corp_codes,
                        business_year=business_year,
                        report_code=report_code,
                        fs_div=fs_div,
                        batch_size=batch_size,
                    )
                )
            except Exception as error:
                if not continue_on_error:
                    raise
                errors.append(
                    CollectionError(
                        corp_codes=tuple(unique_corp_codes),
                        business_year=business_year,
                        report_code=report_code,
                        fs_div=fs_div,
                        error_type=type(error).__name__,
                        message=str(error),
                    )
                )

    return CollectionRun(rows=rows, errors=errors)


def build_analysis_artifacts(
    rows: Iterable[FinancialStatementRow],
    *,
    collection_errors: Iterable[CollectionError] = (),
    expected_corp_codes: Iterable[str] | None = None,
    expected_business_years: Iterable[str | int] | None = None,
    expected_report_codes: Iterable[str] | None = DEFAULT_REPORT_CODES,
    threshold_percent: Decimal = Decimal("20"),
    recent_annual_periods: int = 3,
    recent_quarterly_periods: int = 12,
) -> AnalysisArtifacts:
    copied_rows = list(rows)
    copied_errors = list(collection_errors)
    period_values = build_period_values_from_rows(copied_rows)
    growth_metrics = build_growth_metrics_payload(
        period_values,
        threshold_percent=threshold_percent,
        recent_annual_periods=recent_annual_periods,
        recent_quarterly_periods=recent_quarterly_periods,
    )
    coverage_report = build_coverage_report(
        copied_rows,
        period_values,
        growth_metrics=growth_metrics,
        expected_corp_codes=expected_corp_codes,
        expected_business_years=expected_business_years,
        expected_report_codes=expected_report_codes,
        collection_errors=copied_errors,
    )
    return AnalysisArtifacts(
        financial_statement_rows=copied_rows,
        financial_period_values=period_values,
        growth_metrics=growth_metrics,
        coverage_report=coverage_report,
        collection_errors=copied_errors,
    )


def build_coverage_report(
    rows: Iterable[FinancialStatementRow],
    period_values: Iterable[FinancialPeriodValue],
    *,
    growth_metrics: dict[str, object] | None = None,
    collection_errors: Iterable[CollectionError] = (),
    expected_corp_codes: Iterable[str] | None = None,
    expected_business_years: Iterable[str | int] | None = None,
    expected_report_codes: Iterable[str] | None = DEFAULT_REPORT_CODES,
    expected_metrics: Iterable[str] = CORE_METRICS,
) -> dict[str, object]:
    copied_rows = list(rows)
    copied_values = list(period_values)
    copied_errors = list(collection_errors)
    metrics = list(expected_metrics)

    corp_codes = _dedupe_strings(
        [
            *(str(code) for code in expected_corp_codes or []),
            *(row.corp_code for row in copied_rows),
            *(value.corp_code for value in copied_values),
        ]
    )
    business_years = _sorted_strings(
        [
            *(str(year) for year in expected_business_years or []),
            *(row.business_year for row in copied_rows),
        ]
    )
    report_codes = _sorted_strings(
        [
            *(str(code) for code in expected_report_codes or []),
            *(row.report_code for row in copied_rows),
        ]
    )
    growth_results = _index_growth_filter_results(growth_metrics)
    errors_by_corp_code = _index_collection_errors(copied_errors)

    companies = [
        _build_company_coverage(
            corp_code,
            rows=[row for row in copied_rows if row.corp_code == corp_code],
            values=[value for value in copied_values if value.corp_code == corp_code],
            metrics=metrics,
            growth_results=growth_results,
            collection_errors=errors_by_corp_code.get(corp_code, []),
            expected_business_years=business_years,
            expected_report_codes=report_codes,
        )
        for corp_code in corp_codes
    ]
    missing_metric_entries = sum(
        len(company["missing_metrics"]) for company in companies
    )

    return {
        "summary": {
            "corp_codes": len(corp_codes),
            "raw_rows": len(copied_rows),
            "period_values": len(copied_values),
            "growth_points": _growth_point_count(growth_metrics),
            "business_years": business_years,
            "report_codes": report_codes,
            "expected_metrics": metrics,
            "collection_errors": len(copied_errors),
            "missing_metric_entries": missing_metric_entries,
            "companies_with_all_metrics": sum(
                1 for company in companies if not company["missing_metrics"]
            ),
            "companies_without_rows": sum(
                1 for company in companies if company["raw_rows"] == 0
            ),
            "companies_with_collection_errors": sum(
                1 for company in companies if company["collection_errors"]
            ),
        },
        "companies": companies,
        "collection_errors": _collection_errors_json(copied_errors),
    }


def write_analysis_outputs(
    output_dir: Path,
    artifacts: AnalysisArtifacts,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "financial_statements": output_dir / "financial-statements.json",
        "financial_period_values": output_dir / "financial-period-values.json",
        "growth_metrics": output_dir / "growth-metrics.json",
        "coverage_report": output_dir / "coverage-report.json",
        "collection_errors": output_dir / "collection-errors.json",
    }

    write_financial_statement_rows(
        paths["financial_statements"],
        artifacts.financial_statement_rows,
    )
    write_financial_period_values(
        paths["financial_period_values"],
        artifacts.financial_period_values,
    )
    _write_json(paths["growth_metrics"], artifacts.growth_metrics)
    _write_json(paths["coverage_report"], artifacts.coverage_report)
    _write_json(
        paths["collection_errors"],
        build_collection_error_payload(artifacts.collection_errors),
    )

    return paths


def build_collection_error_payload(
    errors: Iterable[CollectionError],
) -> dict[str, object]:
    copied_errors = list(errors)
    return {
        "summary": {
            "errors": len(copied_errors),
            "business_years": _sorted_strings(
                error.business_year for error in copied_errors
            ),
            "report_codes": _sorted_strings(
                error.report_code for error in copied_errors
            ),
            "error_types": _sorted_strings(error.error_type for error in copied_errors),
        },
        "errors": _collection_errors_json(copied_errors),
    }


def read_corp_codes_from_file(path: Path) -> list[str]:
    content = path.read_text(encoding="utf-8-sig")
    if not content.strip():
        return []

    try:
        payload = json.loads(content)
    except json.JSONDecodeError:
        if path.suffix.lower() == ".csv":
            return _read_corp_codes_from_csv(content)
        return _dedupe_strings(
            code for line in content.splitlines() for code in line.split(",")
        )

    return _dedupe_strings(_iter_corp_codes_from_json(payload))


def resolve_corp_codes(
    inline_values: Iterable[str],
    corp_code_file: Path | None,
) -> list[str]:
    return _dedupe_strings(
        [
            *(code for value in inline_values for code in value.split(",")),
            *(read_corp_codes_from_file(corp_code_file) if corp_code_file else []),
        ]
    )


def parse_business_years(
    inline_values: Iterable[str],
    *,
    year_from: str | int | None = None,
    year_to: str | int | None = None,
) -> list[str]:
    years: list[str] = []
    years.extend(year for value in inline_values for year in value.split(","))

    if year_from is not None or year_to is not None:
        if year_from is None or year_to is None:
            raise ValueError("year_from and year_to must be provided together")
        start = int(year_from)
        end = int(year_to)
        if start > end:
            raise ValueError("year_from must be less than or equal to year_to")
        years.extend(str(year) for year in range(start, end + 1))

    return _dedupe_strings(years)


def _build_company_coverage(
    corp_code: str,
    *,
    rows: list[FinancialStatementRow],
    values: list[FinancialPeriodValue],
    metrics: list[str],
    growth_results: dict[tuple[str, str], list[dict[str, object]]],
    collection_errors: list[CollectionError],
    expected_business_years: list[str],
    expected_report_codes: list[str],
) -> dict[str, object]:
    annual_by_metric: dict[str, list[FinancialPeriodValue]] = {
        metric: [] for metric in metrics
    }
    quarter_by_metric: dict[str, list[FinancialPeriodValue]] = {
        metric: [] for metric in metrics
    }
    for value in values:
        if value.metric not in annual_by_metric:
            continue
        if value.period_type == "annual":
            annual_by_metric[value.metric].append(value)
        elif value.period_type == "quarter":
            quarter_by_metric[value.metric].append(value)

    metric_coverages = [
        _build_metric_coverage(
            metric,
            annual_values=annual_by_metric[metric],
            quarter_values=quarter_by_metric[metric],
            growth_results=growth_results.get((corp_code, metric), []),
            expected_business_years=expected_business_years,
            expected_report_codes=expected_report_codes,
        )
        for metric in metrics
    ]

    corp_name = _first_non_empty(row.corp_name for row in rows)
    stock_code = _first_non_empty(row.stock_code for row in rows)
    business_years = _sorted_strings(row.business_year for row in rows)
    report_codes = _sorted_strings(row.report_code for row in rows)
    missing_metrics = [
        item["metric"]
        for item in metric_coverages
        if not item["has_annual_data"] and not item["has_quarter_data"]
    ]

    return {
        "corp_code": corp_code,
        "corp_name": corp_name,
        "stock_code": stock_code,
        "raw_rows": len(rows),
        "business_years": business_years,
        "missing_business_years": [
            year for year in expected_business_years if year not in business_years
        ],
        "report_codes": report_codes,
        "missing_report_codes": [
            code for code in expected_report_codes if code not in report_codes
        ],
        "fs_divs": _sorted_strings(row.fs_div for row in rows if row.fs_div),
        "collection_errors": _collection_errors_json(collection_errors),
        "missing_metrics": missing_metrics,
        "metrics": metric_coverages,
    }


def _build_metric_coverage(
    metric: str,
    *,
    annual_values: list[FinancialPeriodValue],
    quarter_values: list[FinancialPeriodValue],
    growth_results: list[dict[str, object]],
    expected_business_years: list[str],
    expected_report_codes: list[str],
) -> dict[str, object]:
    annual_years = sorted({value.fiscal_year for value in annual_values})
    quarter_periods = sorted(
        (
            value.period_label
            for value in quarter_values
            if value.fiscal_quarter is not None
        ),
        key=_quarter_label_sort_key,
    )
    expected_annual_years = _expected_annual_years(expected_business_years, expected_report_codes)
    expected_quarter_periods = _expected_quarter_periods(
        expected_business_years,
        expected_report_codes,
    )
    return {
        "metric": metric,
        "has_annual_data": bool(annual_values),
        "has_quarter_data": bool(quarter_values),
        "annual_count": len(annual_values),
        "annual_years": annual_years,
        "missing_annual_years": [
            year for year in expected_annual_years if year not in annual_years
        ],
        "quarter_count": len(quarter_values),
        "quarter_periods": quarter_periods,
        "missing_quarter_periods": [
            period for period in expected_quarter_periods if period not in quarter_periods
        ],
        "growth_filter_results": growth_results,
    }


def _index_collection_errors(
    errors: Iterable[CollectionError],
) -> dict[str, list[CollectionError]]:
    indexed: dict[str, list[CollectionError]] = {}
    for error in errors:
        for corp_code in error.corp_codes:
            indexed.setdefault(corp_code, []).append(error)
    return indexed


def _collection_errors_json(errors: Iterable[CollectionError]) -> list[dict[str, object]]:
    return [_json_ready(asdict(error)) for error in errors]


def _expected_annual_years(
    expected_business_years: list[str],
    expected_report_codes: list[str],
) -> list[int]:
    if REPORT_CODE_ANNUAL not in expected_report_codes:
        return []
    return sorted(int(year) for year in expected_business_years if year.isdigit())


def _expected_quarter_periods(
    expected_business_years: list[str],
    expected_report_codes: list[str],
) -> list[str]:
    quarters = sorted(
        {
            REPORT_CODE_QUARTERS[report_code]
            for report_code in expected_report_codes
            if report_code in REPORT_CODE_QUARTERS
        }
    )
    return [
        f"{year}Q{quarter}"
        for year in expected_business_years
        if year.isdigit()
        for quarter in quarters
    ]


def _index_growth_filter_results(
    growth_metrics: dict[str, object] | None,
) -> dict[tuple[str, str], list[dict[str, object]]]:
    if not growth_metrics:
        return {}
    filter_payload = growth_metrics.get("filter")
    if not isinstance(filter_payload, dict):
        return {}
    raw_results = filter_payload.get("results")
    if not isinstance(raw_results, list):
        return {}

    indexed: dict[tuple[str, str], list[dict[str, object]]] = {}
    for result in raw_results:
        if not isinstance(result, dict):
            continue
        corp_code = str(result.get("corp_code", "")).strip()
        metric = str(result.get("metric", "")).strip()
        if not corp_code or not metric:
            continue
        indexed.setdefault((corp_code, metric), []).append(result)
    return indexed


def _iter_corp_codes_from_json(payload: object) -> Iterable[str]:
    if isinstance(payload, str):
        yield payload
        return
    if isinstance(payload, list):
        for item in payload:
            yield from _iter_corp_codes_from_json(item)
        return
    if not isinstance(payload, dict):
        return

    corp_code = payload.get("corp_code")
    if corp_code is not None:
        yield str(corp_code)

    for key in ("corp_codes", "matched", "companies", "rows", "values"):
        value = payload.get(key)
        if isinstance(value, list):
            for item in value:
                yield from _iter_corp_codes_from_json(item)


def _read_corp_codes_from_csv(content: str) -> list[str]:
    rows = csv.DictReader(content.splitlines())
    corp_codes: list[str] = []
    for row in rows:
        corp_code = row.get("corp_code") or row.get("고유번호")
        if corp_code:
            corp_codes.append(corp_code)
    return _dedupe_strings(corp_codes)


def _dedupe_strings(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _sorted_strings(values: Iterable[str]) -> list[str]:
    return sorted(_dedupe_strings(values))


def _first_non_empty(values: Iterable[str]) -> str:
    for value in values:
        if value:
            return value
    return ""


def _growth_point_count(growth_metrics: dict[str, object] | None) -> int:
    if not growth_metrics:
        return 0
    growth_points = growth_metrics.get("growth_points")
    return len(growth_points) if isinstance(growth_points, list) else 0


def _quarter_label_sort_key(label: str) -> tuple[int, int]:
    year, quarter = label.split("Q", 1)
    return int(year), int(quarter)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _json_ready(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value
