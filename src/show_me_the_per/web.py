from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from html import escape
import os
from pathlib import Path
from typing import Callable, Iterable, Protocol
from urllib.parse import urlencode

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from .growth import ANNUAL_YOY, QUARTERLY_YOY, TRAILING_FOUR_QUARTER_YOY
from .krx import KrxStockPriceClient, KrxStockPriceSnapshot
from .models import (
    DartCompany,
    FinancialPeriodValue,
    FinancialStatementRow,
    normalize_stock_code,
)
from .opendart import OpenDartClient
from .pipeline import (
    DEFAULT_REPORT_CODES,
    AnalysisArtifacts,
    MajorAccountClient,
    build_analysis_artifacts,
    collect_financial_statement_run,
)
from .reports import (
    METRIC_LABELS,
    METRIC_ORDER,
    SERIES_LABELS,
    SERIES_ORDER,
    _format_amount,
    _format_percent,
    _render_growth_chart,
)
from .storage import (
    read_dart_companies_from_database,
    read_latest_equity_price_snapshot,
    read_financial_statement_rows_from_database,
    store_equity_price_snapshot,
    store_analysis_artifacts,
)


DEFAULT_RECENT_YEARS = 10
DEFAULT_THRESHOLD_PERCENT = Decimal("20")
MIN_OPENDART_YEAR = 2015
DEFAULT_WEB_CACHE_DIR = Path("data/web-cache")
DEFAULT_PERIOD_KEY = "quarterly"
DEFAULT_METRIC_KEY = "revenue"
METRIC_SEQUENCE = ("revenue", "operating_income", "net_income")
PERIOD_DEFS = (
    {
        "key": "quarterly",
        "label": "분기",
        "rows_key": "quarterly_rows",
        "growth_label": "YoY 성장률",
        "filter_series_type": QUARTERLY_YOY,
        "include_qoq": True,
    },
    {
        "key": "trailing",
        "label": "4분기 누적",
        "rows_key": "trailing_rows",
        "growth_label": "YoY 성장률",
        "filter_series_type": TRAILING_FOUR_QUARTER_YOY,
        "include_qoq": False,
    },
    {
        "key": "annual",
        "label": "연간",
        "rows_key": "annual_rows",
        "growth_label": "YoY 성장률",
        "filter_series_type": ANNUAL_YOY,
        "include_qoq": False,
    },
)

QUARTER_COLORS = {
    1: "#a5b4fc",
    2: "#fdba74",
    3: "#86efac",
    4: "#fde68a",
}
AMOUNT_BAR_COLOR = "#bfdbfe"
AMOUNT_BAR_STROKE = "#60a5fa"
GROWTH_LINE_COLORS = {
    "growth_rate": "#c4b5fd",
    "qoq_growth_rate": "#f9a8d4",
}
GROWTH_LINE_STROKE_WIDTH = "1.6"
GROWTH_LINE_MARKER_RADIUS = "2.5"
GROWTH_LINE_COMPACT_MARKER_RADIUS = "2.25"
GROWTH_LINE_MARKER_STROKE_WIDTH = "1.0"
COMPARE_LINE_COLORS = {
    "primary": "#2563eb",
    "secondary": "#f97316",
}


class StockPriceClient(Protocol):
    def fetch_stock_price(
        self,
        stock_code: str,
        *,
        base_date: str,
    ) -> KrxStockPriceSnapshot:
        ...


@dataclass(frozen=True)
class AnalysisForm:
    company_query: str = ""
    recent_years: str = str(DEFAULT_RECENT_YEARS)
    end_year: str = ""
    fs_div: str = "CFS"
    threshold_percent: str = str(DEFAULT_THRESHOLD_PERCENT)


@dataclass(frozen=True)
class CompareForm:
    primary_company_query: str = ""
    secondary_company_query: str = ""
    recent_years: str = str(DEFAULT_RECENT_YEARS)
    end_year: str = ""
    fs_div: str = "CFS"
    threshold_percent: str = str(DEFAULT_THRESHOLD_PERCENT)


def create_app(
    client_factory: Callable[[str], MajorAccountClient] = OpenDartClient,
    stock_client_factory: Callable[[str], StockPriceClient] = KrxStockPriceClient,
) -> FastAPI:
    app = FastAPI(title="ShowMeThePER")

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        form = AnalysisForm(end_year=str(default_end_year()))
        return HTMLResponse(render_analysis_page(form=form))

    @app.get("/analysis", response_class=HTMLResponse)
    def analysis(
        company_query: str = "",
        corp_code: str = "",
        recent_years: str = str(DEFAULT_RECENT_YEARS),
        end_year: str = "",
        fs_div: str = "CFS",
        threshold_percent: str = str(DEFAULT_THRESHOLD_PERCENT),
    ) -> HTMLResponse:
        form = AnalysisForm(
            company_query=(company_query or corp_code).strip(),
            recent_years=recent_years.strip(),
            end_year=end_year.strip() or str(default_end_year()),
            fs_div=fs_div.strip().upper() or "CFS",
            threshold_percent=threshold_percent.strip(),
        )

        try:
            request = parse_analysis_request(form)
        except ValueError as error:
            return HTMLResponse(render_analysis_page(form=form, error=str(error)))

        api_key = os.getenv("OPENDART_API_KEY")
        if not api_key:
            return HTMLResponse(
                render_analysis_page(
                    form=form,
                    error="OPENDART_API_KEY 환경변수를 먼저 설정해 주세요.",
                )
            )

        client = client_factory(api_key)
        stock_client = _build_stock_price_client(stock_client_factory)
        try:
            payload = _collect_browser_payload(
                client,
                stock_client=stock_client,
                company_query=request["company_query"],
                request=request,
            )
        except ValueError as error:
            return HTMLResponse(render_analysis_page(form=form, error=str(error)))
        except RuntimeError as error:
            return HTMLResponse(
                render_analysis_page(
                    form=form,
                    error=str(error),
                )
            )

        return HTMLResponse(render_analysis_page(form=form, payload=payload))

    @app.get("/compare", response_class=HTMLResponse)
    def compare(
        primary_company_query: str = "",
        secondary_company_query: str = "",
        recent_years: str = str(DEFAULT_RECENT_YEARS),
        end_year: str = "",
        fs_div: str = "CFS",
        threshold_percent: str = str(DEFAULT_THRESHOLD_PERCENT),
    ) -> HTMLResponse:
        form = CompareForm(
            primary_company_query=primary_company_query.strip(),
            secondary_company_query=secondary_company_query.strip(),
            recent_years=recent_years.strip(),
            end_year=end_year.strip() or str(default_end_year()),
            fs_div=fs_div.strip().upper() or "CFS",
            threshold_percent=threshold_percent.strip(),
        )

        if not form.primary_company_query and not form.secondary_company_query:
            return HTMLResponse(render_compare_page(form=form))

        try:
            request = parse_compare_request(form)
        except ValueError as error:
            return HTMLResponse(render_compare_page(form=form, error=str(error)))

        api_key = os.getenv("OPENDART_API_KEY")
        if not api_key:
            return HTMLResponse(
                render_compare_page(
                    form=form,
                    error="OPENDART_API_KEY 환경변수를 먼저 설정해 주세요.",
                )
            )

        client = client_factory(api_key)
        stock_client = _build_stock_price_client(stock_client_factory)
        try:
            primary_payload = _collect_browser_payload(
                client,
                stock_client=stock_client,
                company_query=request["primary_company_query"],
                request=request,
            )
            secondary_payload = _collect_browser_payload(
                client,
                stock_client=stock_client,
                company_query=request["secondary_company_query"],
                request=request,
            )
        except ValueError as error:
            return HTMLResponse(render_compare_page(form=form, error=str(error)))
        except RuntimeError as error:
            return HTMLResponse(render_compare_page(form=form, error=str(error)))

        return HTMLResponse(
            render_compare_page(
                form=form,
                payload={
                    "primary": primary_payload,
                    "secondary": secondary_payload,
                    "summary": {
                        "recent_years": request["recent_years"],
                        "end_year": request["end_year"],
                        "fs_div": request["fs_div"] or "전체",
                    },
                },
            )
        )

    return app


app = create_app()


def default_end_year(today: date | None = None) -> int:
    current = today or date.today()
    return current.year - 1


def _build_stock_price_client(
    stock_client_factory: Callable[[str], StockPriceClient],
) -> StockPriceClient | None:
    service_key = os.getenv("KRX_SERVICE_KEY", "").strip()
    if not service_key:
        return None
    return stock_client_factory(service_key)


def parse_analysis_request(form: AnalysisForm) -> dict[str, object]:
    company_query = form.company_query.strip()
    if not company_query:
        raise ValueError("기업 이름을 입력해 주세요.")

    recent_years = _parse_int(form.recent_years, field_name="조회 연수")
    if recent_years <= 0:
        raise ValueError("조회 연수는 1 이상이어야 합니다.")

    end_year = _parse_int(
        form.end_year or str(default_end_year()),
        field_name="기준 연도",
    )
    start_year = max(MIN_OPENDART_YEAR, end_year - recent_years + 1)
    if start_year > end_year:
        raise ValueError("기준 연도는 2015년 이상이어야 합니다.")

    threshold_percent = _parse_decimal(
        form.threshold_percent,
        field_name="성장률 기준",
    )
    fs_div = _parse_fs_div(form.fs_div)

    return {
        "company_query": company_query,
        "recent_years": recent_years,
        "start_year": start_year,
        "end_year": end_year,
        "business_years": [str(year) for year in range(start_year, end_year + 1)],
        "fs_div": fs_div,
        "threshold_percent": threshold_percent,
    }


def parse_compare_request(form: CompareForm) -> dict[str, object]:
    if not form.primary_company_query or not form.secondary_company_query:
        raise ValueError("비교할 두 기업을 모두 입력해 주세요.")

    request = parse_analysis_request(
        AnalysisForm(
            company_query=form.primary_company_query,
            recent_years=form.recent_years,
            end_year=form.end_year,
            fs_div=form.fs_div,
            threshold_percent=form.threshold_percent,
        )
    )
    request["primary_company_query"] = form.primary_company_query
    request["secondary_company_query"] = form.secondary_company_query
    return request


def resolve_company_query(
    companies: Iterable[DartCompany],
    query: str,
) -> DartCompany:
    normalized_query = _normalize_company_name(query)
    copied_companies = list(companies)
    if not normalized_query:
        raise ValueError("기업 이름을 입력해 주세요.")

    corp_code_matches = [
        company for company in copied_companies if company.corp_code == query.strip()
    ]
    if len(corp_code_matches) == 1:
        return corp_code_matches[0]
    if not corp_code_matches and query.strip().isdigit() and len(query.strip()) == 8:
        return DartCompany(
            corp_code=query.strip(),
            corp_name="",
            stock_code="",
            modify_date="",
        )

    stock_code = normalize_stock_code(query)
    stock_matches = [
        company
        for company in copied_companies
        if company.normalized_stock_code
        and company.normalized_stock_code == stock_code
    ]
    if len(stock_matches) == 1:
        return stock_matches[0]
    if len(stock_matches) > 1:
        raise ValueError(_ambiguous_company_message(query, stock_matches))

    exact_name_matches = [
        company
        for company in copied_companies
        if _normalize_company_name(company.corp_name) == normalized_query
    ]
    if len(exact_name_matches) == 1:
        return exact_name_matches[0]
    if len(exact_name_matches) > 1:
        raise ValueError(_ambiguous_company_message(query, exact_name_matches))

    partial_name_matches = [
        company
        for company in copied_companies
        if normalized_query in _normalize_company_name(company.corp_name)
    ]
    if len(partial_name_matches) == 1:
        return partial_name_matches[0]
    if len(partial_name_matches) > 1:
        raise ValueError(_ambiguous_company_message(query, partial_name_matches))

    raise ValueError(f"'{query}'에 해당하는 상장기업을 찾지 못했습니다.")


def _collect_browser_payload(
    client: MajorAccountClient,
    *,
    stock_client: StockPriceClient | None,
    company_query: str,
    request: dict[str, object],
) -> dict[str, object]:
    database_path = _web_cache_database_path(str(request["fs_div"]))
    try:
        company, company_source = _resolve_company_for_browser(
            client,
            database_path=database_path,
            company_query=company_query,
        )
    except ValueError:
        raise
    except Exception as error:  # pragma: no cover - exercised through route tests
        raise RuntimeError(
            _format_request_error(
                "기업 목록을 가져오는 중 오류가 발생했습니다.",
                error,
            )
        ) from error

    cached_rows = (
        read_financial_statement_rows_from_database(
            database_path,
            corp_code=company.corp_code,
        )
        if database_path.exists()
        else []
    )
    cached_years = sorted({row.business_year for row in cached_rows})
    missing_years = [
        business_year
        for business_year in request["business_years"]
        if business_year not in cached_years
    ]

    try:
        if missing_years:
            run = collect_financial_statement_run(
                client,
                corp_codes=[company.corp_code],
                business_years=missing_years,
                report_codes=DEFAULT_REPORT_CODES,
                fs_div=request["fs_div"],
                continue_on_error=True,
            )
            merged_rows = _merge_financial_statement_rows(cached_rows, run.rows)
            artifacts = _build_browser_analysis_artifacts(
                rows=merged_rows,
                corp_code=company.corp_code,
                request=request,
                collection_errors=run.errors,
            )
            store_analysis_artifacts(database_path, artifacts)
            source_label = "network" if not cached_rows else "cache+network"
            available_years = sorted({row.business_year for row in merged_rows})
        else:
            artifacts = _build_browser_analysis_artifacts(
                rows=cached_rows,
                corp_code=company.corp_code,
                request=request,
                collection_errors=[],
            )
            source_label = "cache"
            available_years = cached_years
    except Exception as error:  # pragma: no cover - exercised through route tests
        raise RuntimeError(
            _format_request_error(
                "재무제표를 수집하는 중 오류가 발생했습니다.",
                error,
            )
        ) from error

    payload = build_browser_report_payload(
        artifacts,
        company=company,
        company_query=company_query,
        start_year=request["start_year"],
        end_year=request["end_year"],
        recent_years=request["recent_years"],
        fs_div=request["fs_div"],
        threshold_percent=request["threshold_percent"],
    )
    _dict(payload["summary"]).update(
        {
            "cache_database": str(database_path),
            "data_source": source_label,
            "company_source": company_source,
            "cached_business_years": available_years,
            "fetched_business_years": missing_years,
            "cache_status": _describe_cache_status(
                source_label=source_label,
                fetched_business_years=missing_years,
            ),
        }
    )
    market_profile = _load_market_profile(
        database_path=database_path,
        company=company,
        stock_client=stock_client,
    )
    if market_profile:
        payload["market_profile"] = market_profile
    return payload


def build_browser_report_payload(
    artifacts: AnalysisArtifacts,
    *,
    company: DartCompany,
    company_query: str,
    start_year: int,
    end_year: int,
    recent_years: int,
    fs_div: str | None,
    threshold_percent: Decimal,
) -> dict[str, object]:
    values = [
        value
        for value in artifacts.financial_period_values
        if start_year <= value.fiscal_year <= end_year
    ]
    growth_points = [
        point
        for point in _growth_points_from_payload(artifacts.growth_metrics)
        if start_year <= int(point.get("fiscal_year", 0) or 0) <= end_year
    ]
    filter_results = _filter_results_from_payload(artifacts.growth_metrics)
    annual_rows = _pivot_period_values(
        (value for value in values if value.period_type == "annual"),
        growth_points=growth_points,
        series_type=ANNUAL_YOY,
    )
    quarterly_rows = _pivot_period_values(
        (value for value in values if value.period_type == "quarter"),
        growth_points=growth_points,
        series_type=QUARTERLY_YOY,
    )
    trailing_rows = _pivot_growth_amount_rows(
        growth_points,
        series_type=TRAILING_FOUR_QUARTER_YOY,
    )

    payload = {
        "company": _company_from_rows(company, artifacts.financial_statement_rows),
        "summary": {
            "company_query": company_query,
            "start_year": start_year,
            "end_year": end_year,
            "recent_years": recent_years,
            "fs_div": fs_div or "전체",
            "threshold_percent": str(threshold_percent),
            "raw_rows": len(artifacts.financial_statement_rows),
            "period_values": len(values),
            "growth_points": len(growth_points),
            "collection_errors": len(artifacts.collection_errors),
        },
        "annual_rows": annual_rows,
        "quarterly_rows": quarterly_rows,
        "trailing_rows": trailing_rows,
        "filter_results": sorted(
            filter_results,
            key=lambda item: (
                METRIC_ORDER.get(str(item.get("metric", "")), 999),
                SERIES_ORDER.get(str(item.get("series_type", "")), 999),
            ),
        ),
        "growth_sections": _group_growth_points(growth_points),
        "collection_errors": [
            {
                "business_year": error.business_year,
                "report_code": error.report_code,
                "error_type": error.error_type,
                "message": error.message,
            }
            for error in artifacts.collection_errors
        ],
    }
    payload["period_groups"] = _build_period_groups(payload)
    return payload


def _build_browser_analysis_artifacts(
    *,
    rows: list[FinancialStatementRow],
    corp_code: str,
    request: dict[str, object],
    collection_errors: list[object],
) -> AnalysisArtifacts:
    return build_analysis_artifacts(
        rows,
        collection_errors=list(collection_errors),
        expected_corp_codes=[corp_code],
        expected_business_years=[str(year) for year in request["business_years"]],
        expected_report_codes=DEFAULT_REPORT_CODES,
        threshold_percent=request["threshold_percent"],
        recent_annual_periods=3,
        recent_quarterly_periods=12,
    )


def _resolve_company_for_browser(
    client: MajorAccountClient,
    *,
    database_path: Path,
    company_query: str,
) -> tuple[DartCompany, str]:
    cached_company = _resolve_company_from_cache(database_path, company_query)
    if cached_company is not None:
        return cached_company, "cache"

    companies = client.fetch_companies()
    return resolve_company_query(companies, company_query), "opendart"


def _resolve_company_from_cache(
    database_path: Path,
    company_query: str,
) -> DartCompany | None:
    if not database_path.exists():
        return None
    cached_companies = read_dart_companies_from_database(database_path)
    if not cached_companies:
        return None
    try:
        return resolve_company_query(cached_companies, company_query)
    except ValueError:
        return None


def _merge_financial_statement_rows(
    existing_rows: Iterable[FinancialStatementRow],
    new_rows: Iterable[FinancialStatementRow],
) -> list[FinancialStatementRow]:
    merged: dict[tuple[str, ...], FinancialStatementRow] = {}
    for row in [*existing_rows, *new_rows]:
        key = (
            row.corp_code,
            row.business_year,
            row.report_code,
            row.fs_div,
            row.statement_div,
            row.statement_name,
            row.account_id,
            row.account_name,
            row.current_term_name,
        )
        merged[key] = row
    return sorted(
        merged.values(),
        key=lambda row: (
            row.corp_code,
            row.business_year,
            row.report_code,
            row.fs_div,
            row.statement_div,
            row.statement_name,
            row.account_id,
            row.account_name,
            row.current_term_name,
        ),
    )


def _web_cache_database_path(fs_div: str) -> Path:
    base_dir = Path(
        os.getenv(
            "SHOW_ME_THE_PER_WEB_CACHE_DIR",
            str(DEFAULT_WEB_CACHE_DIR),
        )
    )
    return base_dir / f"show-me-the-per-{(fs_div or 'ALL').lower()}.sqlite3"


def _describe_cache_status(
    *,
    source_label: str,
    fetched_business_years: list[object],
) -> str:
    if source_label == "cache":
        return "DB 캐시 사용"
    if not fetched_business_years:
        return "DB 캐시 사용"
    if source_label == "network":
        return f"OpenDART 신규 수집 ({len(fetched_business_years)}개 연도)"
    return f"DB 캐시 + 최신 {len(fetched_business_years)}개 연도 갱신"


def _load_market_profile(
    *,
    database_path: Path,
    company: DartCompany,
    stock_client: StockPriceClient | None,
    today: date | None = None,
) -> dict[str, object]:
    stock_code = normalize_stock_code(company.stock_code)
    if not stock_code:
        return {}

    cached = read_latest_equity_price_snapshot(
        database_path,
        stock_code=stock_code,
    )
    candidate_dates = _candidate_market_dates(today or date.today())

    if cached is not None and cached.base_date in candidate_dates:
        return _market_profile_json(cached, source="cache")

    if stock_client is not None:
        for base_date in candidate_dates:
            try:
                snapshot = stock_client.fetch_stock_price(
                    stock_code,
                    base_date=base_date,
                )
            except LookupError:
                continue
            except Exception:
                break
            store_equity_price_snapshot(database_path, snapshot)
            return _market_profile_json(
                snapshot,
                source="cache" if cached is not None else "network",
            )

    if cached is not None:
        return _market_profile_json(cached, source="cache")

    return {}


def _candidate_market_dates(today: date, attempts: int = 7) -> list[str]:
    dates: list[str] = []
    cursor = today
    while len(dates) < attempts:
        cursor -= timedelta(days=1)
        if cursor.weekday() < 5:
            dates.append(cursor.strftime("%Y%m%d"))
    return dates


def _market_profile_json(
    snapshot: KrxStockPriceSnapshot,
    *,
    source: str,
) -> dict[str, object]:
    return {
        "stock_code": snapshot.stock_code,
        "base_date": snapshot.base_date,
        "market": snapshot.market,
        "item_name": snapshot.item_name,
        "close_price": (
            None if snapshot.close_price is None else str(snapshot.close_price)
        ),
        "listed_stock_count": (
            None
            if snapshot.listed_stock_count is None
            else str(snapshot.listed_stock_count)
        ),
        "market_cap": None if snapshot.market_cap is None else str(snapshot.market_cap),
        "source": source,
    }


def render_analysis_page(
    *,
    form: AnalysisForm,
    payload: dict[str, object] | None = None,
    error: str | None = None,
) -> str:
    company = _dict(payload.get("company")) if payload else {}
    company_title = _company_title(company) if company else ""
    header_html = render_analysis_header(form, payload)
    body_html = (
        render_browser_report(payload)
        if payload is not None
        else render_analysis_empty_state()
    )
    return render_shell(
        company_title=company_title,
        active_tab="financials",
        toolbar_html=header_html,
        content_html=body_html,
        message_html=render_message(error=error),
    )


def render_compare_page(
    *,
    form: CompareForm,
    payload: dict[str, object] | None = None,
    error: str | None = None,
) -> str:
    primary = _dict(_dict(payload or {}).get("primary")).get("company")
    secondary = _dict(_dict(payload or {}).get("secondary")).get("company")
    company_title = ""
    if primary and secondary:
        company_title = (
            f"{_company_title(_dict(primary))} VS {_company_title(_dict(secondary))}"
        )

    return render_shell(
        company_title=company_title,
        active_tab="compare",
        toolbar_html=render_compare_header(form, payload),
        content_html=(
            render_compare_dashboard(_dict(payload))
            if payload is not None
            else render_compare_empty_state(form)
        ),
        message_html=render_message(error=error),
    )


def render_shell(
    *,
    company_title: str,
    active_tab: str,
    toolbar_html: str,
    content_html: str,
    message_html: str = "",
) -> str:
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>ShowMeThePER</title>
  <style>
{_page_styles()}
  </style>
</head>
<body>
  <header class="shell-header">
    <div class="context-bar">
      <div class="context-chip">{escape(company_title or "기업 선택")}</div>
      <div class="context-meta">KOSPI/KOSDAQ 상장기업 재무 데이터 탐색</div>
    </div>
    {render_top_tabs(active_tab)}
  </header>
  <main class="shell-main">
    <section class="notice loading-indicator" id="loading-indicator" aria-live="polite">
      <div class="spinner" aria-hidden="true"></div>
      <div>
        <strong>조회 요청을 처리하고 있습니다.</strong>
        <p class="loading-copy" id="loading-status">OpenDART에서 기업 목록과 재무 데이터를 가져오는 중입니다.</p>
      </div>
    </section>
    {toolbar_html}
    {message_html}
    {content_html}
  </main>
  <script>
{_page_script()}
  </script>
</body>
</html>
"""


def render_top_tabs(active_tab: str) -> str:
    def tab(label: str, href: str, key: str) -> str:
        class_name = "top-tab is-active" if active_tab == key else "top-tab"
        return f'<a class="{class_name}" href="{escape(href)}">{escape(label)}</a>'

    return (
        '<nav class="top-tabs" aria-label="화면 탭">'
        f'{tab("요약", "/analysis", "overview")}'
        f'{tab("재무정보", "/analysis", "financials")}'
        f'{tab("성장률", "/analysis#growth-details", "growth")}'
        f'{tab("VS 기업비교", "/compare", "compare")}'
        "</nav>"
    )


def render_analysis_header(
    form: AnalysisForm,
    payload: dict[str, object] | None,
) -> str:
    company = _dict(payload.get("company")) if payload else {}
    query = str(_dict(payload.get("summary")).get("company_query", "")) if payload else form.company_query
    compare_href = _build_compare_href(
        primary_company_query=query or form.company_query,
        recent_years=form.recent_years,
        end_year=form.end_year or str(default_end_year()),
        fs_div=form.fs_div,
        threshold_percent=form.threshold_percent,
    )
    return f"""
    <section class="toolbar-surface">
      <div class="section-tabs">
        <span class="section-tab is-active">손익계산서</span>
        <span class="section-tab">성장률 요약</span>
        <a class="section-tab" href="{escape(compare_href)}">VS 비교 준비</a>
      </div>
      <form id="analysis-form" class="query-form" data-loading-form method="get" action="/analysis">
        <input id="analysis-recent-years" type="hidden" name="recent_years" value="{escape(form.recent_years or str(DEFAULT_RECENT_YEARS))}">
        <label class="field field-grow">
          <span>기업명</span>
          <input name="company_query" value="{escape(form.company_query)}" placeholder="예: 삼성전자" required>
        </label>
        <label class="field">
          <span>기준 연도</span>
          <input name="end_year" value="{escape(form.end_year or str(default_end_year()))}" inputmode="numeric">
        </label>
        <label class="field">
          <span>재무제표</span>
          <select name="fs_div">
            {_option("CFS", "연결", form.fs_div)}
            {_option("OFS", "별도", form.fs_div)}
            {_option("ALL", "전체", form.fs_div)}
          </select>
        </label>
        <label class="field">
          <span>기본 성장률</span>
          <input name="threshold_percent" value="{escape(form.threshold_percent)}" inputmode="decimal">
        </label>
        <button id="submit-button" type="submit" class="primary-button">
          <span data-submit-label>조회</span>
          <span data-submit-loading hidden>조회 중...</span>
        </button>
      </form>
      <div class="toolbar-row toolbar-row-dense">
        <div class="segmented" role="group" aria-label="조회 연수">
          {render_year_preset_button("3년", "3", form.recent_years, "analysis-form", "analysis-recent-years")}
          {render_year_preset_button("5년", "5", form.recent_years, "analysis-form", "analysis-recent-years")}
          {render_year_preset_button("10년", "10", form.recent_years, "analysis-form", "analysis-recent-years")}
        </div>
        <a class="ghost-link" href="{escape(compare_href)}">VS 기업비교 열기</a>
        {render_toolbar_meta(company, form)}
      </div>
    </section>
    """


def render_compare_header(
    form: CompareForm,
    payload: dict[str, object] | None,
) -> str:
    primary = _dict(_dict(payload or {}).get("primary")).get("company")
    analysis_href = _build_analysis_href(
        company_query=str(form.primary_company_query or _dict(primary).get("corp_name", "")),
        recent_years=form.recent_years,
        end_year=form.end_year or str(default_end_year()),
        fs_div=form.fs_div,
        threshold_percent=form.threshold_percent,
    )
    return f"""
    <section class="toolbar-surface">
      <div class="section-tabs">
        <a class="section-tab" href="{escape(analysis_href)}">재무정보</a>
        <span class="section-tab is-active">VS 기업비교</span>
        <span class="section-tab">비교 요약</span>
      </div>
      <form id="compare-form" class="query-form query-form-compare" data-loading-form method="get" action="/compare">
        <input id="compare-recent-years" type="hidden" name="recent_years" value="{escape(form.recent_years or str(DEFAULT_RECENT_YEARS))}">
        <label class="field field-grow">
          <span>기준 기업</span>
          <input name="primary_company_query" value="{escape(form.primary_company_query)}" placeholder="예: 비나텍" required>
        </label>
        <label class="field field-grow">
          <span>비교 기업</span>
          <input name="secondary_company_query" value="{escape(form.secondary_company_query)}" placeholder="예: 삼성전자" required>
        </label>
        <label class="field">
          <span>기준 연도</span>
          <input name="end_year" value="{escape(form.end_year or str(default_end_year()))}" inputmode="numeric">
        </label>
        <label class="field">
          <span>재무제표</span>
          <select name="fs_div">
            {_option("CFS", "연결", form.fs_div)}
            {_option("OFS", "별도", form.fs_div)}
            {_option("ALL", "전체", form.fs_div)}
          </select>
        </label>
        <label class="field">
          <span>성장률 기준</span>
          <input name="threshold_percent" value="{escape(form.threshold_percent)}" inputmode="decimal">
        </label>
        <button id="compare-submit-button" type="submit" class="primary-button">
          <span data-submit-label>비교</span>
          <span data-submit-loading hidden>비교 중...</span>
        </button>
      </form>
      <div class="toolbar-row toolbar-row-dense">
        <div class="segmented" role="group" aria-label="조회 연수">
          {render_year_preset_button("3년", "3", form.recent_years, "compare-form", "compare-recent-years")}
          {render_year_preset_button("5년", "5", form.recent_years, "compare-form", "compare-recent-years")}
          {render_year_preset_button("10년", "10", form.recent_years, "compare-form", "compare-recent-years")}
        </div>
        <div class="toolbar-note">두 기업의 같은 기간 실적을 같은 축으로 비교합니다.</div>
      </div>
    </section>
    """


def render_year_preset_button(
    label: str,
    years: str,
    current_years: str,
    form_id: str,
    input_id: str,
) -> str:
    class_name = (
        "segmented-button is-active"
        if years == (current_years or str(DEFAULT_RECENT_YEARS))
        else "segmented-button"
    )
    return (
        f'<button type="button" class="{class_name}" '
        f'data-year-preset="{escape(years)}" '
        f'data-year-form="{escape(form_id)}" '
        f'data-year-input="{escape(input_id)}">{escape(label)}</button>'
    )


def render_toolbar_meta(company: dict[str, object], form: AnalysisForm) -> str:
    pills = []
    if company:
        pills.append(
            f'<span class="inline-pill">{escape(_company_title(company))}</span>'
        )
    pills.append(f'<span class="inline-pill">최근 {escape(form.recent_years)}년</span>')
    pills.append(f'<span class="inline-pill">기준 {escape(form.end_year or str(default_end_year()))}</span>')
    return f'<div class="toolbar-meta">{"".join(pills)}</div>'


def render_cache_status_pill(summary: dict[str, object]) -> str:
    status = str(summary.get("cache_status", "") or "").strip()
    if not status:
        return ""
    class_name = "inline-pill"
    if status == "DB 캐시 사용":
        class_name = "inline-pill inline-pill-accent"
    elif "OpenDART 신규 수집" in status:
        class_name = "inline-pill inline-pill-contrast"
    return f'<span class="{class_name}">{escape(status)}</span>'


def render_market_profile_pills(profile: dict[str, object]) -> str:
    if not profile:
        return ""

    pills: list[str] = []
    market = str(profile.get("market", "") or "").strip()
    if market:
        pills.append(f'<span class="inline-pill">{escape(market)}</span>')

    close_price = _format_won(profile.get("close_price"))
    if close_price != "-":
        pills.append(f'<span class="inline-pill">전일 종가 {escape(close_price)}</span>')

    market_cap = _format_market_cap(profile.get("market_cap"))
    if market_cap != "-":
        pills.append(f'<span class="inline-pill">시가총액 {escape(market_cap)}</span>')

    base_date = _format_base_date(profile.get("base_date"))
    if base_date:
        pills.append(f'<span class="inline-pill">기준 {escape(base_date)}</span>')

    return "".join(pills)


def render_message(error: str | None = None, info: str | None = None) -> str:
    if error:
        return f'<section class="notice error">{escape(error)}</section>'
    if info:
        return f'<section class="notice info">{escape(info)}</section>'
    return ""


def render_analysis_empty_state() -> str:
    return """
    <section class="empty-state panel">
      <h2>재무정보 대시보드</h2>
      <p>기업명을 입력하면 최근 N년 동안의 연간, 분기, 4분기 누적 실적과 YoY 성장률을 숫자와 차트로 바로 보여줍니다.</p>
      <ul class="empty-list">
        <li>상단 툴바에서 연결/별도와 기준 연도를 조정할 수 있습니다.</li>
        <li>오른쪽 상단 탭에서 VS 기업비교 화면으로 바로 넘어갈 수 있습니다.</li>
        <li>분기 화면에서는 YoY와 QoQ를 함께 볼 수 있습니다.</li>
      </ul>
    </section>
    """


def render_browser_report(payload: dict[str, object]) -> str:
    company = _dict(payload.get("company"))
    summary = _dict(payload.get("summary"))
    market_profile = _dict(payload.get("market_profile"))
    period_groups = _list(payload.get("period_groups"))
    compare_href = _build_compare_href(
        primary_company_query=str(summary.get("company_query", "")),
        recent_years=str(summary.get("recent_years", DEFAULT_RECENT_YEARS)),
        end_year=str(summary.get("end_year", default_end_year())),
        fs_div="CFS" if str(summary.get("fs_div", "전체")) == "연결" else (
            "OFS" if str(summary.get("fs_div", "전체")) == "별도" else "ALL"
        ),
        threshold_percent=str(summary.get("threshold_percent", DEFAULT_THRESHOLD_PERCENT)),
    )

    return f"""
    <section class="report-shell" data-dashboard data-initial-period="{DEFAULT_PERIOD_KEY}" data-initial-metric="{DEFAULT_METRIC_KEY}">
      <section class="company-header panel">
        <div>
          <h2>{escape(_company_title(company))}</h2>
          <p>고유번호 {escape(str(company.get("corp_code", "")))} · 조회 기간 {escape(str(summary.get("start_year", "")))}-{escape(str(summary.get("end_year", "")))} · 재무제표 {escape(str(summary.get("fs_div", "")))}</p>
        </div>
        <div class="company-actions">
          {render_market_profile_pills(market_profile)}
          {render_cache_status_pill(summary)}
          <span class="inline-pill">원천 row {escape(str(summary.get("raw_rows", 0)))}개</span>
          <span class="inline-pill">성장률 {escape(str(summary.get("growth_points", 0)))}개</span>
          <a class="ghost-link" href="{escape(compare_href)}">이 기업으로 비교 시작</a>
        </div>
      </section>
      {render_collection_errors(_list(payload.get("collection_errors")))}
      {render_dashboard_toolbar()}
      <section class="overview-layout">
        <section class="panel">
          <div class="panel-heading">
            <h3>최근 구간 요약</h3>
            <p>선택한 기간 기준으로 최근 5개 구간의 금액과 성장률을 정리했습니다.</p>
          </div>
          {"".join(render_snapshot_matrix(_dict(group)) for group in period_groups)}
        </section>
        <section class="panel">
          <div class="panel-heading panel-heading-split">
            <div>
              <h3>대표 차트</h3>
              <p>기간과 지표를 바꾸면 오른쪽 차트가 바로 바뀝니다.</p>
            </div>
            {render_metric_switches()}
          </div>
          {"".join(render_focus_panel(_dict(group)) for group in period_groups)}
        </section>
      </section>
      <section class="panel">
        <div class="panel-heading">
          <h3>성장률 필터 결과</h3>
          <p>선택한 기간별로 최근 구간 최소 성장률과 통과 여부를 요약했습니다.</p>
        </div>
      </section>
      <section class="dashboard-grid">
        {"".join(render_period_grid(_dict(group), compare_href) for group in period_groups)}
      </section>
      <section id="growth-details" class="growth-section">
        <details class="panel">
          <summary>성장률 상세 보기</summary>
          {render_growth_sections(_list(payload.get("growth_sections")))}
        </details>
      </section>
    </section>
    """


def render_dashboard_toolbar() -> str:
    period_buttons = []
    for group in PERIOD_DEFS:
        class_name = (
            "segmented-button is-active"
            if group["key"] == DEFAULT_PERIOD_KEY
            else "segmented-button"
        )
        period_buttons.append(
            f'<button type="button" class="{class_name}" data-period-toggle="{escape(str(group["key"]))}">{escape(str(group["label"]))}</button>'
        )
    return (
        '<section class="toolbar-surface toolbar-surface-tight">'
        '<div class="toolbar-row toolbar-row-dense">'
        '<div class="segmented" role="group" aria-label="표시 기간">'
        f'{"".join(period_buttons)}'
        "</div>"
        '<div class="toolbar-note">분기 / 4분기 누적 / 연간 데이터를 같은 구성으로 비교할 수 있습니다.</div>'
        "</div>"
        "</section>"
    )


def render_metric_switches() -> str:
    buttons = []
    for metric in METRIC_SEQUENCE:
        class_name = (
            "metric-switch is-active"
            if metric == DEFAULT_METRIC_KEY
            else "metric-switch"
        )
        buttons.append(
            f'<button type="button" class="{class_name}" data-metric-toggle="{escape(metric)}">{escape(METRIC_LABELS.get(metric, metric))}</button>'
        )
    return f'<div class="metric-switches">{"".join(buttons)}</div>'


def _available_metrics_for_rows(rows: list[object]) -> tuple[str, ...]:
    metrics = {
        metric
        for row in rows
        for metric, cell in _dict(_dict(row).get("values")).items()
        if _to_decimal(_dict(cell).get("amount")) is not None
    }
    return tuple(sorted(metrics, key=lambda metric: METRIC_ORDER.get(metric, 999)))


def render_snapshot_matrix(group: dict[str, object]) -> str:
    rows = [_dict(row) for row in _list(group.get("rows"))[:5]]
    metrics = _available_metrics_for_rows(rows)
    headers = "".join(
        f"<th>{escape(str(row.get('period', '')))}</th>"
        for row in rows
    )
    body = []
    for metric in metrics:
        cells = []
        for row in rows:
            cell = _dict(_dict(row.get("values")).get(metric))
            growth = _growth_class(cell.get("growth_rate"))
            cells.append(
                "<td>"
                f'<div class="matrix-amount">{escape(_format_chart_amount(cell.get("amount")))}</div>'
                f'<div class="matrix-growth {growth}">{escape(_format_percent(cell.get("growth_rate")))}</div>'
                "</td>"
            )
        body.append(
            "<tr>"
            f"<th>{escape(METRIC_LABELS.get(metric, metric))}</th>"
            f"{''.join(cells)}"
            "</tr>"
        )

    return (
        f'<div class="period-panel" data-panel data-period="{escape(str(group.get("key", "")))}"'
        f' {"hidden" if str(group.get("key")) != DEFAULT_PERIOD_KEY else ""}>'
        f'<div class="matrix-heading">{escape(str(group.get("label", "")))}</div>'
        "<table class=\"matrix-table\">"
        f"<thead><tr><th>지표</th>{headers}</tr></thead>"
        f"<tbody>{''.join(body)}</tbody>"
        "</table>"
        "</div>"
    )


def render_focus_panel(group: dict[str, object]) -> str:
    rows = _list(group.get("rows"))
    panels = []
    for metric in METRIC_SEQUENCE:
        panels.append(
            f'<div class="chart-focus-panel" data-panel data-period="{escape(str(group.get("key", "")))}" '
            f'data-metric="{escape(metric)}" {"hidden" if not (group.get("key") == DEFAULT_PERIOD_KEY and metric == DEFAULT_METRIC_KEY) else ""}>'
            f'{render_metric_amount_chart(metric, rows, period_key=str(group.get("key", "")), growth_label=str(group.get("growth_label", "")), include_qoq=bool(group.get("include_qoq")), width=820, height=350)}'
            "</div>"
        )
    return "".join(panels)


def render_period_grid(group: dict[str, object], compare_href: str) -> str:
    rows = _list(group.get("rows"))
    cards = []
    for metric in _available_metrics_for_rows(rows):
        cards.append(
            f'<article class="panel grid-card" data-panel data-period="{escape(str(group.get("key", "")))}" {"hidden" if str(group.get("key")) != DEFAULT_PERIOD_KEY else ""}>'
            f'<div class="panel-heading"><h3>{escape(str(group.get("label", "")))} · {escape(METRIC_LABELS.get(metric, metric))}</h3></div>'
            f'{render_metric_amount_chart(metric, rows, period_key=str(group.get("key", "")), growth_label=str(group.get("growth_label", "")), include_qoq=bool(group.get("include_qoq")), width=540, height=320)}'
            "</article>"
        )

    cards.append(
        f'<article class="panel info-card" data-panel data-period="{escape(str(group.get("key", "")))}" {"hidden" if str(group.get("key")) != DEFAULT_PERIOD_KEY else ""}>'
        f'<div class="panel-heading"><h3>{escape(str(group.get("label", "")))} 필터 상태</h3></div>'
        f'{render_filter_results_for_group(_list(group.get("filter_results")))}'
        '<div class="info-card-footer">'
        f'<a class="ghost-link" href="{escape(compare_href)}">이 구간으로 VS 비교 이어가기</a>'
        "</div>"
        "</article>"
    )

    return "".join(cards)


def render_compare_empty_state(form: CompareForm) -> str:
    preset = escape(form.primary_company_query or "비나텍")
    return f"""
    <section class="empty-state panel">
      <h2>VS 기업비교</h2>
      <p>두 기업의 같은 기간 실적을 같은 화면에서 비교합니다. 기준 기업 하나만 정해둔 상태라면 비교 기업만 추가해도 됩니다.</p>
      <ul class="empty-list">
        <li>예: 기준 기업을 {preset}으로 두고, 비교 기업에 삼성전자를 넣어 보세요.</li>
        <li>기간 토글은 재무정보 화면과 같은 방식으로 동작합니다.</li>
        <li>비교 차트는 같은 축에서 두 회사의 추세를 보여줍니다.</li>
      </ul>
    </section>
    """


def render_compare_dashboard(payload: dict[str, object]) -> str:
    primary = _dict(payload.get("primary"))
    secondary = _dict(payload.get("secondary"))
    primary_company = _dict(primary.get("company"))
    secondary_company = _dict(secondary.get("company"))
    summary = _dict(payload.get("summary"))
    period_groups = _build_compare_period_groups(primary, secondary)

    return f"""
    <section class="report-shell" data-dashboard data-initial-period="{DEFAULT_PERIOD_KEY}" data-initial-metric="{DEFAULT_METRIC_KEY}">
      <section class="company-header panel">
        <div>
          <h2>{escape(_company_title(primary_company))} VS {escape(_company_title(secondary_company))}</h2>
          <p>조회 기간 {escape(str(summary.get("recent_years", DEFAULT_RECENT_YEARS)))}년 · 기준 연도 {escape(str(summary.get("end_year", default_end_year())))} · 재무제표 {escape(str(summary.get("fs_div", "")))}</p>
        </div>
        <div class="company-actions">
          <span class="inline-pill inline-pill-accent">{escape(_company_title(primary_company))}</span>
          <span class="inline-pill inline-pill-contrast">{escape(_company_title(secondary_company))}</span>
        </div>
      </section>
      {render_dashboard_toolbar()}
      <section class="overview-layout compare-overview">
        <section class="panel">
          <div class="panel-heading">
            <h3>최근 값 비교</h3>
            <p>선택한 기간의 최신 구간을 기준으로 두 회사를 비교합니다.</p>
          </div>
          {"".join(render_compare_latest_cards(_dict(group), primary_company, secondary_company) for group in period_groups)}
        </section>
        <section class="panel">
          <div class="panel-heading panel-heading-split">
            <div>
              <h3>대표 비교 차트</h3>
              <p>같은 구간, 같은 지표를 한 축에서 비교합니다.</p>
            </div>
            {render_metric_switches()}
          </div>
          {"".join(render_compare_focus_panel(_dict(group), primary_company, secondary_company) for group in period_groups)}
        </section>
      </section>
      <section class="dashboard-grid">
        {"".join(render_compare_grid(_dict(group), primary_company, secondary_company) for group in period_groups)}
      </section>
    </section>
    """


def render_compare_latest_cards(
    group: dict[str, object],
    primary_company: dict[str, object],
    secondary_company: dict[str, object],
) -> str:
    latest_left = _dict(_list(group.get("primary_rows"))[:1][0]) if _list(group.get("primary_rows")) else {}
    latest_right = _dict(_list(group.get("secondary_rows"))[:1][0]) if _list(group.get("secondary_rows")) else {}
    cards = []
    for metric in METRIC_SEQUENCE:
        left_cell = _dict(_dict(latest_left.get("values")).get(metric))
        right_cell = _dict(_dict(latest_right.get("values")).get(metric))
        delta = _subtract_decimal(left_cell.get("amount"), right_cell.get("amount"))
        cards.append(
            '<article class="compare-stat-card">'
            f'<div class="compare-stat-title">{escape(METRIC_LABELS.get(metric, metric))}</div>'
            f'<div class="compare-stat-period">{escape(str(latest_left.get("period") or latest_right.get("period") or "-"))}</div>'
            f'<div class="compare-stat-row"><span>{escape(_company_title(primary_company))}</span><strong>{escape(_format_chart_amount(left_cell.get("amount")))}</strong><em>{escape(_format_percent(left_cell.get("growth_rate")))}</em></div>'
            f'<div class="compare-stat-row"><span>{escape(_company_title(secondary_company))}</span><strong>{escape(_format_chart_amount(right_cell.get("amount")))}</strong><em>{escape(_format_percent(right_cell.get("growth_rate")))}</em></div>'
            f'<div class="compare-stat-delta">차이 {escape(_format_chart_amount(delta))}</div>'
            "</article>"
        )
    return (
        f'<div class="period-panel compare-stat-grid" data-panel data-period="{escape(str(group.get("key", "")))}"'
        f' {"hidden" if str(group.get("key")) != DEFAULT_PERIOD_KEY else ""}>'
        f"{''.join(cards)}"
        "</div>"
    )


def render_compare_focus_panel(
    group: dict[str, object],
    primary_company: dict[str, object],
    secondary_company: dict[str, object],
) -> str:
    panels = []
    for metric in METRIC_SEQUENCE:
        chart_title = f"{group.get('label')} · {METRIC_LABELS.get(metric, metric)} 비교"
        panels.append(
            f'<div class="chart-focus-panel" data-panel data-period="{escape(str(group.get("key", "")))}" data-metric="{escape(metric)}" '
            f'{"hidden" if not (group.get("key") == DEFAULT_PERIOD_KEY and metric == DEFAULT_METRIC_KEY) else ""}>'
            f'{render_compare_metric_chart(metric, _list(group.get("primary_rows")), _list(group.get("secondary_rows")), period_key=str(group.get("key", "")), primary_name=_company_title(primary_company), secondary_name=_company_title(secondary_company), title=chart_title)}'
            "</div>"
        )
    return "".join(panels)


def render_compare_grid(
    group: dict[str, object],
    primary_company: dict[str, object],
    secondary_company: dict[str, object],
) -> str:
    cards = []
    for metric in METRIC_SEQUENCE:
        chart_title = f"{group.get('label')} · {METRIC_LABELS.get(metric, metric)}"
        cards.append(
            f'<article class="panel grid-card" data-panel data-period="{escape(str(group.get("key", "")))}" {"hidden" if str(group.get("key")) != DEFAULT_PERIOD_KEY else ""}>'
            f'{render_compare_metric_chart(metric, _list(group.get("primary_rows")), _list(group.get("secondary_rows")), period_key=str(group.get("key", "")), primary_name=_company_title(primary_company), secondary_name=_company_title(secondary_company), title=chart_title)}'
            "</article>"
        )
    cards.append(
        f'<article class="panel info-card" data-panel data-period="{escape(str(group.get("key", "")))}" {"hidden" if str(group.get("key")) != DEFAULT_PERIOD_KEY else ""}>'
        f'<div class="panel-heading"><h3>{escape(str(group.get("label", "")))} 최근 비교 표</h3></div>'
        f'{render_compare_table(_list(group.get("primary_rows")), _list(group.get("secondary_rows")), _company_title(primary_company), _company_title(secondary_company))}'
        "</article>"
    )
    return "".join(cards)


def render_filter_results_for_group(results: list[object]) -> str:
    if not results:
        return '<p class="empty">표시할 필터 결과가 없습니다.</p>'

    rows = []
    for item in results:
        result = _dict(item)
        rows.append(
            '<div class="filter-row">'
            f'<span>{escape(METRIC_LABELS.get(str(result.get("metric", "")), str(result.get("metric", ""))))}</span>'
            f'<strong>{escape(_format_percent(result.get("minimum_growth_rate")))}</strong>'
            f'<em class="{_pass_class(result.get("passed"))}">{escape("통과" if result.get("passed") is True else "미통과")}</em>'
            "</div>"
        )
    return f'<div class="filter-list">{"".join(rows)}</div>'


def render_growth_sections(sections: list[object]) -> str:
    if not sections:
        return '<p class="empty">표시할 성장률 데이터가 없습니다.</p>'

    rendered = []
    for item in sections:
        section = _dict(item)
        points = [_dict(point) for point in _list(section.get("points"))]
        rendered.append(
            f"""
            <section class="growth-series">
              <h3>{escape(str(section.get("metric_label", "")))} · {escape(str(section.get("series_label", "")))}</h3>
              {_render_growth_chart(points)}
              {render_growth_table(points)}
            </section>
            """
        )
    return "\n".join(rendered)


def render_growth_table(points: list[dict[str, object]]) -> str:
    rows = []
    for point in reversed(points):
        rows.append(
            "<tr>"
            f"<td>{escape(str(point.get('period_label', '')))}</td>"
            f"<td>{_format_amount(point.get('amount'))}</td>"
            f"<td>{_format_amount(point.get('base_amount'))}</td>"
            f"<td>{_format_percent(point.get('growth_rate'))}</td>"
            "</tr>"
        )
    return (
        "<table>"
        "<thead><tr><th>기간</th><th>금액</th><th>비교 기준 금액</th><th>성장률</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def render_collection_errors(errors: list[object]) -> str:
    if not errors:
        return ""

    rows = []
    for item in errors:
        error = _dict(item)
        rows.append(
            "<tr>"
            f"<td>{escape(str(error.get('business_year', '')))}</td>"
            f"<td>{escape(str(error.get('report_code', '')))}</td>"
            f"<td>{escape(str(error.get('error_type', '')))}</td>"
            f"<td>{escape(str(error.get('message', '')))}</td>"
            "</tr>"
        )

    return (
        '<section class="notice error">'
        "<h3>수집 오류</h3>"
        "<table>"
        "<thead><tr><th>연도</th><th>보고서</th><th>유형</th><th>메시지</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
        "</section>"
    )


def render_metric_amount_chart(
    metric: str,
    rows: list[object],
    *,
    period_key: str,
    growth_label: str,
    include_qoq: bool,
    width: int = 920,
    height: int = 360,
) -> str:
    chart_rows = _build_metric_chart_rows(rows, metric, include_qoq=include_qoq)
    if not chart_rows:
        return '<p class="empty">차트로 표시할 금액 데이터가 없습니다.</p>'

    if period_key == "quarterly":
        return _render_quarterly_metric_amount_chart(
            metric,
            chart_rows,
            growth_label=growth_label,
            include_qoq=include_qoq,
            width=width,
            height=height,
        )

    return _render_series_metric_amount_chart(
        metric,
        chart_rows,
        growth_label=growth_label,
        include_qoq=include_qoq,
        width=width,
        height=height,
    )

    chart_rows: list[dict[str, object]] = []
    for item in reversed(rows[:12]):
        row = _dict(item)
        values = _dict(row.get("values"))
        cell = _dict(values.get(metric))
        amount = _to_decimal(cell.get("amount"))
        if amount is None:
            continue
        chart_rows.append(
            {
                "period": str(row.get("period", "")),
                "amount": amount,
                "growth_rate": cell.get("growth_rate"),
                "qoq_growth_rate": None,
            }
        )

    if not chart_rows:
        return '<p class="empty">차트로 표시할 금액 데이터가 없습니다.</p>'

    if include_qoq:
        previous_amount: Decimal | None = None
        for row in chart_rows:
            row["qoq_growth_rate"] = _calculate_growth_rate(
                row["amount"],
                previous_amount,
            )
            previous_amount = row["amount"]

    top = 36
    left = 86
    right = 96
    bottom = 84
    chart_width = Decimal(width - left - right)
    chart_height = Decimal(height - top - bottom)
    values = [row["amount"] for row in chart_rows]
    min_value = min(values + [Decimal("0")])
    max_value = max(values + [Decimal("0")])
    if min_value == max_value:
        min_value -= Decimal("1")
        max_value += Decimal("1")
    padding = (max_value - min_value) * Decimal("0.08")
    min_value -= padding
    max_value += padding

    def y_for(value: Decimal) -> Decimal:
        return Decimal(top) + chart_height - (
            (value - min_value)
            / (max_value - min_value)
            * chart_height
        )

    zero_y = y_for(Decimal("0"))
    slot_width = chart_width / Decimal(len(chart_rows))
    bar_width = min(Decimal("54"), slot_width * Decimal("0.62"))

    growth_series = [
        {
            "key": "growth_rate",
            "label": growth_label,
            "color": "#2563eb",
        }
    ]
    if include_qoq:
        growth_series.append(
            {
                "key": "qoq_growth_rate",
                "label": "QoQ 성장률",
                "color": "#f59e0b",
            }
        )

    growth_values = [
        rate
        for row in chart_rows
        for series in growth_series
        for rate in [_to_decimal(row.get(series["key"]))]
        if rate is not None
    ]
    growth_min: Decimal | None = None
    growth_max: Decimal | None = None
    growth_zero_y: Decimal | None = None
    growth_y_for = None
    if growth_values:
        growth_min = min(growth_values + [Decimal("0")])
        growth_max = max(growth_values + [Decimal("0")])
        if growth_min == growth_max:
            growth_min -= Decimal("1")
            growth_max += Decimal("1")
        growth_padding = (growth_max - growth_min) * Decimal("0.12")
        growth_min -= growth_padding
        growth_max += growth_padding

        def growth_y_for(value: Decimal) -> Decimal:
            return Decimal(top) + chart_height - (
                (value - growth_min)
                / (growth_max - growth_min)
                * chart_height
            )

        growth_zero_y = growth_y_for(Decimal("0"))

    amount_ticks = _build_amount_axis_ticks(min_value, max_value)
    amount_grid = []
    amount_labels = []
    for tick in amount_ticks:
        y = y_for(tick)
        amount_grid.append(
            f'<line x1="{left}" y1="{_svg_number(y)}" x2="{width - right}" y2="{_svg_number(y)}" stroke="#eef2f5" />'
        )
        amount_labels.append(
            f'<line x1="{left - 6}" y1="{_svg_number(y)}" x2="{left}" y2="{_svg_number(y)}" stroke="#b7c0c8" />'
            f'<text x="{left - 10}" y="{_svg_number(y + Decimal("4"))}" font-size="11" fill="#5d6972" text-anchor="end">{escape(_format_chart_amount(tick))}</text>'
        )

    bars = []
    for index, row in enumerate(chart_rows):
        amount = row["amount"]
        value_y = y_for(amount)
        center_x = Decimal(left) + slot_width * Decimal(index) + (slot_width / Decimal("2"))
        bar_x = center_x - (bar_width / Decimal("2"))
        bar_y = min(value_y, zero_y)
        bar_height = max(abs(zero_y - value_y), Decimal("1"))
        label_y = (
            max(Decimal("16"), bar_y - Decimal("8"))
            if amount >= 0
            else min(Decimal(height - 18), bar_y + bar_height + Decimal("16"))
        )
        tick_bottom = Decimal(height - bottom)
        bars.append(
            "<g>"
            f"<title>{escape(_build_amount_chart_tooltip(row, growth_label=growth_label, include_qoq=include_qoq))}</title>"
            f'<rect x="{_svg_number(bar_x)}" y="{_svg_number(bar_y)}" width="{_svg_number(bar_width)}" height="{_svg_number(bar_height)}" fill="{_amount_chart_fill(row.get("growth_rate"))}" rx="4" />'
            f'<text x="{_svg_number(center_x)}" y="{_svg_number(label_y)}" font-size="12" fill="#5d6972" text-anchor="middle">{escape(_format_chart_amount(amount))}</text>'
            f'<line x1="{_svg_number(center_x)}" y1="{_svg_number(tick_bottom)}" x2="{_svg_number(center_x)}" y2="{_svg_number(tick_bottom + Decimal("6"))}" stroke="#b7c0c8" />'
            f'<text x="{_svg_number(center_x)}" y="{_svg_number(tick_bottom + Decimal("22"))}" font-size="12" fill="#5d6972" text-anchor="middle">{escape(_truncate_label(str(row["period"]), 12))}</text>'
            "</g>"
        )

    growth_ticks = []
    growth_lines = []
    if growth_values and growth_y_for is not None and growth_min is not None and growth_max is not None:
        for tick in _build_growth_axis_ticks(growth_min, growth_max):
            y = growth_y_for(tick)
            growth_ticks.append(
                f'<line x1="{width - right}" y1="{_svg_number(y)}" x2="{width - right + 6}" y2="{_svg_number(y)}" stroke="#b7c0c8" />'
                f'<text x="{width - right + 10}" y="{_svg_number(y + Decimal("4"))}" font-size="11" fill="#5d6972">{escape(_format_percent(tick))}</text>'
            )
        for series in growth_series:
            growth_lines.append(
                _render_amount_growth_series(
                    chart_rows,
                    value_key=str(series["key"]),
                    color=str(series["color"]),
                    left=left,
                    slot_width=slot_width,
                    growth_y_for=growth_y_for,
                )
            )

    legend_items = [
        '<span class="legend-item"><span class="legend-swatch"></span><span>금액 막대</span></span>'
    ]
    for series in growth_series:
        legend_items.append(
            f'<span class="legend-item"><span class="legend-line" style="border-top-color: {escape(str(series["color"]))};"></span><span>{escape(str(series["label"]))}</span></span>'
        )

    return f"""
    <div class="chart-shell">
      <div class="legend">{"".join(legend_items)}</div>
      <svg class="amount-chart" viewBox="0 0 {width} {height}" role="img" aria-label="{escape(METRIC_LABELS.get(metric, metric))} 금액 차트">
        {''.join(amount_grid)}
        {(
            f'<line x1="{left}" y1="{_svg_number(growth_zero_y)}" x2="{width - right}" y2="{_svg_number(growth_zero_y)}" stroke="#dbe7ff" stroke-dasharray="3 4" />'
            if growth_zero_y is not None
            else ""
        )}
        <line x1="{left}" y1="{_svg_number(zero_y)}" x2="{width - right}" y2="{_svg_number(zero_y)}" stroke="#b7c0c8" stroke-dasharray="4 4" />
        <line data-axis="amount-left" x1="{left}" y1="{top}" x2="{left}" y2="{height - bottom}" stroke="#b7c0c8" />
        <line data-axis="growth-right" x1="{width - right}" y1="{top}" x2="{width - right}" y2="{height - bottom}" stroke="#b7c0c8" />
        <line x1="{left}" y1="{height - bottom}" x2="{width - right}" y2="{height - bottom}" stroke="#d7dde2" />
        {''.join(amount_labels)}
        {''.join(bars)}
        {''.join(growth_ticks)}
        {''.join(growth_lines)}
      </svg>
    </div>
    """


def render_compare_metric_chart(
    metric: str,
    primary_rows: list[object],
    secondary_rows: list[object],
    *,
    period_key: str,
    primary_name: str,
    secondary_name: str,
    title: str,
    width: int = 540,
    height: int = 320,
) -> str:
    if period_key == "quarterly":
        return _render_compare_quarterly_metric_chart(
            metric,
            primary_rows,
            secondary_rows,
            primary_name=primary_name,
            secondary_name=secondary_name,
            title=title,
            width=width,
            height=height,
        )

    return _render_compare_series_metric_chart(
        metric,
        primary_rows,
        secondary_rows,
        primary_name=primary_name,
        secondary_name=secondary_name,
        title=title,
        width=width,
        height=height,
    )

    primary_points = _compare_chart_points(primary_rows, metric)
    secondary_points = _compare_chart_points(secondary_rows, metric)
    labels = [point["period"] for point in primary_points or secondary_points]
    if not labels:
        return '<p class="empty">비교 차트로 표시할 데이터가 없습니다.</p>'

    values = [point["amount"] for point in primary_points + secondary_points]
    min_value = min(values + [Decimal("0")])
    max_value = max(values + [Decimal("0")])
    if min_value == max_value:
        min_value -= Decimal("1")
        max_value += Decimal("1")
    padding = (max_value - min_value) * Decimal("0.08")
    min_value -= padding
    max_value += padding

    top = 28
    left = 70
    right = 24
    bottom = 68
    chart_width = Decimal(width - left - right)
    chart_height = Decimal(height - top - bottom)

    def y_for(value: Decimal) -> Decimal:
        return Decimal(top) + chart_height - (
            (value - min_value)
            / (max_value - min_value)
            * chart_height
        )

    slot_width = chart_width / Decimal(max(len(labels), 1))
    ticks = _build_amount_axis_ticks(min_value, max_value)
    grid = []
    labels_left = []
    for tick in ticks:
        y = y_for(tick)
        grid.append(
            f'<line x1="{left}" y1="{_svg_number(y)}" x2="{width - right}" y2="{_svg_number(y)}" stroke="#eef2f5" />'
        )
        labels_left.append(
            f'<text x="{left - 10}" y="{_svg_number(y + Decimal("4"))}" font-size="11" fill="#5d6972" text-anchor="end">{escape(_format_chart_amount(tick))}</text>'
        )

    primary_line = _render_compare_line(
        primary_points,
        color="#0ea5a8",
        left=left,
        slot_width=slot_width,
        y_for=y_for,
        title_prefix=primary_name,
    )
    secondary_line = _render_compare_line(
        secondary_points,
        color="#f43f5e",
        left=left,
        slot_width=slot_width,
        y_for=y_for,
        title_prefix=secondary_name,
    )

    x_labels = []
    for index, label in enumerate(labels):
        center_x = Decimal(left) + slot_width * Decimal(index) + (slot_width / Decimal("2"))
        x_labels.append(
            f'<line x1="{_svg_number(center_x)}" y1="{height - bottom}" x2="{_svg_number(center_x)}" y2="{height - bottom + 6}" stroke="#b7c0c8" />'
            f'<text x="{_svg_number(center_x)}" y="{height - bottom + 20}" font-size="11" fill="#5d6972" text-anchor="middle">{escape(_truncate_label(label, 12))}</text>'
        )

    return f"""
    <div class="panel-heading">
      <h3>{escape(title)}</h3>
    </div>
    <div class="legend">
      <span class="legend-item"><span class="legend-line" style="border-top-color:#0ea5a8;"></span><span>{escape(primary_name)}</span></span>
      <span class="legend-item"><span class="legend-line" style="border-top-color:#f43f5e;"></span><span>{escape(secondary_name)}</span></span>
    </div>
    <svg class="amount-chart" viewBox="0 0 {width} {height}" role="img" aria-label="{escape(title)}">
      {''.join(grid)}
      <line x1="{left}" y1="{top}" x2="{left}" y2="{height - bottom}" stroke="#b7c0c8" />
      <line x1="{left}" y1="{height - bottom}" x2="{width - right}" y2="{height - bottom}" stroke="#d7dde2" />
      {''.join(labels_left)}
      {primary_line}
      {secondary_line}
      {''.join(x_labels)}
    </svg>
    """


def render_compare_table(
    primary_rows: list[object],
    secondary_rows: list[object],
    primary_name: str,
    secondary_name: str,
) -> str:
    paired = []
    left_index = {str(_dict(row).get("period", "")): _dict(row) for row in primary_rows[:5]}
    right_index = {str(_dict(row).get("period", "")): _dict(row) for row in secondary_rows[:5]}
    periods = []
    for row in primary_rows[:5]:
        periods.append(str(_dict(row).get("period", "")))
    for row in secondary_rows[:5]:
        period = str(_dict(row).get("period", ""))
        if period not in periods:
            periods.append(period)

    for period in periods:
        left_row = left_index.get(period, {})
        right_row = right_index.get(period, {})
        paired.append(
            "<tr>"
            f"<td>{escape(period)}</td>"
            f"<td>{escape(_format_chart_amount(_dict(_dict(left_row.get('values')).get(DEFAULT_METRIC_KEY)).get('amount')))}</td>"
            f"<td>{escape(_format_chart_amount(_dict(_dict(right_row.get('values')).get(DEFAULT_METRIC_KEY)).get('amount')))}</td>"
            f"<td>{escape(_format_percent(_dict(_dict(left_row.get('values')).get(DEFAULT_METRIC_KEY)).get('growth_rate')))}</td>"
            f"<td>{escape(_format_percent(_dict(_dict(right_row.get('values')).get(DEFAULT_METRIC_KEY)).get('growth_rate')))}</td>"
            "</tr>"
        )

    return (
        "<table>"
        f"<thead><tr><th>기간</th><th>{escape(primary_name)} 매출</th><th>{escape(secondary_name)} 매출</th><th>{escape(primary_name)} YoY</th><th>{escape(secondary_name)} YoY</th></tr></thead>"
        f"<tbody>{''.join(paired)}</tbody>"
        "</table>"
    )


def _build_period_groups(payload: dict[str, object]) -> list[dict[str, object]]:
    groups = []
    filter_results = [_dict(result) for result in _list(payload.get("filter_results"))]
    for definition in PERIOD_DEFS:
        groups.append(
            {
                "key": definition["key"],
                "label": definition["label"],
                "rows": _list(payload.get(str(definition["rows_key"]))),
                "growth_label": definition["growth_label"],
                "include_qoq": definition["include_qoq"],
                "filter_results": [
                    result
                    for result in filter_results
                    if str(result.get("series_type", "")) == str(definition["filter_series_type"])
                ],
            }
        )
    return groups


def _build_compare_period_groups(
    primary: dict[str, object],
    secondary: dict[str, object],
) -> list[dict[str, object]]:
    groups = []
    for definition in PERIOD_DEFS:
        groups.append(
            {
                "key": definition["key"],
                "label": definition["label"],
                "primary_rows": _list(primary.get(str(definition["rows_key"]))),
                "secondary_rows": _list(secondary.get(str(definition["rows_key"]))),
            }
        )
    return groups


def _compare_chart_points(rows: list[object], metric: str) -> list[dict[str, object]]:
    points: list[dict[str, object]] = []
    for item in reversed(rows[:12]):
        row = _dict(item)
        cell = _dict(_dict(row.get("values")).get(metric))
        amount = _to_decimal(cell.get("amount"))
        if amount is None:
            continue
        points.append(
            {
                "period": str(row.get("period", "")),
                "amount": amount,
                "growth_rate": cell.get("growth_rate"),
            }
        )
    return points


def _render_compare_line(
    points: list[dict[str, object]],
    *,
    color: str,
    left: int,
    slot_width: Decimal,
    y_for: Callable[[Decimal], Decimal],
    title_prefix: str,
) -> str:
    if not points:
        return ""

    coordinates = []
    markers = []
    for index, point in enumerate(points):
        center_x = Decimal(left) + slot_width * Decimal(index) + (slot_width / Decimal("2"))
        y = y_for(point["amount"])
        coordinates.append(f"{_svg_number(center_x)},{_svg_number(y)}")
        markers.append(
            "<g>"
            f"<title>{escape(title_prefix)} · {point['period']} · 금액 {_format_amount(point['amount'])} · YoY {_format_percent(point.get('growth_rate'))}</title>"
            f'<circle cx="{_svg_number(center_x)}" cy="{_svg_number(y)}" r="{GROWTH_LINE_MARKER_RADIUS}" fill="{color}" stroke="#ffffff" stroke-width="{GROWTH_LINE_MARKER_STROKE_WIDTH}" />'
            "</g>"
        )

    return (
        f'<polyline points="{" ".join(coordinates)}" fill="none" stroke="{color}" stroke-width="{GROWTH_LINE_STROKE_WIDTH}" stroke-linejoin="round" stroke-linecap="round" />'
        f'{"".join(markers)}'
    )


def _build_metric_chart_rows(
    rows: list[object],
    metric: str,
    *,
    include_qoq: bool,
) -> list[dict[str, object]]:
    chart_rows: list[dict[str, object]] = []
    for item in reversed(rows):
        row = _dict(item)
        cell = _dict(_dict(row.get("values")).get(metric))
        amount = _to_decimal(cell.get("amount"))
        if amount is None:
            continue
        chart_rows.append(
            {
                "period": str(row.get("period", "")),
                "amount": amount,
                "growth_rate": cell.get("growth_rate"),
                "qoq_growth_rate": None,
                "fiscal_year": _row_fiscal_year(row),
                "fiscal_quarter": _row_fiscal_quarter(row),
            }
        )

    if include_qoq:
        previous_amount: Decimal | None = None
        for row in chart_rows:
            row["qoq_growth_rate"] = _calculate_growth_rate(
                row["amount"],
                previous_amount,
            )
            previous_amount = row["amount"]

    return chart_rows


def _render_series_metric_amount_chart(
    metric: str,
    chart_rows: list[dict[str, object]],
    *,
    growth_label: str,
    include_qoq: bool,
    width: int,
    height: int,
) -> str:
    top = 36
    left = 86
    right = 96
    bottom = 84
    chart_width = Decimal(width - left - right)
    chart_height = Decimal(height - top - bottom)
    amount_range = _build_numeric_range(
        [row["amount"] for row in chart_rows],
        padding_ratio=Decimal("0.08"),
    )
    if amount_range is None:
        return '<p class="empty">차트로 표시할 금액 데이터가 없습니다.</p>'
    min_value, max_value = amount_range

    def y_for(value: Decimal) -> Decimal:
        return Decimal(top) + chart_height - (
            (value - min_value)
            / (max_value - min_value)
            * chart_height
        )

    zero_y = y_for(Decimal("0"))
    slot_width = chart_width / Decimal(max(len(chart_rows), 1))
    bar_width = min(Decimal("54"), slot_width * Decimal("0.62"))
    show_value_labels = len(chart_rows) <= 16
    x_label_stride = _x_label_stride(len(chart_rows))

    growth_series = [
        {
            "key": "growth_rate",
            "label": growth_label,
            "color": GROWTH_LINE_COLORS["growth_rate"],
        }
    ]
    if include_qoq:
        growth_series.append(
            {
                "key": "qoq_growth_rate",
                "label": "QoQ 성장률",
                "color": GROWTH_LINE_COLORS["qoq_growth_rate"],
            }
        )

    growth_values = [
        rate
        for row in chart_rows
        for series in growth_series
        for rate in [_to_decimal(row.get(series["key"]))]
        if rate is not None
    ]
    growth_range = _build_numeric_range(
        growth_values,
        padding_ratio=Decimal("0.12"),
    )
    growth_zero_y: Decimal | None = None
    growth_y_for = None
    if growth_range is not None:
        growth_min, growth_max = growth_range

        def growth_y_for(value: Decimal) -> Decimal:
            return Decimal(top) + chart_height - (
                (value - growth_min)
                / (growth_max - growth_min)
                * chart_height
            )

        growth_zero_y = growth_y_for(Decimal("0"))

    amount_ticks = _build_amount_axis_ticks(min_value, max_value)
    amount_grid = []
    amount_labels = []
    for tick in amount_ticks:
        y = y_for(tick)
        amount_grid.append(
            f'<line x1="{left}" y1="{_svg_number(y)}" x2="{width - right}" y2="{_svg_number(y)}" stroke="#e6edf5" />'
        )
        amount_labels.append(
            f'<line x1="{left - 6}" y1="{_svg_number(y)}" x2="{left}" y2="{_svg_number(y)}" stroke="#94a3b8" />'
            f'<text x="{left - 10}" y="{_svg_number(y + Decimal("4"))}" font-size="11" fill="#475569" text-anchor="end">{escape(_format_chart_amount(tick))}</text>'
        )

    bars = []
    for index, row in enumerate(chart_rows):
        amount = row["amount"]
        value_y = y_for(amount)
        center_x = Decimal(left) + slot_width * Decimal(index) + (slot_width / Decimal("2"))
        bar_x = center_x - (bar_width / Decimal("2"))
        bar_y = min(value_y, zero_y)
        bar_height = max(abs(zero_y - value_y), Decimal("1"))
        tick_bottom = Decimal(height - bottom)
        label_y = (
            max(Decimal("16"), bar_y - Decimal("8"))
            if amount >= 0
            else min(Decimal(height - 18), bar_y + bar_height + Decimal("16"))
        )
        label_markup = ""
        if show_value_labels:
            label_markup = (
                f'<text x="{_svg_number(center_x)}" y="{_svg_number(label_y)}" font-size="12" fill="#475569" text-anchor="middle">{escape(_format_chart_amount(amount))}</text>'
            )
        tick_markup = ""
        if _should_render_x_label(index, len(chart_rows), x_label_stride):
            tick_markup = (
                f'<line x1="{_svg_number(center_x)}" y1="{_svg_number(tick_bottom)}" x2="{_svg_number(center_x)}" y2="{_svg_number(tick_bottom + Decimal("6"))}" stroke="#94a3b8" />'
                f'<text x="{_svg_number(center_x)}" y="{_svg_number(tick_bottom + Decimal("22"))}" font-size="12" fill="#475569" text-anchor="middle">{escape(_truncate_label(str(row["period"]), 12))}</text>'
            )
        bars.append(
            "<g>"
            f"<title>{escape(_build_amount_chart_tooltip(row, growth_label=growth_label, include_qoq=include_qoq))}</title>"
            f'<rect x="{_svg_number(bar_x)}" y="{_svg_number(bar_y)}" width="{_svg_number(bar_width)}" height="{_svg_number(bar_height)}" fill="{AMOUNT_BAR_COLOR}" stroke="{AMOUNT_BAR_STROKE}" stroke-width="1" rx="4" />'
            f"{label_markup}"
            f"{tick_markup}"
            "</g>"
        )

    growth_ticks = []
    growth_lines = []
    if growth_range is not None and growth_y_for is not None and growth_zero_y is not None:
        growth_min, growth_max = growth_range
        for tick in _build_growth_axis_ticks(growth_min, growth_max):
            y = growth_y_for(tick)
            growth_ticks.append(
                f'<line x1="{width - right}" y1="{_svg_number(y)}" x2="{width - right + 6}" y2="{_svg_number(y)}" stroke="#94a3b8" />'
                f'<text x="{width - right + 10}" y="{_svg_number(y + Decimal("4"))}" font-size="11" fill="#475569">{escape(_format_percent(tick))}</text>'
            )
        for series in growth_series:
            growth_lines.append(
                _render_amount_growth_series(
                    chart_rows,
                    value_key=str(series["key"]),
                    color=str(series["color"]),
                    left=left,
                    slot_width=slot_width,
                    growth_y_for=growth_y_for,
                )
            )

    legend_items = [
        f'<span class="legend-item"><span class="legend-swatch" style="background:{AMOUNT_BAR_COLOR};"></span><span>금액</span></span>'
    ]
    for series in growth_series:
        legend_items.append(
            f'<span class="legend-item"><span class="legend-line" style="border-top-color: {escape(str(series["color"]))};"></span><span>{escape(str(series["label"]))}</span></span>'
        )

    return f"""
    <div class="chart-shell">
      <div class="legend">{"".join(legend_items)}</div>
      <svg class="amount-chart" viewBox="0 0 {width} {height}" role="img" aria-label="{escape(METRIC_LABELS.get(metric, metric))} 금액 차트">
        {''.join(amount_grid)}
        {(
            f'<line x1="{left}" y1="{_svg_number(growth_zero_y)}" x2="{width - right}" y2="{_svg_number(growth_zero_y)}" stroke="#e9d5ff" stroke-dasharray="3 4" />'
            if growth_zero_y is not None
            else ""
        )}
        <line x1="{left}" y1="{_svg_number(zero_y)}" x2="{width - right}" y2="{_svg_number(zero_y)}" stroke="#94a3b8" stroke-dasharray="4 4" />
        <line data-axis="amount-left" x1="{left}" y1="{top}" x2="{left}" y2="{height - bottom}" stroke="#94a3b8" />
        <line data-axis="growth-right" x1="{width - right}" y1="{top}" x2="{width - right}" y2="{height - bottom}" stroke="#94a3b8" />
        <line x1="{left}" y1="{height - bottom}" x2="{width - right}" y2="{height - bottom}" stroke="#cbd5e1" />
        {''.join(amount_labels)}
        {''.join(bars)}
        {''.join(growth_ticks)}
        {''.join(growth_lines)}
      </svg>
    </div>
    """


def _render_quarterly_metric_amount_chart(
    metric: str,
    chart_rows: list[dict[str, object]],
    *,
    growth_label: str,
    include_qoq: bool,
    width: int,
    height: int,
    amount_range: tuple[Decimal, Decimal] | None = None,
    growth_range: tuple[Decimal, Decimal] | None = None,
    show_legend: bool = True,
    show_x_axis_labels: bool = True,
    title_prefix: str | None = None,
) -> str:
    top = 36
    left = 72
    right = 96
    bottom = 74 if show_x_axis_labels else 30
    chart_width = Decimal(width - left - right)
    chart_height = Decimal(height - top - bottom)
    amount_range = amount_range or _build_numeric_range(
        [row["amount"] for row in chart_rows],
        padding_ratio=Decimal("0.08"),
    )
    if amount_range is None:
        return '<p class="empty">차트로 표시할 금액 데이터가 없습니다.</p>'
    amount_min, amount_max = amount_range

    def amount_y_for(value: Decimal) -> Decimal:
        return Decimal(top) + chart_height - (
            (value - amount_min)
            / (amount_max - amount_min)
            * chart_height
        )

    amount_zero_y = amount_y_for(Decimal("0"))
    growth_values = [
        rate
        for row in chart_rows
        for key in ("growth_rate", "qoq_growth_rate")
        for rate in [_to_decimal(row.get(key))]
        if rate is not None and (key != "qoq_growth_rate" or include_qoq)
    ]
    growth_range = growth_range or _build_numeric_range(
        growth_values,
        padding_ratio=Decimal("0.12"),
    )
    growth_zero_y: Decimal | None = None
    growth_y_for = None
    if growth_range is not None:
        growth_min, growth_max = growth_range

        def growth_y_for(value: Decimal) -> Decimal:
            return Decimal(top) + chart_height - (
                (value - growth_min)
                / (growth_max - growth_min)
                * chart_height
            )

        growth_zero_y = growth_y_for(Decimal("0"))

    years = []
    for row in chart_rows:
        year = int(row.get("fiscal_year") or 0)
        if year and year not in years:
            years.append(year)
    if not years:
        return '<p class="empty">차트로 표시할 분기 데이터가 없습니다.</p>'

    group_width = chart_width / Decimal(len(years))
    quarter_gap = min(Decimal("6"), max(Decimal("2"), group_width * Decimal("0.05")))
    bar_width = (group_width * Decimal("0.76") - (quarter_gap * Decimal("3"))) / Decimal("4")
    bar_width = min(Decimal("16"), max(Decimal("6"), bar_width))
    group_inner_width = bar_width * Decimal("4") + quarter_gap * Decimal("3")
    group_offset = (group_width - group_inner_width) / Decimal("2")
    year_index = {year: index for index, year in enumerate(years)}

    positioned_rows: list[dict[str, object]] = []
    for row in chart_rows:
        year = int(row.get("fiscal_year") or 0)
        quarter = int(row.get("fiscal_quarter") or 0)
        if year not in year_index or quarter not in QUARTER_COLORS:
            continue
        center_x = (
            Decimal(left)
            + group_width * Decimal(year_index[year])
            + group_offset
            + Decimal(quarter - 1) * (bar_width + quarter_gap)
            + (bar_width / Decimal("2"))
        )
        positioned_rows.append({**row, "center_x": center_x})

    amount_ticks = _build_amount_axis_ticks(amount_min, amount_max)
    amount_grid = []
    amount_labels = []
    for tick in amount_ticks:
        y = amount_y_for(tick)
        amount_grid.append(
            f'<line x1="{left}" y1="{_svg_number(y)}" x2="{width - right}" y2="{_svg_number(y)}" stroke="#e5e7eb" />'
        )
        amount_labels.append(
            f'<line x1="{left - 6}" y1="{_svg_number(y)}" x2="{left}" y2="{_svg_number(y)}" stroke="#94a3b8" />'
            f'<text x="{left - 10}" y="{_svg_number(y + Decimal("4"))}" font-size="11" fill="#475569" text-anchor="end">{escape(_format_chart_amount(tick))}</text>'
        )

    year_guides = []
    year_labels = []
    axis_bottom = Decimal(height - bottom)
    for index, year in enumerate(years):
        boundary_x = Decimal(left) + group_width * Decimal(index)
        year_guides.append(
            f'<line x1="{_svg_number(boundary_x)}" y1="{top}" x2="{_svg_number(boundary_x)}" y2="{_svg_number(axis_bottom)}" stroke="#eef2f7" />'
        )
        if show_x_axis_labels:
            center_x = Decimal(left) + group_width * Decimal(index) + (group_width / Decimal("2"))
            year_labels.append(
                f'<text x="{_svg_number(center_x)}" y="{_svg_number(axis_bottom + Decimal("20"))}" font-size="11" fill="#475569" text-anchor="middle">{year}</text>'
            )
    year_guides.append(
        f'<line x1="{_svg_number(Decimal(left) + group_width * Decimal(len(years)))}" y1="{top}" x2="{_svg_number(Decimal(left) + group_width * Decimal(len(years)))}" y2="{_svg_number(axis_bottom)}" stroke="#eef2f7" />'
    )

    bars = []
    for row in positioned_rows:
        amount = row["amount"]
        value_y = amount_y_for(amount)
        center_x = row["center_x"]
        bar_x = center_x - (bar_width / Decimal("2"))
        bar_y = min(value_y, amount_zero_y)
        bar_height = max(abs(amount_zero_y - value_y), Decimal("1"))
        quarter = int(row.get("fiscal_quarter") or 4)
        bars.append(
            "<g>"
            f"<title>{escape(_build_amount_chart_tooltip(row, growth_label=growth_label, include_qoq=include_qoq, title_prefix=title_prefix))}</title>"
            f'<rect x="{_svg_number(bar_x)}" y="{_svg_number(bar_y)}" width="{_svg_number(bar_width)}" height="{_svg_number(bar_height)}" fill="{QUARTER_COLORS.get(quarter, AMOUNT_BAR_COLOR)}" stroke="#1f2937" stroke-opacity="0.18" stroke-width="1" rx="2" />'
            "</g>"
        )

    growth_ticks = []
    growth_lines = []
    if growth_range is not None and growth_y_for is not None and growth_zero_y is not None:
        growth_min, growth_max = growth_range
        for tick in _build_growth_axis_ticks(growth_min, growth_max):
            y = growth_y_for(tick)
            growth_ticks.append(
                f'<line x1="{width - right}" y1="{_svg_number(y)}" x2="{width - right + 6}" y2="{_svg_number(y)}" stroke="#94a3b8" />'
                f'<text x="{width - right + 10}" y="{_svg_number(y + Decimal("4"))}" font-size="11" fill="#475569">{escape(_format_percent(tick))}</text>'
            )
        growth_lines.append(
            _render_centered_growth_series(
                positioned_rows,
                value_key="growth_rate",
                color=GROWTH_LINE_COLORS["growth_rate"],
                growth_y_for=growth_y_for,
            )
        )
        if include_qoq:
            growth_lines.append(
                _render_centered_growth_series(
                    positioned_rows,
                    value_key="qoq_growth_rate",
                    color=GROWTH_LINE_COLORS["qoq_growth_rate"],
                    growth_y_for=growth_y_for,
                )
            )

    legend_html = ""
    if show_legend:
        legend_html = f'<div class="legend">{"".join(_quarterly_legend_items(include_qoq=include_qoq, growth_label=growth_label))}</div>'

    return f"""
    <div class="chart-shell">
      {legend_html}
      <svg class="amount-chart" viewBox="0 0 {width} {height}" role="img" aria-label="{escape(METRIC_LABELS.get(metric, metric))} 분기 차트">
        {''.join(amount_grid)}
        {''.join(year_guides)}
        {(
            f'<line x1="{left}" y1="{_svg_number(growth_zero_y)}" x2="{width - right}" y2="{_svg_number(growth_zero_y)}" stroke="#f3e8ff" stroke-dasharray="3 4" />'
            if growth_zero_y is not None
            else ""
        )}
        <line x1="{left}" y1="{_svg_number(amount_zero_y)}" x2="{width - right}" y2="{_svg_number(amount_zero_y)}" stroke="#94a3b8" stroke-dasharray="4 4" />
        <line data-axis="amount-left" x1="{left}" y1="{top}" x2="{left}" y2="{_svg_number(axis_bottom)}" stroke="#94a3b8" />
        <line data-axis="growth-right" x1="{width - right}" y1="{top}" x2="{width - right}" y2="{_svg_number(axis_bottom)}" stroke="#94a3b8" />
        <line x1="{left}" y1="{_svg_number(axis_bottom)}" x2="{width - right}" y2="{_svg_number(axis_bottom)}" stroke="#cbd5e1" />
        {''.join(amount_labels)}
        {''.join(bars)}
        {''.join(growth_ticks)}
        {''.join(growth_lines)}
        {''.join(year_labels)}
      </svg>
    </div>
    """


def _render_compare_series_metric_chart(
    metric: str,
    primary_rows: list[object],
    secondary_rows: list[object],
    *,
    primary_name: str,
    secondary_name: str,
    title: str,
    width: int,
    height: int,
) -> str:
    primary_points = _build_metric_chart_rows(primary_rows, metric, include_qoq=False)
    secondary_points = _build_metric_chart_rows(secondary_rows, metric, include_qoq=False)
    labels = _merge_period_labels(primary_points, secondary_points)
    if not labels:
        return '<p class="empty">비교 차트로 표시할 데이터가 없습니다.</p>'

    values = [point["amount"] for point in primary_points + secondary_points]
    amount_range = _build_numeric_range(values, padding_ratio=Decimal("0.08"))
    if amount_range is None:
        return '<p class="empty">비교 차트로 표시할 데이터가 없습니다.</p>'
    min_value, max_value = amount_range

    top = 28
    left = 70
    right = 24
    bottom = 68
    chart_width = Decimal(width - left - right)
    chart_height = Decimal(height - top - bottom)

    def y_for(value: Decimal) -> Decimal:
        return Decimal(top) + chart_height - (
            (value - min_value)
            / (max_value - min_value)
            * chart_height
        )

    period_index = {label: index for index, label in enumerate(labels)}
    slot_width = chart_width / Decimal(max(len(labels), 1))
    x_label_stride = _x_label_stride(len(labels))

    ticks = _build_amount_axis_ticks(min_value, max_value)
    grid = []
    labels_left = []
    for tick in ticks:
        y = y_for(tick)
        grid.append(
            f'<line x1="{left}" y1="{_svg_number(y)}" x2="{width - right}" y2="{_svg_number(y)}" stroke="#e6edf5" />'
        )
        labels_left.append(
            f'<text x="{left - 10}" y="{_svg_number(y + Decimal("4"))}" font-size="11" fill="#475569" text-anchor="end">{escape(_format_chart_amount(tick))}</text>'
        )

    primary_line = _render_compare_metric_line(
        primary_points,
        period_index=period_index,
        color=COMPARE_LINE_COLORS["primary"],
        left=left,
        slot_width=slot_width,
        y_for=y_for,
        title_prefix=primary_name,
    )
    secondary_line = _render_compare_metric_line(
        secondary_points,
        period_index=period_index,
        color=COMPARE_LINE_COLORS["secondary"],
        left=left,
        slot_width=slot_width,
        y_for=y_for,
        title_prefix=secondary_name,
    )

    x_labels = []
    axis_bottom = Decimal(height - bottom)
    for index, label in enumerate(labels):
        if not _should_render_x_label(index, len(labels), x_label_stride):
            continue
        center_x = Decimal(left) + slot_width * Decimal(index) + (slot_width / Decimal("2"))
        x_labels.append(
            f'<line x1="{_svg_number(center_x)}" y1="{_svg_number(axis_bottom)}" x2="{_svg_number(center_x)}" y2="{_svg_number(axis_bottom + Decimal("6"))}" stroke="#94a3b8" />'
            f'<text x="{_svg_number(center_x)}" y="{_svg_number(axis_bottom + Decimal("20"))}" font-size="11" fill="#475569" text-anchor="middle">{escape(_truncate_label(label, 12))}</text>'
        )

    return f"""
    <div class="panel-heading">
      <h3>{escape(title)}</h3>
    </div>
    <div class="legend">
      <span class="legend-item"><span class="legend-line" style="border-top-color:{COMPARE_LINE_COLORS["primary"]};"></span><span>{escape(primary_name)}</span></span>
      <span class="legend-item"><span class="legend-line" style="border-top-color:{COMPARE_LINE_COLORS["secondary"]};"></span><span>{escape(secondary_name)}</span></span>
    </div>
    <svg class="amount-chart" viewBox="0 0 {width} {height}" role="img" aria-label="{escape(title)}">
      {''.join(grid)}
      <line x1="{left}" y1="{top}" x2="{left}" y2="{height - bottom}" stroke="#94a3b8" />
      <line x1="{left}" y1="{height - bottom}" x2="{width - right}" y2="{height - bottom}" stroke="#cbd5e1" />
      {''.join(labels_left)}
      {primary_line}
      {secondary_line}
      {''.join(x_labels)}
    </svg>
    """


def _render_compare_quarterly_metric_chart(
    metric: str,
    primary_rows: list[object],
    secondary_rows: list[object],
    *,
    primary_name: str,
    secondary_name: str,
    title: str,
    width: int,
    height: int,
) -> str:
    primary_points = _build_metric_chart_rows(primary_rows, metric, include_qoq=True)
    secondary_points = _build_metric_chart_rows(secondary_rows, metric, include_qoq=True)
    if not primary_points and not secondary_points:
        return '<p class="empty">비교 차트로 표시할 데이터가 없습니다.</p>'

    amount_range = _build_numeric_range(
        [point["amount"] for point in primary_points + secondary_points],
        padding_ratio=Decimal("0.08"),
    )
    growth_range = _build_numeric_range(
        [
            rate
            for point in primary_points + secondary_points
            for key in ("growth_rate", "qoq_growth_rate")
            for rate in [_to_decimal(point.get(key))]
            if rate is not None
        ],
        padding_ratio=Decimal("0.12"),
    )
    panel_height = max(220, (height - 36) // 2)

    return f"""
    <div class="panel-heading">
      <h3>{escape(title)}</h3>
    </div>
    <div class="legend">{"".join(_quarterly_legend_items(include_qoq=True, growth_label="YoY 성장률"))}</div>
    <div class="compare-chart-stack">
      <section class="compare-chart-panel">
        <div class="compare-panel-title">{escape(primary_name)}</div>
        {_render_quarterly_metric_amount_chart(metric, primary_points, growth_label="YoY 성장률", include_qoq=True, width=width, height=panel_height, amount_range=amount_range, growth_range=growth_range, show_legend=False, show_x_axis_labels=False, title_prefix=primary_name)}
      </section>
      <section class="compare-chart-panel">
        <div class="compare-panel-title">{escape(secondary_name)}</div>
        {_render_quarterly_metric_amount_chart(metric, secondary_points, growth_label="YoY 성장률", include_qoq=True, width=width, height=panel_height, amount_range=amount_range, growth_range=growth_range, show_legend=False, show_x_axis_labels=True, title_prefix=secondary_name)}
      </section>
    </div>
    """


def _render_centered_growth_series(
    chart_rows: list[dict[str, object]],
    *,
    value_key: str,
    color: str,
    growth_y_for: Callable[[Decimal], Decimal],
) -> str:
    segments: list[str] = []
    markers: list[str] = []
    points: list[str] = []

    def flush_points() -> None:
        nonlocal points
        if len(points) >= 2:
            segments.append(
                f'<polyline points="{" ".join(points)}" fill="none" stroke="{color}" stroke-width="{GROWTH_LINE_STROKE_WIDTH}" stroke-linejoin="round" stroke-linecap="round" />'
            )
        points = []

    for row in chart_rows:
        rate = _to_decimal(row.get(value_key))
        center_x = row.get("center_x")
        if rate is None or not isinstance(center_x, Decimal):
            flush_points()
            continue
        y = growth_y_for(rate)
        points.append(f"{_svg_number(center_x)},{_svg_number(y)}")
        markers.append(
            f'<circle cx="{_svg_number(center_x)}" cy="{_svg_number(y)}" r="{GROWTH_LINE_COMPACT_MARKER_RADIUS}" fill="{color}" stroke="#ffffff" stroke-width="{GROWTH_LINE_MARKER_STROKE_WIDTH}" />'
        )

    flush_points()
    return "".join(segments + markers)


def _render_compare_metric_line(
    points: list[dict[str, object]],
    *,
    period_index: dict[str, int],
    color: str,
    left: int,
    slot_width: Decimal,
    y_for: Callable[[Decimal], Decimal],
    title_prefix: str,
) -> str:
    if not points:
        return ""

    coordinates = []
    markers = []
    for point in points:
        index = period_index.get(str(point.get("period", "")))
        if index is None:
            continue
        center_x = Decimal(left) + slot_width * Decimal(index) + (slot_width / Decimal("2"))
        y = y_for(point["amount"])
        coordinates.append(f"{_svg_number(center_x)},{_svg_number(y)}")
        markers.append(
            "<g>"
            f"<title>{escape(title_prefix)} · {point['period']} · 금액 {_format_amount(point['amount'])} · YoY {_format_percent(point.get('growth_rate'))}</title>"
            f'<circle cx="{_svg_number(center_x)}" cy="{_svg_number(y)}" r="{GROWTH_LINE_MARKER_RADIUS}" fill="{color}" stroke="#ffffff" stroke-width="{GROWTH_LINE_MARKER_STROKE_WIDTH}" />'
            "</g>"
        )

    return (
        f'<polyline points="{" ".join(coordinates)}" fill="none" stroke="{color}" stroke-width="{GROWTH_LINE_STROKE_WIDTH}" stroke-linejoin="round" stroke-linecap="round" />'
        f'{"".join(markers)}'
    )


def _quarterly_legend_items(
    *,
    include_qoq: bool,
    growth_label: str,
) -> list[str]:
    items = [
        f'<span class="legend-item"><span class="legend-swatch" style="background:{QUARTER_COLORS[1]};"></span><span>Q1</span></span>',
        f'<span class="legend-item"><span class="legend-swatch" style="background:{QUARTER_COLORS[2]};"></span><span>Q2</span></span>',
        f'<span class="legend-item"><span class="legend-swatch" style="background:{QUARTER_COLORS[3]};"></span><span>Q3</span></span>',
        f'<span class="legend-item"><span class="legend-swatch" style="background:{QUARTER_COLORS[4]};"></span><span>Q4</span></span>',
        f'<span class="legend-item"><span class="legend-line" style="border-top-color:{GROWTH_LINE_COLORS["growth_rate"]};"></span><span>{escape(growth_label)}</span></span>',
    ]
    if include_qoq:
        items.append(
            f'<span class="legend-item"><span class="legend-line" style="border-top-color:{GROWTH_LINE_COLORS["qoq_growth_rate"]};"></span><span>QoQ 성장률</span></span>'
        )
    return items


def _build_numeric_range(
    values: list[Decimal],
    *,
    padding_ratio: Decimal,
) -> tuple[Decimal, Decimal] | None:
    if not values:
        return None
    min_value = min(values + [Decimal("0")])
    max_value = max(values + [Decimal("0")])
    if min_value == max_value:
        min_value -= Decimal("1")
        max_value += Decimal("1")
    padding = (max_value - min_value) * padding_ratio
    return (min_value - padding, max_value + padding)


def _merge_period_labels(
    primary_points: list[dict[str, object]],
    secondary_points: list[dict[str, object]],
) -> list[str]:
    labels: list[str] = []
    for point in primary_points + secondary_points:
        period = str(point.get("period", ""))
        if period and period not in labels:
            labels.append(period)
    return labels


def _x_label_stride(total_points: int) -> int:
    if total_points <= 12:
        return 1
    if total_points <= 24:
        return 2
    return 4


def _should_render_x_label(index: int, total_points: int, stride: int) -> bool:
    return index == 0 or index == total_points - 1 or index % max(stride, 1) == 0


def _row_fiscal_year(row: dict[str, object]) -> int | None:
    value = row.get("fiscal_year")
    if value is not None:
        try:
            return int(value)
        except (TypeError, ValueError):
            pass
    period = str(row.get("period", ""))
    if "Q" in period:
        period = period.split("Q", maxsplit=1)[0]
    try:
        return int(period)
    except ValueError:
        return None


def _row_fiscal_quarter(row: dict[str, object]) -> int | None:
    value = row.get("fiscal_quarter")
    if value is not None:
        try:
            return int(value)
        except (TypeError, ValueError):
            pass
    period = str(row.get("period", ""))
    if "Q" not in period:
        return None
    try:
        return int(period.split("Q", maxsplit=1)[1])
    except ValueError:
        return None


def _build_growth_axis_ticks(growth_min: Decimal, growth_max: Decimal) -> list[Decimal]:
    ticks: list[Decimal] = []
    seen: set[str] = set()
    for value in (growth_max, Decimal("0"), growth_min):
        key = str(value.quantize(Decimal("0.1")))
        if key in seen:
            continue
        seen.add(key)
        ticks.append(value)
    return ticks


def _build_amount_axis_ticks(min_value: Decimal, max_value: Decimal) -> list[Decimal]:
    candidates = [max_value]
    if min_value < 0 < max_value:
        candidates.append(Decimal("0"))
    else:
        candidates.append((max_value + min_value) / Decimal("2"))
    candidates.append(min_value)

    ticks: list[Decimal] = []
    seen: set[str] = set()
    for value in candidates:
        key = str(value.quantize(Decimal("0.1")))
        if key in seen:
            continue
        seen.add(key)
        ticks.append(value)
    return ticks


def _render_amount_growth_series(
    chart_rows: list[dict[str, object]],
    *,
    value_key: str,
    color: str,
    left: int,
    slot_width: Decimal,
    growth_y_for: Callable[[Decimal], Decimal],
) -> str:
    segments: list[str] = []
    markers: list[str] = []
    points: list[str] = []

    def flush_points() -> None:
        nonlocal points
        if len(points) >= 2:
            segments.append(
                f'<polyline points="{" ".join(points)}" fill="none" stroke="{color}" stroke-width="{GROWTH_LINE_STROKE_WIDTH}" stroke-linejoin="round" stroke-linecap="round" />'
            )
        points = []

    for index, row in enumerate(chart_rows):
        rate = _to_decimal(row.get(value_key))
        if rate is None:
            flush_points()
            continue
        center_x = Decimal(left) + slot_width * Decimal(index) + (slot_width / Decimal("2"))
        y = growth_y_for(rate)
        points.append(f"{_svg_number(center_x)},{_svg_number(y)}")
        markers.append(
            f'<circle cx="{_svg_number(center_x)}" cy="{_svg_number(y)}" r="{GROWTH_LINE_MARKER_RADIUS}" fill="{color}" stroke="#ffffff" stroke-width="{GROWTH_LINE_MARKER_STROKE_WIDTH}" />'
        )

    flush_points()
    return "".join(segments + markers)


def _calculate_growth_rate(
    current_amount: Decimal,
    previous_amount: Decimal | None,
) -> Decimal | None:
    if previous_amount is None or previous_amount <= 0:
        return None
    return (current_amount - previous_amount) / previous_amount * Decimal("100")


def _format_chart_amount(value: object) -> str:
    parsed = _to_decimal(value)
    if parsed is None:
        return "-"

    absolute = abs(parsed)
    units = [
        (Decimal("1000000000000"), "T"),
        (Decimal("1000000000"), "B"),
        (Decimal("1000000"), "M"),
        (Decimal("1000"), "K"),
    ]
    for factor, suffix in units:
        if absolute >= factor:
            scaled = parsed / factor
            text = format(
                scaled.quantize(Decimal("0.1")),
                "f",
            ).rstrip("0").rstrip(".")
            return f"{text}{suffix}"
    return _format_amount(parsed)


def _format_won(value: object) -> str:
    parsed = _to_decimal(value)
    if parsed is None:
        return "-"
    normalized = parsed.quantize(Decimal("1")) if parsed == parsed.to_integral() else parsed
    return f"{format(normalized, ',f').rstrip('0').rstrip('.')}원"


def _format_market_cap(value: object) -> str:
    parsed = _to_decimal(value)
    if parsed is None:
        return "-"
    absolute = abs(parsed)
    if absolute >= Decimal("1000000000000"):
        scaled = (parsed / Decimal("1000000000000")).quantize(Decimal("0.01"))
        return f"{format(scaled, 'f').rstrip('0').rstrip('.')}조원"
    if absolute >= Decimal("100000000"):
        scaled = (parsed / Decimal("100000000")).quantize(Decimal("0.01"))
        return f"{format(scaled, 'f').rstrip('0').rstrip('.')}억원"
    return _format_won(parsed)


def _format_base_date(value: object) -> str:
    text = str(value or "").strip()
    if len(text) != 8 or not text.isdigit():
        return text
    return f"{text[:4]}-{text[4:6]}-{text[6:]}"


def _build_amount_chart_tooltip(
    row: dict[str, object],
    *,
    growth_label: str,
    include_qoq: bool,
    title_prefix: str | None = None,
) -> str:
    lines = []
    if title_prefix:
        lines.append(title_prefix)
    lines.extend(
        [
            str(row.get("period", "")),
            f"금액: {_format_amount(row.get('amount'))}",
            f"{growth_label}: {_format_percent(row.get('growth_rate'))}",
        ]
    )
    if include_qoq:
        lines.append(f"QoQ 성장률: {_format_percent(row.get('qoq_growth_rate'))}")
    return "\n".join(lines)


def _pivot_period_values(
    values: Iterable[FinancialPeriodValue],
    *,
    growth_points: list[dict[str, object]],
    series_type: str,
) -> list[dict[str, object]]:
    growth_index = _index_growth_rates(growth_points, series_type=series_type)
    rows: dict[str, dict[str, object]] = {}
    for value in values:
        period = value.period_label
        row = rows.setdefault(
            period,
            {
                "period": period,
                "sort": _period_sort_key(value),
                "fiscal_year": value.fiscal_year,
                "fiscal_quarter": value.fiscal_quarter,
                "values": {},
            },
        )
        _dict(row["values"])[value.metric] = {
            "amount": str(value.amount),
            "growth_rate": growth_index.get((value.metric, period)),
        }

    return [
        {key: value for key, value in row.items() if key != "sort"}
        for row in sorted(rows.values(), key=lambda item: int(item["sort"]), reverse=True)
    ]


def _pivot_growth_amount_rows(
    growth_points: list[dict[str, object]],
    *,
    series_type: str,
) -> list[dict[str, object]]:
    rows: dict[str, dict[str, object]] = {}
    for point in growth_points:
        if str(point.get("series_type", "")) != series_type:
            continue
        period = str(point.get("period_label", ""))
        fiscal_year = int(point.get("fiscal_year", 0) or 0)
        fiscal_quarter = int(point.get("fiscal_quarter", 0) or 0)
        row = rows.setdefault(
            period,
            {
                "period": period,
                "sort": fiscal_year * 4 + fiscal_quarter,
                "fiscal_year": fiscal_year,
                "fiscal_quarter": fiscal_quarter,
                "values": {},
            },
        )
        _dict(row["values"])[str(point.get("metric", ""))] = {
            "amount": point.get("amount"),
            "growth_rate": point.get("growth_rate"),
        }

    return [
        {key: value for key, value in row.items() if key != "sort"}
        for row in sorted(rows.values(), key=lambda item: int(item["sort"]), reverse=True)
    ]


def _group_growth_points(points: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], list[dict[str, object]]] = {}
    for point in points:
        key = (str(point.get("metric", "")), str(point.get("series_type", "")))
        grouped.setdefault(key, []).append(point)

    sections = []
    for (metric, series_type), section_points in grouped.items():
        section_points.sort(
            key=lambda point: (
                int(point.get("fiscal_year", 0) or 0),
                int(point.get("fiscal_quarter", 0) or 0),
            )
        )
        sections.append(
            {
                "metric": metric,
                "metric_label": METRIC_LABELS.get(metric, metric),
                "series_type": series_type,
                "series_label": SERIES_LABELS.get(series_type, series_type),
                "points": section_points,
            }
        )

    sections.sort(
        key=lambda section: (
            METRIC_ORDER.get(str(section["metric"]), 999),
            SERIES_ORDER.get(str(section["series_type"]), 999),
        )
    )
    return sections


def _growth_points_from_payload(payload: dict[str, object]) -> list[dict[str, object]]:
    raw_points = payload.get("growth_points", [])
    if not isinstance(raw_points, list):
        return []
    points = []
    for item in raw_points:
        if not isinstance(item, dict):
            continue
        fiscal_quarter = item.get("fiscal_quarter")
        points.append(
            {
                **item,
                "period_label": (
                    f"{item.get('fiscal_year')}Q{fiscal_quarter}"
                    if fiscal_quarter is not None
                    else str(item.get("fiscal_year", ""))
                ),
            }
        )
    return points


def _filter_results_from_payload(payload: dict[str, object]) -> list[dict[str, object]]:
    filter_payload = payload.get("filter")
    if not isinstance(filter_payload, dict):
        return []
    raw_results = filter_payload.get("results", [])
    if not isinstance(raw_results, list):
        return []
    return [item for item in raw_results if isinstance(item, dict)]


def _company_from_rows(
    company: DartCompany,
    rows: list[FinancialStatementRow],
) -> dict[str, str]:
    for row in rows:
        if row.corp_code == company.corp_code:
            return {
                "corp_code": row.corp_code,
                "corp_name": row.corp_name,
                "stock_code": row.stock_code,
            }
    return {
        "corp_code": company.corp_code,
        "corp_name": company.corp_name,
        "stock_code": company.stock_code,
    }


def _company_title(company: dict[str, object]) -> str:
    name = str(company.get("corp_name") or "").strip()
    stock_code = str(company.get("stock_code") or "").strip()
    corp_code = str(company.get("corp_code") or "").strip()
    if name and stock_code:
        return f"{name} ({stock_code})"
    return name or stock_code or corp_code


def _period_sort_key(value: FinancialPeriodValue) -> int:
    quarter = value.fiscal_quarter if value.fiscal_quarter is not None else 4
    return value.fiscal_year * 4 + quarter


def _index_growth_rates(
    growth_points: list[dict[str, object]],
    *,
    series_type: str,
) -> dict[tuple[str, str], object]:
    index: dict[tuple[str, str], object] = {}
    for point in growth_points:
        if str(point.get("series_type", "")) != series_type:
            continue
        key = (str(point.get("metric", "")), str(point.get("period_label", "")))
        index[key] = point.get("growth_rate")
    return index


def _format_amount_cell(cell: dict[str, object]) -> str:
    amount = cell.get("amount")
    if amount is None:
        return "-"
    return _format_amount_with_growth(amount, cell.get("growth_rate"))


def _format_amount_with_growth(amount: object, growth_rate: object) -> str:
    return f"{_format_amount(amount)} ({_format_percent(growth_rate)})"


def _amount_chart_fill(growth_rate: object) -> str:
    growth = _to_decimal(growth_rate)
    if growth is None:
        return "#b7c0c8"
    if growth > 0:
        return "rgb(62, 230, 165)"
    if growth < 0:
        return "rgb(240, 81, 81)"
    return "#b7c0c8"


def _growth_class(value: object) -> str:
    parsed = _to_decimal(value)
    if parsed is None:
        return "is-neutral"
    if parsed > 0:
        return "is-positive"
    if parsed < 0:
        return "is-negative"
    return "is-neutral"


def _pass_class(value: object) -> str:
    return "is-pass" if value is True else "is-fail"


def _subtract_decimal(left: object, right: object) -> Decimal | None:
    left_value = _to_decimal(left)
    right_value = _to_decimal(right)
    if left_value is None or right_value is None:
        return None
    return left_value - right_value


def _to_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def _svg_number(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.01")))


def _truncate_label(value: str, max_length: int) -> str:
    if len(value) <= max_length:
        return value
    return value[: max_length - 1] + "…"


def _parse_int(value: str, *, field_name: str) -> int:
    try:
        return int(value)
    except ValueError as error:
        raise ValueError(f"{field_name}은 숫자로 입력해 주세요.") from error


def _parse_decimal(value: str, *, field_name: str) -> Decimal:
    try:
        return Decimal(value)
    except InvalidOperation as error:
        raise ValueError(f"{field_name}은 숫자로 입력해 주세요.") from error


def _parse_fs_div(value: str) -> str | None:
    normalized = value.strip().upper()
    if normalized in {"", "ALL"}:
        return None
    if normalized not in {"CFS", "OFS"}:
        raise ValueError("재무제표는 연결, 별도, 전체 중 하나를 선택해 주세요.")
    return normalized


def _normalize_company_name(value: str) -> str:
    return "".join(value.casefold().split())


def _ambiguous_company_message(
    query: str,
    companies: list[DartCompany],
) -> str:
    candidates = ", ".join(
        f"{company.corp_name}({company.stock_code or company.corp_code})"
        for company in companies[:5]
    )
    return f"'{query}'에 해당하는 기업이 여러 개입니다: {candidates}"


def _format_request_error(prefix: str, error: Exception) -> str:
    message = str(error).strip()
    if not message:
        return prefix
    return f"{prefix} {message}"


def _option(value: str, label: str, selected: str) -> str:
    is_selected = value == selected.upper()
    return (
        f'<option value="{escape(value)}" selected>{escape(label)}</option>'
        if is_selected
        else f'<option value="{escape(value)}">{escape(label)}</option>'
    )


def _build_analysis_href(
    *,
    company_query: str,
    recent_years: str,
    end_year: str,
    fs_div: str,
    threshold_percent: str,
) -> str:
    params = {
        "company_query": company_query,
        "recent_years": recent_years,
        "end_year": end_year,
        "fs_div": fs_div,
        "threshold_percent": threshold_percent,
    }
    filtered = {key: value for key, value in params.items() if str(value).strip()}
    query = urlencode(filtered)
    return f"/analysis?{query}" if query else "/analysis"


def _build_compare_href(
    *,
    primary_company_query: str,
    recent_years: str,
    end_year: str,
    fs_div: str,
    threshold_percent: str,
) -> str:
    params = {
        "primary_company_query": primary_company_query,
        "recent_years": recent_years,
        "end_year": end_year,
        "fs_div": fs_div,
        "threshold_percent": threshold_percent,
    }
    filtered = {key: value for key, value in params.items() if str(value).strip()}
    query = urlencode(filtered)
    return f"/compare?{query}" if query else "/compare"


def _dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _list(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def _page_styles() -> str:
    return """
    :root {
      color-scheme: light;
      --ink: #18212a;
      --muted: #607080;
      --line: #d7dee6;
      --surface: #f4f7fb;
      --surface-alt: #eef3f8;
      --panel: #ffffff;
      --accent: #2563eb;
      --accent-soft: #e8f0ff;
      --accent-strong: #0f4bcf;
      --teal: rgb(62, 230, 165);
      --red: rgb(240, 81, 81);
      --warning: #f59e0b;
      --shadow: 0 10px 24px rgba(22, 34, 51, 0.06);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Arial, "Malgun Gothic", sans-serif;
      color: var(--ink);
      background: #f1f4f8;
      line-height: 1.45;
    }
    a { color: inherit; text-decoration: none; }
    button, input, select {
      font: inherit;
    }
    .shell-header {
      border-bottom: 1px solid var(--line);
      background: linear-gradient(180deg, #f7f9fc 0%, #eef3f8 100%);
    }
    .context-bar {
      display: flex;
      align-items: center;
      gap: 12px;
      min-height: 48px;
      padding: 0 20px;
      border-bottom: 1px solid var(--line);
    }
    .context-chip {
      padding: 8px 12px;
      border: 1px solid #c7d3df;
      border-radius: 6px;
      background: #ffffff;
      color: var(--accent-strong);
      font-weight: 700;
      font-size: 14px;
    }
    .context-meta {
      color: var(--muted);
      font-size: 13px;
    }
    .top-tabs {
      display: flex;
      flex-wrap: wrap;
      gap: 2px;
      padding: 0 14px;
      background: #ffffff;
    }
    .top-tab {
      padding: 13px 12px 12px;
      color: #415264;
      font-size: 14px;
      font-weight: 700;
      border-bottom: 2px solid transparent;
    }
    .top-tab.is-active {
      color: var(--ink);
      border-bottom-color: var(--accent);
    }
    .shell-main {
      max-width: 1360px;
      margin: 0 auto;
      padding: 18px 16px 40px;
    }
    .toolbar-surface,
    .panel,
    .notice {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      box-shadow: var(--shadow);
    }
    .toolbar-surface {
      padding: 12px 14px;
      margin-bottom: 14px;
    }
    .toolbar-surface-tight {
      padding: 10px 14px;
    }
    .section-tabs {
      display: flex;
      gap: 10px;
      padding-bottom: 12px;
      margin-bottom: 14px;
      border-bottom: 1px solid var(--line);
      overflow-x: auto;
    }
    .section-tab {
      color: #516273;
      font-size: 14px;
      font-weight: 700;
      white-space: nowrap;
    }
    .section-tab.is-active {
      color: var(--ink);
    }
    .query-form {
      display: grid;
      grid-template-columns: minmax(280px, 1.5fr) repeat(3, minmax(120px, 0.7fr)) auto;
      gap: 12px;
      align-items: end;
    }
    .query-form-compare {
      grid-template-columns: minmax(220px, 1fr) minmax(220px, 1fr) repeat(3, minmax(120px, 0.7fr)) auto;
    }
    .field {
      display: grid;
      gap: 6px;
      color: #3b4b5b;
      font-size: 13px;
      font-weight: 700;
    }
    .field-grow {
      min-width: 0;
    }
    input,
    select {
      min-height: 40px;
      border: 1px solid #cbd5df;
      border-radius: 6px;
      padding: 8px 10px;
      color: var(--ink);
      background: #ffffff;
    }
    input:focus,
    select:focus {
      outline: 2px solid #dbe7ff;
      border-color: #9db8f8;
    }
    .primary-button,
    .ghost-link,
    .segmented-button,
    .metric-switch {
      border-radius: 6px;
    }
    .primary-button {
      min-height: 40px;
      padding: 10px 16px;
      border: 0;
      color: #ffffff;
      background: linear-gradient(180deg, #3b7cff 0%, #1f63ea 100%);
      font-weight: 700;
      cursor: pointer;
    }
    .primary-button[disabled] {
      opacity: 0.75;
      cursor: wait;
    }
    .ghost-link {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 36px;
      padding: 0 12px;
      border: 1px solid #bfd0e6;
      background: #ffffff;
      color: #33547d;
      font-size: 13px;
      font-weight: 700;
    }
    .toolbar-row {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }
    .toolbar-row-dense {
      margin-top: 12px;
      padding-top: 12px;
      border-top: 1px solid var(--line);
    }
    .toolbar-note {
      color: var(--muted);
      font-size: 13px;
    }
    .toolbar-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
    }
    .inline-pill {
      display: inline-flex;
      align-items: center;
      min-height: 30px;
      padding: 0 10px;
      border: 1px solid #d3dce5;
      border-radius: 999px;
      background: #ffffff;
      color: #536273;
      font-size: 12px;
      font-weight: 700;
    }
    .inline-pill-accent {
      background: #ebfbf7;
      border-color: #b6f1df;
      color: #0f766e;
    }
    .inline-pill-contrast {
      background: #fff0f4;
      border-color: #ffccd7;
      color: #be123c;
    }
    .segmented {
      display: inline-flex;
      gap: 6px;
      flex-wrap: wrap;
    }
    .segmented-button,
    .metric-switch {
      border: 1px solid #ccd8e4;
      background: #f8fafc;
      color: #4a5b6c;
      min-height: 34px;
      padding: 0 12px;
      font-weight: 700;
      cursor: pointer;
    }
    .segmented-button.is-active,
    .metric-switch.is-active {
      border-color: #8fb0ff;
      background: var(--accent-soft);
      color: var(--accent-strong);
    }
    .notice {
      padding: 12px 14px;
      margin-bottom: 14px;
    }
    .notice.error {
      border-color: #f2c5c5;
      background: #fff6f6;
      color: #b3261e;
    }
    .notice.info {
      border-color: #cfe0ff;
      background: #f4f8ff;
      color: #30558c;
    }
    .loading-indicator {
      display: none;
      align-items: center;
      gap: 12px;
      border-color: #b7d9d0;
      background: #f2fbf8;
      color: #16423b;
    }
    body.is-loading .loading-indicator {
      display: flex;
    }
    .spinner {
      width: 18px;
      height: 18px;
      border: 2px solid #c5ddd6;
      border-top-color: #0f766e;
      border-radius: 50%;
      animation: spin 0.9s linear infinite;
      flex: 0 0 auto;
    }
    .loading-copy {
      margin: 4px 0 0;
      color: #16423b;
      font-size: 13px;
    }
    .report-shell {
      display: grid;
      gap: 14px;
    }
    .company-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      padding: 16px 18px;
    }
    .company-header h2 {
      margin: 0 0 4px;
      font-size: 24px;
    }
    .company-header p {
      margin: 0;
      color: var(--muted);
      font-size: 14px;
    }
    .company-actions {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 8px;
      justify-content: flex-end;
    }
    .overview-layout {
      display: grid;
      grid-template-columns: minmax(340px, 1fr) minmax(420px, 1.1fr);
      gap: 14px;
      align-items: start;
    }
    .compare-overview {
      grid-template-columns: minmax(320px, 0.9fr) minmax(420px, 1.1fr);
    }
    .panel {
      padding: 16px;
    }
    .panel-heading {
      display: flex;
      flex-direction: column;
      gap: 4px;
      margin-bottom: 12px;
    }
    .panel-heading h3 {
      margin: 0;
      font-size: 18px;
    }
    .panel-heading p {
      margin: 0;
      color: var(--muted);
      font-size: 13px;
    }
    .panel-heading-split {
      flex-direction: row;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
    }
    .metric-switches {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      justify-content: flex-end;
    }
    .period-panel[hidden],
    .chart-focus-panel[hidden],
    .grid-card[hidden],
    .info-card[hidden],
    .compare-stat-grid[hidden] {
      display: none !important;
    }
    .matrix-heading {
      margin-bottom: 8px;
      color: #415264;
      font-size: 13px;
      font-weight: 700;
    }
    .matrix-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }
    .matrix-table th,
    .matrix-table td {
      padding: 10px 8px;
      border-bottom: 1px solid #ebeff4;
      text-align: right;
      vertical-align: top;
    }
    .matrix-table th:first-child,
    .matrix-table td:first-child {
      text-align: left;
      white-space: nowrap;
    }
    .matrix-table thead th {
      color: var(--muted);
      font-weight: 700;
      background: #fafbfd;
    }
    .matrix-amount {
      font-weight: 700;
      color: var(--ink);
    }
    .matrix-growth {
      margin-top: 4px;
      font-size: 12px;
    }
    .matrix-growth.is-positive {
      color: #0f766e;
    }
    .matrix-growth.is-negative {
      color: #d14343;
    }
    .matrix-growth.is-neutral {
      color: var(--muted);
    }
    .chart-shell {
      display: grid;
      gap: 8px;
    }
    .legend {
      display: flex;
      flex-wrap: wrap;
      gap: 14px;
      color: var(--muted);
      font-size: 12px;
    }
    .legend-item {
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }
    .legend-swatch {
      width: 12px;
      height: 12px;
      border-radius: 4px;
      background: #b7c0c8;
    }
    .legend-line {
      width: 18px;
      height: 0;
      border-top: 3px solid #2563eb;
      border-radius: 999px;
    }
    .amount-chart {
      width: 100%;
      min-height: 220px;
      border: 1px solid #dbe3ec;
      border-radius: 8px;
      background: linear-gradient(180deg, #fbfcfd 0%, #ffffff 100%);
    }
    .dashboard-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
    }
    .grid-card,
    .info-card {
      display: grid;
      gap: 8px;
    }
    .filter-list {
      display: grid;
      gap: 10px;
    }
    .filter-row {
      display: grid;
      grid-template-columns: 1fr auto auto;
      gap: 10px;
      align-items: center;
      padding-bottom: 8px;
      border-bottom: 1px solid #ebeff4;
      font-size: 13px;
    }
    .filter-row strong {
      font-size: 13px;
    }
    .filter-row em {
      font-style: normal;
      font-weight: 700;
    }
    .filter-row em.is-pass {
      color: #0f766e;
    }
    .filter-row em.is-fail {
      color: #d14343;
    }
    .info-card-footer {
      margin-top: auto;
      padding-top: 10px;
      border-top: 1px solid #ebeff4;
    }
    .growth-section details,
    details.panel {
      padding: 14px 16px;
    }
    details summary {
      cursor: pointer;
      font-weight: 700;
      color: #314150;
    }
    details[open] summary {
      margin-bottom: 12px;
    }
    .growth-series {
      display: grid;
      gap: 10px;
      padding-top: 12px;
      margin-top: 12px;
      border-top: 1px solid #ebeff4;
    }
    .growth-series h3 {
      margin: 0;
      font-size: 16px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }
    th,
    td {
      padding: 8px 10px;
      border-bottom: 1px solid #ebeff4;
      text-align: right;
      white-space: nowrap;
    }
    th:first-child,
    td:first-child {
      text-align: left;
    }
    th {
      background: #fafbfd;
      color: #415264;
      font-weight: 700;
    }
    .compare-stat-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
    }
    .compare-chart-stack {
      display: grid;
      gap: 12px;
    }
    .compare-chart-panel {
      display: grid;
      gap: 8px;
    }
    .compare-panel-title {
      font-size: 13px;
      font-weight: 700;
      color: #334155;
    }
    .compare-stat-card {
      border: 1px solid #e5ebf2;
      border-radius: 8px;
      padding: 12px;
      background: #fbfcfe;
    }
    .compare-stat-title {
      font-weight: 700;
      color: var(--ink);
      margin-bottom: 2px;
    }
    .compare-stat-period {
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 10px;
    }
    .compare-stat-row {
      display: grid;
      grid-template-columns: 1fr auto auto;
      gap: 8px;
      align-items: center;
      padding: 6px 0;
      font-size: 13px;
    }
    .compare-stat-row strong {
      font-size: 14px;
    }
    .compare-stat-row em {
      color: var(--muted);
      font-style: normal;
      font-size: 12px;
    }
    .compare-stat-delta {
      margin-top: 8px;
      padding-top: 8px;
      border-top: 1px solid #e7edf4;
      font-size: 12px;
      color: var(--muted);
      font-weight: 700;
    }
    .empty-state {
      padding: 18px;
    }
    .empty-state h2 {
      margin: 0 0 8px;
      font-size: 22px;
    }
    .empty-state p {
      margin: 0 0 14px;
      color: var(--muted);
    }
    .empty-list {
      margin: 0;
      padding-left: 18px;
      color: #445667;
    }
    .empty-list li + li {
      margin-top: 6px;
    }
    .empty {
      color: var(--muted);
    }
    @keyframes spin {
      from { transform: rotate(0deg); }
      to { transform: rotate(360deg); }
    }
    @media (max-width: 1180px) {
      .query-form,
      .query-form-compare {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      .overview-layout,
      .compare-overview {
        grid-template-columns: 1fr;
      }
    }
    @media (max-width: 840px) {
      .shell-main {
        padding: 14px 12px 32px;
      }
      .query-form,
      .query-form-compare,
      .dashboard-grid,
      .compare-stat-grid {
        grid-template-columns: 1fr;
      }
      .company-header {
        flex-direction: column;
        align-items: flex-start;
      }
      .panel-heading-split {
        flex-direction: column;
      }
      .top-tabs {
        overflow-x: auto;
        white-space: nowrap;
      }
    }
    """


def _page_script() -> str:
    return """
    (() => {
      const loadingForms = document.querySelectorAll("[data-loading-form]");
      const loadingStatus = document.getElementById("loading-status");
      const loadingMessages = [
        "OpenDART에서 기업 목록을 확인하고 있습니다.",
        "연도별 재무제표 데이터를 가져오고 있습니다.",
        "성장률을 계산하고 차트를 준비하고 있습니다.",
      ];
      let loadingTimer = null;

      const resetLoading = () => {
        document.body.classList.remove("is-loading");
        document.querySelectorAll("[data-submit-label]").forEach((node) => {
          node.hidden = false;
        });
        document.querySelectorAll("[data-submit-loading]").forEach((node) => {
          node.hidden = true;
        });
        document.querySelectorAll(".primary-button").forEach((node) => {
          node.disabled = false;
        });
        if (loadingStatus) {
          loadingStatus.textContent = loadingMessages[0];
        }
        if (loadingTimer !== null) {
          window.clearInterval(loadingTimer);
          loadingTimer = null;
        }
      };

      loadingForms.forEach((form) => {
        form.addEventListener("submit", () => {
          document.body.classList.add("is-loading");
          const button = form.querySelector(".primary-button");
          if (button) {
            button.disabled = true;
          }
          const label = form.querySelector("[data-submit-label]");
          const loading = form.querySelector("[data-submit-loading]");
          if (label) {
            label.hidden = true;
          }
          if (loading) {
            loading.hidden = false;
          }
          let messageIndex = 0;
          if (loadingStatus) {
            loadingStatus.textContent = loadingMessages[messageIndex];
          }
          if (loadingTimer !== null) {
            window.clearInterval(loadingTimer);
          }
          loadingTimer = window.setInterval(() => {
            if (messageIndex < loadingMessages.length - 1) {
              messageIndex += 1;
              if (loadingStatus) {
                loadingStatus.textContent = loadingMessages[messageIndex];
              }
            }
          }, 2200);
        });
      });

      window.addEventListener("pageshow", resetLoading);

      document.querySelectorAll("[data-year-preset]").forEach((button) => {
        button.addEventListener("click", () => {
          const formId = button.getAttribute("data-year-form");
          const inputId = button.getAttribute("data-year-input");
          const years = button.getAttribute("data-year-preset");
          const form = formId ? document.getElementById(formId) : null;
          const input = inputId ? document.getElementById(inputId) : null;
          if (!form || !input || !years) {
            return;
          }
          input.value = years;
          if (typeof form.requestSubmit === "function") {
            form.requestSubmit();
          } else {
            form.submit();
          }
        });
      });

      document.querySelectorAll("[data-dashboard]").forEach((dashboard) => {
        const periodButtons = Array.from(
          dashboard.querySelectorAll("[data-period-toggle]")
        );
        const metricButtons = Array.from(
          dashboard.querySelectorAll("[data-metric-toggle]")
        );
        const panels = Array.from(dashboard.querySelectorAll("[data-panel]"));
        let activePeriod =
          dashboard.getAttribute("data-initial-period") ||
          (periodButtons[0] && periodButtons[0].getAttribute("data-period-toggle")) ||
          "";
        let activeMetric =
          dashboard.getAttribute("data-initial-metric") ||
          (metricButtons[0] && metricButtons[0].getAttribute("data-metric-toggle")) ||
          "";

        const applyState = () => {
          periodButtons.forEach((button) => {
            const selected =
              button.getAttribute("data-period-toggle") === activePeriod;
            button.classList.toggle("is-active", selected);
          });
          metricButtons.forEach((button) => {
            const selected =
              button.getAttribute("data-metric-toggle") === activeMetric;
            button.classList.toggle("is-active", selected);
          });
          panels.forEach((panel) => {
            const period = panel.getAttribute("data-period");
            const metric = panel.getAttribute("data-metric");
            const periodMatch = !period || period === activePeriod;
            const metricMatch = !metric || metric === activeMetric;
            panel.hidden = !(periodMatch && metricMatch);
          });
        };

        periodButtons.forEach((button) => {
          button.addEventListener("click", () => {
            const nextPeriod = button.getAttribute("data-period-toggle");
            if (!nextPeriod) {
              return;
            }
            activePeriod = nextPeriod;
            applyState();
          });
        });

        metricButtons.forEach((button) => {
          button.addEventListener("click", () => {
            const nextMetric = button.getAttribute("data-metric-toggle");
            if (!nextMetric) {
              return;
            }
            activeMetric = nextMetric;
            applyState();
          });
        });

        applyState();
      });
    })();
    """
