from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from html import escape
import os
from typing import Callable, Iterable

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from .models import FinancialPeriodValue, FinancialStatementRow
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


DEFAULT_RECENT_YEARS = 10
DEFAULT_THRESHOLD_PERCENT = Decimal("20")
MIN_OPENDART_YEAR = 2015


@dataclass(frozen=True)
class AnalysisForm:
    corp_code: str = ""
    recent_years: str = str(DEFAULT_RECENT_YEARS)
    end_year: str = ""
    fs_div: str = "CFS"
    threshold_percent: str = str(DEFAULT_THRESHOLD_PERCENT)


def create_app(
    client_factory: Callable[[str], MajorAccountClient] = OpenDartClient,
) -> FastAPI:
    app = FastAPI(title="ShowMeThePER")

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        form = AnalysisForm(end_year=str(default_end_year()))
        return HTMLResponse(render_page(form=form))

    @app.get("/analysis", response_class=HTMLResponse)
    def analysis(
        corp_code: str = "",
        recent_years: str = str(DEFAULT_RECENT_YEARS),
        end_year: str = "",
        fs_div: str = "CFS",
        threshold_percent: str = str(DEFAULT_THRESHOLD_PERCENT),
    ) -> HTMLResponse:
        form = AnalysisForm(
            corp_code=corp_code.strip(),
            recent_years=recent_years.strip(),
            end_year=end_year.strip() or str(default_end_year()),
            fs_div=fs_div.strip().upper() or "CFS",
            threshold_percent=threshold_percent.strip(),
        )

        try:
            request = parse_analysis_request(form)
        except ValueError as error:
            return HTMLResponse(render_page(form=form, error=str(error)))

        api_key = os.getenv("OPENDART_API_KEY")
        if not api_key:
            return HTMLResponse(
                render_page(
                    form=form,
                    error="OPENDART_API_KEY 환경변수를 먼저 설정해 주세요.",
                )
            )

        run = collect_financial_statement_run(
            client_factory(api_key),
            corp_codes=[request["corp_code"]],
            business_years=request["business_years"],
            report_codes=DEFAULT_REPORT_CODES,
            fs_div=request["fs_div"],
            continue_on_error=True,
        )
        artifacts = build_analysis_artifacts(
            run.rows,
            collection_errors=run.errors,
            expected_corp_codes=[request["corp_code"]],
            expected_business_years=request["business_years"],
            expected_report_codes=DEFAULT_REPORT_CODES,
            threshold_percent=request["threshold_percent"],
            recent_annual_periods=3,
            recent_quarterly_periods=12,
        )
        payload = build_browser_report_payload(
            artifacts,
            corp_code=request["corp_code"],
            start_year=request["start_year"],
            end_year=request["end_year"],
            recent_years=request["recent_years"],
            fs_div=request["fs_div"],
            threshold_percent=request["threshold_percent"],
        )
        return HTMLResponse(render_page(form=form, payload=payload))

    return app


app = create_app()


def default_end_year(today: date | None = None) -> int:
    current = today or date.today()
    return current.year - 1


def parse_analysis_request(form: AnalysisForm) -> dict[str, object]:
    corp_code = form.corp_code.strip()
    if not corp_code:
        raise ValueError("OpenDART 고유번호를 입력해 주세요.")

    recent_years = _parse_int(form.recent_years, field_name="조회 연수")
    if recent_years <= 0:
        raise ValueError("조회 연수는 1 이상이어야 합니다.")

    end_year = _parse_int(form.end_year or str(default_end_year()), field_name="기준 연도")
    start_year = max(MIN_OPENDART_YEAR, end_year - recent_years + 1)
    if start_year > end_year:
        raise ValueError("기준 연도는 2015년 이상이어야 합니다.")

    threshold_percent = _parse_decimal(
        form.threshold_percent,
        field_name="성장률 기준",
    )
    fs_div = _parse_fs_div(form.fs_div)

    return {
        "corp_code": corp_code,
        "recent_years": recent_years,
        "start_year": start_year,
        "end_year": end_year,
        "business_years": [str(year) for year in range(start_year, end_year + 1)],
        "fs_div": fs_div,
        "threshold_percent": threshold_percent,
    }


def build_browser_report_payload(
    artifacts: AnalysisArtifacts,
    *,
    corp_code: str,
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

    return {
        "company": _company_from_rows(corp_code, artifacts.financial_statement_rows),
        "summary": {
            "corp_code": corp_code,
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
        "annual_rows": _pivot_period_values(
            value for value in values if value.period_type == "annual"
        ),
        "quarterly_rows": _pivot_period_values(
            value for value in values if value.period_type == "quarter"
        ),
        "filter_results": sorted(
            filter_results,
            key=lambda item: (
                METRIC_ORDER.get(str(item.get("metric", "")), 999),
                SERIES_ORDER.get(str(item.get("series_type", "")), 999),
            ),
        ),
        "growth_sections": _group_growth_points(growth_points),
        "coverage": artifacts.coverage_report,
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


def render_page(
    *,
    form: AnalysisForm,
    payload: dict[str, object] | None = None,
    error: str | None = None,
) -> str:
    result_html = ""
    if error:
        result_html = f'<section class="notice error">{escape(error)}</section>'
    elif payload is not None:
        result_html = render_browser_report(payload)

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>ShowMeThePER</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #172026;
      --muted: #5d6972;
      --line: #d7dde2;
      --surface: #f5f7f9;
      --accent: #0b6b5c;
      --danger: #b3261e;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Arial, "Malgun Gothic", sans-serif;
      color: var(--ink);
      background: #ffffff;
      line-height: 1.5;
    }}
    header {{
      padding: 28px 24px 18px;
      background: var(--surface);
      border-bottom: 1px solid var(--line);
    }}
    main {{
      max-width: 1180px;
      margin: 0 auto;
      padding: 24px;
    }}
    h1, h2, h3 {{
      margin: 0 0 10px;
      letter-spacing: 0;
    }}
    h1 {{ font-size: 28px; }}
    h2 {{ font-size: 22px; margin-top: 28px; }}
    h3 {{ font-size: 18px; margin-top: 18px; }}
    p {{ margin: 6px 0 14px; color: var(--muted); }}
    form {{
      display: grid;
      grid-template-columns: repeat(5, minmax(130px, 1fr));
      gap: 12px;
      align-items: end;
      padding: 18px 0 8px;
    }}
    label {{
      display: grid;
      gap: 6px;
      color: #25313a;
      font-size: 14px;
      font-weight: 700;
    }}
    input, select {{
      min-height: 40px;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 8px 10px;
      color: var(--ink);
      background: #ffffff;
      font: inherit;
    }}
    button {{
      min-height: 40px;
      border: 0;
      border-radius: 8px;
      padding: 8px 14px;
      color: #ffffff;
      background: var(--accent);
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin: 10px 0 18px;
      font-size: 14px;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 8px 10px;
      text-align: right;
      vertical-align: top;
      white-space: nowrap;
    }}
    th:first-child, td:first-child,
    th:nth-child(2), td:nth-child(2) {{
      text-align: left;
    }}
    th {{
      background: #eef2f5;
      color: #25313a;
      font-weight: 700;
    }}
    .meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin: 10px 0 18px;
    }}
    .pill {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 6px 10px;
      background: #ffffff;
      color: var(--muted);
      font-size: 13px;
    }}
    .notice {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px 14px;
      margin: 16px 0;
      background: #ffffff;
    }}
    .error {{
      border-color: #e5b9b6;
      color: var(--danger);
      background: #fff7f6;
    }}
    .series {{
      border-top: 1px solid var(--line);
      padding-top: 12px;
      margin-top: 16px;
    }}
    .chart {{
      width: 100%;
      min-height: 240px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
    }}
    .empty {{ color: var(--muted); }}
    @media (max-width: 840px) {{
      main {{ padding: 16px; }}
      header {{ padding: 22px 16px 14px; }}
      h1 {{ font-size: 24px; }}
      form {{ grid-template-columns: 1fr; }}
      table {{ display: block; overflow-x: auto; }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>ShowMeThePER</h1>
    <p>OpenDART 고유번호와 조회 기간을 입력하면 매출, 영업이익, 순이익과 성장률을 바로 계산합니다.</p>
  </header>
  <main>
    {render_form(form)}
    {result_html}
  </main>
</body>
</html>
"""


def render_form(form: AnalysisForm) -> str:
    return f"""
    <form method="get" action="/analysis">
      <label>
        OpenDART 고유번호
        <input name="corp_code" value="{escape(form.corp_code)}" placeholder="00126380" required>
      </label>
      <label>
        조회 연수
        <input name="recent_years" value="{escape(form.recent_years)}" inputmode="numeric">
      </label>
      <label>
        기준 연도
        <input name="end_year" value="{escape(form.end_year or str(default_end_year()))}" inputmode="numeric">
      </label>
      <label>
        재무제표
        <select name="fs_div">
          {_option("CFS", "연결", form.fs_div)}
          {_option("OFS", "별도", form.fs_div)}
          {_option("ALL", "전체", form.fs_div)}
        </select>
      </label>
      <label>
        성장률 기준
        <input name="threshold_percent" value="{escape(form.threshold_percent)}" inputmode="decimal">
      </label>
      <button type="submit">조회</button>
    </form>
    """


def render_browser_report(payload: dict[str, object]) -> str:
    company = _dict(payload.get("company"))
    summary = _dict(payload.get("summary"))
    return f"""
    <section>
      <h2>{escape(_company_title(company))}</h2>
      <div class="meta">
        <span class="pill">고유번호 {escape(str(summary.get("corp_code", "")))}</span>
        <span class="pill">기간 {escape(str(summary.get("start_year", "")))}-{escape(str(summary.get("end_year", "")))}</span>
        <span class="pill">재무제표 {escape(str(summary.get("fs_div", "")))}</span>
        <span class="pill">원천 row {escape(str(summary.get("raw_rows", 0)))}개</span>
        <span class="pill">성장률 {escape(str(summary.get("growth_points", 0)))}개</span>
      </div>
      {render_collection_errors(_list(payload.get("collection_errors")))}
      <h2>연간 금액</h2>
      {render_amount_table(_list(payload.get("annual_rows")))}
      <h2>분기 금액</h2>
      {render_amount_table(_list(payload.get("quarterly_rows")))}
      <h2>성장률 필터 결과</h2>
      {render_filter_results(_list(payload.get("filter_results")))}
      <h2>성장률 차트</h2>
      {render_growth_sections(_list(payload.get("growth_sections")))}
    </section>
    """


def render_amount_table(rows: list[object]) -> str:
    if not rows:
        return '<p class="empty">표시할 금액 데이터가 없습니다.</p>'

    body = []
    for item in rows:
        row = _dict(item)
        values = _dict(row.get("values"))
        body.append(
            "<tr>"
            f"<td>{escape(str(row.get('period', '')))}</td>"
            f"<td>{_format_amount(values.get('revenue'))}</td>"
            f"<td>{_format_amount(values.get('operating_income'))}</td>"
            f"<td>{_format_amount(values.get('net_income'))}</td>"
            "</tr>"
        )

    return (
        "<table>"
        "<thead><tr><th>기간</th><th>매출</th><th>영업이익</th><th>순이익</th></tr></thead>"
        f"<tbody>{''.join(body)}</tbody>"
        "</table>"
    )


def render_filter_results(results: list[object]) -> str:
    if not results:
        return '<p class="empty">표시할 성장률 필터 결과가 없습니다.</p>'

    rows = []
    for item in results:
        result = _dict(item)
        rows.append(
            "<tr>"
            f"<td>{escape(METRIC_LABELS.get(str(result.get('metric', '')), str(result.get('metric', ''))))}</td>"
            f"<td>{escape(SERIES_LABELS.get(str(result.get('series_type', '')), str(result.get('series_type', ''))))}</td>"
            f"<td>{escape(str(result.get('recent_periods', '')))}</td>"
            f"<td>{_format_percent(result.get('minimum_growth_rate'))}</td>"
            f"<td>{'통과' if result.get('passed') is True else '미통과'}</td>"
            "</tr>"
        )

    return (
        "<table>"
        "<thead><tr><th>지표</th><th>기준</th><th>기간 수</th><th>최소 성장률</th><th>판정</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def render_growth_sections(sections: list[object]) -> str:
    if not sections:
        return '<p class="empty">표시할 성장률 데이터가 없습니다.</p>'

    rendered = []
    for item in sections:
        section = _dict(item)
        points = [_dict(point) for point in _list(section.get("points"))]
        rendered.append(
            f"""
            <section class="series">
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


def _pivot_period_values(values: Iterable[FinancialPeriodValue]) -> list[dict[str, object]]:
    rows: dict[str, dict[str, object]] = {}
    for value in values:
        period = value.period_label
        row = rows.setdefault(
            period,
            {
                "period": period,
                "sort": _period_sort_key(value),
                "values": {},
            },
        )
        _dict(row["values"])[value.metric] = str(value.amount)

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
    corp_code: str,
    rows: list[FinancialStatementRow],
) -> dict[str, str]:
    for row in rows:
        if row.corp_code == corp_code:
            return {
                "corp_code": row.corp_code,
                "corp_name": row.corp_name,
                "stock_code": row.stock_code,
            }
    return {"corp_code": corp_code, "corp_name": "", "stock_code": ""}


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


def _option(value: str, label: str, selected: str) -> str:
    is_selected = value == selected.upper()
    return (
        f'<option value="{escape(value)}" selected>{escape(label)}</option>'
        if is_selected
        else f'<option value="{escape(value)}">{escape(label)}</option>'
    )


def _dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _list(value: object) -> list[object]:
    return value if isinstance(value, list) else []
