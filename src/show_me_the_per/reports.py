from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from html import escape
from pathlib import Path
from typing import Iterable

from .growth import (
    ANNUAL_YOY,
    QUARTERLY_YOY,
    TRAILING_FOUR_QUARTER_YOY,
)
from .models import GrowthPoint
from .storage import (
    build_database_growth_ranking_payload,
    read_company_profile_from_database,
    read_growth_filter_results_from_database,
    read_growth_points_from_database,
)


METRIC_LABELS = {
    "revenue": "매출",
    "operating_income": "영업이익",
    "net_income": "순이익",
}

METRIC_LABELS["eps"] = "EPS"

SERIES_LABELS = {
    ANNUAL_YOY: "연간 YoY",
    QUARTERLY_YOY: "분기 YoY",
    TRAILING_FOUR_QUARTER_YOY: "최근 4분기 합산 YoY",
}

SERIES_ORDER = {
    ANNUAL_YOY: 0,
    QUARTERLY_YOY: 1,
    TRAILING_FOUR_QUARTER_YOY: 2,
}

METRIC_ORDER = {
    "revenue": 0,
    "operating_income": 1,
    "net_income": 2,
}


METRIC_ORDER["eps"] = 3

def build_company_growth_report_payload(
    database_path: Path,
    *,
    corp_code: str,
    recent_years: int = 10,
) -> dict[str, object]:
    all_points = read_growth_points_from_database(
        database_path,
        corp_code=corp_code,
    )
    points = _filter_recent_years(all_points, recent_years)
    filter_results = read_growth_filter_results_from_database(
        database_path,
        corp_code=corp_code,
    )
    company = read_company_profile_from_database(database_path, corp_code)

    metric_sections = []
    grouped = _group_points(points)
    for metric in sorted(grouped, key=lambda value: METRIC_ORDER.get(value, 999)):
        series_sections = []
        for series_type in sorted(
            grouped[metric],
            key=lambda value: SERIES_ORDER.get(value, 999),
        ):
            series_points = grouped[metric][series_type]
            series_sections.append(
                {
                    "series_type": series_type,
                    "series_label": SERIES_LABELS.get(series_type, series_type),
                    "points": [_point_json(point) for point in series_points],
                }
            )
        metric_sections.append(
            {
                "metric": metric,
                "metric_label": METRIC_LABELS.get(metric, metric),
                "series": series_sections,
            }
        )

    return {
        "summary": {
            "database": str(database_path),
            "corp_code": corp_code,
            "recent_years": recent_years,
            "growth_points": len(points),
            "filter_results": len(filter_results),
        },
        "company": company,
        "filter_results": sorted(
            filter_results,
            key=lambda item: (
                METRIC_ORDER.get(str(item["metric"]), 999),
                SERIES_ORDER.get(str(item["series_type"]), 999),
            ),
        ),
        "metrics": metric_sections,
    }


def write_company_growth_report_html(
    output_path: Path,
    payload: dict[str, object],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_company_growth_report_html(payload),
        encoding="utf-8",
    )


def build_growth_ranking_report_payload(
    database_path: Path,
    *,
    growth_metric: str | None = None,
    growth_series_type: str | None = None,
    include_failed_growth: bool = False,
    limit: int | None = None,
) -> dict[str, object]:
    payload = build_database_growth_ranking_payload(
        database_path,
        growth_metric=growth_metric,
        growth_series_type=growth_series_type,
        include_failed_growth=include_failed_growth,
        limit=limit,
    )
    rankings = [
        {
            **_dict(ranking),
            "metric_label": METRIC_LABELS.get(
                str(_dict(ranking).get("metric", "")),
                str(_dict(ranking).get("metric", "")),
            ),
            "series_label": SERIES_LABELS.get(
                str(_dict(ranking).get("series_type", "")),
                str(_dict(ranking).get("series_type", "")),
            ),
        }
        for ranking in _list(payload.get("growth_rankings"))
    ]

    return {
        **payload,
        "display": {
            "metric_label": (
                METRIC_LABELS.get(growth_metric, growth_metric)
                if growth_metric is not None
                else "전체 지표"
            ),
            "series_label": (
                SERIES_LABELS.get(growth_series_type, growth_series_type)
                if growth_series_type is not None
                else "전체 기준"
            ),
        },
        "growth_rankings": rankings,
    }


def write_growth_ranking_report_html(
    output_path: Path,
    payload: dict[str, object],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_growth_ranking_report_html(payload),
        encoding="utf-8",
    )


def render_company_growth_report_html(payload: dict[str, object]) -> str:
    company = _dict(payload.get("company"))
    summary = _dict(payload.get("summary"))
    metrics = _list(payload.get("metrics"))
    filter_results = _list(payload.get("filter_results"))
    title = _report_title(company)

    metric_sections = "\n".join(
        _render_metric_section(_dict(metric))
        for metric in metrics
    )
    if not metric_sections:
        metric_sections = "<p>표시할 성장률 데이터가 없습니다.</p>"

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)}</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #172026;
      --muted: #5d6972;
      --line: #d7dde2;
      --surface: #f5f7f9;
      --accent: #0b6b5c;
      --accent-2: #b3261e;
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
      border-bottom: 1px solid var(--line);
      background: var(--surface);
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
    h2 {{ font-size: 22px; margin-top: 26px; }}
    h3 {{ font-size: 18px; margin-top: 18px; }}
    p {{ margin: 6px 0 14px; color: var(--muted); }}
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
      margin-top: 12px;
    }}
    .pill {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 6px 10px;
      background: #ffffff;
      color: var(--muted);
      font-size: 13px;
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
    .positive {{ color: var(--accent); font-weight: 700; }}
    .negative {{ color: var(--accent-2); font-weight: 700; }}
    .empty {{ color: var(--muted); }}
    @media (max-width: 720px) {{
      main {{ padding: 16px; }}
      header {{ padding: 22px 16px 14px; }}
      h1 {{ font-size: 24px; }}
      table {{ display: block; overflow-x: auto; }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>{escape(title)}</h1>
    <p>DB에 저장된 성장률 데이터를 기준으로 생성한 정적 리포트입니다.</p>
    <div class="meta">
      <span class="pill">고유번호 {escape(str(company.get("corp_code", "")))}</span>
      <span class="pill">종목코드 {escape(str(company.get("stock_code", "")))}</span>
      <span class="pill">최근 {escape(str(summary.get("recent_years", "")))}년</span>
      <span class="pill">성장률 포인트 {escape(str(summary.get("growth_points", 0)))}개</span>
    </div>
  </header>
  <main>
    <h2>성장률 필터 결과</h2>
    {_render_filter_table(filter_results)}
    {metric_sections}
  </main>
</body>
</html>
"""


def render_growth_ranking_report_html(payload: dict[str, object]) -> str:
    summary = _dict(payload.get("summary"))
    filters = _dict(payload.get("filters"))
    display = _dict(payload.get("display"))
    rankings = [_dict(ranking) for ranking in _list(payload.get("growth_rankings"))]

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>성장률 랭킹 리포트</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #172026;
      --muted: #5d6972;
      --line: #d7dde2;
      --surface: #f5f7f9;
      --accent: #0b6b5c;
      --accent-2: #b3261e;
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
      border-bottom: 1px solid var(--line);
      background: var(--surface);
    }}
    main {{
      max-width: 1180px;
      margin: 0 auto;
      padding: 24px;
    }}
    h1, h2 {{
      margin: 0 0 10px;
      letter-spacing: 0;
    }}
    h1 {{ font-size: 28px; }}
    h2 {{ font-size: 22px; margin-top: 26px; }}
    p {{ margin: 6px 0 14px; color: var(--muted); }}
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
    th:nth-child(2), td:nth-child(2),
    th:nth-child(3), td:nth-child(3),
    th:nth-child(4), td:nth-child(4),
    th:nth-child(5), td:nth-child(5) {{
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
      margin-top: 12px;
    }}
    .pill {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 6px 10px;
      background: #ffffff;
      color: var(--muted);
      font-size: 13px;
    }}
    .ranking-chart {{
      width: 100%;
      min-height: 240px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
    }}
    .positive {{ color: var(--accent); font-weight: 700; }}
    .negative {{ color: var(--accent-2); font-weight: 700; }}
    .empty {{ color: var(--muted); }}
    @media (max-width: 720px) {{
      main {{ padding: 16px; }}
      header {{ padding: 22px 16px 14px; }}
      h1 {{ font-size: 24px; }}
      table {{ display: block; overflow-x: auto; }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>성장률 랭킹 리포트</h1>
    <p>DB에 저장된 성장률 필터 결과를 기준으로 최소 성장률이 높은 순서대로 정렬했습니다.</p>
    <div class="meta">
      <span class="pill">지표 {escape(str(display.get("metric_label", "")))}</span>
      <span class="pill">기준 {escape(str(display.get("series_label", "")))}</span>
      <span class="pill">필터 결과 {escape(str(summary.get("filter_results", 0)))}개</span>
      <span class="pill">랭킹 {escape(str(summary.get("growth_rankings", 0)))}개</span>
      <span class="pill">실패 포함 {'예' if filters.get("include_failed_growth") is True else '아니오'}</span>
    </div>
  </header>
  <main>
    <h2>상위 성장률</h2>
    {_render_ranking_bar_chart(rankings)}
    <h2>랭킹 목록</h2>
    {_render_ranking_table(rankings)}
  </main>
</body>
</html>
"""


def _render_filter_table(filter_results: list[object]) -> str:
    if not filter_results:
        return "<p>저장된 성장률 필터 결과가 없습니다.</p>"
    rows = []
    for item in filter_results:
        result = _dict(item)
        passed = result.get("passed") is True
        rows.append(
            "<tr>"
            f"<td>{escape(METRIC_LABELS.get(str(result.get('metric', '')), str(result.get('metric', ''))))}</td>"
            f"<td>{escape(SERIES_LABELS.get(str(result.get('series_type', '')), str(result.get('series_type', ''))))}</td>"
            f"<td>{escape(str(result.get('recent_periods', '')))}</td>"
            f"<td>{_format_percent(result.get('minimum_growth_rate'))}</td>"
            f"<td>{'통과' if passed else '미통과'}</td>"
            "</tr>"
        )
    return (
        "<table>"
        "<thead><tr><th>지표</th><th>기준</th><th>기간 수</th><th>최소 성장률</th><th>판정</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def _render_ranking_table(rankings: list[dict[str, object]]) -> str:
    if not rankings:
        return "<p>표시할 성장률 랭킹이 없습니다.</p>"

    rows = []
    for ranking in rankings:
        rows.append(
            "<tr>"
            f"<td>{escape(str(ranking.get('rank', '')))}</td>"
            f"<td>{escape(_ranking_company_name(ranking))}</td>"
            f"<td>{escape(str(ranking.get('stock_code', '')))}</td>"
            f"<td>{escape(str(ranking.get('corp_code', '')))}</td>"
            f"<td>{escape(str(ranking.get('metric_label', ranking.get('metric', ''))))}</td>"
            f"<td>{escape(str(ranking.get('series_label', ranking.get('series_type', ''))))}</td>"
            f"<td>{escape(str(ranking.get('recent_periods', '')))}</td>"
            f"<td>{_format_percent(ranking.get('minimum_growth_rate'))}</td>"
            f"<td>{'통과' if ranking.get('passed') is True else '미통과'}</td>"
            "</tr>"
        )

    return (
        "<table>"
        "<thead><tr><th>순위</th><th>회사</th><th>종목코드</th><th>고유번호</th>"
        "<th>지표</th><th>기준</th><th>기간 수</th><th>최소 성장률</th><th>판정</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def _render_ranking_bar_chart(rankings: list[dict[str, object]]) -> str:
    chart_rows = [
        (ranking, _to_decimal(ranking.get("minimum_growth_rate")))
        for ranking in rankings[:25]
    ]
    numeric_rows = [
        (ranking, value)
        for ranking, value in chart_rows
        if value is not None
    ]
    if not numeric_rows:
        return '<p class="empty">차트로 표시할 성장률 랭킹이 없습니다.</p>'

    width = 900
    row_height = 30
    top = 30
    left = 220
    right = 78
    bottom = 30
    height = top + bottom + row_height * len(numeric_rows)
    values = [value for _, value in numeric_rows]
    min_value = min(values + [Decimal("0")])
    max_value = max(values + [Decimal("0")])
    if min_value == max_value:
        min_value -= Decimal("1")
        max_value += Decimal("1")
    padding = (max_value - min_value) * Decimal("0.08")
    min_value -= padding
    max_value += padding

    def x_for(value: Decimal) -> Decimal:
        return Decimal(left) + (
            (value - min_value)
            / (max_value - min_value)
            * Decimal(width - left - right)
        )

    zero_x = x_for(Decimal("0"))
    rows = []
    for index, (ranking, value) in enumerate(numeric_rows):
        y = Decimal(top + index * row_height + 6)
        value_x = x_for(value)
        bar_x = min(zero_x, value_x)
        bar_width = abs(value_x - zero_x)
        label = escape(_truncate_label(_ranking_company_name(ranking), 22))
        color = "#0b6b5c" if value >= 0 else "#b3261e"
        rows.append(
            f'<text x="12" y="{_svg_number(y + Decimal("14"))}" font-size="13" fill="#172026">{label}</text>'
            f'<rect x="{_svg_number(bar_x)}" y="{_svg_number(y)}" width="{_svg_number(bar_width)}" height="18" fill="{color}" rx="4" />'
            f'<text x="{_svg_number(value_x + Decimal("6") if value >= 0 else value_x - Decimal("6"))}" '
            f'y="{_svg_number(y + Decimal("14"))}" font-size="12" fill="#5d6972" '
            f'text-anchor="{"start" if value >= 0 else "end"}">{escape(_format_percent(value))}</text>'
        )

    return f"""
      <svg class="ranking-chart" viewBox="0 0 {width} {height}" role="img" aria-label="성장률 랭킹 차트">
        <line x1="{_svg_number(zero_x)}" y1="{top}" x2="{_svg_number(zero_x)}" y2="{height - bottom}" stroke="#b7c0c8" stroke-dasharray="4 4" />
        {''.join(rows)}
      </svg>
    """


def _render_metric_section(metric: dict[str, object]) -> str:
    series_html = "\n".join(
        _render_series_section(_dict(series))
        for series in _list(metric.get("series"))
    )
    return f"""
    <section>
      <h2>{escape(str(metric.get("metric_label", metric.get("metric", ""))))}</h2>
      {series_html}
    </section>
    """


def _render_series_section(series: dict[str, object]) -> str:
    points = [_dict(point) for point in _list(series.get("points"))]
    return f"""
    <section class="series">
      <h3>{escape(str(series.get("series_label", series.get("series_type", ""))))}</h3>
      {_render_growth_chart(points)}
      {_render_points_table(points)}
    </section>
    """


def _render_points_table(points: list[dict[str, object]]) -> str:
    if not points:
        return "<p>표시할 데이터가 없습니다.</p>"
    rows = []
    for point in points:
        growth_rate = point.get("growth_rate")
        rows.append(
            "<tr>"
            f"<td>{escape(str(point.get('period_label', '')))}</td>"
            f"<td>{_format_amount(point.get('amount'))}</td>"
            f"<td>{_format_amount(point.get('base_amount'))}</td>"
            f"<td>{_format_percent(growth_rate)}</td>"
            "</tr>"
        )
    return (
        "<table>"
        "<thead><tr><th>기간</th><th>금액</th><th>비교 기준 금액</th><th>성장률</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def _render_growth_chart(points: list[dict[str, object]]) -> str:
    chart_points = [
        (index, _to_decimal(point.get("growth_rate")))
        for index, point in enumerate(points)
    ]
    numeric_points = [(index, value) for index, value in chart_points if value is not None]
    if len(numeric_points) < 2:
        return '<p class="empty">차트로 표시할 성장률 데이터가 부족합니다.</p>'

    width = 760
    height = 250
    left = 58
    right = 22
    top = 24
    bottom = 40
    values = [value for _, value in numeric_points]
    min_value = min(values + [Decimal("0")])
    max_value = max(values + [Decimal("0")])
    if min_value == max_value:
        min_value -= Decimal("1")
        max_value += Decimal("1")
    padding = (max_value - min_value) * Decimal("0.08")
    min_value -= padding
    max_value += padding

    def x_for(index: int) -> Decimal:
        if len(points) <= 1:
            return Decimal(left)
        return Decimal(left) + (
            Decimal(index)
            / Decimal(len(points) - 1)
            * Decimal(width - left - right)
        )

    def y_for(value: Decimal) -> Decimal:
        return Decimal(top) + (
            (max_value - value)
            / (max_value - min_value)
            * Decimal(height - top - bottom)
        )

    zero_y = y_for(Decimal("0"))
    segments = _polyline_segments(
        [
            None if value is None else (x_for(index), y_for(value))
            for index, value in chart_points
        ]
    )
    polylines = "\n".join(
        f'<polyline fill="none" stroke="#0b6b5c" stroke-width="3" points="{escape(_points_attr(segment))}" />'
        for segment in segments
        if len(segment) > 1
    )
    circles = "\n".join(
        f'<circle cx="{_svg_number(x_for(index))}" cy="{_svg_number(y_for(value))}" r="3.5" fill="#0b6b5c" />'
        for index, value in numeric_points
    )
    first_label = escape(str(points[0].get("period_label", "")))
    last_label = escape(str(points[-1].get("period_label", "")))

    return f"""
      <svg class="chart" viewBox="0 0 {width} {height}" role="img" aria-label="성장률 차트">
        <line x1="{left}" y1="{_svg_number(zero_y)}" x2="{width - right}" y2="{_svg_number(zero_y)}" stroke="#b7c0c8" stroke-dasharray="4 4" />
        <line x1="{left}" y1="{top}" x2="{left}" y2="{height - bottom}" stroke="#d7dde2" />
        <line x1="{left}" y1="{height - bottom}" x2="{width - right}" y2="{height - bottom}" stroke="#d7dde2" />
        {polylines}
        {circles}
        <text x="10" y="{_svg_number(y_for(max_value))}" font-size="12" fill="#5d6972">{escape(_format_percent(max_value))}</text>
        <text x="10" y="{_svg_number(y_for(min_value))}" font-size="12" fill="#5d6972">{escape(_format_percent(min_value))}</text>
        <text x="{left}" y="{height - 12}" font-size="12" fill="#5d6972">{first_label}</text>
        <text x="{width - right}" y="{height - 12}" font-size="12" fill="#5d6972" text-anchor="end">{last_label}</text>
      </svg>
    """


def _filter_recent_years(
    points: Iterable[GrowthPoint],
    recent_years: int,
) -> list[GrowthPoint]:
    copied_points = list(points)
    if not copied_points:
        return []
    latest_year = max(point.fiscal_year for point in copied_points)
    first_year = latest_year - recent_years + 1
    return [
        point
        for point in copied_points
        if point.fiscal_year >= first_year
    ]


def _group_points(
    points: Iterable[GrowthPoint],
) -> dict[str, dict[str, list[GrowthPoint]]]:
    grouped: dict[str, dict[str, list[GrowthPoint]]] = defaultdict(lambda: defaultdict(list))
    for point in points:
        grouped[point.metric][point.series_type].append(point)
    for by_series in grouped.values():
        for series_points in by_series.values():
            series_points.sort(key=lambda point: (point.fiscal_year, point.fiscal_quarter or 0))
    return grouped


def _point_json(point: GrowthPoint) -> dict[str, object]:
    return {
        "corp_code": point.corp_code,
        "metric": point.metric,
        "series_type": point.series_type,
        "period_label": point.period_label,
        "fiscal_year": point.fiscal_year,
        "fiscal_quarter": point.fiscal_quarter,
        "amount": None if point.amount is None else str(point.amount),
        "base_amount": None if point.base_amount is None else str(point.base_amount),
        "growth_rate": None if point.growth_rate is None else str(point.growth_rate),
    }


def _report_title(company: dict[str, object]) -> str:
    name = str(company.get("corp_name") or "").strip()
    stock_code = str(company.get("stock_code") or "").strip()
    corp_code = str(company.get("corp_code") or "").strip()
    identifier = stock_code or corp_code
    if name and identifier:
        return f"{name} ({identifier}) 성장률 리포트"
    if identifier:
        return f"{identifier} 성장률 리포트"
    return "성장률 리포트"


def _ranking_company_name(ranking: dict[str, object]) -> str:
    name = str(ranking.get("corp_name") or "").strip()
    stock_code = str(ranking.get("stock_code") or "").strip()
    corp_code = str(ranking.get("corp_code") or "").strip()
    if name:
        return name
    return stock_code or corp_code


def _truncate_label(value: str, max_length: int) -> str:
    if len(value) <= max_length:
        return value
    return value[: max_length - 1] + "…"


def _polyline_segments(
    points: list[tuple[Decimal, Decimal] | None],
) -> list[list[tuple[Decimal, Decimal]]]:
    segments: list[list[tuple[Decimal, Decimal]]] = []
    current: list[tuple[Decimal, Decimal]] = []
    for point in points:
        if point is None:
            if current:
                segments.append(current)
                current = []
            continue
        current.append(point)
    if current:
        segments.append(current)
    return segments


def _points_attr(points: list[tuple[Decimal, Decimal]]) -> str:
    return " ".join(
        f"{_svg_number(x)},{_svg_number(y)}"
        for x, y in points
    )


def _svg_number(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _format_percent(value: object) -> str:
    parsed = _to_decimal(value)
    if parsed is None:
        return "-"
    formatted = parsed.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{formatted}%"


def _format_amount(value: object) -> str:
    parsed = _to_decimal(value)
    if parsed is None:
        return "-"
    return f"{parsed.quantize(Decimal('1'), rounding=ROUND_HALF_UP):,}"


def _to_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def _dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _list(value: object) -> list[object]:
    return value if isinstance(value, list) else []
