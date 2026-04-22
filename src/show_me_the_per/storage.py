from __future__ import annotations

import json
import sqlite3
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable

from .financials import read_financial_statement_rows
from .growth import build_growth_metrics_payload, read_financial_period_values
from .krx import KrxStockPriceSnapshot
from .models import DartCompany, FinancialPeriodValue, FinancialStatementRow, GrowthPoint
from .pipeline import AnalysisArtifacts, CollectionError
from .rankings import (
    ValuationSnapshot,
    build_screening_rows,
    rank_growth_filter_results,
)


SCHEMA_VERSION = 2


SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS schema_version (
        version INTEGER PRIMARY KEY
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS financial_statement_rows (
        corp_code TEXT NOT NULL,
        corp_name TEXT NOT NULL,
        stock_code TEXT NOT NULL,
        business_year TEXT NOT NULL,
        report_code TEXT NOT NULL,
        fs_div TEXT NOT NULL,
        fs_name TEXT NOT NULL,
        statement_div TEXT NOT NULL,
        statement_name TEXT NOT NULL,
        account_id TEXT NOT NULL,
        account_name TEXT NOT NULL,
        current_term_name TEXT NOT NULL,
        current_amount TEXT,
        previous_term_name TEXT NOT NULL,
        previous_amount TEXT,
        before_previous_term_name TEXT NOT NULL,
        before_previous_amount TEXT,
        PRIMARY KEY (
            corp_code,
            business_year,
            report_code,
            fs_div,
            statement_div,
            statement_name,
            account_id,
            account_name,
            current_term_name
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS financial_period_values (
        corp_code TEXT NOT NULL,
        metric TEXT NOT NULL,
        period_type TEXT NOT NULL,
        fiscal_year INTEGER NOT NULL,
        fiscal_quarter INTEGER NOT NULL,
        amount TEXT NOT NULL,
        PRIMARY KEY (
            corp_code,
            metric,
            period_type,
            fiscal_year,
            fiscal_quarter
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS growth_points (
        corp_code TEXT NOT NULL,
        metric TEXT NOT NULL,
        series_type TEXT NOT NULL,
        fiscal_year INTEGER NOT NULL,
        fiscal_quarter INTEGER NOT NULL,
        amount TEXT NOT NULL,
        base_amount TEXT,
        growth_rate TEXT,
        PRIMARY KEY (
            corp_code,
            metric,
            series_type,
            fiscal_year,
            fiscal_quarter
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS growth_filter_results (
        corp_code TEXT NOT NULL,
        metric TEXT NOT NULL,
        series_type TEXT NOT NULL,
        recent_periods INTEGER NOT NULL,
        minimum_growth_rate TEXT,
        passed INTEGER NOT NULL,
        PRIMARY KEY (corp_code, metric, series_type)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS collection_errors (
        corp_codes TEXT NOT NULL,
        business_year TEXT NOT NULL,
        report_code TEXT NOT NULL,
        fs_div TEXT NOT NULL,
        error_type TEXT NOT NULL,
        message TEXT NOT NULL,
        PRIMARY KEY (
            corp_codes,
            business_year,
            report_code,
            fs_div,
            error_type,
            message
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS equity_price_snapshots (
        stock_code TEXT NOT NULL,
        base_date TEXT NOT NULL,
        market TEXT NOT NULL,
        item_name TEXT NOT NULL,
        close_price TEXT,
        listed_stock_count TEXT,
        market_cap TEXT,
        PRIMARY KEY (stock_code, base_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS valuation_snapshots (
        stock_code TEXT NOT NULL,
        corp_code TEXT NOT NULL,
        corp_name TEXT NOT NULL,
        base_date TEXT NOT NULL,
        market TEXT,
        close_price TEXT,
        market_cap TEXT,
        per TEXT,
        pbr TEXT,
        roe TEXT,
        eps TEXT,
        source TEXT NOT NULL,
        fetched_at TEXT NOT NULL,
        PRIMARY KEY (stock_code, base_date)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_financial_rows_corp_year
    ON financial_statement_rows (corp_code, business_year)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_period_values_corp_metric
    ON financial_period_values (corp_code, metric, period_type)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_growth_points_corp_metric
    ON growth_points (corp_code, metric, series_type)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_equity_price_snapshots_stock_code
    ON equity_price_snapshots (stock_code, base_date DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_valuation_snapshots_stock_code
    ON valuation_snapshots (stock_code, base_date DESC, fetched_at DESC)
    """,
)


def initialize_database(database_path: Path) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database_path) as connection:
        for statement in SCHEMA_STATEMENTS:
            connection.execute(statement)
        connection.execute("DELETE FROM schema_version")
        connection.execute(
            "INSERT INTO schema_version (version) VALUES (?)",
            (SCHEMA_VERSION,),
        )


def store_analysis_artifacts(
    database_path: Path,
    artifacts: AnalysisArtifacts,
) -> dict[str, int]:
    initialize_database(database_path)
    growth_points = _parse_growth_points_payload(artifacts.growth_metrics)
    filter_results = _parse_growth_filter_results_payload(artifacts.growth_metrics)

    with sqlite3.connect(database_path) as connection:
        store_financial_statement_rows(
            connection,
            artifacts.financial_statement_rows,
        )
        store_financial_period_values(
            connection,
            artifacts.financial_period_values,
        )
        store_growth_points(connection, growth_points)
        store_growth_filter_results(connection, filter_results)
        store_collection_errors(connection, artifacts.collection_errors)

    return summarize_database(database_path)


def store_analysis_directory(
    database_path: Path,
    input_dir: Path,
) -> dict[str, int]:
    artifacts = AnalysisArtifacts(
        financial_statement_rows=read_financial_statement_rows(
            input_dir / "financial-statements.json"
        ),
        financial_period_values=read_financial_period_values(
            input_dir / "financial-period-values.json"
        ),
        growth_metrics=json.loads(
            (input_dir / "growth-metrics.json").read_text(encoding="utf-8")
        ),
        coverage_report=json.loads(
            (input_dir / "coverage-report.json").read_text(encoding="utf-8")
        ),
        collection_errors=read_collection_errors(input_dir / "collection-errors.json"),
    )
    return store_analysis_artifacts(database_path, artifacts)


def store_financial_statement_rows(
    connection: sqlite3.Connection,
    rows: Iterable[FinancialStatementRow],
) -> None:
    connection.executemany(
        """
        INSERT OR REPLACE INTO financial_statement_rows (
            corp_code,
            corp_name,
            stock_code,
            business_year,
            report_code,
            fs_div,
            fs_name,
            statement_div,
            statement_name,
            account_id,
            account_name,
            current_term_name,
            current_amount,
            previous_term_name,
            previous_amount,
            before_previous_term_name,
            before_previous_amount
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row.corp_code,
                row.corp_name,
                row.stock_code,
                row.business_year,
                row.report_code,
                row.fs_div,
                row.fs_name,
                row.statement_div,
                row.statement_name,
                row.account_id,
                row.account_name,
                row.current_term_name,
                _decimal_to_text(row.current_amount),
                row.previous_term_name,
                _decimal_to_text(row.previous_amount),
                row.before_previous_term_name,
                _decimal_to_text(row.before_previous_amount),
            )
            for row in rows
        ],
    )


def store_financial_period_values(
    connection: sqlite3.Connection,
    values: Iterable[FinancialPeriodValue],
) -> None:
    connection.executemany(
        """
        INSERT OR REPLACE INTO financial_period_values (
            corp_code,
            metric,
            period_type,
            fiscal_year,
            fiscal_quarter,
            amount
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                value.corp_code,
                value.metric,
                value.period_type,
                value.fiscal_year,
                _quarter_to_storage(value.fiscal_quarter),
                str(value.amount),
            )
            for value in values
        ],
    )


def store_growth_points(
    connection: sqlite3.Connection,
    points: Iterable[GrowthPoint],
) -> None:
    connection.executemany(
        """
        INSERT OR REPLACE INTO growth_points (
            corp_code,
            metric,
            series_type,
            fiscal_year,
            fiscal_quarter,
            amount,
            base_amount,
            growth_rate
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                point.corp_code,
                point.metric,
                point.series_type,
                point.fiscal_year,
                _quarter_to_storage(point.fiscal_quarter),
                str(point.amount),
                _decimal_to_text(point.base_amount),
                _decimal_to_text(point.growth_rate),
            )
            for point in points
        ],
    )


def store_growth_filter_results(
    connection: sqlite3.Connection,
    results: Iterable[dict[str, object]],
) -> None:
    connection.executemany(
        """
        INSERT OR REPLACE INTO growth_filter_results (
            corp_code,
            metric,
            series_type,
            recent_periods,
            minimum_growth_rate,
            passed
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                str(result.get("corp_code", "")),
                str(result.get("metric", "")),
                str(result.get("series_type", "")),
                int(result.get("recent_periods", 0) or 0),
                _optional_text(result.get("minimum_growth_rate")),
                1 if result.get("passed") is True else 0,
            )
            for result in results
        ],
    )


def store_collection_errors(
    connection: sqlite3.Connection,
    errors: Iterable[CollectionError],
) -> None:
    connection.executemany(
        """
        INSERT OR REPLACE INTO collection_errors (
            corp_codes,
            business_year,
            report_code,
            fs_div,
            error_type,
            message
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                ",".join(error.corp_codes),
                error.business_year,
                error.report_code,
                error.fs_div or "",
                error.error_type,
                error.message,
            )
            for error in errors
        ],
    )


def store_equity_price_snapshot(
    database_path: Path,
    snapshot: KrxStockPriceSnapshot,
) -> None:
    initialize_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO equity_price_snapshots (
                stock_code,
                base_date,
                market,
                item_name,
                close_price,
                listed_stock_count,
                market_cap
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.stock_code,
                snapshot.base_date,
                snapshot.market,
                snapshot.item_name,
                _decimal_to_text(snapshot.close_price),
                _decimal_to_text(snapshot.listed_stock_count),
                _decimal_to_text(snapshot.market_cap),
            ),
        )


def store_valuation_snapshot(
    database_path: Path,
    snapshot: ValuationSnapshot,
) -> None:
    initialize_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO valuation_snapshots (
                stock_code,
                corp_code,
                corp_name,
                base_date,
                market,
                close_price,
                market_cap,
                per,
                pbr,
                roe,
                eps,
                source,
                fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.stock_code,
                snapshot.corp_code,
                snapshot.corp_name,
                snapshot.base_date,
                snapshot.market or "",
                _decimal_to_text(snapshot.close_price),
                _decimal_to_text(snapshot.market_cap),
                _decimal_to_text(snapshot.per),
                _decimal_to_text(snapshot.pbr),
                _decimal_to_text(snapshot.roe),
                _decimal_to_text(snapshot.eps),
                snapshot.source or "",
                snapshot.fetched_at or "",
            ),
        )


def read_financial_statement_rows_from_database(
    database_path: Path,
    *,
    corp_code: str | None = None,
    business_years: Iterable[str] | None = None,
) -> list[FinancialStatementRow]:
    clauses: list[str] = []
    params: list[object] = []
    if corp_code is not None:
        clauses.append("corp_code = ?")
        params.append(corp_code)
    if business_years is not None:
        years = [str(year) for year in business_years]
        if years:
            placeholders = ", ".join("?" for _ in years)
            clauses.append(f"business_year IN ({placeholders})")
            params.extend(years)

    query = """
        SELECT
            corp_code,
            corp_name,
            stock_code,
            business_year,
            report_code,
            fs_div,
            fs_name,
            statement_div,
            statement_name,
            account_id,
            account_name,
            current_term_name,
            current_amount,
            previous_term_name,
            previous_amount,
            before_previous_term_name,
            before_previous_amount
        FROM financial_statement_rows
    """
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += (
        " ORDER BY corp_code, business_year, report_code, fs_div, "
        "statement_div, statement_name, account_id, account_name, current_term_name"
    )

    with sqlite3.connect(database_path) as connection:
        rows = connection.execute(query, params).fetchall()

    return [
        FinancialStatementRow(
            corp_code=row[0],
            corp_name=row[1],
            stock_code=row[2],
            business_year=row[3],
            report_code=row[4],
            fs_div=row[5],
            fs_name=row[6],
            statement_div=row[7],
            statement_name=row[8],
            account_id=row[9],
            account_name=row[10],
            current_term_name=row[11],
            current_amount=_parse_decimal(row[12]),
            previous_term_name=row[13],
            previous_amount=_parse_decimal(row[14]),
            before_previous_term_name=row[15],
            before_previous_amount=_parse_decimal(row[16]),
        )
        for row in rows
    ]


def read_latest_equity_price_snapshot(
    database_path: Path,
    *,
    stock_code: str,
) -> KrxStockPriceSnapshot | None:
    initialize_database(database_path)
    with sqlite3.connect(database_path) as connection:
        row = connection.execute(
            """
            SELECT
                stock_code,
                base_date,
                market,
                item_name,
                close_price,
                listed_stock_count,
                market_cap
            FROM equity_price_snapshots
            WHERE stock_code = ?
            ORDER BY base_date DESC
            LIMIT 1
            """,
            (stock_code,),
        ).fetchone()

    if row is None:
        return None

    return KrxStockPriceSnapshot(
        stock_code=row[0],
        base_date=row[1],
        market=row[2],
        item_name=row[3],
        close_price=_parse_decimal(row[4]),
        listed_stock_count=_parse_decimal(row[5]),
        market_cap=_parse_decimal(row[6]),
    )


def read_latest_equity_price_snapshots(
    database_path: Path,
) -> dict[str, KrxStockPriceSnapshot]:
    initialize_database(database_path)
    with sqlite3.connect(database_path) as connection:
        rows = connection.execute(
            """
            SELECT
                stock_code,
                base_date,
                market,
                item_name,
                close_price,
                listed_stock_count,
                market_cap
            FROM equity_price_snapshots
            ORDER BY stock_code ASC, base_date DESC
            """
        ).fetchall()

    latest: dict[str, KrxStockPriceSnapshot] = {}
    for row in rows:
        stock_code = row[0]
        if stock_code in latest:
            continue
        latest[stock_code] = KrxStockPriceSnapshot(
            stock_code=stock_code,
            base_date=row[1],
            market=row[2],
            item_name=row[3],
            close_price=_parse_decimal(row[4]),
            listed_stock_count=_parse_decimal(row[5]),
            market_cap=_parse_decimal(row[6]),
        )
    return latest


def read_latest_valuation_snapshot(
    database_path: Path,
    *,
    stock_code: str,
) -> ValuationSnapshot | None:
    initialize_database(database_path)
    with sqlite3.connect(database_path) as connection:
        row = connection.execute(
            """
            SELECT
                stock_code,
                corp_code,
                corp_name,
                base_date,
                market,
                close_price,
                market_cap,
                per,
                pbr,
                roe,
                eps,
                source,
                fetched_at
            FROM valuation_snapshots
            WHERE stock_code = ?
            ORDER BY base_date DESC, fetched_at DESC
            LIMIT 1
            """,
            (stock_code,),
        ).fetchone()

    if row is None:
        return None
    return _row_to_valuation_snapshot(row)


def read_latest_valuation_snapshots(
    database_path: Path,
) -> list[ValuationSnapshot]:
    initialize_database(database_path)
    with sqlite3.connect(database_path) as connection:
        rows = connection.execute(
            """
            SELECT
                stock_code,
                corp_code,
                corp_name,
                base_date,
                market,
                close_price,
                market_cap,
                per,
                pbr,
                roe,
                eps,
                source,
                fetched_at
            FROM valuation_snapshots
            ORDER BY stock_code ASC, base_date DESC, fetched_at DESC
            """
        ).fetchall()

    latest: dict[str, ValuationSnapshot] = {}
    for row in rows:
        stock_code = row[0]
        if stock_code in latest:
            continue
        latest[stock_code] = _row_to_valuation_snapshot(row)
    return list(latest.values())


def read_financial_period_values_from_database(
    database_path: Path,
    *,
    corp_code: str | None = None,
    metric: str | None = None,
    period_type: str | None = None,
) -> list[FinancialPeriodValue]:
    clauses: list[str] = []
    params: list[object] = []
    if corp_code is not None:
        clauses.append("corp_code = ?")
        params.append(corp_code)
    if metric is not None:
        clauses.append("metric = ?")
        params.append(metric)
    if period_type is not None:
        clauses.append("period_type = ?")
        params.append(period_type)

    query = """
        SELECT
            corp_code,
            metric,
            period_type,
            fiscal_year,
            fiscal_quarter,
            amount
        FROM financial_period_values
    """
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY corp_code, metric, period_type, fiscal_year, fiscal_quarter"

    with sqlite3.connect(database_path) as connection:
        rows = connection.execute(query, params).fetchall()

    return [
        FinancialPeriodValue(
            corp_code=row[0],
            metric=row[1],
            period_type=row[2],
            fiscal_year=row[3],
            fiscal_quarter=_quarter_from_storage(row[4]),
            amount=Decimal(row[5]),
        )
        for row in rows
    ]


def read_growth_points_from_database(
    database_path: Path,
    *,
    corp_code: str | None = None,
    metric: str | None = None,
    series_type: str | None = None,
) -> list[GrowthPoint]:
    clauses: list[str] = []
    params: list[object] = []
    if corp_code is not None:
        clauses.append("corp_code = ?")
        params.append(corp_code)
    if metric is not None:
        clauses.append("metric = ?")
        params.append(metric)
    if series_type is not None:
        clauses.append("series_type = ?")
        params.append(series_type)

    query = """
        SELECT
            corp_code,
            metric,
            series_type,
            fiscal_year,
            fiscal_quarter,
            amount,
            base_amount,
            growth_rate
        FROM growth_points
    """
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY corp_code, metric, series_type, fiscal_year, fiscal_quarter"

    with sqlite3.connect(database_path) as connection:
        rows = connection.execute(query, params).fetchall()

    return [
        GrowthPoint(
            corp_code=row[0],
            metric=row[1],
            series_type=row[2],
            fiscal_year=row[3],
            fiscal_quarter=_quarter_from_storage(row[4]),
            amount=Decimal(row[5]),
            base_amount=_parse_decimal(row[6]),
            growth_rate=_parse_decimal(row[7]),
        )
        for row in rows
    ]


def read_growth_filter_results_from_database(
    database_path: Path,
    *,
    corp_code: str | None = None,
    metric: str | None = None,
    series_type: str | None = None,
    passed: bool | None = None,
) -> list[dict[str, object]]:
    clauses: list[str] = []
    params: list[object] = []
    if corp_code is not None:
        clauses.append("corp_code = ?")
        params.append(corp_code)
    if metric is not None:
        clauses.append("metric = ?")
        params.append(metric)
    if series_type is not None:
        clauses.append("series_type = ?")
        params.append(series_type)
    if passed is not None:
        clauses.append("passed = ?")
        params.append(1 if passed else 0)

    query = """
        SELECT
            corp_code,
            metric,
            series_type,
            recent_periods,
            minimum_growth_rate,
            passed
        FROM growth_filter_results
    """
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY corp_code, metric, series_type"

    with sqlite3.connect(database_path) as connection:
        rows = connection.execute(query, params).fetchall()

    return [
        {
            "corp_code": row[0],
            "metric": row[1],
            "series_type": row[2],
            "recent_periods": row[3],
            "minimum_growth_rate": row[4],
            "passed": bool(row[5]),
        }
        for row in rows
    ]


def build_database_growth_ranking_payload(
    database_path: Path,
    *,
    growth_metric: str | None = None,
    growth_series_type: str | None = None,
    include_failed_growth: bool = False,
    limit: int | None = None,
) -> dict[str, object]:
    filter_results = read_growth_filter_results_from_database(
        database_path,
        metric=growth_metric,
        series_type=growth_series_type,
        passed=None if include_failed_growth else True,
    )
    rankings = rank_growth_filter_results(
        filter_results,
        metric=growth_metric,
        series_type=growth_series_type,
        include_failed=include_failed_growth,
    )
    if limit is not None:
        rankings = rankings[:limit]

    company_index = _read_company_index(database_path)
    enriched_rankings = [
        {
            **ranking,
            **company_index.get(str(ranking["corp_code"]), {}),
        }
        for ranking in rankings
    ]

    return {
        "summary": {
            "database": str(database_path),
            "filter_results": len(filter_results),
            "growth_rankings": len(enriched_rankings),
        },
        "filters": {
            "growth_metric": growth_metric,
            "growth_series_type": growth_series_type,
            "include_failed_growth": include_failed_growth,
            "limit": limit,
        },
        "growth_rankings": enriched_rankings,
    }


def build_database_company_screening_payload(
    database_path: Path,
    *,
    start_year: int,
    end_year: int,
    fs_div: str | None = None,
    growth_metric: str | None = None,
    growth_series_type: str | None = None,
    include_failed_growth: bool = False,
    threshold_percent: Decimal = Decimal("20"),
    recent_annual_periods: int = 3,
    recent_quarterly_periods: int = 12,
    max_per: Decimal | None = None,
    max_pbr: Decimal | None = None,
    min_roe: Decimal | None = None,
    market: str | None = None,
    sort_by: str = "market_cap",
) -> dict[str, object]:
    calculation_start_year = max(0, start_year - 1)
    values = [
        value
        for value in read_financial_period_values_from_database(database_path)
        if calculation_start_year <= value.fiscal_year <= end_year
    ]
    growth_payload = build_growth_metrics_payload(
        values,
        threshold_percent=threshold_percent,
        recent_annual_periods=recent_annual_periods,
        recent_quarterly_periods=recent_quarterly_periods,
    )
    valuation_snapshots = read_latest_valuation_snapshots(database_path)
    company_index = _read_company_index(database_path)
    latest_equity_price_snapshots = read_latest_equity_price_snapshots(database_path)
    price_index = {
        profile.get("corp_code", corp_code): {
            "market": snapshot.market,
            "close_price": _decimal_to_text(snapshot.close_price),
            "market_cap": _decimal_to_text(snapshot.market_cap),
            "base_date": snapshot.base_date,
            "source": "krx_cache",
        }
        for corp_code, profile in company_index.items()
        for snapshot in [latest_equity_price_snapshots.get(profile.get("stock_code", ""))]
        if snapshot is not None
    }

    rows = build_screening_rows(
        growth_payload.get("filter", {}).get("results", []),  # type: ignore[arg-type]
        valuation_snapshots,
        company_index=company_index,
        price_index=price_index,
        growth_metric=growth_metric,
        growth_series_type=growth_series_type,
        include_failed_growth=include_failed_growth,
        max_per=max_per,
        max_pbr=max_pbr,
        min_roe=min_roe,
        market=market,
        sort_by=sort_by,
    )

    return {
        "summary": {
            "database": str(database_path),
            "values": len(values),
            "valuation_snapshots": len(valuation_snapshots),
            "screening_rows": len(rows),
            "start_year": start_year,
            "end_year": end_year,
        },
        "filters": {
            "fs_div": fs_div,
            "growth_metric": growth_metric,
            "growth_series_type": growth_series_type,
            "include_failed_growth": include_failed_growth,
            "threshold_percent": str(threshold_percent),
            "recent_annual_periods": recent_annual_periods,
            "recent_quarterly_periods": recent_quarterly_periods,
            "max_per": _decimal_to_text(max_per),
            "max_pbr": _decimal_to_text(max_pbr),
            "min_roe": _decimal_to_text(min_roe),
            "market": (market or "").strip().upper(),
            "sort_by": sort_by,
        },
        "screening_rows": rows,
    }


def read_company_profile_from_database(
    database_path: Path,
    corp_code: str,
) -> dict[str, str]:
    company_index = _read_company_index(database_path)
    return {
        "corp_code": corp_code,
        **company_index.get(corp_code, {"corp_name": "", "stock_code": ""}),
    }


def read_dart_companies_from_database(database_path: Path) -> list[DartCompany]:
    company_index = _read_company_index(database_path)
    return [
        DartCompany(
            corp_code=corp_code,
            corp_name=profile.get("corp_name", ""),
            stock_code=profile.get("stock_code", ""),
            modify_date="",
        )
        for corp_code, profile in sorted(
            company_index.items(),
            key=lambda item: (item[1].get("corp_name", ""), item[0]),
        )
    ]


def read_collection_errors(path: Path) -> list[CollectionError]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw_errors = payload.get("errors", []) if isinstance(payload, dict) else payload
    return [_parse_collection_error(item) for item in raw_errors]


def summarize_database(database_path: Path) -> dict[str, int]:
    initialize_database(database_path)
    with sqlite3.connect(database_path) as connection:
        return {
            "financial_statement_rows": _table_count(
                connection,
                "financial_statement_rows",
            ),
            "financial_period_values": _table_count(
                connection,
                "financial_period_values",
            ),
            "growth_points": _table_count(connection, "growth_points"),
            "growth_filter_results": _table_count(
                connection,
                "growth_filter_results",
            ),
            "collection_errors": _table_count(connection, "collection_errors"),
            "equity_price_snapshots": _table_count(
                connection,
                "equity_price_snapshots",
            ),
            "valuation_snapshots": _table_count(
                connection,
                "valuation_snapshots",
            ),
        }


def _read_company_index(database_path: Path) -> dict[str, dict[str, str]]:
    with sqlite3.connect(database_path) as connection:
        rows = connection.execute(
            """
            SELECT corp_code, corp_name, stock_code
            FROM financial_statement_rows
            ORDER BY corp_code, corp_name DESC, stock_code DESC
            """
        ).fetchall()

    companies: dict[str, dict[str, str]] = {}
    for corp_code, corp_name, stock_code in rows:
        companies.setdefault(
            corp_code,
            {
                "corp_name": corp_name,
                "stock_code": stock_code,
            },
        )
    return companies


def _row_to_valuation_snapshot(row: tuple[object, ...]) -> ValuationSnapshot:
    return ValuationSnapshot(
        stock_code=str(row[0]),
        corp_code=str(row[1]),
        corp_name=str(row[2]),
        base_date=str(row[3]),
        market=_optional_text(row[4]),
        close_price=_parse_decimal(row[5]),
        market_cap=_parse_decimal(row[6]),
        per=_parse_decimal(row[7]),
        pbr=_parse_decimal(row[8]),
        roe=_parse_decimal(row[9]),
        eps=_parse_decimal(row[10]),
        source=str(row[11]),
        fetched_at=str(row[12]),
    )


def _parse_growth_points_payload(payload: dict[str, object]) -> list[GrowthPoint]:
    raw_points = payload.get("growth_points", [])
    if not isinstance(raw_points, list):
        return []
    return [
        _parse_growth_point(point)
        for point in raw_points
        if isinstance(point, dict)
    ]


def _parse_growth_point(item: dict[str, object]) -> GrowthPoint:
    amount = _parse_decimal(item.get("amount"))
    if amount is None:
        raise ValueError("growth point amount is required")
    fiscal_quarter = item.get("fiscal_quarter")
    return GrowthPoint(
        corp_code=str(item.get("corp_code", "")),
        metric=str(item.get("metric", "")),
        series_type=str(item.get("series_type", "")),
        fiscal_year=int(item.get("fiscal_year", 0)),
        fiscal_quarter=None if fiscal_quarter is None else int(fiscal_quarter),
        amount=amount,
        base_amount=_parse_decimal(item.get("base_amount")),
        growth_rate=_parse_decimal(item.get("growth_rate")),
    )


def _parse_growth_filter_results_payload(
    payload: dict[str, object],
) -> list[dict[str, object]]:
    filter_payload = payload.get("filter")
    if not isinstance(filter_payload, dict):
        return []
    raw_results = filter_payload.get("results")
    if not isinstance(raw_results, list):
        return []
    return [result for result in raw_results if isinstance(result, dict)]


def _parse_collection_error(item: dict[str, object]) -> CollectionError:
    corp_codes = item.get("corp_codes", [])
    if isinstance(corp_codes, str):
        parsed_corp_codes = tuple(
            corp_code.strip()
            for corp_code in corp_codes.split(",")
            if corp_code.strip()
        )
    else:
        parsed_corp_codes = tuple(str(corp_code) for corp_code in corp_codes)

    return CollectionError(
        corp_codes=parsed_corp_codes,
        business_year=str(item.get("business_year", "")),
        report_code=str(item.get("report_code", "")),
        fs_div=_optional_text(item.get("fs_div")),
        error_type=str(item.get("error_type", "")),
        message=str(item.get("message", "")),
    )


def _table_count(connection: sqlite3.Connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0])


def _quarter_to_storage(value: int | None) -> int:
    return 0 if value is None else value


def _quarter_from_storage(value: int) -> int | None:
    return None if value == 0 else value


def _decimal_to_text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _parse_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    return str(value)
