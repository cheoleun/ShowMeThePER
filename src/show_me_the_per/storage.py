from __future__ import annotations

from datetime import date, datetime, timedelta
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
    normalize_growth_conditions,
    rank_growth_filter_results,
)


SCHEMA_VERSION = 3
COMPANY_MASTER_STALE_DAYS = 7
RESETTABLE_CACHE_TABLES = (
    "financial_statement_rows",
    "financial_period_values",
    "growth_points",
    "growth_filter_results",
    "collection_errors",
    "equity_price_snapshots",
    "valuation_snapshots",
    "company_master_entries",
    "refresh_jobs",
    "refresh_job_items",
)


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
    CREATE TABLE IF NOT EXISTS company_master_entries (
        corp_code TEXT PRIMARY KEY,
        corp_name TEXT NOT NULL,
        stock_code TEXT NOT NULL,
        market TEXT NOT NULL,
        item_name TEXT NOT NULL,
        modify_date TEXT NOT NULL,
        matched_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS refresh_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scope TEXT NOT NULL,
        fs_div TEXT NOT NULL,
        year_from INTEGER NOT NULL,
        year_to INTEGER NOT NULL,
        batch_size INTEGER NOT NULL,
        status TEXT NOT NULL,
        total_companies INTEGER NOT NULL,
        completed_companies INTEGER NOT NULL DEFAULT 0,
        failed_companies INTEGER NOT NULL DEFAULT 0,
        skipped_companies INTEGER NOT NULL DEFAULT 0,
        last_processed_corp_code TEXT NOT NULL DEFAULT '',
        last_processed_corp_name TEXT NOT NULL DEFAULT '',
        last_error TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS refresh_job_items (
        job_id INTEGER NOT NULL,
        corp_code TEXT NOT NULL,
        corp_name TEXT NOT NULL,
        stock_code TEXT NOT NULL,
        market TEXT NOT NULL,
        status TEXT NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        last_error TEXT NOT NULL DEFAULT '',
        updated_at TEXT NOT NULL,
        PRIMARY KEY (job_id, corp_code)
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
    """
    CREATE INDEX IF NOT EXISTS idx_company_master_entries_market
    ON company_master_entries (market, corp_name)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_refresh_jobs_updated_at
    ON refresh_jobs (updated_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_refresh_job_items_status
    ON refresh_job_items (job_id, status, updated_at ASC)
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
    with sqlite3.connect(database_path) as connection:
        _store_analysis_artifacts_in_connection(connection, artifacts)

    return summarize_database(database_path)


def replace_company_analysis_artifacts(
    database_path: Path,
    *,
    corp_code: str,
    artifacts: AnalysisArtifacts,
) -> dict[str, int]:
    initialize_database(database_path)
    with sqlite3.connect(database_path) as connection:
        _delete_company_analysis_rows(connection, corp_code=corp_code)
        _store_analysis_artifacts_in_connection(connection, artifacts)

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


def _store_analysis_artifacts_in_connection(
    connection: sqlite3.Connection,
    artifacts: AnalysisArtifacts,
) -> None:
    growth_points = _parse_growth_points_payload(artifacts.growth_metrics)
    filter_results = _parse_growth_filter_results_payload(artifacts.growth_metrics)
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


def _delete_company_analysis_rows(
    connection: sqlite3.Connection,
    *,
    corp_code: str,
) -> None:
    for table_name in (
        "financial_statement_rows",
        "financial_period_values",
        "growth_points",
        "growth_filter_results",
    ):
        connection.execute(
            f"DELETE FROM {table_name} WHERE corp_code = ?",
            (corp_code,),
        )
    connection.execute(
        """
        DELETE FROM collection_errors
        WHERE corp_codes = ?
           OR corp_codes LIKE ?
           OR corp_codes LIKE ?
           OR corp_codes LIKE ?
        """,
        (
            corp_code,
            f"{corp_code},%",
            f"%,{corp_code}",
            f"%,{corp_code},%",
        ),
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


def store_company_master_entries(
    database_path: Path,
    entries: Iterable[dict[str, object]],
) -> dict[str, object]:
    initialize_database(database_path)
    matched_at = _utc_now_text()
    normalized_entries = [
        {
            "corp_code": str(entry.get("corp_code", "")).strip(),
            "corp_name": str(entry.get("corp_name", "")).strip(),
            "stock_code": str(entry.get("stock_code", "")).strip(),
            "market": str(entry.get("market", "")).strip().upper(),
            "item_name": str(entry.get("item_name", "")).strip(),
            "modify_date": str(entry.get("modify_date", "")).strip(),
            "matched_at": str(entry.get("matched_at", "")).strip() or matched_at,
        }
        for entry in entries
        if str(entry.get("corp_code", "")).strip()
    ]

    with sqlite3.connect(database_path) as connection:
        connection.executemany(
            """
            INSERT OR REPLACE INTO company_master_entries (
                corp_code,
                corp_name,
                stock_code,
                market,
                item_name,
                modify_date,
                matched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    entry["corp_code"],
                    entry["corp_name"],
                    entry["stock_code"],
                    entry["market"],
                    entry["item_name"],
                    entry["modify_date"],
                    entry["matched_at"],
                )
                for entry in normalized_entries
            ],
        )

    return read_company_master_status(database_path)


def read_company_master_entries(
    database_path: Path,
    *,
    market: str | None = None,
) -> list[dict[str, str]]:
    initialize_database(database_path)
    clauses: list[str] = []
    params: list[object] = []
    normalized_market = (market or "").strip().upper()
    if normalized_market:
        clauses.append("market = ?")
        params.append(normalized_market)

    query = """
        SELECT
            corp_code,
            corp_name,
            stock_code,
            market,
            item_name,
            modify_date,
            matched_at
        FROM company_master_entries
    """
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY corp_name ASC, corp_code ASC"

    with sqlite3.connect(database_path) as connection:
        rows = connection.execute(query, params).fetchall()

    return [
        {
            "corp_code": row[0],
            "corp_name": row[1],
            "stock_code": row[2],
            "market": row[3],
            "item_name": row[4],
            "modify_date": row[5],
            "matched_at": row[6],
        }
        for row in rows
    ]


def read_company_master_status(
    database_path: Path,
    *,
    today: date | None = None,
) -> dict[str, object]:
    initialize_database(database_path)
    with sqlite3.connect(database_path) as connection:
        row = connection.execute(
            """
            SELECT COUNT(*), MAX(matched_at)
            FROM company_master_entries
            """
        ).fetchone()

    count = int(row[0] or 0)
    last_synced_at = str(row[1] or "")
    stale = True
    if count > 0 and last_synced_at:
        stale = _is_timestamp_stale(
            last_synced_at,
            days=COMPANY_MASTER_STALE_DAYS,
            today=today,
        )
    return {
        "count": count,
        "last_synced_at": last_synced_at,
        "is_stale": stale if count > 0 else True,
    }


def create_refresh_job(
    database_path: Path,
    *,
    scope: str,
    fs_div: str,
    year_from: int,
    year_to: int,
    batch_size: int,
    companies: Iterable[dict[str, object]],
    status: str = "running",
) -> dict[str, object]:
    initialize_database(database_path)
    normalized_companies = [
        {
            "corp_code": str(company.get("corp_code", "")).strip(),
            "corp_name": str(company.get("corp_name", "")).strip(),
            "stock_code": str(company.get("stock_code", "")).strip(),
            "market": str(company.get("market", "")).strip().upper(),
        }
        for company in companies
        if str(company.get("corp_code", "")).strip()
    ]
    if not normalized_companies:
        raise ValueError("no companies available for the refresh job")

    timestamp = _utc_now_text()
    with sqlite3.connect(database_path) as connection:
        cursor = connection.execute(
            """
            INSERT INTO refresh_jobs (
                scope,
                fs_div,
                year_from,
                year_to,
                batch_size,
                status,
                total_companies,
                completed_companies,
                failed_companies,
                skipped_companies,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?)
            """,
            (
                scope,
                fs_div,
                year_from,
                year_to,
                batch_size,
                status,
                len(normalized_companies),
                timestamp,
                timestamp,
            ),
        )
        job_id = int(cursor.lastrowid)
        connection.executemany(
            """
            INSERT INTO refresh_job_items (
                job_id,
                corp_code,
                corp_name,
                stock_code,
                market,
                status,
                attempt_count,
                last_error,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, 'pending', 0, '', ?)
            """,
            [
                (
                    job_id,
                    company["corp_code"],
                    company["corp_name"],
                    company["stock_code"],
                    company["market"],
                    timestamp,
                )
                for company in normalized_companies
            ],
        )

    return read_refresh_job(database_path, job_id=job_id) or {}


def read_refresh_job(
    database_path: Path,
    *,
    job_id: int,
) -> dict[str, object] | None:
    initialize_database(database_path)
    with sqlite3.connect(database_path) as connection:
        row = connection.execute(
            """
            SELECT
                id,
                scope,
                fs_div,
                year_from,
                year_to,
                batch_size,
                status,
                total_companies,
                completed_companies,
                failed_companies,
                skipped_companies,
                last_processed_corp_code,
                last_processed_corp_name,
                last_error,
                created_at,
                updated_at
            FROM refresh_jobs
            WHERE id = ?
            """,
            (job_id,),
        ).fetchone()
        if row is None:
            return None

        item_counts = connection.execute(
            """
            SELECT
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END)
            FROM refresh_job_items
            WHERE job_id = ?
            """,
            (job_id,),
        ).fetchone()
        recent_item = connection.execute(
            """
            SELECT corp_name, corp_code, last_error, status
            FROM refresh_job_items
            WHERE job_id = ?
            ORDER BY updated_at DESC, corp_code ASC
            LIMIT 1
            """,
            (job_id,),
        ).fetchone()

    pending_count = int(item_counts[0] or 0)
    success_count = int(item_counts[1] or 0)
    failed_count = int(item_counts[2] or 0)
    skipped_count = int(item_counts[3] or 0)
    total_count = int(row[7] or 0)
    remaining_count = max(
        total_count - success_count - failed_count - skipped_count,
        0,
    )

    return {
        "id": int(row[0]),
        "scope": row[1],
        "fs_div": row[2],
        "year_from": int(row[3]),
        "year_to": int(row[4]),
        "batch_size": int(row[5]),
        "status": row[6],
        "total_companies": total_count,
        "completed_companies": success_count,
        "failed_companies": failed_count,
        "skipped_companies": skipped_count,
        "pending_companies": pending_count,
        "remaining_companies": remaining_count,
        "last_processed_corp_code": row[11],
        "last_processed_corp_name": row[12],
        "last_error": row[13],
        "created_at": row[14],
        "updated_at": row[15],
        "recent_item": (
            None
            if recent_item is None
            else {
                "corp_name": recent_item[0],
                "corp_code": recent_item[1],
                "last_error": recent_item[2],
                "status": recent_item[3],
            }
        ),
    }


def read_latest_refresh_job(database_path: Path) -> dict[str, object] | None:
    initialize_database(database_path)
    with sqlite3.connect(database_path) as connection:
        row = connection.execute(
            """
            SELECT id
            FROM refresh_jobs
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        return None
    return read_refresh_job(database_path, job_id=int(row[0]))


def read_refresh_job_items(
    database_path: Path,
    *,
    job_id: int,
    statuses: Iterable[str] | None = None,
    limit: int | None = None,
) -> list[dict[str, object]]:
    initialize_database(database_path)
    clauses = ["job_id = ?"]
    params: list[object] = [job_id]
    status_values = [str(status).strip() for status in (statuses or []) if str(status).strip()]
    if status_values:
        placeholders = ", ".join("?" for _ in status_values)
        clauses.append(f"status IN ({placeholders})")
        params.extend(status_values)

    query = """
        SELECT
            job_id,
            corp_code,
            corp_name,
            stock_code,
            market,
            status,
            attempt_count,
            last_error,
            updated_at
        FROM refresh_job_items
        WHERE
    """ + " AND ".join(clauses) + " ORDER BY updated_at ASC, corp_code ASC"
    if limit is not None:
        query += f" LIMIT {int(limit)}"

    with sqlite3.connect(database_path) as connection:
        rows = connection.execute(query, params).fetchall()

    return [
        {
            "job_id": int(row[0]),
            "corp_code": row[1],
            "corp_name": row[2],
            "stock_code": row[3],
            "market": row[4],
            "status": row[5],
            "attempt_count": int(row[6] or 0),
            "last_error": row[7],
            "updated_at": row[8],
        }
        for row in rows
    ]


def update_refresh_job_status(
    database_path: Path,
    *,
    job_id: int,
    status: str,
    last_error: str | None = None,
) -> dict[str, object] | None:
    initialize_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            UPDATE refresh_jobs
            SET status = ?,
                last_error = CASE
                    WHEN ? IS NULL THEN last_error
                    ELSE ?
                END,
                updated_at = ?
            WHERE id = ?
            """,
            (status, last_error, last_error or "", _utc_now_text(), job_id),
        )
    return read_refresh_job(database_path, job_id=job_id)


def record_refresh_job_item_result(
    database_path: Path,
    *,
    job_id: int,
    corp_code: str,
    corp_name: str,
    status: str,
    last_error: str = "",
) -> dict[str, object] | None:
    initialize_database(database_path)
    timestamp = _utc_now_text()
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            UPDATE refresh_job_items
            SET status = ?,
                attempt_count = attempt_count + 1,
                last_error = ?,
                updated_at = ?
            WHERE job_id = ? AND corp_code = ?
            """,
            (status, last_error, timestamp, job_id, corp_code),
        )
        counts = connection.execute(
            """
            SELECT
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END)
            FROM refresh_job_items
            WHERE job_id = ?
            """,
            (job_id,),
        ).fetchone()

        success_count = int(counts[0] or 0)
        failed_count = int(counts[1] or 0)
        skipped_count = int(counts[2] or 0)
        pending_count = int(counts[3] or 0)

        job_status = "running"
        if pending_count == 0:
            job_status = "failed" if failed_count > 0 else "completed"

        connection.execute(
            """
            UPDATE refresh_jobs
            SET status = ?,
                completed_companies = ?,
                failed_companies = ?,
                skipped_companies = ?,
                last_processed_corp_code = ?,
                last_processed_corp_name = ?,
                last_error = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                job_status,
                success_count,
                failed_count,
                skipped_count,
                corp_code,
                corp_name,
                last_error,
                timestamp,
                job_id,
            ),
        )

    return read_refresh_job(database_path, job_id=job_id)


def retry_failed_refresh_job_items(
    database_path: Path,
    *,
    job_id: int,
) -> dict[str, object] | None:
    initialize_database(database_path)
    timestamp = _utc_now_text()
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            UPDATE refresh_job_items
            SET status = 'pending',
                last_error = '',
                updated_at = ?
            WHERE job_id = ? AND status = 'failed'
            """,
            (timestamp, job_id),
        )
        connection.execute(
            """
            UPDATE refresh_jobs
            SET status = 'running',
                last_error = '',
                updated_at = ?
            WHERE id = ?
            """,
            (timestamp, job_id),
        )
    return read_refresh_job(database_path, job_id=job_id)


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
    growth_conditions: Iterable[dict[str, object] | str] | None = None,
    growth_metric: str | None = None,
    growth_series_type: str | None = None,
    include_failed_growth: bool = False,
    limit: int | None = None,
) -> dict[str, object]:
    initialize_database(database_path)
    normalized_conditions = normalize_growth_conditions(
        growth_conditions,
        growth_metric=growth_metric,
        growth_series_type=growth_series_type,
    )
    primary_condition = normalized_conditions[0]
    filter_results = read_growth_filter_results_from_database(
        database_path,
        metric=primary_condition["metric"],
        series_type=primary_condition["series_type"],
        passed=None if include_failed_growth else True,
    )
    rankings = rank_growth_filter_results(
        filter_results,
        metric=primary_condition["metric"],
        series_type=primary_condition["series_type"],
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
            "growth_conditions": normalized_conditions,
            "growth_metric": primary_condition["metric"],
            "growth_series_type": primary_condition["series_type"],
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
    growth_conditions: Iterable[dict[str, object] | str] | None = None,
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
    initialize_database(database_path)
    calculation_start_year = max(0, start_year - 1)
    values = [
        value
        for value in read_financial_period_values_from_database(database_path)
        if calculation_start_year <= value.fiscal_year <= end_year
    ]
    normalized_conditions = normalize_growth_conditions(
        growth_conditions,
        growth_metric=growth_metric,
        growth_series_type=growth_series_type,
    )
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
        growth_conditions=normalized_conditions,
        include_failed_growth=include_failed_growth,
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
            "growth_conditions": normalized_conditions,
            "growth_metric": normalized_conditions[0]["metric"],
            "growth_series_type": normalized_conditions[0]["series_type"],
            "include_failed_growth": include_failed_growth,
            "threshold_percent": str(threshold_percent),
            "recent_annual_periods": recent_annual_periods,
            "recent_quarterly_periods": recent_quarterly_periods,
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
    company_master_entries = read_company_master_entries(database_path)
    if company_master_entries:
        return [
            DartCompany(
                corp_code=str(entry.get("corp_code", "")),
                corp_name=str(entry.get("corp_name", "")),
                stock_code=str(entry.get("stock_code", "")),
                modify_date=str(entry.get("modify_date", "")),
            )
            for entry in company_master_entries
        ]

    company_index = _read_company_index(database_path)
    return [
        DartCompany(
            corp_code=corp_code,
            corp_name=profile.get("corp_name", ""),
            stock_code=profile.get("stock_code", ""),
            modify_date=profile.get("modify_date", ""),
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
            "company_master_entries": _table_count(
                connection,
                "company_master_entries",
            ),
            "refresh_jobs": _table_count(connection, "refresh_jobs"),
            "refresh_job_items": _table_count(connection, "refresh_job_items"),
        }


def reset_database_cache(database_path: Path) -> dict[str, object]:
    if not database_path.exists():
        return {
            "path": str(database_path),
            "status": "skipped",
            "before": {},
            "after": {},
            "cleared_rows": 0,
        }

    initialize_database(database_path)
    before = summarize_database(database_path)

    with sqlite3.connect(database_path) as connection:
        for table_name in RESETTABLE_CACHE_TABLES:
            connection.execute(f"DELETE FROM {table_name}")

    after = summarize_database(database_path)
    cleared_rows = sum(before.get(table_name, 0) for table_name in RESETTABLE_CACHE_TABLES)
    return {
        "path": str(database_path),
        "status": "cleared",
        "before": before,
        "after": after,
        "cleared_rows": cleared_rows,
    }


def _read_company_index(database_path: Path) -> dict[str, dict[str, str]]:
    with sqlite3.connect(database_path) as connection:
        master_rows = connection.execute(
            """
            SELECT corp_code, corp_name, stock_code, market, item_name, modify_date
            FROM company_master_entries
            ORDER BY corp_code ASC
            """
        ).fetchall()
        rows = connection.execute(
            """
            SELECT corp_code, corp_name, stock_code
            FROM financial_statement_rows
            ORDER BY corp_code, corp_name DESC, stock_code DESC
            """
        ).fetchall()

    companies: dict[str, dict[str, str]] = {}
    for corp_code, corp_name, stock_code, market, item_name, modify_date in master_rows:
        companies.setdefault(
            corp_code,
            {
                "corp_name": corp_name,
                "stock_code": stock_code,
                "market": market,
                "item_name": item_name,
                "modify_date": modify_date,
            },
        )
    for corp_code, corp_name, stock_code in rows:
        profile = companies.setdefault(corp_code, {})
        if not profile.get("corp_name"):
            profile["corp_name"] = corp_name
        if not profile.get("stock_code"):
            profile["stock_code"] = stock_code
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


def _utc_now_text() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _is_timestamp_stale(
    value: str,
    *,
    days: int,
    today: date | None = None,
) -> bool:
    text = str(value or "").strip()
    if not text:
        return True
    normalized = text[:-1] if text.endswith("Z") else text
    try:
        timestamp = datetime.fromisoformat(normalized)
    except ValueError:
        return True
    current = today or date.today()
    return timestamp.date() < (current - timedelta(days=days))
