from __future__ import annotations

import argparse
from decimal import Decimal
import json
import os
from pathlib import Path
import sys

from .company_master import write_company_master_outputs
from .financials import (
    build_period_values_from_rows,
    read_financial_statement_rows,
    write_financial_period_values,
    write_financial_statement_rows,
)
from .growth import read_financial_period_values, write_growth_metrics_payload
from .krx import KrxClient
from .matching import match_listings_to_dart
from .opendart import OpenDartClient
from .pipeline import (
    DEFAULT_REPORT_CODES,
    build_analysis_artifacts,
    collect_financial_statement_run,
    parse_business_years,
    resolve_corp_codes,
    write_analysis_outputs,
)
from .rankings import (
    read_growth_metrics_payload,
    read_valuation_snapshots,
    write_ranking_payload,
)
from .reports import (
    build_company_growth_report_payload,
    write_company_growth_report_html,
)
from .storage import (
    build_database_growth_ranking_payload,
    store_analysis_artifacts,
    store_analysis_directory,
    summarize_database,
)


COMMANDS = {
    "company-master",
    "financial-statements",
    "financial-period-values",
    "growth-metrics",
    "rank-companies",
    "collect-analysis",
    "analysis-to-db",
    "database-summary",
    "rank-growth-from-db",
    "company-growth-report",
}


def main(argv: list[str] | None = None) -> None:
    args_list = list(sys.argv[1:] if argv is None else argv)
    if args_list and args_list[0] not in COMMANDS and args_list[0] not in {
        "-h",
        "--help",
    }:
        args_list.insert(0, "company-master")

    parser = argparse.ArgumentParser(
        description="Collect and match Korean listed company identifiers."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    company_parser = subparsers.add_parser(
        "company-master",
        help="Collect KRX listings and OpenDART corp codes, then build a company master.",
    )
    company_parser.add_argument(
        "--krx-service-key",
        default=os.getenv("KRX_SERVICE_KEY"),
        help="Public Data Portal service key. Defaults to KRX_SERVICE_KEY.",
    )
    company_parser.add_argument(
        "--opendart-api-key",
        default=os.getenv("OPENDART_API_KEY"),
        help="OpenDART API key. Defaults to OPENDART_API_KEY.",
    )
    company_parser.add_argument(
        "--base-date",
        help="Optional KRX base date in YYYYMMDD format.",
    )
    company_parser.add_argument(
        "--max-krx-pages",
        type=int,
        help="Optional max KRX pages for smoke runs.",
    )
    company_parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to write matched company master JSON.",
    )
    company_parser.add_argument(
        "--matched-csv",
        type=Path,
        help="Optional path to write matched companies as CSV.",
    )
    company_parser.add_argument(
        "--unmatched-csv",
        type=Path,
        help="Optional path to write unmatched KRX listings as CSV.",
    )
    company_parser.add_argument(
        "--ambiguous-json",
        type=Path,
        help="Optional path to write ambiguous match candidates as JSON.",
    )
    company_parser.add_argument(
        "--report",
        type=Path,
        help="Optional path to write a Markdown match report.",
    )

    financial_parser = subparsers.add_parser(
        "financial-statements",
        help="Collect OpenDART multi-company major financial accounts.",
    )
    financial_parser.add_argument(
        "--opendart-api-key",
        default=os.getenv("OPENDART_API_KEY"),
        help="OpenDART API key. Defaults to OPENDART_API_KEY.",
    )
    financial_parser.add_argument(
        "--corp-code",
        action="append",
        default=[],
        help="OpenDART corp code. May be repeated or comma-separated.",
    )
    financial_parser.add_argument(
        "--business-year",
        required=True,
        help="Business year, for example 2025.",
    )
    financial_parser.add_argument(
        "--report-code",
        required=True,
        help="OpenDART report code, for example 11011.",
    )
    financial_parser.add_argument(
        "--fs-div",
        choices=["CFS", "OFS"],
        help="Optional financial statement division.",
    )
    financial_parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to write financial statement rows JSON.",
    )

    period_values_parser = subparsers.add_parser(
        "financial-period-values",
        help="Normalize collected financial statement rows into growth input values.",
    )
    period_values_parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to financial statement rows JSON.",
    )
    period_values_parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to write normalized financial period values JSON.",
    )

    growth_parser = subparsers.add_parser(
        "growth-metrics",
        help="Calculate YoY growth metrics from normalized financial period values.",
    )
    growth_parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to financial period values JSON.",
    )
    growth_parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to write growth metrics JSON.",
    )
    growth_parser.add_argument(
        "--threshold-percent",
        type=Decimal,
        default=Decimal("20"),
        help="Minimum growth threshold for filter results. Defaults to 20.",
    )
    growth_parser.add_argument(
        "--recent-annual-periods",
        type=int,
        default=3,
        help="Annual periods that must all pass the threshold. Defaults to 3.",
    )
    growth_parser.add_argument(
        "--recent-quarterly-periods",
        type=int,
        default=12,
        help="Quarterly periods that must all pass the threshold. Defaults to 12.",
    )

    ranking_parser = subparsers.add_parser(
        "rank-companies",
        help="Build growth and valuation rankings from analysis JSON files.",
    )
    ranking_parser.add_argument(
        "--growth-input",
        type=Path,
        required=True,
        help="Path to growth metrics JSON.",
    )
    ranking_parser.add_argument(
        "--valuation-input",
        type=Path,
        help="Optional path to PER/PBR/ROE valuation JSON.",
    )
    ranking_parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to write ranking JSON.",
    )
    ranking_parser.add_argument(
        "--growth-metric",
        help="Optional growth metric to rank, for example revenue.",
    )
    ranking_parser.add_argument(
        "--growth-series-type",
        help="Optional growth series to rank, for example annual_yoy.",
    )
    ranking_parser.add_argument(
        "--include-failed-growth",
        action="store_true",
        help="Include failed growth filters in growth rankings.",
    )
    ranking_parser.add_argument(
        "--max-per",
        type=Decimal,
        help="Optional PER upper bound for valuation filtering.",
    )
    ranking_parser.add_argument(
        "--max-pbr",
        type=Decimal,
        help="Optional PBR upper bound for valuation filtering.",
    )
    ranking_parser.add_argument(
        "--min-roe",
        type=Decimal,
        help="Optional ROE lower bound for valuation filtering.",
    )
    ranking_parser.add_argument(
        "--rank-valuation-by",
        choices=["per", "pbr", "roe"],
        default="roe",
        help="Valuation ranking metric. Defaults to roe.",
    )

    collect_parser = subparsers.add_parser(
        "collect-analysis",
        help="Collect financial statements and write analysis artifacts together.",
    )
    collect_parser.add_argument(
        "--opendart-api-key",
        default=os.getenv("OPENDART_API_KEY"),
        help="OpenDART API key. Defaults to OPENDART_API_KEY.",
    )
    collect_parser.add_argument(
        "--corp-code",
        action="append",
        default=[],
        help="OpenDART corp code. May be repeated or comma-separated.",
    )
    collect_parser.add_argument(
        "--corp-code-file",
        type=Path,
        help="Optional JSON, CSV, or text file containing corp codes.",
    )
    collect_parser.add_argument(
        "--business-year",
        action="append",
        default=[],
        help="Business year. May be repeated or comma-separated.",
    )
    collect_parser.add_argument(
        "--year-from",
        help="First business year to collect, for example 2015.",
    )
    collect_parser.add_argument(
        "--year-to",
        help="Last business year to collect, for example 2025.",
    )
    collect_parser.add_argument(
        "--report-code",
        action="append",
        default=[],
        help="OpenDART report code. Defaults to quarterly and annual reports.",
    )
    collect_parser.add_argument(
        "--fs-div",
        choices=["CFS", "OFS"],
        help="Optional financial statement division.",
    )
    collect_parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="OpenDART corp code batch size. Defaults to 100.",
    )
    collect_parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop immediately when an OpenDART collection request fails.",
    )
    collect_parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to write analysis JSON artifacts.",
    )
    collect_parser.add_argument(
        "--database",
        type=Path,
        help="Optional SQLite database path to store collected analysis artifacts.",
    )
    collect_parser.add_argument(
        "--threshold-percent",
        type=Decimal,
        default=Decimal("20"),
        help="Minimum growth threshold for filter results. Defaults to 20.",
    )
    collect_parser.add_argument(
        "--recent-annual-periods",
        type=int,
        default=3,
        help="Annual periods that must all pass the threshold. Defaults to 3.",
    )
    collect_parser.add_argument(
        "--recent-quarterly-periods",
        type=int,
        default=12,
        help="Quarterly periods that must all pass the threshold. Defaults to 12.",
    )

    db_parser = subparsers.add_parser(
        "analysis-to-db",
        help="Store an existing analysis output directory into a SQLite database.",
    )
    db_parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Directory containing collect-analysis JSON artifacts.",
    )
    db_parser.add_argument(
        "--database",
        type=Path,
        required=True,
        help="SQLite database path.",
    )
    db_parser.add_argument(
        "--summary-output",
        type=Path,
        help="Optional path to write database summary JSON.",
    )

    db_summary_parser = subparsers.add_parser(
        "database-summary",
        help="Show SQLite database table counts.",
    )
    db_summary_parser.add_argument(
        "--database",
        type=Path,
        required=True,
        help="SQLite database path.",
    )
    db_summary_parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write database summary JSON.",
    )

    db_growth_rank_parser = subparsers.add_parser(
        "rank-growth-from-db",
        help="Build growth rankings from stored SQLite growth filter results.",
    )
    db_growth_rank_parser.add_argument(
        "--database",
        type=Path,
        required=True,
        help="SQLite database path.",
    )
    db_growth_rank_parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write growth ranking JSON.",
    )
    db_growth_rank_parser.add_argument(
        "--growth-metric",
        help="Optional growth metric to rank, for example revenue.",
    )
    db_growth_rank_parser.add_argument(
        "--growth-series-type",
        help="Optional growth series to rank, for example annual_yoy.",
    )
    db_growth_rank_parser.add_argument(
        "--include-failed-growth",
        action="store_true",
        help="Include failed growth filters in DB growth rankings.",
    )
    db_growth_rank_parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum number of ranking rows.",
    )

    company_report_parser = subparsers.add_parser(
        "company-growth-report",
        help="Build a static HTML growth report for one company from SQLite data.",
    )
    company_report_parser.add_argument(
        "--database",
        type=Path,
        required=True,
        help="SQLite database path.",
    )
    company_report_parser.add_argument(
        "--corp-code",
        required=True,
        help="OpenDART corp code.",
    )
    company_report_parser.add_argument(
        "--recent-years",
        type=int,
        default=10,
        help="Recent fiscal years to include. Defaults to 10.",
    )
    company_report_parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to write static HTML report.",
    )

    args = parser.parse_args(args_list)
    if args.command == "company-master":
        run_company_master(args, parser)
    elif args.command == "financial-statements":
        run_financial_statements(args, parser)
    elif args.command == "financial-period-values":
        run_financial_period_values(args)
    elif args.command == "growth-metrics":
        run_growth_metrics(args)
    elif args.command == "rank-companies":
        run_rank_companies(args)
    elif args.command == "collect-analysis":
        run_collect_analysis(args, parser)
    elif args.command == "analysis-to-db":
        run_analysis_to_db(args)
    elif args.command == "database-summary":
        run_database_summary(args)
    elif args.command == "rank-growth-from-db":
        run_rank_growth_from_db(args)
    elif args.command == "company-growth-report":
        run_company_growth_report(args)


def run_company_master(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    if not args.krx_service_key:
        parser.error("--krx-service-key or KRX_SERVICE_KEY is required.")
    if not args.opendart_api_key:
        parser.error("--opendart-api-key or OPENDART_API_KEY is required.")

    krx_listings = KrxClient(args.krx_service_key).fetch_listings(
        base_date=args.base_date,
        max_pages=args.max_krx_pages,
    )
    dart_companies = OpenDartClient(args.opendart_api_key).fetch_companies()
    result = match_listings_to_dart(krx_listings, dart_companies)

    write_company_master_outputs(
        result=result,
        output_json=args.output,
        matched_csv=args.matched_csv,
        unmatched_csv=args.unmatched_csv,
        ambiguous_json=args.ambiguous_json,
        report_markdown=args.report,
    )


def run_financial_statements(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    if not args.opendart_api_key:
        parser.error("--opendart-api-key or OPENDART_API_KEY is required.")

    corp_codes = parse_corp_code_args(args.corp_code)
    if not corp_codes:
        parser.error("--corp-code is required at least once.")

    rows = OpenDartClient(args.opendart_api_key).fetch_major_accounts(
        corp_codes=corp_codes,
        business_year=args.business_year,
        report_code=args.report_code,
        fs_div=args.fs_div,
    )
    write_financial_statement_rows(args.output, rows)


def parse_corp_code_args(values: list[str]) -> list[str]:
    corp_codes: list[str] = []
    for value in values:
        corp_codes.extend(
            code.strip()
            for code in value.split(",")
            if code.strip()
        )
    return corp_codes


def run_growth_metrics(args: argparse.Namespace) -> None:
    values = read_financial_period_values(args.input)
    write_growth_metrics_payload(
        args.output,
        values,
        threshold_percent=args.threshold_percent,
        recent_annual_periods=args.recent_annual_periods,
        recent_quarterly_periods=args.recent_quarterly_periods,
    )


def run_financial_period_values(args: argparse.Namespace) -> None:
    rows = read_financial_statement_rows(args.input)
    values = build_period_values_from_rows(rows)
    write_financial_period_values(args.output, values)


def run_rank_companies(args: argparse.Namespace) -> None:
    growth_payload = read_growth_metrics_payload(args.growth_input)
    valuations = (
        read_valuation_snapshots(args.valuation_input)
        if args.valuation_input is not None
        else []
    )
    write_ranking_payload(
        args.output,
        growth_payload,
        valuations,
        growth_metric=args.growth_metric,
        growth_series_type=args.growth_series_type,
        include_failed_growth=args.include_failed_growth,
        max_per=args.max_per,
        max_pbr=args.max_pbr,
        min_roe=args.min_roe,
        rank_valuation_by=args.rank_valuation_by,
    )


def run_collect_analysis(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    if not args.opendart_api_key:
        parser.error("--opendart-api-key or OPENDART_API_KEY is required.")

    corp_codes = resolve_corp_codes(args.corp_code, args.corp_code_file)
    if not corp_codes:
        parser.error("--corp-code or --corp-code-file is required.")

    try:
        business_years = parse_business_years(
            args.business_year,
            year_from=args.year_from,
            year_to=args.year_to,
        )
    except ValueError as error:
        parser.error(str(error))
    if not business_years:
        parser.error("--business-year or --year-from/--year-to is required.")

    report_codes = parse_corp_code_args(args.report_code) or list(DEFAULT_REPORT_CODES)
    run = collect_financial_statement_run(
        OpenDartClient(args.opendart_api_key),
        corp_codes=corp_codes,
        business_years=business_years,
        report_codes=report_codes,
        fs_div=args.fs_div,
        batch_size=args.batch_size,
        continue_on_error=not args.fail_fast,
    )
    artifacts = build_analysis_artifacts(
        run.rows,
        collection_errors=run.errors,
        expected_corp_codes=corp_codes,
        expected_business_years=business_years,
        expected_report_codes=report_codes,
        threshold_percent=args.threshold_percent,
        recent_annual_periods=args.recent_annual_periods,
        recent_quarterly_periods=args.recent_quarterly_periods,
    )
    write_analysis_outputs(args.output_dir, artifacts)
    if args.database is not None:
        store_analysis_artifacts(args.database, artifacts)


def run_analysis_to_db(args: argparse.Namespace) -> None:
    summary = store_analysis_directory(args.database, args.input_dir)
    write_or_print_json(summary, args.summary_output)


def run_database_summary(args: argparse.Namespace) -> None:
    summary = summarize_database(args.database)
    write_or_print_json(summary, args.output)


def run_rank_growth_from_db(args: argparse.Namespace) -> None:
    payload = build_database_growth_ranking_payload(
        args.database,
        growth_metric=args.growth_metric,
        growth_series_type=args.growth_series_type,
        include_failed_growth=args.include_failed_growth,
        limit=args.limit,
    )
    write_or_print_json(payload, args.output)


def run_company_growth_report(args: argparse.Namespace) -> None:
    payload = build_company_growth_report_payload(
        args.database,
        corp_code=args.corp_code,
        recent_years=args.recent_years,
    )
    write_company_growth_report_html(args.output, payload)


def write_or_print_json(payload: dict[str, object], output: Path | None) -> None:
    content = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    if output is None:
        print(content, end="")
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    main()
