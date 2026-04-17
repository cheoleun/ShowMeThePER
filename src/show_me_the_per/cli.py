from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

from .company_master import write_company_master_outputs
from .financials import write_financial_statement_rows
from .krx import KrxClient
from .matching import match_listings_to_dart
from .opendart import OpenDartClient


COMMANDS = {"company-master", "financial-statements"}


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

    args = parser.parse_args(args_list)
    if args.command == "company-master":
        run_company_master(args, parser)
    elif args.command == "financial-statements":
        run_financial_statements(args, parser)


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


if __name__ == "__main__":
    main()
