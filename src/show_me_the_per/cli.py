from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict
from pathlib import Path

from .krx import KrxClient
from .matching import match_listings_to_dart
from .opendart import OpenDartClient


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect and match Korean listed company identifiers."
    )
    parser.add_argument(
        "--krx-service-key",
        default=os.getenv("KRX_SERVICE_KEY"),
        help="Public Data Portal service key. Defaults to KRX_SERVICE_KEY.",
    )
    parser.add_argument(
        "--opendart-api-key",
        default=os.getenv("OPENDART_API_KEY"),
        help="OpenDART API key. Defaults to OPENDART_API_KEY.",
    )
    parser.add_argument(
        "--base-date",
        help="Optional KRX base date in YYYYMMDD format.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to write matched company master JSON.",
    )

    args = parser.parse_args()
    if not args.krx_service_key:
        parser.error("--krx-service-key or KRX_SERVICE_KEY is required.")
    if not args.opendart_api_key:
        parser.error("--opendart-api-key or OPENDART_API_KEY is required.")

    krx_listings = KrxClient(args.krx_service_key).fetch_listings(
        base_date=args.base_date
    )
    dart_companies = OpenDartClient(args.opendart_api_key).fetch_companies()
    result = match_listings_to_dart(krx_listings, dart_companies)

    payload = {
        "summary": {
            "matched": len(result.matched),
            "unmatched_listings": len(result.unmatched_listings),
            "ambiguous_matches": len(result.ambiguous_matches),
            "total_listings": result.total_listings,
        },
        "matched": [asdict(company) for company in result.matched],
        "unmatched_listings": [
            asdict(listing) for listing in result.unmatched_listings
        ],
        "ambiguous_matches": [
            {
                "listing": asdict(match.listing),
                "candidates": [asdict(candidate) for candidate in match.candidates],
            }
            for match in result.ambiguous_matches
        ],
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
