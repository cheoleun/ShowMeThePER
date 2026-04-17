from __future__ import annotations

import json
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path

from .models import FinancialStatementRow


def build_financial_statement_payload(
    rows: list[FinancialStatementRow],
) -> dict[str, object]:
    return {
        "summary": {
            "rows": len(rows),
            "corp_codes": sorted({row.corp_code for row in rows}),
            "business_years": sorted({row.business_year for row in rows}),
            "report_codes": sorted({row.report_code for row in rows}),
        },
        "rows": [_json_ready(asdict(row)) for row in rows],
    }


def write_financial_statement_rows(
    path: Path,
    rows: list[FinancialStatementRow],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            build_financial_statement_payload(rows),
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _json_ready(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value
