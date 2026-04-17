from __future__ import annotations

import json
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path

from .models import (
    FinancialPeriodValue,
    FinancialStatementRow,
    parse_decimal_amount,
)


METRIC_REVENUE = "revenue"
METRIC_OPERATING_INCOME = "operating_income"
METRIC_NET_INCOME = "net_income"
METRIC_ORDER = {
    METRIC_REVENUE: 0,
    METRIC_OPERATING_INCOME: 1,
    METRIC_NET_INCOME: 2,
}

REPORT_CODE_ANNUAL = "11011"
REPORT_CODE_Q1 = "11013"
REPORT_CODE_HALF = "11012"
REPORT_CODE_Q3 = "11014"

REPORT_CODE_QUARTERS = {
    REPORT_CODE_Q1: 1,
    REPORT_CODE_HALF: 2,
    REPORT_CODE_Q3: 3,
    REPORT_CODE_ANNUAL: 4,
}

ACCOUNT_ID_METRICS = {
    "ifrs-full_Revenue": METRIC_REVENUE,
    "ifrs-full_RevenueFromContractsWithCustomers": METRIC_REVENUE,
    "dart_OperatingIncomeLoss": METRIC_OPERATING_INCOME,
    "ifrs-full_ProfitLoss": METRIC_NET_INCOME,
}

ACCOUNT_NAME_METRICS = (
    (METRIC_OPERATING_INCOME, ("영업이익", "operating income", "operating profit")),
    (METRIC_REVENUE, ("매출액", "수익(매출액)", "revenue")),
    (
        METRIC_NET_INCOME,
        ("당기순이익", "순이익", "net income", "profit loss", "profit (loss)"),
    ),
)


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


def read_financial_statement_rows(path: Path) -> list[FinancialStatementRow]:
    payload = json.loads(path.read_text("utf-8"))
    raw_rows = payload["rows"] if isinstance(payload, dict) else payload
    return [_parse_financial_statement_row(row) for row in raw_rows]


def build_annual_period_values_from_rows(
    rows: list[FinancialStatementRow],
) -> list[FinancialPeriodValue]:
    selected_values: dict[tuple[str, str, int], tuple[int, FinancialPeriodValue]] = {}
    for row in rows:
        if row.report_code != REPORT_CODE_ANNUAL:
            continue

        metric = map_financial_account_to_metric(row.account_id, row.account_name)
        if metric is None:
            continue

        try:
            current_year = int(row.business_year)
        except ValueError:
            continue

        for source_year, value in _annual_values_for_amounts(
            row=row,
            metric=metric,
            current_year=current_year,
        ):
            key = (value.corp_code, value.metric, value.fiscal_year)
            previous = selected_values.get(key)
            if previous is None or source_year > previous[0]:
                selected_values[key] = (source_year, value)

    return [
        value
        for _, value in sorted(
            selected_values.values(),
            key=lambda item: (
                item[1].corp_code,
                METRIC_ORDER.get(item[1].metric, 999),
                -item[1].fiscal_year,
            ),
        )
    ]


def build_quarterly_period_values_from_rows(
    rows: list[FinancialStatementRow],
) -> list[FinancialPeriodValue]:
    cumulative_values: dict[tuple[str, str, int], dict[int, Decimal]] = {}

    for row in rows:
        quarter = REPORT_CODE_QUARTERS.get(row.report_code)
        if quarter is None or row.current_amount is None:
            continue

        metric = map_financial_account_to_metric(row.account_id, row.account_name)
        if metric is None:
            continue

        try:
            fiscal_year = int(row.business_year)
        except ValueError:
            continue

        key = (row.corp_code, metric, fiscal_year)
        cumulative_values.setdefault(key, {})[quarter] = row.current_amount

    values: list[FinancialPeriodValue] = []
    for (corp_code, metric, fiscal_year), by_quarter in sorted(
        cumulative_values.items(),
        key=lambda item: (
            item[0][0],
            METRIC_ORDER.get(item[0][1], 999),
            item[0][2],
        ),
    ):
        previous_cumulative = Decimal("0")
        for quarter in range(1, 5):
            cumulative_amount = by_quarter.get(quarter)
            if cumulative_amount is None:
                break

            amount = cumulative_amount - previous_cumulative
            values.append(
                FinancialPeriodValue(
                    corp_code=corp_code,
                    metric=metric,
                    period_type="quarter",
                    fiscal_year=fiscal_year,
                    fiscal_quarter=quarter,
                    amount=amount,
                )
            )
            previous_cumulative = cumulative_amount

    return values


def build_period_values_from_rows(
    rows: list[FinancialStatementRow],
) -> list[FinancialPeriodValue]:
    return [
        *build_annual_period_values_from_rows(rows),
        *build_quarterly_period_values_from_rows(rows),
    ]


def map_financial_account_to_metric(
    account_id: str,
    account_name: str,
) -> str | None:
    if account_id in ACCOUNT_ID_METRICS:
        return ACCOUNT_ID_METRICS[account_id]

    normalized_name = account_name.strip().lower()
    for metric, patterns in ACCOUNT_NAME_METRICS:
        if any(pattern in normalized_name for pattern in patterns):
            return metric

    return None


def build_financial_period_value_payload(
    values: list[FinancialPeriodValue],
) -> dict[str, object]:
    return {
        "summary": {
            "values": len(values),
            "corp_codes": sorted({value.corp_code for value in values}),
            "metrics": sorted({value.metric for value in values}),
            "period_types": sorted({value.period_type for value in values}),
            "fiscal_years": sorted({value.fiscal_year for value in values}),
        },
        "values": [_json_ready(asdict(value)) for value in values],
    }


def write_financial_period_values(
    path: Path,
    values: list[FinancialPeriodValue],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            build_financial_period_value_payload(values),
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _annual_values_for_amounts(
    *,
    row: FinancialStatementRow,
    metric: str,
    current_year: int,
) -> list[tuple[int, FinancialPeriodValue]]:
    amounts = (
        (current_year, row.current_amount),
        (current_year - 1, row.previous_amount),
        (current_year - 2, row.before_previous_amount),
    )
    return [
        (
            current_year,
            FinancialPeriodValue(
                corp_code=row.corp_code,
                metric=metric,
                period_type="annual",
                fiscal_year=year,
                amount=amount,
            ),
        )
        for year, amount in amounts
        if amount is not None
    ]


def _parse_financial_statement_row(item: dict[str, object]) -> FinancialStatementRow:
    return FinancialStatementRow(
        corp_code=_field(item, "corp_code"),
        corp_name=_field(item, "corp_name"),
        stock_code=_field(item, "stock_code"),
        business_year=_field(item, "business_year"),
        report_code=_field(item, "report_code"),
        fs_div=_field(item, "fs_div"),
        fs_name=_field(item, "fs_name"),
        statement_div=_field(item, "statement_div"),
        statement_name=_field(item, "statement_name"),
        account_id=_field(item, "account_id"),
        account_name=_field(item, "account_name"),
        current_term_name=_field(item, "current_term_name"),
        current_amount=parse_decimal_amount(_field(item, "current_amount")),
        previous_term_name=_field(item, "previous_term_name"),
        previous_amount=parse_decimal_amount(_field(item, "previous_amount")),
        before_previous_term_name=_field(item, "before_previous_term_name"),
        before_previous_amount=parse_decimal_amount(
            _field(item, "before_previous_amount")
        ),
    )


def _field(item: dict[str, object], key: str) -> str:
    value = item.get(key, "")
    if value is None:
        return ""
    return str(value).strip()


def _json_ready(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value
