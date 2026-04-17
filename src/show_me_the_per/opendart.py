from __future__ import annotations

from io import BytesIO
import json
from typing import Any, Iterable
from urllib.parse import urlencode
from urllib.request import urlopen
from zipfile import ZipFile
import xml.etree.ElementTree as ET

from .models import DartCompany, FinancialStatementRow, parse_decimal_amount


DEFAULT_DART_CORP_CODE_ENDPOINT = "https://opendart.fss.or.kr/api/corpCode.xml"
DEFAULT_DART_MULTI_ACCOUNT_ENDPOINT = (
    "https://opendart.fss.or.kr/api/fnlttMultiAcnt.json"
)


class OpenDartClient:
    def __init__(
        self,
        api_key: str,
        corp_code_endpoint: str = DEFAULT_DART_CORP_CODE_ENDPOINT,
        multi_account_endpoint: str = DEFAULT_DART_MULTI_ACCOUNT_ENDPOINT,
        timeout_seconds: int = 30,
    ) -> None:
        self.api_key = api_key
        self.corp_code_endpoint = corp_code_endpoint
        self.multi_account_endpoint = multi_account_endpoint
        self.timeout_seconds = timeout_seconds

    def fetch_companies(self) -> list[DartCompany]:
        params = urlencode({"crtfc_key": self.api_key})
        url = f"{self.corp_code_endpoint}?{params}"
        with urlopen(url, timeout=self.timeout_seconds) as response:
            return parse_corp_code_zip(response.read())

    def fetch_major_accounts(
        self,
        corp_codes: list[str],
        business_year: str,
        report_code: str,
        fs_div: str | None = None,
        batch_size: int = 100,
    ) -> list[FinancialStatementRow]:
        rows: list[FinancialStatementRow] = []
        for batch in chunked(corp_codes, batch_size):
            params = {
                "crtfc_key": self.api_key,
                "corp_code": ",".join(batch),
                "bsns_year": business_year,
                "reprt_code": report_code,
            }
            if fs_div:
                params["fs_div"] = fs_div

            url = f"{self.multi_account_endpoint}?{urlencode(params)}"
            with urlopen(url, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
            parsed_rows = parse_major_accounts_payload(payload)
            if fs_div:
                parsed_rows = [
                    row for row in parsed_rows if row.fs_div.upper() == fs_div.upper()
                ]
            rows.extend(parsed_rows)

        return rows


def parse_corp_code_zip(content: bytes) -> list[DartCompany]:
    with ZipFile(BytesIO(content)) as archive:
        xml_names = [name for name in archive.namelist() if name.lower().endswith(".xml")]
        if not xml_names:
            raise ValueError("OpenDART corp code archive does not contain an XML file.")
        with archive.open(xml_names[0]) as xml_file:
            return parse_corp_code_xml(xml_file.read())


def parse_corp_code_xml(content: bytes | str) -> list[DartCompany]:
    root = ET.fromstring(content)
    companies: list[DartCompany] = []

    for element in _iter_company_elements(root):
        companies.append(
            DartCompany(
                corp_code=_text(element, "corp_code"),
                corp_name=_text(element, "corp_name"),
                stock_code=_text(element, "stock_code"),
                modify_date=_text(element, "modify_date"),
            )
        )

    return companies


def parse_major_accounts_payload(
    payload: dict[str, Any],
) -> list[FinancialStatementRow]:
    status = str(payload.get("status", "")).strip()
    if status and status not in {"000", "013"}:
        message = payload.get("message", "Unknown OpenDART error")
        raise ValueError(f"OpenDART major account request failed: {status} {message}")
    if status == "013":
        return []

    rows: list[FinancialStatementRow] = []
    for item in payload.get("list", []) or []:
        if not isinstance(item, dict):
            continue

        rows.append(
            FinancialStatementRow(
                corp_code=_field(item, "corp_code"),
                corp_name=_field(item, "corp_name"),
                stock_code=_field(item, "stock_code"),
                business_year=_field(item, "bsns_year"),
                report_code=_field(item, "reprt_code"),
                fs_div=_field(item, "fs_div"),
                fs_name=_field(item, "fs_nm"),
                statement_div=_field(item, "sj_div"),
                statement_name=_field(item, "sj_nm"),
                account_id=_field(item, "account_id"),
                account_name=_field(item, "account_nm"),
                current_term_name=_field(item, "thstrm_nm"),
                current_amount=parse_decimal_amount(_field(item, "thstrm_amount")),
                previous_term_name=_field(item, "frmtrm_nm"),
                previous_amount=parse_decimal_amount(_field(item, "frmtrm_amount")),
                before_previous_term_name=_field(item, "bfefrmtrm_nm"),
                before_previous_amount=parse_decimal_amount(
                    _field(item, "bfefrmtrm_amount")
                ),
            )
        )

    return rows


def chunked(values: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        raise ValueError("chunk size must be greater than zero.")
    return [values[index : index + size] for index in range(0, len(values), size)]


def _iter_company_elements(root: ET.Element) -> Iterable[ET.Element]:
    if root.tag == "list":
        yield root
        return

    yield from root.findall(".//list")


def _text(element: ET.Element, child_name: str) -> str:
    child = element.find(child_name)
    if child is None or child.text is None:
        return ""
    return child.text.strip()


def _field(item: dict[str, Any], key: str) -> str:
    value = item.get(key, "")
    if value is None:
        return ""
    return str(value).strip()
