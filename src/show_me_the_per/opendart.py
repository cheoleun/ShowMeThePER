from __future__ import annotations

from io import BytesIO
from typing import Iterable
from urllib.parse import urlencode
from urllib.request import urlopen
from zipfile import ZipFile
import xml.etree.ElementTree as ET

from .models import DartCompany


DEFAULT_DART_CORP_CODE_ENDPOINT = "https://opendart.fss.or.kr/api/corpCode.xml"


class OpenDartClient:
    def __init__(
        self,
        api_key: str,
        corp_code_endpoint: str = DEFAULT_DART_CORP_CODE_ENDPOINT,
        timeout_seconds: int = 30,
    ) -> None:
        self.api_key = api_key
        self.corp_code_endpoint = corp_code_endpoint
        self.timeout_seconds = timeout_seconds

    def fetch_companies(self) -> list[DartCompany]:
        params = urlencode({"crtfc_key": self.api_key})
        url = f"{self.corp_code_endpoint}?{params}"
        with urlopen(url, timeout=self.timeout_seconds) as response:
            return parse_corp_code_zip(response.read())


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
