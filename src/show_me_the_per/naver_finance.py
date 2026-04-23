from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from html import unescape
from html.parser import HTMLParser
import re
from urllib.request import Request, urlopen

from .models import normalize_stock_code, parse_decimal_amount


DEFAULT_NAVER_FINANCE_ITEM_URL = "https://finance.naver.com/item/main.naver"


@dataclass(frozen=True)
class NaverFinanceSnapshot:
    stock_code: str
    corp_name: str
    market: str | None = None
    close_price: Decimal | None = None
    market_cap: Decimal | None = None
    per: Decimal | None = None
    pbr: Decimal | None = None
    roe: Decimal | None = None
    eps: Decimal | None = None
    base_date: str = ""
    source: str = "naver_finance"
    fetched_at: str = ""


class NaverFinanceClient:
    def __init__(
        self,
        item_url: str = DEFAULT_NAVER_FINANCE_ITEM_URL,
        timeout_seconds: int = 30,
    ) -> None:
        self.item_url = item_url
        self.timeout_seconds = timeout_seconds

    def fetch_snapshot(self, stock_code: str) -> NaverFinanceSnapshot:
        normalized_stock_code = normalize_stock_code(stock_code)
        if not normalized_stock_code:
            raise ValueError("stock code is required")

        request = Request(
            f"{self.item_url}?code={normalized_stock_code}",
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            },
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:
            html = response.read().decode("euc-kr", errors="replace")
        return parse_naver_finance_snapshot(
            html,
            stock_code=normalized_stock_code,
        )


def parse_naver_finance_snapshot(
    html: str,
    *,
    stock_code: str,
    fetched_at: datetime | None = None,
) -> NaverFinanceSnapshot:
    normalized_stock_code = normalize_stock_code(stock_code)
    if not normalized_stock_code:
        raise ValueError("stock code is required")

    text = _html_to_text(html)
    corp_name = _extract_company_name(text, normalized_stock_code)
    market = _extract_market(text, normalized_stock_code)
    close_price = _extract_decimal(text, r"현재가\s+([0-9,.-]+)")
    market_cap = _extract_market_cap(text)
    per = _extract_decimal(
        text,
        r"PER/EPS.*?([\-0-9.,N/A]+)\s*배\s*l\s*([\-0-9.,N/A]+)\s*원",
        group=1,
    )
    eps = _extract_decimal(
        text,
        r"PER/EPS.*?([\-0-9.,N/A]+)\s*배\s*l\s*([\-0-9.,N/A]+)\s*원",
        group=2,
    )
    pbr = _extract_decimal(
        text,
        r"PBR\s*l\s*BPS.*?([\-0-9.,N/A]+)\s*배",
    )
    roe = _extract_decimal(
        text,
        r"ROE\(%\)\s+([\-0-9.,N/A]+)",
    )
    base_date = _extract_base_date(text)
    fetched_text = (
        (fetched_at or datetime.utcnow()).replace(microsecond=0).isoformat() + "Z"
    )

    return NaverFinanceSnapshot(
        stock_code=normalized_stock_code,
        corp_name=corp_name,
        market=market,
        close_price=close_price,
        market_cap=market_cap,
        per=per,
        pbr=pbr,
        roe=roe,
        eps=eps,
        base_date=base_date,
        fetched_at=fetched_text,
    )


def _html_to_text(html: str) -> str:
    parser = _TextExtractor()
    parser.feed(html)
    parser.close()
    text = unescape(" ".join(parser.parts))
    return re.sub(r"\s+", " ", text).strip()


def _extract_company_name(text: str, stock_code: str) -> str:
    pattern = re.compile(
        rf"종목명\s+(?P<name>.+?)\s+종목코드\s+{re.escape(stock_code)}\s+"
        r"(?P<market>코스피|코스닥|KOSPI|KOSDAQ)",
        re.IGNORECASE,
    )
    match = pattern.search(text)
    if match is not None:
        return str(match.group("name")).strip()

    fallback = re.search(
        rf"##\s*(.+?)\s+{re.escape(stock_code)}\s+(코스피|코스닥|KOSPI|KOSDAQ)",
        text,
        re.IGNORECASE,
    )
    if fallback is not None:
        return str(fallback.group(1)).strip()

    raise ValueError(f"failed to parse company name for {stock_code}")


def _extract_market(text: str, stock_code: str) -> str | None:
    pattern = re.compile(
        rf"종목명\s+.+?\s+종목코드\s+{re.escape(stock_code)}\s+"
        r"(?P<market>코스피|코스닥|KOSPI|KOSDAQ)",
        re.IGNORECASE,
    )
    match = pattern.search(text)
    if match is None:
        match = re.search(
            rf"{re.escape(stock_code)}\s+(코스피|코스닥|KOSPI|KOSDAQ)",
            text,
            re.IGNORECASE,
        )
    if match is None:
        return None
    return _normalize_market(match.group("market"))


def _normalize_market(value: str) -> str:
    normalized = value.strip().upper()
    if normalized == "코스피".upper():
        return "KOSPI"
    if normalized == "코스닥".upper():
        return "KOSDAQ"
    return normalized


def _extract_decimal(text: str, pattern: str, *, group: int = 1) -> Decimal | None:
    match = re.search(pattern, text, re.IGNORECASE)
    if match is None:
        return None
    return parse_decimal_amount(match.group(group))


def _extract_market_cap(text: str) -> Decimal | None:
    match = re.search(
        r"시가총액\s+시가총액\s+([0-9,\s조억만원]+)",
        text,
        re.IGNORECASE,
    )
    if match is None:
        return None
    return _parse_korean_amount(match.group(1))


def _parse_korean_amount(value: str) -> Decimal | None:
    text = re.sub(r"\s+", " ", value).strip()
    if not text:
        return None

    total = Decimal("0")
    units = {
        "조": Decimal("1000000000000"),
        "억": Decimal("100000000"),
        "만": Decimal("10000"),
    }
    found = False
    for suffix, factor in units.items():
        match = re.search(rf"([0-9,]+)\s*{suffix}", text)
        if match is None:
            continue
        number = parse_decimal_amount(match.group(1))
        if number is None:
            continue
        total += number * factor
        found = True

    if found:
        return total

    cleaned = (
        text.replace("원", "")
        .replace(",", "")
        .replace(" ", "")
        .strip()
    )
    return parse_decimal_amount(cleaned)


def _extract_base_date(text: str) -> str:
    match = re.search(r"날짜\s+(\d{4})\.(\d{2})\.(\d{2})\s+기준", text)
    if match is not None:
        return "".join(match.groups())

    match = re.search(r"(\d{4})년\s*(\d{2})월\s*(\d{2})일", text)
    if match is not None:
        return "".join(match.groups())
    return ""


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"script", "style"}:
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style"} and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        cleaned = data.strip()
        if cleaned:
            self.parts.append(cleaned)
