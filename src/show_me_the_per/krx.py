from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import json
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

from .models import KOREAN_EQUITY_MARKETS, KrxListing, normalize_stock_code, parse_decimal_amount


DEFAULT_KRX_LISTED_INFO_ENDPOINT = (
    "https://apis.data.go.kr/1160100/service/"
    "GetKrxListedInfoService/getItemInfo"
)
DEFAULT_KRX_STOCK_PRICE_ENDPOINT = (
    "https://apis.data.go.kr/1160100/service/"
    "GetStockSecuritiesInfoService/getStockPriceInfo"
)


@dataclass(frozen=True)
class KrxStockPriceSnapshot:
    base_date: str
    stock_code: str
    item_name: str
    market: str
    close_price: Decimal | None
    market_cap: Decimal | None
    listed_stock_count: Decimal | None


class KrxClient:
    def __init__(
        self,
        service_key: str,
        endpoint: str = DEFAULT_KRX_LISTED_INFO_ENDPOINT,
        timeout_seconds: int = 30,
    ) -> None:
        self.service_key = service_key
        self.endpoint = endpoint
        self.timeout_seconds = timeout_seconds

    def fetch_listings(
        self,
        base_date: str | None = None,
        page_size: int = 1000,
        max_pages: int | None = None,
    ) -> list[KrxListing]:
        listings: list[KrxListing] = []
        page_no = 1
        total_count: int | None = None

        while True:
            payload = self._fetch_page(base_date, page_no, page_size)
            page_items = _extract_items(payload)
            listings.extend(parse_krx_listings(payload))
            total_count = _read_total_count(payload)

            if max_pages is not None and page_no >= max_pages:
                break
            if total_count is not None and len(listings) >= total_count:
                break
            if not page_items or (total_count is None and len(page_items) < page_size):
                break

            page_no += 1

        return [listing for listing in listings if listing.is_supported_market]

    def _fetch_page(
        self,
        base_date: str | None,
        page_no: int,
        page_size: int,
    ) -> dict[str, Any]:
        params = {
            "serviceKey": self.service_key,
            "resultType": "json",
            "numOfRows": str(page_size),
            "pageNo": str(page_no),
        }
        if base_date:
            params["basDt"] = base_date

        url = f"{self.endpoint}?{urlencode(params)}"
        with urlopen(url, timeout=self.timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))


class KrxStockPriceClient:
    def __init__(
        self,
        service_key: str,
        endpoint: str = DEFAULT_KRX_STOCK_PRICE_ENDPOINT,
        timeout_seconds: int = 30,
    ) -> None:
        self.service_key = service_key
        self.endpoint = endpoint
        self.timeout_seconds = timeout_seconds

    def fetch_stock_price(
        self,
        stock_code: str,
        *,
        base_date: str,
    ) -> KrxStockPriceSnapshot:
        normalized_stock_code = normalize_stock_code(stock_code)
        if not normalized_stock_code:
            raise ValueError("stock code is required")

        params = {
            "serviceKey": self.service_key,
            "resultType": "json",
            "numOfRows": "50",
            "pageNo": "1",
            "basDt": base_date,
            "likeSrtnCd": normalized_stock_code,
        }
        url = f"{self.endpoint}?{urlencode(params)}"
        with urlopen(url, timeout=self.timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))

        for snapshot in parse_stock_price_payload(payload):
            if snapshot.stock_code == normalized_stock_code:
                return snapshot

        raise LookupError(
            f"stock price snapshot not found for {normalized_stock_code} on {base_date}"
        )


def parse_krx_listings(payload: dict[str, Any]) -> list[KrxListing]:
    listings: list[KrxListing] = []

    for item in _extract_items(payload):
        listing = KrxListing(
            base_date=str(item.get("basDt", "")).strip(),
            short_code=str(item.get("srtnCd", "")).strip(),
            isin_code=str(item.get("isinCd", "")).strip(),
            market=str(item.get("mrktCtg", "")).strip().upper(),
            item_name=str(item.get("itmsNm", "")).strip(),
            corporation_registration_number=str(item.get("crno", "")).strip(),
            corporation_name=str(item.get("corpNm", "")).strip(),
        )
        if listing.market in KOREAN_EQUITY_MARKETS:
            listings.append(listing)

    return listings


def parse_stock_price_payload(payload: dict[str, Any]) -> list[KrxStockPriceSnapshot]:
    result_code = (
        payload.get("response", {})
        .get("header", {})
        .get("resultCode", "")
    )
    if result_code and str(result_code).strip() not in {"00"}:
        result_message = (
            payload.get("response", {})
            .get("header", {})
            .get("resultMsg", "Unknown KRX stock price error")
        )
        raise ValueError(
            f"KRX stock price request failed: {result_code} {result_message}"
        )

    snapshots: list[KrxStockPriceSnapshot] = []
    for item in _extract_items(payload):
        snapshots.append(
            KrxStockPriceSnapshot(
                base_date=str(item.get("basDt", "")).strip(),
                stock_code=normalize_stock_code(str(item.get("srtnCd", "")).strip()),
                item_name=str(item.get("itmsNm", "")).strip(),
                market=str(
                    item.get("mrktCtg", item.get("mrktCls", ""))
                ).strip().upper(),
                close_price=parse_decimal_amount(str(item.get("clpr", "")).strip()),
                market_cap=parse_decimal_amount(
                    str(item.get("mrktTotAmt", item.get("mrkttotamt", ""))).strip()
                ),
                listed_stock_count=parse_decimal_amount(
                    str(item.get("lstgStCnt", item.get("lstgstcnt", ""))).strip()
                ),
            )
        )

    return snapshots


def _extract_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = (
        payload.get("response", {})
        .get("body", {})
        .get("items", {})
        .get("item", [])
    )
    if isinstance(items, dict):
        return [items]
    if isinstance(items, list):
        return [item for item in items if isinstance(item, dict)]
    return []


def _read_total_count(payload: dict[str, Any]) -> int | None:
    raw_total = payload.get("response", {}).get("body", {}).get("totalCount")
    try:
        return int(raw_total)
    except (TypeError, ValueError):
        return None
