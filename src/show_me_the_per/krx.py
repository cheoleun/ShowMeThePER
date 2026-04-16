from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

from .models import KOREAN_EQUITY_MARKETS, KrxListing


DEFAULT_KRX_LISTED_INFO_ENDPOINT = (
    "https://apis.data.go.kr/1160100/service/"
    "GetKrxListedInfoService/getItemInfo"
)


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
