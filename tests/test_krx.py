import httpx
from unittest.mock import patch

from show_me_the_per.krx import (
    KrxApiError,
    KrxClient,
    KrxStockPriceClient,
    parse_krx_listings,
    parse_stock_price_payload,
)
import unittest


class KrxParserTests(unittest.TestCase):
    def test_parse_krx_listings_keeps_supported_markets_only(self) -> None:
        payload = {
            "response": {
                "body": {
                    "items": {
                        "item": [
                            {
                                "basDt": "20260415",
                                "srtnCd": "005930",
                                "isinCd": "KR7005930003",
                                "mrktCtg": "KOSPI",
                                "itmsNm": "Samsung Electronics",
                                "crno": "1301110006246",
                                "corpNm": "Samsung Electronics Co., Ltd.",
                            },
                            {
                                "basDt": "20260415",
                                "srtnCd": "123456",
                                "isinCd": "KR7123456000",
                                "mrktCtg": "KONEX",
                                "itmsNm": "Unsupported",
                                "crno": "0000000000000",
                                "corpNm": "Unsupported Co.",
                            },
                        ]
                    }
                }
            }
        }

        listings = parse_krx_listings(payload)

        self.assertEqual(len(listings), 1)
        self.assertEqual(listings[0].short_code, "005930")
        self.assertEqual(listings[0].market, "KOSPI")

    def test_parse_krx_listings_accepts_single_item_payload(self) -> None:
        payload = {
            "response": {
                "body": {
                    "items": {
                        "item": {
                            "basDt": "20260415",
                            "srtnCd": "035720",
                            "isinCd": "KR7035720002",
                            "mrktCtg": "KOSDAQ",
                            "itmsNm": "Kakao",
                            "crno": "1101111122334",
                            "corpNm": "Kakao Corp.",
                        }
                    }
                }
            }
        }

        listings = parse_krx_listings(payload)

        self.assertEqual(len(listings), 1)
        self.assertEqual(listings[0].normalized_short_code, "035720")

    def test_parse_stock_price_payload_extracts_close_and_market_cap(self) -> None:
        payload = {
            "response": {
                "header": {
                    "resultCode": "00",
                    "resultMsg": "NORMAL SERVICE.",
                },
                "body": {
                    "items": {
                        "item": {
                            "basDt": "20260421",
                            "srtnCd": "126340",
                            "itmsNm": "Vinatac",
                            "mrktCtg": "KOSDAQ",
                            "clpr": "37100",
                            "lstgStCnt": "15123456",
                            "mrktTotAmt": "561080217600",
                        }
                    }
                },
            }
        }

        snapshots = parse_stock_price_payload(payload)

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].stock_code, "126340")
        self.assertEqual(str(snapshots[0].close_price), "37100")
        self.assertEqual(str(snapshots[0].market_cap), "561080217600")
        self.assertEqual(snapshots[0].market, "KOSDAQ")

    def test_fetch_listings_translates_http_403_to_korean_message(self) -> None:
        client = KrxClient("bad-key")
        request = httpx.Request("GET", "https://example.com")
        response = httpx.Response(403, request=request)

        with patch(
            "show_me_the_per.krx.httpx.get",
            return_value=response,
        ):
            with self.assertRaises(KrxApiError) as context:
                client.fetch_listings()

        self.assertEqual(context.exception.status_code, 403)
        self.assertIn("KRX 회사 목록 조회가 403으로 거부되었습니다.", str(context.exception))
        self.assertIn("KRX_SERVICE_KEY", str(context.exception))

    def test_fetch_stock_price_translates_network_error(self) -> None:
        client = KrxStockPriceClient("test-key")
        request = httpx.Request("GET", "https://example.com")

        with patch(
            "show_me_the_per.krx.httpx.get",
            side_effect=httpx.ConnectError("temporary outage", request=request),
        ):
            with self.assertRaises(KrxApiError) as context:
                client.fetch_stock_price("126340", base_date="20260421")

        self.assertIsNone(context.exception.status_code)
        self.assertIn("KRX 시세 조회 중 네트워크 오류가 발생했습니다.", str(context.exception))

    def test_encoded_service_key_is_normalized_before_urlencode(self) -> None:
        captured: dict[str, object] = {}

        def fake_get(url: str, **kwargs: object) -> object:
            captured["url"] = url
            captured["params"] = kwargs.get("params")
            request = httpx.Request("GET", url, params=kwargs.get("params"))
            return httpx.Response(403, request=request)

        client = KrxClient("abc%2Bdef%3D%3D")

        with patch("show_me_the_per.krx.httpx.get", side_effect=fake_get):
            with self.assertRaises(KrxApiError):
                client.fetch_listings()

        self.assertEqual(captured["params"]["serviceKey"], "abc+def==")

    def test_fetch_listings_adds_browser_like_headers(self) -> None:
        captured: dict[str, object] = {}

        def fake_get(url: str, **kwargs: object) -> object:
            captured["headers"] = kwargs.get("headers")
            captured["trust_env"] = kwargs.get("trust_env")
            request = httpx.Request("GET", url, params=kwargs.get("params"))
            return httpx.Response(403, request=request)

        client = KrxClient("test-key")

        with patch("show_me_the_per.krx.httpx.get", side_effect=fake_get):
            with self.assertRaises(KrxApiError):
                client.fetch_listings()

        headers = captured["headers"]
        self.assertEqual(headers["User-Agent"], "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36")
        self.assertEqual(headers["Accept"], "application/json,text/plain,*/*")
        self.assertEqual(captured["trust_env"], False)


if __name__ == "__main__":
    unittest.main()
