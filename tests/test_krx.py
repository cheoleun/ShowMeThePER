from show_me_the_per.krx import parse_krx_listings
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


if __name__ == "__main__":
    unittest.main()
