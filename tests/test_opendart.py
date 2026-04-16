from io import BytesIO
from zipfile import ZipFile
import unittest

from show_me_the_per.opendart import parse_corp_code_xml, parse_corp_code_zip


CORP_CODE_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<result>
  <list>
    <corp_code>00126380</corp_code>
    <corp_name>Samsung Electronics</corp_name>
    <stock_code>005930</stock_code>
    <modify_date>20240101</modify_date>
  </list>
  <list>
    <corp_code>00434003</corp_code>
    <corp_name>Private Company</corp_name>
    <stock_code></stock_code>
    <modify_date>20240101</modify_date>
  </list>
</result>
"""


class OpenDartParserTests(unittest.TestCase):
    def test_parse_corp_code_xml_extracts_companies(self) -> None:
        companies = parse_corp_code_xml(CORP_CODE_XML)

        self.assertEqual(len(companies), 2)
        self.assertEqual(companies[0].corp_code, "00126380")
        self.assertEqual(companies[0].normalized_stock_code, "005930")
        self.assertFalse(companies[1].is_listed)

    def test_parse_corp_code_zip_extracts_first_xml_file(self) -> None:
        archive_content = BytesIO()
        with ZipFile(archive_content, "w") as archive:
            archive.writestr("CORPCODE.xml", CORP_CODE_XML)

        companies = parse_corp_code_zip(archive_content.getvalue())

        self.assertEqual(
            [company.corp_code for company in companies],
            [
                "00126380",
                "00434003",
            ],
        )


if __name__ == "__main__":
    unittest.main()
