import json
import tempfile
import unittest
from pathlib import Path

from show_me_the_per.cli import main, parse_corp_code_args


class CliTests(unittest.TestCase):
    def test_parse_corp_code_args_accepts_repeated_and_comma_separated_values(
        self,
    ) -> None:
        self.assertEqual(
            parse_corp_code_args(["00126380, 00434003", "12345678"]),
            ["00126380", "00434003", "12345678"],
        )

    def test_growth_metrics_command_writes_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            input_path = Path(directory) / "values.json"
            output_path = Path(directory) / "growth.json"
            input_path.write_text(
                json.dumps(
                    {
                        "values": [
                            {
                                "corp_code": "00126380",
                                "metric": "revenue",
                                "period_type": "annual",
                                "fiscal_year": 2023,
                                "amount": "100",
                            },
                            {
                                "corp_code": "00126380",
                                "metric": "revenue",
                                "period_type": "annual",
                                "fiscal_year": 2024,
                                "amount": "130",
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )

            main(
                [
                    "growth-metrics",
                    "--input",
                    str(input_path),
                    "--output",
                    str(output_path),
                    "--recent-annual-periods",
                    "1",
                ]
            )

            payload = json.loads(output_path.read_text("utf-8"))

        self.assertEqual(payload["summary"]["growth_points"], 2)
        self.assertTrue(payload["filter"]["results"][0]["passed"])


if __name__ == "__main__":
    unittest.main()
