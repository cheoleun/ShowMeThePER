import unittest

from show_me_the_per.cli import parse_corp_code_args


class CliTests(unittest.TestCase):
    def test_parse_corp_code_args_accepts_repeated_and_comma_separated_values(
        self,
    ) -> None:
        self.assertEqual(
            parse_corp_code_args(["00126380, 00434003", "12345678"]),
            ["00126380", "00434003", "12345678"],
        )


if __name__ == "__main__":
    unittest.main()
