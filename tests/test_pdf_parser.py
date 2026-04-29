from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from services.pdf_parser import parse_statement_pdf


class PdfParserTests(unittest.TestCase):
    def test_parses_inline_debit_amounts(self):
        fake_pdf = Path("fake.pdf")
        fake_lines = [
            "10 ABR 2026 Total de saidas - 46,90",
            "Compra no debito RESTAURANTE ESTACAO DA 46,90",
            "12 ABR 2026 Total de saidas - 12,90",
            "Compra no debito via NuPay EBW*Spotify 12,90",
        ]

        with patch("services.pdf_parser.read_pdf_lines", return_value=fake_lines):
            transactions = parse_statement_pdf(fake_pdf)

        self.assertEqual(len(transactions), 2)
        self.assertEqual(transactions[0].description, "Debito RESTAURANT...")
        self.assertEqual(transactions[0].amount, 46.90)
        self.assertEqual(transactions[0].payment_method, "💳 Débito")
        self.assertEqual(transactions[1].description, "Debito EBW*Spotify")
        self.assertEqual(transactions[1].amount, 12.90)


if __name__ == "__main__":
    unittest.main()
