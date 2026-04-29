from __future__ import annotations

import unittest
from unittest.mock import Mock

from services.google_sheets import GoogleSheetsService, _aggregate_income_transactions, _merge_income_sources, _parse_br_date
from services.pdf_parser import Transaction


class GoogleSheetsServiceTests(unittest.TestCase):
    def setUp(self):
        self.service = GoogleSheetsService(spreadsheet_id="sheet-123", service_account_json='{"type":"service_account"}')
        self.api = Mock()
        self.values_api = Mock()
        self.spreadsheets_api = Mock()
        self.spreadsheets_api.values.return_value = self.values_api
        self.spreadsheets_api.get.return_value.execute.return_value = {
            "sheets": [{"properties": {"title": "ABRIL", "sheetId": 123}}]
        }
        self.spreadsheets_api.batchUpdate.return_value.execute.return_value = {}
        self.api.spreadsheets.return_value = self.spreadsheets_api

    def test_write_expenses_expands_when_block_is_full(self):
        existing_rows = [
            ["Pix para A", "10", "01/04/2026", "Cat", "Pix", "✔️"],
            ["Pix para B", "20", "02/04/2026", "Cat", "Pix", "✔️"],
        ]
        pending = [
            Transaction(description="Pix para C", amount=30.0, date="03/04/2026"),
            Transaction(description="Pix para D", amount=40.0, date="04/04/2026"),
        ]

        self.service._find_expense_start_row = Mock(return_value=50)
        self.service._get_range_values = Mock(return_value=existing_rows)
        self.service._insert_rows_before = Mock()
        self.service._get_sheet_row_count = Mock(return_value=51)
        self.service._normalize_expense_block_layout = Mock()
        self.values_api.update.return_value.execute.return_value = {}
        self.values_api.clear.return_value.execute.return_value = {}

        inserted = self.service._write_expenses(self.api, "ABRIL", pending)

        self.assertEqual(inserted, 2)
        self.service._insert_rows_before.assert_called()

    def test_write_expenses_deduplicates_existing_rows(self):
        existing_rows = [
            ["Pix para A", "10", "01/04/2026", "Cat", "Pix", "✔️"],
        ]
        duplicate = Transaction(description="Pix para A", amount=10.0, date="01/04/2026")

        self.service._find_expense_start_row = Mock(return_value=50)
        self.service._get_range_values = Mock(return_value=existing_rows)
        self.service._get_sheet_row_count = Mock(return_value=500)
        self.service._normalize_expense_block_layout = Mock()
        self.values_api.clear.return_value.execute.return_value = {}

        inserted = self.service._write_expenses(self.api, "ABRIL", [duplicate])

        self.assertEqual(inserted, 0)

    def test_aggregate_income_transactions_keeps_same_source_in_different_dates_separate(self):
        imported = [
            Transaction(description="Pix de SORIGINAL", amount=100.0, date="05/05/2026", kind="income"),
            Transaction(description="Pix de SORIGINAL", amount=150.0, date="20/05/2026", kind="income"),
        ]

        aggregated = _aggregate_income_transactions(imported)

        self.assertEqual(len(aggregated), 2)
        self.assertEqual(aggregated[0].description, "Pix de SORIGINAL - 05/05")
        self.assertEqual(aggregated[0].amount, 100.0)
        self.assertEqual(aggregated[1].description, "Pix de SORIGINAL - 20/05")
        self.assertEqual(aggregated[1].amount, 150.0)

    def test_merge_income_sources_matches_existing_row_with_embedded_date(self):
        existing = [
            Transaction(description="Pix de SORIGIN... - 05/05", amount=100.0, date="01/01/1900", kind="income"),
        ]
        imported = [
            Transaction(description="Pix de SORIGIN... - 05/05", amount=100.0, date="05/05/2026", kind="income"),
            Transaction(description="Pix de SORIGIN... - 20/05", amount=150.0, date="20/05/2026", kind="income"),
        ]

        merged = _merge_income_sources(existing, imported)

        self.assertEqual(len(merged), 2)
        self.assertEqual([row.description for row in merged], ["Pix de SORIGIN... - 05/05", "Pix de SORIGIN... - 20/05"])

    def test_parse_br_date_accepts_short_day_month(self):
        parsed = _parse_br_date("20/04")

        self.assertEqual(parsed.day, 20)
        self.assertEqual(parsed.month, 4)
        self.assertEqual(parsed.year, 1900)


if __name__ == "__main__":
    unittest.main()
