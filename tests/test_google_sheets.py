from __future__ import annotations

import unittest
from unittest.mock import Mock

from services.google_sheets import GoogleSheetsService
from services.pdf_parser import Transaction


class GoogleSheetsServiceTests(unittest.TestCase):
    def setUp(self):
        self.service = GoogleSheetsService(spreadsheet_id="sheet-123", service_account_json='{"type":"service_account"}')
        self.api = Mock()
        self.values_api = Mock()
        self.spreadsheets_api = Mock()
        self.spreadsheets_api.values.return_value = self.values_api
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


if __name__ == "__main__":
    unittest.main()
