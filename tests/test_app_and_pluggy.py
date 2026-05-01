from __future__ import annotations

import io
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from app import UPLOAD_DIR, app
from services.pdf_parser import Transaction
from services.pluggy import PluggyClient


class UploadCleanupTests(unittest.TestCase):
    def test_upload_removes_saved_pdf_after_processing(self):
        saved_path = UPLOAD_DIR / "extrato.pdf"
        if saved_path.exists():
            saved_path.unlink()

        fake_sheets = Mock()
        fake_sheets.is_configured.return_value = True
        fake_sheets.append_transactions.return_value = 1
        fake_history = Mock()
        fake_history.record_import.return_value = {"id": "history-1"}

        with patch("app.parse_statement_pdf", return_value=[Transaction(description="Pix", amount=10.0, date="01/04/2026")]), \
            patch("app.get_google_sheets_service", return_value=fake_sheets), \
            patch("app.get_import_history_service", return_value=fake_history):
            with app.test_client() as client:
                response = client.post(
                    "/api/upload-pdf",
                    data={"pdf": (io.BytesIO(b"%PDF-1.4 fake"), "extrato.pdf")},
                    content_type="multipart/form-data",
                )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(saved_path.exists())

    def test_upload_returns_success_when_only_secondary_sheet_adjustment_warns(self):
        fake_sheets = Mock()
        fake_sheets.is_configured.return_value = True
        fake_sheets.append_transactions.return_value = 2
        fake_sheets.last_warnings = ["ABRIL: ajuste secundario"]
        fake_history = Mock()
        fake_history.record_import.return_value = {"id": "history-2"}

        with patch("app.parse_statement_pdf", return_value=[Transaction(description="Pix", amount=10.0, date="01/04/2026")]), \
            patch("app.get_google_sheets_service", return_value=fake_sheets), \
            patch("app.get_import_history_service", return_value=fake_history):
            with app.test_client() as client:
                response = client.post(
                    "/api/upload-pdf",
                    data={"pdf": (io.BytesIO(b"%PDF-1.4 fake"), "extrato.pdf")},
                    content_type="multipart/form-data",
                )

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertIn("Observacao", payload["message"])


class PluggyClientTests(unittest.TestCase):
    def test_sync_transactions_keeps_credit_as_income(self):
        client = PluggyClient("id", "secret", "item")
        client._authenticate = Mock(return_value="api-key")
        payload = {
            "results": [
                {"type": "CREDIT", "amount": 150.75, "description": "Recebimento", "date": "2026-04-20T12:00:00Z"},
                {"type": "DEBIT", "amount": 42.10, "description": "Pagamento", "date": "2026-04-20T12:00:00Z"},
            ]
        }

        fake_response = Mock()
        fake_response.json.return_value = payload
        fake_response.raise_for_status.return_value = None

        with patch("services.pluggy.requests.get", return_value=fake_response):
            transactions = client.sync_transactions()

        self.assertEqual(len(transactions), 2)
        self.assertEqual(transactions[0].kind, "income")
        self.assertEqual(transactions[0].amount, 150.75)
        self.assertEqual(transactions[1].kind, "expense")
        self.assertEqual(transactions[1].amount, 42.10)


if __name__ == "__main__":
    unittest.main()
