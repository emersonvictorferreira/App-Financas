from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterable

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from services.pdf_parser import Transaction


MONTH_NAMES = {
    1: "JANEIRO",
    2: "FEVEREIRO",
    3: "MARCO",
    4: "ABRIL",
    5: "MAIO",
    6: "JUNHO",
    7: "JULHO",
    8: "AGOSTO",
    9: "SETEMBRO",
    10: "OUTUBRO",
    11: "NOVEMBRO",
    12: "DEZEMBRO",
}

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class GoogleSheetsService:
    def __init__(
        self,
        credentials_path: str = "",
        spreadsheet_id: str = "",
        range_template: str = "{MES}!L:Q",
        service_account_json: str = "",
    ) -> None:
        self.credentials_path = Path(credentials_path) if credentials_path else Path()
        self.spreadsheet_id = spreadsheet_id
        self.range_template = range_template
        self.service_account_json = service_account_json
        self._sheet_metadata_cache: dict[str, dict[str, int]] = {}
        self.last_warnings: list[str] = []

    def is_configured(self) -> bool:
        return bool(self.spreadsheet_id) and (bool(self.service_account_json) or self.credentials_path.exists())

    def append_transactions(self, transactions: Iterable[Transaction]) -> int:
        self._sheet_metadata_cache = {}
        self.last_warnings = []
        by_month: dict[str, dict[str, list[Transaction]]] = defaultdict(lambda: {"income": [], "expense": []})
        for transaction in transactions:
            month_name = _month_name_from_date(transaction.date)
            by_month[month_name][transaction.kind].append(transaction)

        total_rows = 0
        service = self._build_service()
        for month_name, grouped in by_month.items():
            total_rows += self._write_expenses(service, month_name, _sort_transactions(grouped["expense"]))
            total_rows += self._write_incomes(service, month_name, _sort_transactions(grouped["income"]))
            sheet_name = _normalize_sheet_name(month_name)
            try:
                self._sync_month_dashboard_formulas(service, sheet_name)
            except Exception as exc:
                self.last_warnings.append(f"{sheet_name}: {exc}")
        return total_rows

    def _write_expenses(self, service, month_name: str, transactions: list[Transaction]) -> int:
        sheet_name = _normalize_sheet_name(month_name)
        start_row = self._find_expense_start_row(service, sheet_name)
        preview_end_row = start_row + 399
        preview_range = f"{sheet_name}!L{start_row}:Q{preview_end_row}"
        values = self._get_range_values(service, preview_range)

        existing_transactions = _existing_expense_transactions(values)
        merged = _merge_expenses(existing_transactions, transactions)
        end_row = max(start_row + 160, start_row + len(merged) + 20)
        row_count = self._get_sheet_row_count(service, sheet_name)
        required_extra_rows = end_row - row_count
        if required_extra_rows > 0:
            self._insert_rows_before(service, sheet_name, row_count + 1, required_extra_rows)
        target_range = f"{sheet_name}!L{start_row}:Q{end_row}"
        batch = merged[: end_row - start_row + 1]

        self._clear_values(service, target_range)
        if batch:
            service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{sheet_name}!L{start_row}:N{start_row + len(batch) - 1}",
                valueInputOption="USER_ENTERED",
                body={"values": [[row[0], row[1], row[2]] for row in (transaction.to_expense_row() for transaction in batch)]},
            ).execute()
            self._normalize_expense_block_layout(service, sheet_name, start_row, start_row + len(batch) - 1)
            self._write_expense_dropdown_values(service, sheet_name, start_row, [transaction.to_expense_row() for transaction in batch])

        previous_signatures = {_expense_signature(transaction.to_expense_row()) for transaction in existing_transactions}
        current_signatures = {_expense_signature(transaction.to_expense_row()) for transaction in batch}
        return len(current_signatures - previous_signatures)

    def _write_incomes(self, service, month_name: str, transactions: list[Transaction]) -> int:
        sheet_name = _normalize_sheet_name(month_name)
        start_row = 4
        total_row = self._find_income_total_row(service, sheet_name)
        end_row = total_row - 1

        values = self._get_range_values(service, f"{sheet_name}!G{start_row}:I{end_row}")
        existing_transactions = _existing_income_transactions(values)
        imported_transactions = _aggregate_income_transactions(transactions)
        merged = _merge_income_sources(existing_transactions, imported_transactions)

        required_rows = len(merged)
        available_rows = max(0, end_row - start_row + 1)
        if required_rows > available_rows:
            rows_to_insert = required_rows - available_rows
            self._insert_rows_before(service, sheet_name, total_row, rows_to_insert)
            self._copy_income_row_layout(service, sheet_name, end_row, total_row, rows_to_insert)
            total_row += rows_to_insert
            end_row = total_row - 1

        target_range = f"{sheet_name}!G{start_row}:I{end_row}"
        self._clear_values(service, target_range)
        if merged:
            service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{sheet_name}!G{start_row}:I{start_row + len(merged) - 1}",
                valueInputOption="USER_ENTERED",
                body={"values": [transaction.to_income_row() for transaction in merged]},
            ).execute()
            self._normalize_income_block_layout(service, sheet_name, start_row, start_row + len(merged) - 1)

        self._sync_income_total_formula(service, sheet_name, start_row, total_row)

        previous_keys = {_income_source_key(transaction.description): transaction.amount for transaction in existing_transactions}
        current_keys = {_income_source_key(transaction.description): transaction.amount for transaction in merged}
        changed = 0
        for key, amount in current_keys.items():
            if key not in previous_keys or amount > previous_keys[key]:
                changed += 1
        return changed

    def _get_range_values(self, service, target_range: str) -> list[list[str]]:
        response = service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=target_range,
        ).execute()
        return response.get("values", [])

    def _clear_values(self, service, target_range: str) -> None:
        service.spreadsheets().values().clear(
            spreadsheetId=self.spreadsheet_id,
            range=target_range,
            body={},
        ).execute()

    def _find_income_total_row(self, service, sheet_name: str) -> int:
        values = self._get_range_values(service, f"{sheet_name}!G4:G200")
        row_number = 4
        for row in values:
            cell_value = _normalized_text(row[0] if row else "")
            if cell_value == "total":
                return row_number
            row_number += 1
        raise ValueError(f"Nao foi possivel localizar a linha TOTAL na aba {sheet_name}.")

    def _find_expense_start_row(self, service, sheet_name: str) -> int:
        values = self._get_range_values(service, f"{sheet_name}!L1:Q250")
        row_number = 1
        expected_header = [
            "descricao",
            "valor",
            "data",
            "categoria",
            "forma de pagamento",
            "essencial?",
        ]
        for row in values:
            normalized = [_strip_accents(_normalized_text(value)) for value in row[:6]]
            if normalized == expected_header:
                return row_number + 1
            row_number += 1
        raise ValueError(f"Nao foi possivel localizar o cabecalho de gastos na aba {sheet_name}.")

    def _find_expense_end_row(self, service, sheet_name: str, start_row: int) -> int:
        row_count = self._get_sheet_row_count(service, sheet_name)
        values = self._get_range_values(service, f"{sheet_name}!L{start_row}:Q{row_count}")
        row_number = start_row
        saw_expense = False
        for row in values:
            if _is_expense_row(row):
                saw_expense = True
            elif saw_expense and not row:
                return row_number - 1
            row_number += 1
        return row_count

    def _insert_rows_before(self, service, sheet_name: str, row_number: int, amount: int) -> None:
        if amount <= 0:
            return

        sheet_id = self._get_sheet_id(service, sheet_name)
        start_index = row_number - 1
        service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={
                "requests": [
                    {
                        "insertDimension": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "ROWS",
                                "startIndex": start_index,
                                "endIndex": start_index + amount,
                            },
                            "inheritFromBefore": True,
                        }
                    }
                ]
            },
        ).execute()

    def _copy_income_row_layout(self, service, sheet_name: str, template_row: int, destination_row: int, amount: int) -> None:
        if amount <= 0:
            return

        sheet_id = self._get_sheet_id(service, sheet_name)
        requests = [
            {
                "copyPaste": {
                    "source": {
                        "sheetId": sheet_id,
                        "startRowIndex": template_row - 1,
                        "endRowIndex": template_row,
                        "startColumnIndex": 6,
                        "endColumnIndex": 10,
                    },
                    "destination": {
                        "sheetId": sheet_id,
                        "startRowIndex": destination_row - 1,
                        "endRowIndex": destination_row - 1 + amount,
                        "startColumnIndex": 6,
                        "endColumnIndex": 10,
                    },
                    "pasteType": "PASTE_FORMAT",
                    "pasteOrientation": "NORMAL",
                }
            }
        ]

        pixel_size = self._get_row_height(service, sheet_name, template_row)
        if pixel_size is not None:
            requests.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": destination_row - 1,
                            "endIndex": destination_row - 1 + amount,
                        },
                        "properties": {"pixelSize": pixel_size},
                        "fields": "pixelSize",
                    }
                }
            )

        service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={"requests": requests},
        ).execute()

    def _copy_expense_row_layout(self, service, sheet_name: str, template_row: int, destination_row: int, amount: int) -> None:
        if amount <= 0:
            return

        sheet_id = self._get_sheet_id(service, sheet_name)
        requests = [
            {
                "copyPaste": {
                    "source": {
                        "sheetId": sheet_id,
                        "startRowIndex": template_row - 1,
                        "endRowIndex": template_row,
                        "startColumnIndex": 11,
                        "endColumnIndex": 17,
                    },
                    "destination": {
                        "sheetId": sheet_id,
                        "startRowIndex": destination_row - 1,
                        "endRowIndex": destination_row - 1 + amount,
                        "startColumnIndex": 11,
                        "endColumnIndex": 17,
                    },
                    "pasteType": "PASTE_NORMAL",
                    "pasteOrientation": "NORMAL",
                }
            },
            {
                "copyPaste": {
                    "source": {
                        "sheetId": sheet_id,
                        "startRowIndex": template_row - 1,
                        "endRowIndex": template_row,
                        "startColumnIndex": 11,
                        "endColumnIndex": 17,
                    },
                    "destination": {
                        "sheetId": sheet_id,
                        "startRowIndex": destination_row - 1,
                        "endRowIndex": destination_row - 1 + amount,
                        "startColumnIndex": 11,
                        "endColumnIndex": 17,
                    },
                    "pasteType": "PASTE_FORMAT",
                    "pasteOrientation": "NORMAL",
                }
            },
            {
                "copyPaste": {
                    "source": {
                        "sheetId": sheet_id,
                        "startRowIndex": template_row - 1,
                        "endRowIndex": template_row,
                        "startColumnIndex": 11,
                        "endColumnIndex": 17,
                    },
                    "destination": {
                        "sheetId": sheet_id,
                        "startRowIndex": destination_row - 1,
                        "endRowIndex": destination_row - 1 + amount,
                        "startColumnIndex": 11,
                        "endColumnIndex": 17,
                    },
                    "pasteType": "PASTE_DATA_VALIDATION",
                    "pasteOrientation": "NORMAL",
                }
            }
        ]

        pixel_size = self._get_row_height(service, sheet_name, template_row)
        if pixel_size is not None:
            requests.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": destination_row - 1,
                            "endIndex": destination_row - 1 + amount,
                        },
                        "properties": {"pixelSize": pixel_size},
                        "fields": "pixelSize",
                    }
                }
            )

        service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={"requests": requests},
        ).execute()

    def _normalize_income_block_layout(self, service, sheet_name: str, start_row: int, end_row: int) -> None:
        if end_row < start_row:
            return

        sheet_id = self._get_sheet_id(service, sheet_name)
        self._unmerge_income_rows(service, sheet_id, start_row, end_row)
        odd_template_row = start_row
        even_template_row = start_row + 1 if end_row > start_row else start_row

        requests = []
        for row_number in range(start_row, end_row + 1):
            template_row = odd_template_row if (row_number - start_row) % 2 == 0 else even_template_row
            requests.append(
                {
                    "copyPaste": {
                        "source": {
                            "sheetId": sheet_id,
                            "startRowIndex": template_row - 1,
                            "endRowIndex": template_row,
                            "startColumnIndex": 6,
                            "endColumnIndex": 10,
                        },
                        "destination": {
                            "sheetId": sheet_id,
                            "startRowIndex": row_number - 1,
                            "endRowIndex": row_number,
                            "startColumnIndex": 6,
                            "endColumnIndex": 10,
                        },
                        "pasteType": "PASTE_FORMAT",
                        "pasteOrientation": "NORMAL",
                    }
                }
            )

        requests.extend(
            [
                {
                "updateBorders": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": end_row - 1,
                        "endRowIndex": end_row,
                        "startColumnIndex": 6,
                        "endColumnIndex": 10,
                    },
                    "bottom": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {},
                    },
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row - 1,
                        "endRowIndex": end_row,
                        "startColumnIndex": 8,
                        "endColumnIndex": 10,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                        }
                    },
                    "fields": "userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment",
                }
            },
            ]
        )

        pixel_size = self._get_row_height(service, sheet_name, even_template_row)
        if pixel_size is not None:
            requests.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": start_row - 1,
                            "endIndex": end_row,
                        },
                        "properties": {"pixelSize": pixel_size},
                        "fields": "pixelSize",
                    }
                }
            )

        service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={"requests": requests},
        ).execute()
        self._remerge_income_rows(service, sheet_id, start_row, end_row)

    def _unmerge_income_rows(self, service, sheet_id: int, start_row: int, end_row: int) -> None:
        service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={
                "requests": [
                    {
                        "unmergeCells": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": start_row - 1,
                                "endRowIndex": end_row,
                                "startColumnIndex": 6,
                                "endColumnIndex": 10,
                            }
                        }
                    }
                ]
            },
        ).execute()

    def _remerge_income_rows(self, service, sheet_id: int, start_row: int, end_row: int) -> None:
        requests = []

        for row_number in range(start_row, end_row + 1):
            requests.append(
                {
                    "mergeCells": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": row_number - 1,
                            "endRowIndex": row_number,
                            "startColumnIndex": 6,
                            "endColumnIndex": 8,
                        },
                        "mergeType": "MERGE_ALL",
                    }
                }
            )
            requests.append(
                {
                    "mergeCells": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": row_number - 1,
                            "endRowIndex": row_number,
                            "startColumnIndex": 8,
                            "endColumnIndex": 10,
                        },
                        "mergeType": "MERGE_ALL",
                    }
                }
            )

        service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={"requests": requests},
        ).execute()

    def _normalize_expense_block_layout(self, service, sheet_name: str, start_row: int, end_row: int) -> None:
        if end_row < start_row:
            return

        sheet_id = self._get_sheet_id(service, sheet_name)
        first_template_row = start_row
        body_template_row = start_row + 1 if end_row > start_row else start_row
        existing_values = self._get_range_values(service, f"{sheet_name}!L{start_row}:Q{end_row}")

        requests = []
        for row_number in range(start_row, end_row + 1):
            template_row = first_template_row if (row_number - start_row) % 2 == 0 else body_template_row
            requests.append(
                {
                    "copyPaste": {
                        "source": {
                            "sheetId": sheet_id,
                            "startRowIndex": template_row - 1,
                            "endRowIndex": template_row,
                            "startColumnIndex": 11,
                            "endColumnIndex": 17,
                        },
                        "destination": {
                            "sheetId": sheet_id,
                            "startRowIndex": row_number - 1,
                            "endRowIndex": row_number,
                            "startColumnIndex": 11,
                            "endColumnIndex": 17,
                        },
                        "pasteType": "PASTE_FORMAT",
                        "pasteOrientation": "NORMAL",
                    }
                }
            )

        requests.extend(
            [
                {
                "updateBorders": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": end_row - 1,
                        "endRowIndex": end_row,
                        "startColumnIndex": 11,
                        "endColumnIndex": 17,
                    },
                    "bottom": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {},
                    },
                }
                },
            ]
        )

        pixel_size = self._get_row_height(service, sheet_name, body_template_row)
        if pixel_size is not None:
            requests.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": start_row - 1,
                            "endIndex": end_row,
                        },
                        "properties": {"pixelSize": pixel_size},
                        "fields": "pixelSize",
                    }
                }
            )

        service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={"requests": requests},
        ).execute()
        self._apply_expense_dropdown_validations(service, sheet_id, start_row, end_row)

        if existing_values:
            service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{sheet_name}!L{start_row}:N{start_row + len(existing_values) - 1}",
                valueInputOption="USER_ENTERED",
                body={"values": [[row[0] if len(row) > 0 else "", row[1] if len(row) > 1 else "", row[2] if len(row) > 2 else ""] for row in existing_values]},
            ).execute()
            self._write_expense_dropdown_values(service, sheet_name, start_row, existing_values)

    def _write_expense_dropdown_values(self, service, sheet_name: str, start_row: int, rows: list[list[str | float]]) -> None:
        if not rows:
            return

        sheet_id = self._get_sheet_id(service, sheet_name)
        request_rows = []
        for row in rows:
            category = _canonical_expense_category(str(row[3] if len(row) > 3 else ""))
            payment_method = _canonical_payment_method(str(row[4] if len(row) > 4 else ""))
            essential = _canonical_essential(str(row[5] if len(row) > 5 else ""))
            request_rows.append(
                {
                    "values": [
                        {"userEnteredValue": {"stringValue": category}},
                        {"userEnteredValue": {"stringValue": payment_method}},
                        {"userEnteredValue": {"stringValue": essential}},
                    ]
                }
            )

        service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={
                "requests": [
                    {
                        "updateCells": {
                            "rows": request_rows,
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": start_row - 1,
                                "endRowIndex": start_row - 1 + len(request_rows),
                                "startColumnIndex": 14,
                                "endColumnIndex": 17,
                            },
                            "fields": "userEnteredValue",
                        }
                    }
                ]
            },
        ).execute()

    def _apply_expense_dropdown_validations(self, service, sheet_id: int, start_row: int, end_row: int) -> None:
        service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={
                "requests": [
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": start_row - 1,
                                "endRowIndex": end_row,
                                "startColumnIndex": 14,
                                "endColumnIndex": 15,
                            },
                            "cell": {
                                "dataValidation": {
                                    "condition": {
                                        "type": "ONE_OF_RANGE",
                                        "values": [{"userEnteredValue": "='UTILITÁRIO'!$H$31:$H$44"}],
                                    },
                                    "strict": True,
                                    "showCustomUi": True,
                                }
                            },
                            "fields": "dataValidation",
                        }
                    },
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": start_row - 1,
                                "endRowIndex": end_row,
                                "startColumnIndex": 15,
                                "endColumnIndex": 16,
                            },
                            "cell": {
                                "dataValidation": {
                                    "condition": {
                                        "type": "ONE_OF_RANGE",
                                        "values": [{"userEnteredValue": "='UTILITÁRIO'!$E$31:$E$35"}],
                                    },
                                    "strict": True,
                                    "showCustomUi": True,
                                }
                            },
                            "fields": "dataValidation",
                        }
                    },
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": start_row - 1,
                                "endRowIndex": end_row,
                                "startColumnIndex": 16,
                                "endColumnIndex": 17,
                            },
                            "cell": {
                                "dataValidation": {
                                    "condition": {
                                        "type": "ONE_OF_LIST",
                                        "values": [{"userEnteredValue": "✔️"}, {"userEnteredValue": "❌"}],
                                    },
                                    "strict": True,
                                    "showCustomUi": True,
                                }
                            },
                            "fields": "dataValidation",
                        }
                    },
                ]
            },
        ).execute()

    def _sync_income_total_formula(self, service, sheet_name: str, start_row: int, total_row: int) -> None:
        service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"{sheet_name}!G{total_row}:I{total_row}",
            valueInputOption="USER_ENTERED",
            body={"values": [["TOTAL", "", f"=SUM(I{start_row}:I{total_row - 1})"]]},
        ).execute()

    def _sync_month_dashboard_formulas(self, service, sheet_name: str) -> None:
        income_total_row = self._find_income_total_row(service, sheet_name)
        self._sync_income_total_formula(service, sheet_name, 4, income_total_row)
        expense_start_row = self._find_expense_start_row(service, sheet_name)
        expense_end_row = self._find_expense_end_row(service, sheet_name, expense_start_row)
        fixed_row = self._find_label_row(service, sheet_name, "G", "gastos fixos", 16, 200)
        variable_row = self._find_label_row(service, sheet_name, "G", "gastos variaveis", 16, 200)
        expense_total_row = self._find_label_row(service, sheet_name, "G", "total", fixed_row, 200, occurrence=1)
        investment_total_row = self._find_label_row(service, sheet_name, "G", "total reservado esse mes", 16, 200)

        card_updates = [f"=I{income_total_row}", f"=I{expense_total_row}", f"=I{investment_total_row}", "=SUM(B9-C9-D9)"]

        service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"{sheet_name}!B9:E9",
            valueInputOption="USER_ENTERED",
            body={"values": [card_updates]},
        ).execute()
        service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"{sheet_name}!I{fixed_row}",
            valueInputOption="USER_ENTERED",
            body={"values": [["=0"]]},
        ).execute()
        service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"{sheet_name}!I{variable_row}",
            valueInputOption="USER_ENTERED",
            body={"values": [[f"=SUM(M{expense_start_row}:M{expense_end_row})"]]},
        ).execute()
        service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"{sheet_name}!I{expense_total_row}",
            valueInputOption="USER_ENTERED",
            body={"values": [[f"=SUM(I{fixed_row}:I{variable_row})"]]},
        ).execute()

    def _find_label_row(
        self,
        service,
        sheet_name: str,
        column_letter: str,
        label: str,
        start_row: int,
        end_row: int,
        occurrence: int = 1,
    ) -> int:
        values = self._get_range_values(service, f"{sheet_name}!{column_letter}{start_row}:{column_letter}{end_row}")
        normalized_target = _strip_accents(_normalized_text(label))
        matches = 0
        for offset, row in enumerate(values):
            cell_value = row[0] if row else ""
            normalized_value = _strip_accents(_normalized_text(cell_value))
            if normalized_value == normalized_target:
                matches += 1
                if matches == occurrence:
                    return start_row + offset
        raise ValueError(f"Nao foi possivel localizar a linha '{label}' na aba {sheet_name}.")

    def _get_sheet_id(self, service, sheet_name: str) -> int:
        metadata = self._get_sheet_metadata(service, sheet_name)
        return metadata["sheetId"]

    def _get_row_height(self, service, sheet_name: str, row_number: int) -> int | None:
        response = service.spreadsheets().get(
            spreadsheetId=self.spreadsheet_id,
            ranges=[f"{sheet_name}!G{row_number}:I{row_number}"],
            fields="sheets(data(rowMetadata(pixelSize),startRow),properties(title))",
        ).execute()
        for sheet in response.get("sheets", []):
            if sheet.get("properties", {}).get("title") != sheet_name:
                continue
            data = sheet.get("data", [])
            if not data:
                continue
            row_metadata = data[0].get("rowMetadata", [])
            if not row_metadata:
                continue
            return row_metadata[0].get("pixelSize")
        return None

    def _get_sheet_row_count(self, service, sheet_name: str) -> int:
        metadata = self._get_sheet_metadata(service, sheet_name)
        return metadata.get("rowCount", 1000)

    def _get_sheet_metadata(self, service, sheet_name: str) -> dict[str, int]:
        cached = self._sheet_metadata_cache.get(sheet_name)
        if cached is not None:
            return cached

        response = service.spreadsheets().get(
            spreadsheetId=self.spreadsheet_id,
            fields="sheets(properties(sheetId,title,gridProperties(rowCount)))",
        ).execute()
        for sheet in response.get("sheets", []):
            properties = sheet.get("properties", {})
            title = properties.get("title")
            if not title:
                continue
            metadata = {
                "sheetId": properties.get("sheetId", 0),
                "rowCount": properties.get("gridProperties", {}).get("rowCount", 1000),
            }
            self._sheet_metadata_cache[title] = metadata

        if sheet_name not in self._sheet_metadata_cache:
            raise ValueError(f"Nao foi possivel localizar a aba {sheet_name}.")
        return self._sheet_metadata_cache[sheet_name]

    def _build_service(self):
        if self.service_account_json:
            credentials = Credentials.from_service_account_info(json.loads(self.service_account_json), scopes=SCOPES)
        else:
            credentials = Credentials.from_service_account_file(str(self.credentials_path), scopes=SCOPES)
        return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def _expense_signature(row: list[str | float]) -> tuple[str, str, str]:
    description = _expense_description_key(row[0] if len(row) > 0 else "")
    amount = _normalized_amount(row[1] if len(row) > 1 else "")
    date = _normalized_text(row[2] if len(row) > 2 else "")
    return description, amount, date


def _is_expense_row(row: list[str | float]) -> bool:
    if len(row) < 3:
        return False
    description = _normalized_text(row[0])
    date = str(row[2] or "").strip()
    if not description or not _looks_like_date(date):
        return False
    try:
        _as_float(row[1])
    except ValueError:
        return False
    return True


def _existing_expense_transactions(values: list[list[str]]) -> list[Transaction]:
    transactions: list[Transaction] = []
    for row in values:
        if not _is_expense_row(row):
            continue
        transactions.append(
            Transaction(
                description=str(row[0]).strip(),
                amount=_as_float(row[1]),
                date=str(row[2]).replace("'", "").strip(),
                category=_canonical_expense_category(str(row[3]).strip() if len(row) > 3 else ""),
                payment_method=_canonical_payment_method(str(row[4]).strip() if len(row) > 4 else ""),
                essential=_canonical_essential(str(row[5]).strip() if len(row) > 5 else ""),
                kind="expense",
            )
        )
    return transactions


def _existing_income_transactions(values: list[list[str]]) -> list[Transaction]:
    transactions: list[Transaction] = []
    for row in values:
        if not row:
            continue
        description = str(row[0]).strip() if len(row) > 0 else ""
        if not description:
            continue
        amount = _as_float(row[2] if len(row) > 2 else 0)
        embedded_date = _extract_income_display_date(description)
        transactions.append(
            Transaction(
                description=description,
                amount=amount,
                date=embedded_date or "01/01/1900",
                kind="income",
            )
        )
    return transactions


def _normalized_text(value) -> str:
    return " ".join(str(value or "").replace("'", "").split()).strip().lower()


def _normalized_amount(value) -> str:
    return f"{_as_float(value):.2f}"


def _as_float(value) -> float:
    if isinstance(value, (int, float)):
        return round(float(value), 2)

    text = str(value or "").strip()
    if not text:
        return 0.0
    text = text.replace("R$", "").replace(".", "").replace(",", ".").strip()
    return round(float(text), 2)


def _looks_like_date(value: str) -> bool:
    try:
        datetime.strptime(value.replace("'", ""), "%d/%m/%Y")
        return True
    except ValueError:
        return False


def _merge_expenses(existing_transactions: list[Transaction], imported_transactions: list[Transaction]) -> list[Transaction]:
    grouped_existing: dict[tuple[str, str, str], list[tuple[Transaction, str]]] = defaultdict(list)
    grouped_imported: dict[tuple[str, str, str], list[tuple[Transaction, str]]] = defaultdict(list)

    for transaction in existing_transactions:
        normalized_transaction = Transaction(
            description=_canonical_expense_description(transaction.description),
            amount=transaction.amount,
            date=transaction.date,
            category=transaction.category,
            payment_method=transaction.payment_method,
            essential=transaction.essential,
            kind=transaction.kind,
        )
        signature = _expense_signature(normalized_transaction.to_expense_row())
        grouped_existing[signature].append((normalized_transaction, _normalized_text(transaction.description)))

    for transaction in imported_transactions:
        normalized_transaction = Transaction(
            description=_canonical_expense_description(transaction.description),
            amount=transaction.amount,
            date=transaction.date,
            category=transaction.category,
            payment_method=transaction.payment_method,
            essential=transaction.essential,
            kind=transaction.kind,
        )
        signature = _expense_signature(normalized_transaction.to_expense_row())
        grouped_imported[signature].append((normalized_transaction, _normalized_text(transaction.description)))

    merged: list[Transaction] = []
    for signature in grouped_existing.keys() | grouped_imported.keys():
        entries = grouped_existing.get(signature, []) + grouped_imported.get(signature, [])
        exemplar = max((transaction for transaction, _raw in entries), key=lambda transaction: _expense_description_priority(transaction.description))
        imported_counts = Counter(raw_description for _transaction, raw_description in grouped_imported.get(signature, []))
        existing_counts = Counter(raw_description for _transaction, raw_description in grouped_existing.get(signature, []))
        if imported_counts:
            count = max(imported_counts.values())
        else:
            count = min(max(existing_counts.values(), default=0), 1)
        merged.extend([exemplar] * count)
    return _sort_transactions(merged)


def _aggregate_income_transactions(transactions: list[Transaction]) -> list[Transaction]:
    normalized: list[Transaction] = []
    for transaction in transactions:
        normalized.append(
            Transaction(
                description=_income_display_description(transaction.description, transaction.date),
                amount=transaction.amount,
                date=transaction.date,
                category=transaction.category,
                payment_method=transaction.payment_method,
                essential=transaction.essential,
                kind="income",
            )
        )

    return sorted(normalized, key=lambda transaction: (_parse_br_date(transaction.date), _normalized_text(transaction.description), transaction.amount))


def _merge_income_sources(existing_transactions: list[Transaction], imported_transactions: list[Transaction]) -> list[Transaction]:
    existing_grouped: dict[tuple[str, str], list[Transaction]] = defaultdict(list)
    imported_grouped: dict[tuple[str, str], list[Transaction]] = defaultdict(list)

    for transaction in existing_transactions:
        key = _income_merge_key(transaction)
        existing_grouped[key].append(
            Transaction(
                description=_normalize_existing_income_description(transaction),
                amount=transaction.amount,
                date=transaction.date,
                category=transaction.category,
                payment_method=transaction.payment_method,
                essential=transaction.essential,
                kind="income",
            )
        )

    for transaction in imported_transactions:
        imported_grouped[_income_merge_key(transaction)].append(transaction)

    merged: list[Transaction] = []
    for key in existing_grouped.keys() | imported_grouped.keys():
        if imported_grouped.get(key):
            merged.extend(imported_grouped[key])
        else:
            merged.extend(existing_grouped[key])

    return sorted(merged, key=lambda transaction: (_parse_br_date(transaction.date), _normalized_text(transaction.description), transaction.amount))


def _month_name_from_date(date_str: str) -> str:
    parsed = datetime.strptime(date_str, "%d/%m/%Y")
    return MONTH_NAMES[parsed.month]


def _normalize_sheet_name(month_name: str) -> str:
    if month_name == "MARCO":
        return "MAR\u00c7O"
    return month_name


def _sort_transactions(transactions: list[Transaction]) -> list[Transaction]:
    return sorted(
        transactions,
        key=lambda transaction: (_parse_br_date(transaction.date), _normalized_text(transaction.description), transaction.amount),
    )


def _parse_br_date(value: str) -> datetime:
    cleaned = str(value or "").replace("'", "").strip()
    for fmt in ("%d/%m/%Y", "%d/%m"):
        try:
            parsed = datetime.strptime(cleaned, fmt)
            if fmt == "%d/%m":
                return parsed.replace(year=1900)
            return parsed
        except ValueError:
            continue
    raise ValueError(f"Data invalida no formato brasileiro: {value}")


def _income_source_key(value: str) -> str:
    return _strip_accents(_normalized_text(_canonical_income_name(value)))


def _canonical_expense_category(value: str) -> str:
    normalized = _strip_accents(_normalized_text(value))
    mapping = {
        "supermercado": "🛒 Supermercado",
        "alimentacao": "🍔 Alimentação",
        "transporte": "🚗 Transporte",
        "lazer": "🎉 Lazer",
        "gastos pessoais": "💆‍♂️ Gastos Pessoais",
        "saude e bem estar": "🩺 Saúde e bem-estar",
        "presentes": "🎁 Presentes",
        "pets": "🐾 Pets",
        "moradia": "🏠 Moradia",
        "assinaturas": "📺 Assinaturas",
        "servicos domesticos": "⚡ Serviços domésticos",
        "parcelamentos": "💳 Parcelamentos",
        "mensalidades": "💪 Mensalidades",
        "outros": "🧾 Outros",
    }
    return mapping.get(normalized, "🧾 Outros")


def _canonical_payment_method(value: str) -> str:
    normalized = _strip_accents(_normalized_text(value))
    mapping = {
        "dinheiro / pix": "💸 Dinheiro / Pix",
        "credito": "💳 Crédito",
        "debito": "💳 Débito",
        "vale": "🎟️ Vale",
        "boleto": "💲 Boleto",
    }
    return mapping.get(normalized, "💸 Dinheiro / Pix")


def _canonical_essential(value: str) -> str:
    text = str(value or "").strip()
    if text in {"❌", "NAO", "NÃO", "nao", "não", "x"}:
        return "❌"
    return "✔️"


def _expense_description_key(value: str) -> str:
    return _strip_accents(_normalized_text(_canonical_expense_description(str(value or ""))))


def _canonical_expense_description(value: str) -> str:
    text = " ".join(str(value or "").split()).strip()
    lowered = _strip_accents(_normalized_text(text))

    replacements = {
        "pix para game hubstore": "Pix para GAME HUBSTORE",
        "pix para game": "Pix para GAME HUBSTORE",
        "pix para shpp brasil": "Pix para SHPP BRASIL",
        "pix para shpp": "Pix para SHPP BRASIL",
        "pix para ng celulares": "Pix para NG CELULARES",
        "pix para ng": "Pix para NG CELULARES",
        "pix para lucieda marques": "Pix para LUCIEDA MARQUES",
        "pix para lucieda": "Pix para LUCIEDA MARQUES",
        "pix para ryan oliveira": "Pix para Ryan Oliveira",
        "pix para ryan oli": "Pix para Ryan Oliveira",
        "pix para edson roberto": "Pix para Edson Roberto",
        "pix para edson ro": "Pix para Edson Roberto",
        "pix para jose guilherme": "Pix para Jose Guilherme",
        "pix para jose gui": "Pix para Jose Guilherme",
        "pix para pix marketplace": "Pix para PIX Marketplace",
        "pix para mercado pago": "Pix para Mercado Pago",
    }
    for source, target in replacements.items():
        if lowered == source or lowered.startswith(f"{source} "):
            return target

    return text


def _expense_description_priority(value: str) -> tuple[int, int]:
    text = str(value or "")
    return (0 if "..." in text else 1, len(text))


def _strip_accents(value: str) -> str:
    translation_table = str.maketrans(
        "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ",
        "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC",
    )
    return value.translate(translation_table)


def _canonical_income_description(value: str) -> str:
    return _canonical_income_name(value)


def _income_display_description(value: str, date: str) -> str:
    base = _canonical_income_description(value)
    return _limit_income_text(f"{base} - {_display_income_date(date)}", 24)


def _normalize_existing_income_description(transaction: Transaction) -> str:
    embedded_date = _extract_income_display_date(transaction.description)
    if embedded_date:
        return _limit_income_text(f"{_canonical_income_description(transaction.description)} - {embedded_date}", 24)
    if transaction.date and transaction.date != "01/01/1900":
        return _income_display_description(transaction.description, transaction.date)
    return _limit_income_text(_canonical_income_description(transaction.description), 24)


def _income_merge_key(transaction: Transaction) -> tuple[str, str]:
    extracted = _extract_income_display_date(transaction.description)
    if extracted:
        date_key = extracted
    elif transaction.date and transaction.date != "01/01/1900":
        date_key = _display_income_date(transaction.date)
    else:
        date_key = "sem-data"
    return _income_source_key(transaction.description), date_key


def _extract_income_display_date(description: str) -> str | None:
    match = re.search(r"(?:\((\d{2})/(\d{2})\)|-\s*(\d{2})/(\d{2}))\s*$", str(description or "").strip())
    if not match:
        return None
    day = match.group(1) or match.group(3)
    month = match.group(2) or match.group(4)
    return f"{day}/{month}"


def _display_income_date(date: str) -> str:
    parsed = _parse_br_date(date)
    return parsed.strftime("%d/%m")


def _limit_income_text(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text

    suffix_match = re.search(r"\s(?:\(\d{2}/\d{2}\)|-\s\d{2}/\d{2})\s*$", text)
    if not suffix_match:
        return _limit_text(text, max_len)

    suffix = suffix_match.group(0)
    base = text[: -len(suffix)].rstrip()
    available = max_len - len(suffix)
    if available <= 3:
        return _limit_text(text, max_len)
    return base[: available - 3].rstrip() + "..." + suffix


def _canonical_income_name(value: str) -> str:
    raw = _strip_accents(_normalized_text(value))
    raw = re.sub(r"(?:\(\d{2}/\d{2}\)|-\s*\d{2}/\d{2})\s*$", "", raw)
    raw = re.sub(r"^pix de\s+", "", raw)
    raw = re.sub(r"^transferencia recebida\s*", "", raw)
    raw = re.sub(r"^transferencia rec(?:ebida)?\s*", "", raw)
    raw = re.sub(r"^pelo\s+", "", raw)
    raw = re.sub(r"\b(recebida|recebido|pix|ted|doc)\b", " ", raw)
    raw = re.sub(r"[^a-z0-9]+", " ", raw)
    raw = " ".join(raw.split())

    alias_map = {
        "bit corretora": "BIT",
        "direct bh": "DIRECT",
        "direct": "DIRECT",
        "emerson victor": "EMERSON",
        "emerson": "EMERSON",
        "fernando mont": "Fernando",
        "fernando": "Fernando",
        "future tecnologia": "FUTURE",
        "future": "FUTURE",
        "nexabet recebi": "NEXABET",
        "nexabet": "NEXABET",
        "nexumpay": "NEXUMPAY",
        "nexus solucoes": "NEXUS",
        "nexus": "NEXUS",
        "royal crest": "ROYAL",
        "royal": "ROYAL",
        "rvls compre": "RVLS",
        "rvls": "RVLS",
        "soriginal": "SORIGINAL",
        "python tecnologia": "Typhon",
        "typhon": "Typhon",
        "univebet gaming": "Univebet",
        "univebet": "Univebet",
        "phoenix gaming": "Phoenix",
        "phoenix ga": "Phoenix",
        "phoenix": "Phoenix",
        "smart": "Smart Cluster",
        "smart cluster": "Smart Cluster",
        "r torres": "R Torres",
        "x vit": "X Vit",
        "gdsp technology": "GDSP",
        "gdsp": "GDSP",
        "jrr intermediacao": "JRR",
        "jrr": "JRR",
        "go tecnologia": "GO",
        "go": "GO",
        "verdata tecnol": "Verdata",
        "verdata": "Verdata",
        "rlopes10": "Rlopes10",
        "p": "PD",
        "p d": "PD",
        "sandra vilela": "Sandra",
        "sandra": "Sandra",
    }
    for source, target in alias_map.items():
        if raw == source or raw.startswith(f"{source} "):
            return target

    words = raw.split()
    if not words:
        return "Receita"

    if len(words) >= 2:
        pair = " ".join(words[:2])
        keep_two_words = {"smart cluster", "r torres", "x vit"}
        if pair in keep_two_words:
            return " ".join(word.capitalize() if len(word) > 3 else word.upper() for word in words[:2])

    first = words[0]
    if len(first) <= 4:
        return first.upper()
    return first.capitalize()
