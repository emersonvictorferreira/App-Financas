from __future__ import annotations

import json
from collections import defaultdict
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

    def is_configured(self) -> bool:
        return bool(self.spreadsheet_id) and (bool(self.service_account_json) or self.credentials_path.exists())

    def append_transactions(self, transactions: Iterable[Transaction]) -> int:
        by_month: dict[str, dict[str, list[Transaction]]] = defaultdict(lambda: {"income": [], "expense": []})
        for transaction in transactions:
            month_name = _month_name_from_date(transaction.date)
            by_month[month_name][transaction.kind].append(transaction)

        total_rows = 0
        service = self._build_service()
        for month_name, grouped in by_month.items():
            total_rows += self._write_expenses(service, month_name, grouped["expense"])
            total_rows += self._write_incomes(service, month_name, grouped["income"])
        return total_rows

    def _write_expenses(self, service, month_name: str, transactions: list[Transaction]) -> int:
        if not transactions:
            return 0

        sheet_name = _normalize_sheet_name(month_name)
        start_row = self._find_expense_start_row(service, sheet_name)
        end_row = start_row + 160
        values = self._get_range_values(service, f"{sheet_name}!L{start_row}:Q{end_row}")

        existing_signatures = {_expense_signature(row) for row in values if _is_expense_row(row)}
        pending = [transaction for transaction in transactions if _expense_signature(transaction.to_expense_row()) not in existing_signatures]
        if not pending:
            return 0

        next_row = _find_next_empty_row(values, start_row)
        batch = pending[: max(0, end_row - next_row + 1)]
        if not batch:
            return 0

        service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"{sheet_name}!L{next_row}:Q{next_row + len(batch) - 1}",
            valueInputOption="USER_ENTERED",
            body={"values": [transaction.to_expense_row() for transaction in batch]},
        ).execute()
        return len(batch)

    def _write_incomes(self, service, month_name: str, transactions: list[Transaction]) -> int:
        if not transactions:
            return 0

        sheet_name = _normalize_sheet_name(month_name)
        grouped = _group_income_sources(transactions)
        start_row = 4
        total_row = self._find_income_total_row(service, sheet_name)
        end_row = total_row - 1

        values = self._get_range_values(service, f"{sheet_name}!G{start_row}:I{end_row}")
        existing = _existing_income_rows(values, start_row)

        new_transactions = [transaction for transaction in grouped if _normalized_text(transaction.description) not in existing]
        required_rows = len(existing) + len(new_transactions)
        available_rows = max(0, end_row - start_row + 1)
        if required_rows > available_rows:
            rows_to_insert = required_rows - available_rows
            self._insert_rows_before(service, sheet_name, total_row, rows_to_insert)
            self._copy_income_row_layout(service, sheet_name, end_row, total_row, rows_to_insert)
            total_row += rows_to_insert
            end_row = total_row - 1
            values = self._get_range_values(service, f"{sheet_name}!G{start_row}:I{end_row}")
            existing = _existing_income_rows(values, start_row)

        changed = 0
        for transaction in grouped:
            row_info = existing.get(_normalized_text(transaction.description))
            if row_info is not None:
                row_number, existing_amount = row_info
                if transaction.amount > existing_amount:
                    service.spreadsheets().values().update(
                        spreadsheetId=self.spreadsheet_id,
                        range=f"{sheet_name}!G{row_number}:I{row_number}",
                        valueInputOption="USER_ENTERED",
                        body={"values": [transaction.to_income_row()]},
                    ).execute()
                    changed += 1

        next_row = _find_next_empty_row(values, start_row)
        batch = new_transactions[: max(0, end_row - next_row + 1)]
        if batch:
            service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{sheet_name}!G{next_row}:I{next_row + len(batch) - 1}",
                valueInputOption="USER_ENTERED",
                body={"values": [transaction.to_income_row() for transaction in batch]},
            ).execute()
            changed += len(batch)

        self._sync_income_total_formula(service, sheet_name, start_row, total_row)
        return changed

    def _get_range_values(self, service, target_range: str) -> list[list[str]]:
        response = service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=target_range,
        ).execute()
        return response.get("values", [])

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
            "descrição",
            "valor",
            "data",
            "categoria",
            "forma de pagamento",
            "essencial?",
        ]
        for row in values:
            normalized = [_normalized_text(value) for value in row[:6]]
            if normalized == expected_header:
                return row_number + 1
            row_number += 1
        raise ValueError(f"Nao foi possivel localizar o cabecalho de gastos na aba {sheet_name}.")

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
                        "endColumnIndex": 9,
                    },
                    "destination": {
                        "sheetId": sheet_id,
                        "startRowIndex": destination_row - 1,
                        "endRowIndex": destination_row - 1 + amount,
                        "startColumnIndex": 6,
                        "endColumnIndex": 9,
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

    def _sync_income_total_formula(self, service, sheet_name: str, start_row: int, total_row: int) -> None:
        service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"{sheet_name}!G{total_row}:I{total_row}",
            valueInputOption="USER_ENTERED",
            body={"values": [["TOTAL", "", f"=SUM(I{start_row}:I{total_row - 1})"]]},
        ).execute()

    def _get_sheet_id(self, service, sheet_name: str) -> int:
        response = service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
        for sheet in response.get("sheets", []):
            properties = sheet.get("properties", {})
            if properties.get("title") == sheet_name:
                return properties["sheetId"]
        raise ValueError(f"Nao foi possivel localizar a aba {sheet_name}.")

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

    def _build_service(self):
        if self.service_account_json:
            credentials = Credentials.from_service_account_info(json.loads(self.service_account_json), scopes=SCOPES)
        else:
            credentials = Credentials.from_service_account_file(str(self.credentials_path), scopes=SCOPES)
        return build("sheets", "v4", credentials=credentials)


def _find_next_empty_row(values: list[list[str]], start_row: int) -> int:
    row = start_row
    for row_values in values:
        first_value = row_values[0] if row_values else ""
        if not str(first_value).strip():
            return row
        row += 1
    return row


def _expense_signature(row: list[str | float]) -> tuple[str, str, str]:
    description = _normalized_text(row[0] if len(row) > 0 else "")
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


def _existing_income_rows(values: list[list[str]], start_row: int) -> dict[str, tuple[int, float]]:
    rows: dict[str, tuple[int, float]] = {}
    row_number = start_row
    for row in values:
        if row:
            description = _normalized_text(row[0] if len(row) > 0 else "")
            if description:
                amount = _as_float(row[2] if len(row) > 2 else 0)
                rows[description] = (row_number, amount)
        row_number += 1
    return rows


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


def _group_income_sources(transactions: list[Transaction]) -> list[Transaction]:
    grouped: dict[str, float] = defaultdict(float)
    exemplar: dict[str, Transaction] = {}
    for transaction in transactions:
        key = transaction.description
        grouped[key] += transaction.amount
        exemplar.setdefault(key, transaction)

    result: list[Transaction] = []
    for description, amount in grouped.items():
        source = exemplar[description]
        result.append(
            Transaction(
                description=description,
                amount=round(amount, 2),
                date=source.date,
                category=source.category,
                payment_method=source.payment_method,
                essential=source.essential,
                kind="income",
            )
        )
    return result


def _month_name_from_date(date_str: str) -> str:
    parsed = datetime.strptime(date_str, "%d/%m/%Y")
    return MONTH_NAMES[parsed.month]


def _normalize_sheet_name(month_name: str) -> str:
    if month_name == "MARCO":
        return "MARÇO"
    return month_name
