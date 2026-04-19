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
        start_row, end_row = 22, 92
        values = self._get_range_values(service, f"{sheet_name}!L{start_row}:Q{end_row}")
        self._fix_existing_dropdown_values(service, sheet_name, values, start_row)
        values = self._get_range_values(service, f"{sheet_name}!L{start_row}:Q{end_row}")

        existing_signatures = {_expense_signature(row) for row in values if row}
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

    def _fix_existing_dropdown_values(self, service, sheet_name: str, values: list[list[str]], start_row: int) -> None:
        updates: list[tuple[int, str, str]] = []
        row_number = start_row
        for row in values:
            if row and any(str(cell).strip() for cell in row):
                raw_category = _normalized_text(row[3] if len(row) > 3 else "")
                raw_payment = _normalized_text(row[4] if len(row) > 4 else "")
                raw_essential = _normalized_text(row[5] if len(row) > 5 else "")

                if raw_category in {"pix", "outros", "🧾 outros", "ðÿ§¾ outros"}:
                    updates.append((row_number, "O", "🧾 Outros"))
                if raw_payment in {"pix", "dinheiro / pix", "dinheiro/pix", "💸 dinheiro / pix", "ðÿ’¸ dinheiro / pix"}:
                    updates.append((row_number, "P", "💸 Dinheiro / Pix"))
                if raw_essential in {"sim", "nao", "não", "", "✔️", "âœ”ï¸"}:
                    updates.append((row_number, "Q", "✔️"))
            row_number += 1

        for row_number, column_letter, value in updates:
            service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{sheet_name}!{column_letter}{row_number}",
                valueInputOption="USER_ENTERED",
                body={"values": [[value]]},
            ).execute()

    def _write_incomes(self, service, month_name: str, transactions: list[Transaction]) -> int:
        if not transactions:
            return 0

        sheet_name = _normalize_sheet_name(month_name)
        start_row, end_row = 4, 13
        values = self._get_range_values(service, f"{sheet_name}!G{start_row}:I{end_row}")
        grouped = _group_income_sources(transactions)
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
        new_transactions = [transaction for transaction in grouped if _normalized_text(transaction.description) not in existing]
        batch = new_transactions[: max(0, end_row - next_row + 1)]
        if batch:
            service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{sheet_name}!G{next_row}:I{next_row + len(batch) - 1}",
                valueInputOption="USER_ENTERED",
                body={"values": [transaction.to_income_row() for transaction in batch]},
            ).execute()
            changed += len(batch)
        return changed

    def _get_range_values(self, service, target_range: str) -> list[list[str]]:
        response = service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=target_range,
        ).execute()
        return response.get("values", [])

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
