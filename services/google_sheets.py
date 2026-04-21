from __future__ import annotations

import json
import re
import unicodedata
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
            total_rows += self._write_expenses(service, month_name, _sort_transactions(grouped["expense"]))
            total_rows += self._write_incomes(service, month_name, _sort_transactions(grouped["income"]))
        return total_rows

    def _write_expenses(self, service, month_name: str, transactions: list[Transaction]) -> int:
        sheet_name = _normalize_sheet_name(month_name)
        start_row = self._find_expense_start_row(service, sheet_name)
        end_row = start_row + 160
        target_range = f"{sheet_name}!L{start_row}:Q{end_row}"
        values = self._get_range_values(service, target_range)

        existing_transactions = _existing_expense_transactions(values)
        merged = _merge_expenses(existing_transactions + transactions)
        batch = merged[: end_row - start_row + 1]

        self._clear_values(service, target_range)
        if batch:
            service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{sheet_name}!L{start_row}:Q{start_row + len(batch) - 1}",
                valueInputOption="USER_ENTERED",
                body={"values": [transaction.to_expense_row() for transaction in batch]},
            ).execute()

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
                category=str(row[3]).strip() if len(row) > 3 else "Outros",
                payment_method=str(row[4]).strip() if len(row) > 4 else "Dinheiro / Pix",
                essential=str(row[5]).strip() if len(row) > 5 else "SIM",
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
        transactions.append(
            Transaction(
                description=description,
                amount=amount,
                date="01/01/1900",
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


def _merge_expenses(transactions: list[Transaction]) -> list[Transaction]:
    by_signature: dict[tuple[str, str, str], Transaction] = {}
    for transaction in transactions:
        signature = _expense_signature(transaction.to_expense_row())
        by_signature.setdefault(signature, transaction)
    return _sort_transactions(list(by_signature.values()))


def _aggregate_income_transactions(transactions: list[Transaction]) -> list[Transaction]:
    grouped: dict[str, Transaction] = {}
    for transaction in transactions:
        key = _income_source_key(transaction.description)
        canonical_description = _canonical_income_description(transaction.description)
        current = grouped.get(key)
        if current is None:
            grouped[key] = Transaction(
                description=canonical_description,
                amount=transaction.amount,
                date=transaction.date,
                category=transaction.category,
                payment_method=transaction.payment_method,
                essential=transaction.essential,
                kind="income",
            )
            continue

        grouped[key] = Transaction(
            description=current.description,
            amount=round(current.amount + transaction.amount, 2),
            date=min(current.date, transaction.date, key=_parse_br_date),
            category=current.category,
            payment_method=current.payment_method,
            essential=current.essential,
            kind="income",
        )

    return sorted(grouped.values(), key=lambda transaction: _normalized_text(transaction.description))


def _merge_income_sources(existing_transactions: list[Transaction], imported_transactions: list[Transaction]) -> list[Transaction]:
    grouped: dict[str, Transaction] = {}

    for transaction in existing_transactions:
        key = _income_source_key(transaction.description)
        grouped[key] = Transaction(
            description=_canonical_income_description(transaction.description),
            amount=transaction.amount,
            date=transaction.date,
            category=transaction.category,
            payment_method=transaction.payment_method,
            essential=transaction.essential,
            kind="income",
        )

    for transaction in imported_transactions:
        key = _income_source_key(transaction.description)
        current = grouped.get(key)
        if current is None or transaction.amount >= current.amount:
            grouped[key] = transaction

    return sorted(grouped.values(), key=lambda transaction: _normalized_text(transaction.description))


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
    return datetime.strptime(value, "%d/%m/%Y")


def _income_source_key(value: str) -> str:
    normalized = _strip_accents(_normalized_text(value))
    normalized = re.sub(r"^pix de\s+", "", normalized)
    normalized = re.sub(r"^transferencia recebida\s*", "", normalized)
    normalized = re.sub(r"^transferencia rec(?:ebida)?\s*", "", normalized)
    normalized = re.sub(r"\b(recebida|recebido|pix|ted|doc)\b", " ", normalized)
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return " ".join(normalized.split())


def _strip_accents(value: str) -> str:
    return "".join(char for char in unicodedata.normalize("NFKD", value) if not unicodedata.combining(char))


def _canonical_income_description(value: str) -> str:
    raw = _strip_accents(_normalized_text(value))
    raw = re.sub(r"^pix de\s+", "", raw)
    raw = re.sub(r"^transferencia recebida\s*", "", raw)
    raw = re.sub(r"^transferencia rec(?:ebida)?\s*", "", raw)
    raw = re.sub(r"\s+", " ", raw).strip()

    alias_map = {
        "bit corretora": "BIT",
        "direct bh": "DIRECT",
        "emerson victor": "EMERSON",
        "fernando mont": "FERNANDO",
        "future tecnologia": "FUTURE",
        "nexabet receb": "NEXABET",
        "nexus solucoes": "NEXUS",
        "royal crest": "ROYAL",
        "rvls compre": "RVLS",
        "python tecnologia": "TYPHON",
        "univebet gaming": "Univebet",
        "phoenix gaming": "Phoenix Gaming",
        "smart cluster": "Smart Cluster",
        "r torres": "R Torres",
        "x vit": "X Vit",
    }
    for source, target in alias_map.items():
        if raw.startswith(source):
            return f"Pix de {target}"

    words = raw.split()
    if not words:
        return "Pix de Receita"

    if len(words) >= 2:
        pair = " ".join(words[:2])
        keep_two_words = {"smart cluster", "r torres", "x vit", "phoenix gaming"}
        if pair in keep_two_words:
            label = " ".join(word.capitalize() if not word.isupper() else word for word in words[:2])
            return f"Pix de {label}"

    first = words[0]
    label = first.upper() if len(first) <= 4 else first.capitalize()
    return f"Pix de {label}"
