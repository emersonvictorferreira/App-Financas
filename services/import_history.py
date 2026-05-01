from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from services.google_sheets import MONTH_NAMES
from services.pdf_parser import Transaction


class ImportHistoryService:
    def __init__(self, storage_path: Path) -> None:
        self.storage_path = storage_path
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)

    def list_entries(self, limit: int = 20) -> list[dict[str, Any]]:
        payload = self._load()
        entries = payload.get("entries", [])
        return entries[:limit]

    def record_import(
        self,
        *,
        source_type: str,
        source_name: str,
        inserted_rows: int,
        transactions: list[Transaction],
        warnings: list[str] | None = None,
    ) -> dict[str, Any]:
        entry = self._build_entry(
            source_type=source_type,
            source_name=source_name,
            inserted_rows=inserted_rows,
            transactions=transactions,
            warnings=warnings or [],
        )
        payload = self._load()
        payload.setdefault("entries", [])
        payload["entries"].insert(0, entry)
        payload["entries"] = payload["entries"][:100]
        self._save(payload)
        return entry

    def latest_entry_for_month(self, month_name: str) -> dict[str, Any] | None:
        month_name = month_name.upper()
        for entry in self._load().get("entries", []):
            if month_name in entry.get("months", []):
                return entry
        return None

    def get_entry(self, entry_id: str) -> dict[str, Any] | None:
        for entry in self._load().get("entries", []):
            if entry.get("id") == entry_id:
                return entry
        return None

    def _build_entry(
        self,
        *,
        source_type: str,
        source_name: str,
        inserted_rows: int,
        transactions: list[Transaction],
        warnings: list[str],
    ) -> dict[str, Any]:
        income_transactions = [transaction for transaction in transactions if transaction.kind == "income"]
        expense_transactions = [transaction for transaction in transactions if transaction.kind == "expense"]
        months = sorted({_month_name_from_date(transaction.date) for transaction in transactions})
        created_at = datetime.now().isoformat(timespec="seconds")
        return {
            "id": uuid4().hex,
            "created_at": created_at,
            "source_type": source_type,
            "source_name": source_name,
            "inserted_rows": inserted_rows,
            "months": months,
            "income_count": len(income_transactions),
            "income_total": round(sum(transaction.amount for transaction in income_transactions), 2),
            "expense_count": len(expense_transactions),
            "expense_total": round(sum(transaction.amount for transaction in expense_transactions), 2),
            "warnings": warnings,
            "transactions": [transaction.to_dict() for transaction in transactions],
        }

    def _load(self) -> dict[str, Any]:
        if not self.storage_path.exists():
            return {"entries": []}
        try:
            return json.loads(self.storage_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"entries": []}

    def _save(self, payload: dict[str, Any]) -> None:
        self.storage_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _month_name_from_date(date_str: str) -> str:
    parsed = datetime.strptime(date_str, "%d/%m/%Y")
    return MONTH_NAMES[parsed.month]
