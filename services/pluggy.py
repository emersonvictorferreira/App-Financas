from __future__ import annotations

from datetime import datetime
from typing import Any

import requests

from services.pdf_parser import Transaction


class PluggyClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        item_id: str,
        base_url: str = "https://api.pluggy.ai",
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.item_id = item_id
        self.base_url = base_url.rstrip("/")
        self._api_key: str | None = None

    def is_configured(self) -> bool:
        return all([self.client_id, self.client_secret, self.item_id])

    def sync_transactions(self) -> list[Transaction]:
        if not self.is_configured():
            raise ValueError("Credenciais da Pluggy não configuradas.")

        api_key = self._authenticate()
        response = requests.get(
            f"{self.base_url}/transactions",
            params={"itemId": self.item_id, "pageSize": 50},
            headers={"X-API-KEY": api_key},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        records = payload.get("results", [])

        transactions: list[Transaction] = []
        for record in records:
            record_type = (record.get("type") or "").upper()
            amount = float(record.get("amount") or 0)
            if record_type == "CREDIT" or amount <= 0:
                continue

            date_value = record.get("date") or record.get("paymentDate") or datetime.now().isoformat()
            transactions.append(
                Transaction(
                    description=record.get("description") or record.get("merchant") or "Lançamento Pluggy",
                    amount=round(amount, 2),
                    date=_format_date(date_value),
                    category=record.get("category") or "Pluggy",
                    payment_method=record.get("paymentData", {}).get("method") or "Conta bancária",
                    essential="SIM",
                )
            )

        return transactions

    def _authenticate(self) -> str:
        if self._api_key:
            return self._api_key

        response = requests.post(
            f"{self.base_url}/auth",
            json={"clientId": self.client_id, "clientSecret": self.client_secret},
            timeout=30,
        )
        response.raise_for_status()
        payload: dict[str, Any] = response.json()
        api_key = payload.get("apiKey")
        if not api_key:
            raise ValueError("Não foi possível obter a chave da Pluggy.")
        self._api_key = api_key
        return api_key


def _format_date(value: str) -> str:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%d/%m/%Y")
    except ValueError:
        return value
