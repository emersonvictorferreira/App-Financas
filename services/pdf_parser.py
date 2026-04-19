from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from pypdf import PdfReader


MONTH_MAP = {
    "JAN": 1,
    "FEV": 2,
    "MAR": 3,
    "ABR": 4,
    "MAI": 5,
    "JUN": 6,
    "JUL": 7,
    "AGO": 8,
    "SET": 9,
    "OUT": 10,
    "NOV": 11,
    "DEZ": 12,
}

DATE_HEADER_PATTERN = re.compile(r"^(?P<day>\d{2})\s+(?P<month>[A-Z]{3})\s+(?P<year>\d{4})(?P<rest>.*)$")
AMOUNT_ONLY_PATTERN = re.compile(r"^\d[\d\.,]*$")

IGNORED_PREFIXES = (
    "Emerson ",
    "CPF ",
    "Agência Conta",
    "Saldo ",
    "Rendimento ",
    "Movimentações",
    "VALORES EM R$",
    "Extrato gerado dia",
    "Tem alguma dúvida?",
    "Caso a solução",
    "Atendimento ",
    "Nu Financeira",
    "Nu Pagamentos",
    "CNPJ:",
    "Asseguramos",
    "Não nos responsabilizamos",
    "O saldo líquido",
)

IGNORED_CONTAINS = (
    "0001 CPF Agência Conta",
    "nubank.com.br/contatos#ouvidoria",
    "metropolitanas",
    "demais localidades",
)


@dataclass
class Transaction:
    description: str
    amount: float
    date: str
    category: str = "\U0001F9FE Outros"
    payment_method: str = "\U0001F4B8 Dinheiro / Pix"
    essential: str = "\u2714\ufe0f"
    kind: str = "expense"

    def to_expense_row(self) -> list[str | float]:
        return [
            self.description,
            self.amount,
            f"'{self.date}",
            self.category,
            self.payment_method,
            self.essential,
        ]

    def to_income_row(self) -> list[str | float]:
        return [
            self.description,
            "",
            self.amount,
        ]

    def to_dict(self) -> dict:
        return asdict(self)


def read_pdf_text(pdf_path: Path) -> str:
    reader = PdfReader(str(pdf_path))
    return "\n".join((page.extract_text() or "") for page in reader.pages)


def parse_statement_pdf(pdf_path: Path) -> list[Transaction]:
    lines = _clean_lines(read_pdf_text(pdf_path).splitlines())
    transactions = _parse_nubank_statement(lines)
    if transactions:
        return transactions

    fallback_description = pdf_path.stem
    today = datetime.now().strftime("%d/%m/%Y")
    return [
        Transaction(
            description=f"Importacao manual: {fallback_description}",
            amount=0.0,
            date=today,
            category="Revisar PDF",
            payment_method="Conta bancaria",
            essential="\u2714\ufe0f",
            kind="expense",
        )
    ]


def _clean_lines(lines: list[str]) -> list[str]:
    cleaned: list[str] = []
    for raw_line in lines:
        line = " ".join(raw_line.split())
        if not line:
            continue
        if any(line.startswith(prefix) for prefix in IGNORED_PREFIXES):
            continue
        if any(fragment in line for fragment in IGNORED_CONTAINS):
            continue
        if re.fullmatch(r"\d+ de \d+", line):
            continue
        cleaned.append(line)
    return cleaned


def _parse_nubank_statement(lines: list[str]) -> list[Transaction]:
    transactions: list[Transaction] = []
    current_date: str | None = None
    current_section: str | None = None
    description_parts: list[str] = []

    def flush_transaction(amount_text: str) -> None:
        nonlocal description_parts
        if not description_parts or current_date is None or current_section not in {"saidas", "entradas"}:
            description_parts = []
            return

        description = " ".join(description_parts).strip(" -")
        description = re.sub(r"\s+", " ", description)
        amount = _parse_currency(amount_text)
        kind = "income" if current_section == "entradas" else "expense"
        transactions.append(
            Transaction(
                description=_compact_description(description, kind),
                amount=amount,
                date=current_date,
                category=_detect_category(description),
                payment_method="\U0001F4B8 Dinheiro / Pix" if "pix" in description.lower() else "\U0001F4B2 Boleto",
                essential="\u2714\ufe0f",
                kind=kind,
            )
        )
        description_parts = []

    for index, line in enumerate(lines):
        date_match = DATE_HEADER_PATTERN.match(line)
        if date_match:
            current_date = _format_date(
                int(date_match.group("day")),
                date_match.group("month"),
                int(date_match.group("year")),
            )
            rest = date_match.group("rest").strip()
            if "Total de entradas" in rest:
                current_section = "entradas"
            elif "Total de saídas" in rest or "Total de saidas" in rest:
                current_section = "saidas"
            else:
                current_section = None
            description_parts = []
            continue

        if line.startswith("Total de entradas"):
            current_section = "entradas"
            description_parts = []
            continue

        if line.startswith("Total de saídas") or line.startswith("Total de saidas"):
            current_section = "saidas"
            description_parts = []
            continue

        if AMOUNT_ONLY_PATTERN.fullmatch(line):
            next_line = lines[index + 1] if index + 1 < len(lines) else ""
            if _is_account_number_fragment(line, description_parts, next_line):
                description_parts.append(line)
                continue
            flush_transaction(line)
            continue

        if current_date is None:
            continue

        if line.startswith("Conta:") and description_parts:
            description_parts.append(line)
            continue

        if line.startswith("Agência:") and description_parts:
            description_parts.append(line)
            continue

        if line.startswith("Transferência") or line.startswith("Pagamento") or description_parts:
            description_parts.append(line)

    return transactions


def _is_account_number_fragment(line: str, description_parts: list[str], next_line: str) -> bool:
    if not description_parts:
        return False

    previous = description_parts[-1]
    if len(line) > 2:
        return False

    if not AMOUNT_ONLY_PATTERN.fullmatch(next_line):
        return False

    return previous.endswith("-") or previous.endswith("Conta:")


def _compact_description(description: str, kind: str) -> str:
    cleaned = re.sub(r"\s+", " ", description).strip()
    pix_match = re.search(r"Transferência (?:enviada|recebida) pelo Pix (.+)", cleaned, re.IGNORECASE)
    if pix_match:
        tail = pix_match.group(1)
        name = re.split(r"\s-\s", tail, maxsplit=1)[0].strip(" -")
        name = _shorten_name(name)
        prefix = "Pix para" if kind == "expense" else "Pix de"
        return _limit_text(f"{prefix} {name}", 20)

    pagamento_match = re.search(r"Pagamento (.+)", cleaned, re.IGNORECASE)
    if pagamento_match:
        return _limit_text(_shorten_name(pagamento_match.group(1).strip()), 20)

    return _limit_text(_shorten_name(cleaned), 20)


def _shorten_name(text: str) -> str:
    shortened = text
    replacements = {
        "MARQUES FERREIRA DE SOUSA": "Marques Sousa",
        "Jose Guilherme Ferreira de Sousa": "Jose Guilherme",
        "LUCIEDA MARQUES FERREIRA DE SOUSA": "Lucieda Sousa",
        "SMART CLUSTER SERVICOS TECNOLOGICOS LTDA": "Smart Cluster",
        "R TORRES PARTICIPACOES LTDA": "R Torres",
        "GDSP TECHNOLOGY LTDA": "GDSP",
        "X VIT E COMERCE LTDA": "X Vit",
        "PHOENIX GAMING LTDA": "Phoenix Gaming",
        "MERCADO PAGO INSTITUICAO DE PAGAMENTO LTDA": "Mercado Pago",
    }
    for source, target in replacements.items():
        shortened = re.sub(source, target, shortened, flags=re.IGNORECASE)

    shortened = re.sub(r"\bLTDA\b|\bS\.A\.\b|\bS/A\b|\bE COMERCE\b", "", shortened, flags=re.IGNORECASE)
    shortened = re.sub(r"\s+", " ", shortened).strip(" -")
    words = shortened.split()
    if not words:
        return shortened

    if len(words) == 1:
        return words[0]

    if words[0].upper() == words[0] and len(words[0]) <= 12:
        return words[0]

    return " ".join(words[:2])


def _limit_text(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3].rstrip() + "..."


def _format_date(day: int, month_code: str, year: int) -> str:
    month = MONTH_MAP[month_code]
    return datetime(year, month, day).strftime("%d/%m/%Y")


def _parse_currency(value: str) -> float:
    normalized = value.replace(".", "").replace(",", ".")
    return round(float(normalized), 2)


def _detect_category(description: str) -> str:
    lowered = description.lower()
    if "claro" in lowered or "mercado pago" in lowered:
        return "\u26a1 Servicos domesticos"
    if "mercado" in lowered:
        return "\U0001F6D2 Supermercado"
    if "phoenix" in lowered or "soriginal" in lowered:
        return "\U0001F389 Lazer"
    return "\U0001F9FE Outros"
