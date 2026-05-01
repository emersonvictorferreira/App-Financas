from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request
from flask import Response
from googleapiclient.errors import HttpError
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.utils import secure_filename

from services.google_sheets import GoogleSheetsService
from services.import_history import ImportHistoryService
from services.pdf_parser import parse_statement_pdf
from services.pdf_parser import Transaction
from services.pluggy import PluggyClient


load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / os.getenv("UPLOAD_DIR", "uploads")
UPLOAD_DIR.mkdir(exist_ok=True)
HISTORY_PATH = BASE_DIR / "data" / "import_history.json"

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret-key")
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024


def get_google_sheets_service() -> GoogleSheetsService:
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    if credentials_path and not Path(credentials_path).is_absolute():
        credentials_path = str(BASE_DIR / credentials_path)

    return GoogleSheetsService(
        credentials_path=credentials_path,
        spreadsheet_id=os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", ""),
        range_template=os.getenv("GOOGLE_SHEETS_EXPENSE_RANGE_TEMPLATE", "{MES}!L:Q"),
        service_account_json=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", ""),
    )


def get_pluggy_client() -> PluggyClient:
    return PluggyClient(
        client_id=os.getenv("PLUGGY_CLIENT_ID", ""),
        client_secret=os.getenv("PLUGGY_CLIENT_SECRET", ""),
        item_id=os.getenv("PLUGGY_ITEM_ID", ""),
        base_url=os.getenv("PLUGGY_BASE_URL", "https://api.pluggy.ai"),
    )


def get_import_history_service() -> ImportHistoryService:
    return ImportHistoryService(HISTORY_PATH)


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/health")
def health():
    return jsonify({"ok": True, "message": "Servidor online."})


@app.get("/api/import-history")
def import_history():
    history = get_import_history_service()
    return jsonify({"ok": True, "entries": history.list_entries()})


@app.get("/favicon.ico")
def favicon():
    return Response(status=204)


@app.post("/api/preview-pdf")
def preview_pdf():
    uploaded = request.files.get("pdf")
    if not uploaded or not uploaded.filename:
        return jsonify({"ok": False, "message": "Selecione um PDF para visualizar."}), 400

    if not uploaded.filename.lower().endswith(".pdf"):
        return jsonify({"ok": False, "message": "Envie um arquivo PDF valido."}), 400

    filename = secure_filename(uploaded.filename)
    destination = UPLOAD_DIR / filename
    uploaded.save(destination)

    try:
        transactions = parse_statement_pdf(destination)
    except Exception as exc:
        return jsonify({"ok": False, "message": f"Erro ao ler o PDF: {exc}", "transactions": []}), 500
    finally:
        destination.unlink(missing_ok=True)

    return jsonify(
        {
            "ok": True,
            "message": "Prévia gerada com sucesso.",
            "summary": _build_transaction_summary(transactions),
            "transactions": [transaction.to_dict() for transaction in transactions],
        }
    )


@app.post("/api/upload-pdf")
def upload_pdf():
    uploaded = request.files.get("pdf")
    if not uploaded or not uploaded.filename:
        return jsonify({"ok": False, "message": "Selecione um PDF para importar."}), 400

    if not uploaded.filename.lower().endswith(".pdf"):
        return jsonify({"ok": False, "message": "Envie um arquivo PDF valido."}), 400

    filename = secure_filename(uploaded.filename)
    destination = UPLOAD_DIR / filename
    uploaded.save(destination)

    transactions = []
    try:
        transactions = parse_statement_pdf(destination)
        sheets = get_google_sheets_service()
        if not sheets.is_configured():
            return jsonify(
                {
                    "ok": False,
                    "message": "PDF recebido, mas falta configurar o Google Sheets.",
                    "transactions": [transaction.to_dict() for transaction in transactions],
                }
            ), 400

        inserted_rows = sheets.append_transactions(transactions)
        warnings = getattr(sheets, "last_warnings", [])
        entry = get_import_history_service().record_import(
            source_type="pluggy",
            source_name="Pluggy",
            inserted_rows=inserted_rows,
            transactions=transactions,
            warnings=warnings,
        )
        entry = get_import_history_service().record_import(
            source_type="pdf",
            source_name=uploaded.filename,
            inserted_rows=inserted_rows,
            transactions=transactions,
            warnings=warnings,
        )
    except HttpError as exc:
        return _google_error_response(exc, transactions)
    except Exception as exc:
        return jsonify(
            {
                "ok": False,
                "message": f"Erro ao enviar PDF para o Google Sheets: {exc}",
                "transactions": [transaction.to_dict() for transaction in transactions],
            }
        ), 500
    finally:
        destination.unlink(missing_ok=True)

    return jsonify(
        {
            "ok": True,
            "message": _upload_success_message(inserted_rows, warnings),
            "transactions": [transaction.to_dict() for transaction in transactions],
            "history_entry": entry,
        }
    )


@app.post("/api/sync-pluggy")
def sync_pluggy():
    pluggy = get_pluggy_client()
    if not pluggy.is_configured():
        return jsonify({"ok": False, "message": "Configure PLUGGY_CLIENT_ID, PLUGGY_CLIENT_SECRET e PLUGGY_ITEM_ID."}), 400

    transactions = pluggy.sync_transactions()
    if not transactions:
        return jsonify({"ok": True, "message": "Nenhum lancamento novo retornado pela Pluggy.", "transactions": []})

    sheets = get_google_sheets_service()
    if not sheets.is_configured():
        return jsonify(
            {
                "ok": False,
                "message": "Pluggy conectou, mas falta configurar o Google Sheets.",
                "transactions": [transaction.to_dict() for transaction in transactions],
            }
        ), 400

    try:
        inserted_rows = sheets.append_transactions(transactions)
        warnings = getattr(sheets, "last_warnings", [])
    except HttpError as exc:
        return _google_error_response(exc, transactions)
    except Exception as exc:
        return jsonify(
            {
                "ok": False,
                "message": f"Erro ao sincronizar Pluggy com Google Sheets: {exc}",
                "transactions": [transaction.to_dict() for transaction in transactions],
            }
        ), 500

    return jsonify(
        {
            "ok": True,
            "message": _sync_success_message(inserted_rows, warnings),
            "transactions": [transaction.to_dict() for transaction in transactions],
            "history_entry": entry,
        }
    )


@app.post("/api/reprocess-month")
def reprocess_month():
    payload = request.get_json(silent=True) or {}
    month_name = str(payload.get("month") or "").strip().upper()
    entry_id = str(payload.get("entry_id") or "").strip()
    if not month_name and not entry_id:
        return jsonify({"ok": False, "message": "Informe o mês ou o histórico para reprocessar."}), 400

    history = get_import_history_service()
    entry = history.get_entry(entry_id) if entry_id else history.latest_entry_for_month(month_name)
    if not entry:
        return jsonify({"ok": False, "message": "Nenhum histórico encontrado para esse mês."}), 404

    target_month = month_name or str(entry.get("months", [""])[0]).upper()
    entry_transactions = [_transaction_from_dict(item) for item in entry.get("transactions", [])]
    month_transactions = [transaction for transaction in entry_transactions if _month_name_from_date(transaction.date) == target_month]
    if not month_transactions:
        return jsonify({"ok": False, "message": "Esse histórico não possui lançamentos para o mês solicitado."}), 400

    sheets = get_google_sheets_service()
    try:
        replaced_rows = sheets.replace_month_transactions(target_month, month_transactions)
        warnings = getattr(sheets, "last_warnings", [])
    except HttpError as exc:
        return _google_error_response(exc, month_transactions)
    except Exception as exc:
        return jsonify(
            {
                "ok": False,
                "message": f"Erro ao reprocessar o mês: {exc}",
                "transactions": [transaction.to_dict() for transaction in month_transactions],
            }
        ), 500

    return jsonify(
        {
            "ok": True,
            "message": _reprocess_success_message(target_month, replaced_rows, warnings),
            "transactions": [transaction.to_dict() for transaction in month_transactions],
            "history_entry": entry,
        }
    )


@app.errorhandler(RequestEntityTooLarge)
def handle_large_file(_error):
    return jsonify({"ok": False, "message": "O arquivo excede o limite de 10 MB.", "transactions": []}), 413


@app.errorhandler(Exception)
def handle_unexpected_error(error):
    if isinstance(error, HttpError):
        return _google_error_response(error, [])
    return jsonify({"ok": False, "message": f"Erro interno: {error}", "transactions": []}), 500


def _google_error_response(exc: HttpError, transactions) -> tuple:
    status_code = getattr(exc.resp, "status", 500)
    if status_code == 403:
        message = "O Google Sheets recusou a escrita. Verifique as permissoes da conta de servico."
    elif status_code == 429:
        message = "O Google Sheets atingiu o limite temporario de escritas. Aguarde um minuto e tente novamente."
    else:
        message = f"Erro do Google Sheets: {exc}"

    return (
        jsonify(
            {
                "ok": False,
                "message": message,
                "transactions": [transaction.to_dict() for transaction in transactions],
            }
        ),
        status_code,
    )


def _upload_success_message(inserted_rows: int, warnings: list[str]) -> str:
    message = f"{inserted_rows} lancamentos enviados para o Google Sheets."
    if warnings:
        return f"{message} Observacao: a planilha foi atualizada, mas houve um ajuste secundario pendente."
    return message


def _sync_success_message(inserted_rows: int, warnings: list[str]) -> str:
    message = f"{inserted_rows} lancamentos da Pluggy enviados para o Google Sheets."
    if warnings:
        return f"{message} Observacao: a planilha foi atualizada, mas houve um ajuste secundario pendente."
    return message


def _reprocess_success_message(month_name: str, replaced_rows: int, warnings: list[str]) -> str:
    message = f"{replaced_rows} lancamentos reprocessados em {month_name}."
    if warnings:
        return f"{message} Observacao: a planilha foi atualizada, mas houve um ajuste secundario pendente."
    return message


def _build_transaction_summary(transactions: list[Transaction]) -> dict:
    income = [transaction for transaction in transactions if transaction.kind == "income"]
    expense = [transaction for transaction in transactions if transaction.kind == "expense"]
    months = sorted({_month_name_from_date(transaction.date) for transaction in transactions})
    return {
        "transaction_count": len(transactions),
        "income_count": len(income),
        "income_total": round(sum(transaction.amount for transaction in income), 2),
        "expense_count": len(expense),
        "expense_total": round(sum(transaction.amount for transaction in expense), 2),
        "months": months,
    }


def _transaction_from_dict(payload: dict) -> Transaction:
    return Transaction(
        description=str(payload.get("description", "")),
        amount=float(payload.get("amount", 0) or 0),
        date=str(payload.get("date", "")),
        category=str(payload.get("category", "🧾 Outros")),
        payment_method=str(payload.get("payment_method", "💸 Dinheiro / Pix")),
        essential=str(payload.get("essential", "✔️")),
        kind=str(payload.get("kind", "expense")),
    )


def _month_name_from_date(date_str: str) -> str:
    from datetime import datetime

    months = {
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
    return months[datetime.strptime(date_str, "%d/%m/%Y").month]


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
