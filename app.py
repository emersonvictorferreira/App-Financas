from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request
from googleapiclient.errors import HttpError
from werkzeug.utils import secure_filename

from services.google_sheets import GoogleSheetsService
from services.pdf_parser import parse_statement_pdf
from services.pluggy import PluggyClient


load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / os.getenv("UPLOAD_DIR", "uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

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


@app.get("/")
def index():
    return render_template("index.html")


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

    transactions = parse_statement_pdf(destination)
    sheets = get_google_sheets_service()
    if not sheets.is_configured():
        return jsonify(
            {
                "ok": False,
                "message": "PDF recebido, mas falta configurar o Google Sheets no arquivo .env.",
                "transactions": [transaction.to_dict() for transaction in transactions],
            }
        ), 400

    try:
        inserted_rows = sheets.append_transactions(transactions)
    except HttpError as exc:
        return _google_error_response(exc, transactions)
    except Exception as exc:
        return jsonify({"ok": False, "message": f"Erro ao enviar PDF para o Google Sheets: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "message": f"{inserted_rows} lancamentos enviados para o Google Sheets.",
            "transactions": [transaction.to_dict() for transaction in transactions],
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
                "message": "Pluggy conectou, mas falta configurar o Google Sheets no arquivo .env.",
                "transactions": [transaction.to_dict() for transaction in transactions],
            }
        ), 400

    try:
        inserted_rows = sheets.append_transactions(transactions)
    except HttpError as exc:
        return _google_error_response(exc, transactions)
    except Exception as exc:
        return jsonify({"ok": False, "message": f"Erro ao sincronizar Pluggy com Google Sheets: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "message": f"{inserted_rows} lancamentos da Pluggy enviados para o Google Sheets.",
            "transactions": [transaction.to_dict() for transaction in transactions],
        }
    )


def _google_error_response(exc: HttpError, transactions) -> tuple:
    status_code = getattr(exc.resp, "status", 500)
    if status_code == 403:
        message = (
            "O Google Sheets recusou a escrita. Compartilhe a planilha com a conta de servico "
            "como Editor e tente novamente."
        )
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


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
