const pdfInput = document.getElementById("pdf-input");
const pluggyButton = document.getElementById("pluggy-button");
const statusPanel = document.getElementById("status");
const resultList = document.getElementById("result-list");
const resultCount = document.getElementById("result-count");
const historyList = document.getElementById("history-list");
const historyCount = document.getElementById("history-count");
const previewCard = document.getElementById("preview-card");
const previewSummary = document.getElementById("preview-summary");
const previewConfirm = document.getElementById("preview-confirm");
const previewCancel = document.getElementById("preview-cancel");

let pendingPdfFile = null;
let pendingPreview = null;

warmUpBackend();
loadHistory();

pdfInput.addEventListener("change", async (event) => {
  const [file] = event.target.files;
  if (!file) return;

  if (file.size > 10 * 1024 * 1024) {
    setStatus("O PDF excede o limite de 10 MB.", true);
    resetPreview();
    pdfInput.value = "";
    return;
  }

  pendingPdfFile = file;
  await previewPdf(file);
  pdfInput.value = "";
});

previewConfirm.addEventListener("click", async () => {
  if (!pendingPdfFile) return;

  const formData = new FormData();
  formData.append("pdf", pendingPdfFile);
  await sendRequest("/api/upload-pdf", {
    method: "POST",
    body: formData,
  });
  resetPreview();
  await loadHistory();
});

previewCancel.addEventListener("click", () => {
  resetPreview();
  setStatus("Previa cancelada.");
});

pluggyButton.addEventListener("click", async () => {
  await sendRequest("/api/sync-pluggy", {
    method: "POST",
  });
  await loadHistory();
});

historyList.addEventListener("click", async (event) => {
  const button = event.target.closest("[data-month][data-entry-id]");
  if (!button) return;

  await sendRequest("/api/reprocess-month", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      month: button.dataset.month,
      entry_id: button.dataset.entryId,
    }),
  });
  await loadHistory();
});

async function previewPdf(file) {
  setStatus("Gerando previa...");
  const formData = new FormData();
  formData.append("pdf", file);

  try {
    const response = await fetch("/api/preview-pdf", {
      method: "POST",
      body: formData,
    });
    const payload = await parseResponse(response);

    if (!response.ok) {
      setStatus(payload.message || "Nao foi possivel gerar a previa.", true);
      resetPreview();
      renderTransactions(payload.transactions || []);
      return;
    }

    pendingPreview = payload;
    renderPreview(file.name, payload.summary || {});
    renderTransactions(payload.transactions || []);
    setStatus(payload.message || "Previa pronta.");
  } catch {
    setStatus("Nao foi possivel gerar a previa do PDF.", true);
    resetPreview();
  }
}

async function sendRequest(url, options) {
  setStatus("Processando...");

  try {
    let response = await fetch(url, options);
    if (response.status === 502 || response.status === 503) {
      setStatus("Servidor acordando no Render. Tentando novamente...", true);
      await delay(6000);
      response = await fetch(url, options);
    }

    const payload = await parseResponse(response);

    if (!response.ok) {
      setStatus(payload.message || "A operacao falhou.", true);
      renderTransactions(payload.transactions || []);
      return;
    }

    setStatus(payload.message || "Concluido.");
    renderTransactions(payload.transactions || []);
  } catch (error) {
    setStatus(
      "Nao foi possivel concluir a operacao. Se estiver no iPhone, aguarde alguns segundos e tente novamente.",
      true
    );
  }
}

async function loadHistory() {
  try {
    const response = await fetch("/api/import-history", { cache: "no-store" });
    const payload = await parseResponse(response);
    if (!response.ok) {
      return;
    }
    renderHistory(payload.entries || []);
  } catch {
    historyCount.textContent = "0 itens";
  }
}

function renderPreview(filename, summary) {
  previewCard.classList.remove("hidden");
  previewSummary.innerHTML = `
    <article class="preview-metric">
      <span class="preview-label">Arquivo</span>
      <span class="preview-value">${escapeHtml(filename)}</span>
    </article>
    <div class="preview-grid">
      <article class="preview-metric">
        <span class="preview-label">Receitas</span>
        <span class="preview-value">${summary.income_count || 0} itens · ${formatCurrency(summary.income_total || 0)}</span>
      </article>
      <article class="preview-metric">
        <span class="preview-label">Despesas</span>
        <span class="preview-value">${summary.expense_count || 0} itens · ${formatCurrency(summary.expense_total || 0)}</span>
      </article>
      <article class="preview-metric">
        <span class="preview-label">Lancamentos</span>
        <span class="preview-value">${summary.transaction_count || 0}</span>
      </article>
      <article class="preview-metric">
        <span class="preview-label">Meses</span>
        <span class="preview-value">${(summary.months || []).join(", ") || "-"}</span>
      </article>
    </div>
  `;
}

function resetPreview() {
  pendingPdfFile = null;
  pendingPreview = null;
  previewSummary.innerHTML = "";
  previewCard.classList.add("hidden");
}

async function warmUpBackend() {
  try {
    const response = await fetch("/api/health", { cache: "no-store" });
    if (response.ok) {
      const data = await response.json();
      setStatus(data.message || "Servidor online.");
    }
  } catch {
    setStatus("Servidor iniciando no Render. Aguarde alguns segundos.", true);
  }
}

async function parseResponse(response) {
  const contentType = response.headers.get("content-type") || "";

  if (contentType.includes("application/json")) {
    return response.json();
  }

  const text = await response.text();
  return {
    message: text || `Erro ${response.status}`,
    transactions: [],
  };
}

function setStatus(message, isError = false) {
  statusPanel.textContent = message;
  statusPanel.style.background = isError
    ? "rgba(179, 59, 54, 0.12)"
    : "rgba(18, 106, 82, 0.08)";
  statusPanel.style.color = isError ? "#8b1f1b" : "#0f4f3f";
}

function renderTransactions(transactions) {
  resultCount.textContent = `${transactions.length} item${transactions.length === 1 ? "" : "s"}`;

  if (!transactions.length) {
    resultList.innerHTML = '<p class="empty-state">Nenhum lancamento retornado nesta operacao.</p>';
    return;
  }

  resultList.innerHTML = transactions
    .slice(0, 25)
    .map(
      (transaction) => `
        <article class="result-item">
          <strong>${escapeHtml(transaction.description)}</strong>
          <div class="result-meta">
            ${formatCurrency(transaction.amount || 0)} - ${escapeHtml(transaction.date)} - ${escapeHtml(transaction.category || "")}
          </div>
        </article>
      `
    )
    .join("");
}

function renderHistory(entries) {
  historyCount.textContent = `${entries.length} item${entries.length === 1 ? "" : "s"}`;

  if (!entries.length) {
    historyList.innerHTML = '<p class="empty-state">As ultimas importacoes vao aparecer aqui.</p>';
    return;
  }

  historyList.innerHTML = entries
    .map(
      (entry) => `
        <article class="result-item history-item">
          <div class="history-topline">
            <strong>${escapeHtml(entry.source_name || entry.source_type || "Importacao")}</strong>
            <span>${formatDateTime(entry.created_at)}</span>
          </div>
          <div class="result-meta">
            ${escapeHtml((entry.months || []).join(", "))} · Entradas ${formatCurrency(entry.income_total || 0)} · Saidas ${formatCurrency(entry.expense_total || 0)}
          </div>
          <div class="result-meta">
            ${entry.income_count || 0} receitas · ${entry.expense_count || 0} despesas · ${entry.inserted_rows || 0} lancamentos novos
          </div>
          <div class="history-actions">
            <button
              class="history-action"
              type="button"
              data-entry-id="${escapeHtml(entry.id || "")}"
              data-month="${escapeHtml((entry.months || [])[0] || "")}"
            >
              Reprocessar mes
            </button>
          </div>
        </article>
      `
    )
    .join("");
}

function formatCurrency(value) {
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
  }).format(Number(value || 0));
}

function formatDateTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("pt-BR", {
    dateStyle: "short",
    timeStyle: "short",
  }).format(date);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
