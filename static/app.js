const pdfInput = document.getElementById("pdf-input");
const pluggyButton = document.getElementById("pluggy-button");
const statusPanel = document.getElementById("status");
const resultList = document.getElementById("result-list");
const resultCount = document.getElementById("result-count");

warmUpBackend();

pdfInput.addEventListener("change", async (event) => {
  const [file] = event.target.files;
  if (!file) return;

  if (file.size > 10 * 1024 * 1024) {
    setStatus("O PDF excede o limite de 10 MB.", true);
    pdfInput.value = "";
    return;
  }

  const formData = new FormData();
  formData.append("pdf", file);
  await sendRequest("/api/upload-pdf", {
    method: "POST",
    body: formData,
  });
  pdfInput.value = "";
});

pluggyButton.addEventListener("click", async () => {
  await sendRequest("/api/sync-pluggy", {
    method: "POST",
  });
});

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
    .map(
      (transaction) => `
        <article class="result-item">
          <strong>${escapeHtml(transaction.description)}</strong>
          <div class="result-meta">
            R$ ${Number(transaction.amount || 0).toFixed(2)} - ${escapeHtml(transaction.date)} - ${escapeHtml(transaction.category || "")}
          </div>
        </article>
      `
    )
    .join("");
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
