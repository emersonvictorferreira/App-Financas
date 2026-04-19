const pdfInput = document.getElementById("pdf-input");
const pluggyButton = document.getElementById("pluggy-button");
const statusPanel = document.getElementById("status");
const resultList = document.getElementById("result-list");
const resultCount = document.getElementById("result-count");

pdfInput.addEventListener("change", async (event) => {
  const [file] = event.target.files;
  if (!file) return;

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
    const response = await fetch(url, options);
    const data = await response.json();
    setStatus(data.message || "Concluído.", !response.ok);
    renderTransactions(data.transactions || []);
  } catch (error) {
    setStatus("Não foi possível concluir a operação. Verifique o backend.", true);
  }
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
    resultList.innerHTML = '<p class="empty-state">Nenhum lançamento retornado nesta operação.</p>';
    return;
  }

  resultList.innerHTML = transactions
    .map(
      (transaction) => `
        <article class="result-item">
          <strong>${escapeHtml(transaction.description)}</strong>
          <div class="result-meta">
            R$ ${Number(transaction.amount || 0).toFixed(2)} • ${escapeHtml(transaction.date)} • ${escapeHtml(transaction.category || "")}
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
