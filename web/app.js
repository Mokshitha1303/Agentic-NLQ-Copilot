const form = document.getElementById("query-form");
const runBtn = document.getElementById("run-btn");
const responseEl = document.getElementById("response");

const apiBase = "/api";

function setResponse(payload) {
  responseEl.textContent = JSON.stringify(payload, null, 2);
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  runBtn.disabled = true;
  runBtn.textContent = "Running...";

  const dbId = document.getElementById("db-id").value.trim();
  const question = document.getElementById("question").value.trim();
  const maxRows = Number(document.getElementById("max-rows").value || "200");

  const payload = {
    db_id: dbId,
    question,
    max_rows: maxRows,
  };

  try {
    const result = await fetch(`${apiBase}/v1/query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const body = await result.json();
    if (!result.ok) {
      setResponse({
        status: "error",
        http_status: result.status,
        details: body,
      });
    } else {
      setResponse(body);
    }
  } catch (error) {
    setResponse({
      status: "error",
      details: String(error),
    });
  } finally {
    runBtn.disabled = false;
    runBtn.textContent = "Run Query";
  }
});
