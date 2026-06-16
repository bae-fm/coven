// The page: collect an S3 config, open the library in the Worker, and render the
// shared notes list with an "add note" box and a sync indicator.
//
// All coven work happens in worker.js (OPFS is Worker-only); this file only talks
// to that Worker and updates the DOM. Open the same bucket config in two tabs and
// notes added in one appear in the other within a sync interval.

const worker = new Worker(new URL("./worker.js", import.meta.url), { type: "module" });

const els = {
  config: document.getElementById("config"),
  app: document.getElementById("app"),
  form: document.getElementById("config-form"),
  noteForm: document.getElementById("note-form"),
  noteBody: document.getElementById("note-body"),
  notes: document.getElementById("notes"),
  status: document.getElementById("status"),
  error: document.getElementById("error"),
};

// A note id this tab mints. The demo uses a random id per note; the device id (a
// random per-tab value) keeps two tabs' writes independent.
const deviceId = `tab-${crypto.randomUUID().slice(0, 8)}`;

function showError(message) {
  els.error.textContent = message;
  els.error.hidden = false;
}

worker.onmessage = (message) => {
  const data = message.data;
  switch (data.type) {
    case "opened":
      els.config.hidden = true;
      els.app.hidden = false;
      break;
    case "notes":
      renderNotes(data.rows);
      break;
    case "syncing":
      els.status.textContent = data.value ? "sync: running" : "sync: stopped";
      els.status.dataset.on = String(data.value);
      break;
    case "error":
      showError(data.message);
      break;
  }
};

els.form.addEventListener("submit", (event) => {
  event.preventDefault();
  els.error.hidden = true;
  const form = new FormData(els.form);
  const config = {
    bucket: form.get("bucket").trim(),
    region: form.get("region").trim(),
    endpoint: form.get("endpoint").trim() || null,
    access_key: form.get("access_key").trim(),
    secret_key: form.get("secret_key").trim(),
    key_prefix: form.get("key_prefix").trim() || null,
    library_id: form.get("library_id").trim(),
    // The simplest first config: a plaintext, browsable bucket with readable blob
    // paths. Flip these on (and supply an `encryption_key_hex`) for an end-to-end
    // encrypted home; see the README.
    encrypted: false,
    encryption_key_hex: null,
    obfuscate_blob_paths: false,
    device_id: deviceId,
  };
  worker.postMessage({ type: "open", config });
});

els.noteForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const body = els.noteBody.value.trim();
  if (!body) return;
  worker.postMessage({ type: "addNote", id: crypto.randomUUID(), body });
  els.noteBody.value = "";
});

function renderNotes(rows) {
  els.notes.replaceChildren();
  for (const row of rows) {
    const li = document.createElement("li");
    li.textContent = row.body;
    els.notes.append(li);
  }
}

// Pull in remote notes: re-query on an interval so notes another tab pushed (and
// this tab's sync loop has since pulled into its own database) appear here. The
// query reads local state; the sync loop in the Worker is what fetches remote
// changes into that state.
setInterval(() => worker.postMessage({ type: "list" }), 2_000);
