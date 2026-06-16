// The dedicated Worker that owns the coven library.
//
// coven's browser database is backed by the OPFS sahpool VFS, whose
// FileSystemSyncAccessHandles exist only on a dedicated Worker — not the main
// thread. So the `CovenLibrary` (the wasm facade) and every call into it run
// here, and the page talks to this Worker by message:
//
//   page → worker:  { type: "open",     config }   (open the library)
//                   { type: "addNote",  id, body }  (insert a note + sync now)
//                   { type: "list" }                (read all notes)
//                   { type: "syncNow" }             (force a sync cycle)
//   worker → page:  { type: "opened" }
//                   { type: "notes",  rows }
//                   { type: "syncing", value }      (sync loop running?)
//                   { type: "error",  message }
//
// The pkg/ directory is the `wasm-pack build --target web` output (see README).

import init, { CovenLibrary } from "../../pkg/coven.js";

let library = null;

// A note's last-writer-wins register. The synced-table contract requires every
// synced row to carry an `_updated_at` Hybrid Logical Clock stamp; coven advances
// the clock past pulled rows so a later local write always sorts after them. A
// real app mints this with the `UpdatedAtStamper` that the (native) `Database`
// open returns; the wasm facade does not surface a stamper yet, so this demo
// builds a stamp of the same shape — millisecond counter, a zero sub-counter, and
// the device id — monotonic per tab, which is enough to order this demo's writes.
let stampCounter = 0;
function updatedAtStamp(deviceId) {
  const millis = Date.now();
  const seq = stampCounter++;
  // `{millis}-{counter}-{device_id}`, zero-padded so string compare orders it.
  return `${String(millis).padStart(15, "0")}-${String(seq).padStart(6, "0")}-${deviceId}`;
}

let deviceId = "";

async function handle(message) {
  const data = message.data;
  switch (data.type) {
    case "open": {
      // Initialize the wasm module, then open the library against the config the
      // page collected. `CovenLibrary.open` installs the OPFS VFS, opens the
      // database, and builds the sync runtime; `start_sync` begins the loop.
      await init();
      deviceId = data.config.device_id;
      library = await CovenLibrary.open(data.config);
      library.start_sync();
      self.postMessage({ type: "opened" });
      self.postMessage({ type: "syncing", value: library.is_syncing() });
      await sendNotes();
      break;
    }
    case "addNote": {
      const stamp = updatedAtStamp(deviceId);
      const createdAt = new Date().toISOString();
      // Parameterized through coven's `exec`. The values are escaped here for the
      // demo's single-quote-free inputs; a real app would pass bound parameters.
      const id = sqlString(data.id);
      const body = sqlString(data.body);
      library.exec(
        `INSERT INTO notes (id, body, _updated_at, created_at) ` +
          `VALUES (${id}, ${body}, ${sqlString(stamp)}, ${sqlString(createdAt)}) ` +
          `ON CONFLICT(id) DO UPDATE SET body = excluded.body, _updated_at = excluded._updated_at`,
      );
      // Push this write now rather than waiting out the idle interval.
      library.sync_now();
      await sendNotes();
      break;
    }
    case "list": {
      await sendNotes();
      break;
    }
    case "syncNow": {
      library.sync_now();
      break;
    }
    default:
      self.postMessage({ type: "error", message: `unknown message: ${data.type}` });
  }
}

async function sendNotes() {
  const rows = await library.query(
    "SELECT id, body, _updated_at FROM notes ORDER BY _updated_at DESC",
  );
  self.postMessage({ type: "notes", rows });
}

// Escape a string for inlining into SQL: wrap in single quotes, double any single
// quote inside. Sufficient for this demo's free-text notes; a real app binds
// parameters instead.
function sqlString(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

self.onmessage = (message) => {
  handle(message).catch((e) => {
    self.postMessage({ type: "error", message: String(e) });
  });
};
