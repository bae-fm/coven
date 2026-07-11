// The dedicated Worker that owns the coven store.
//
// coven's browser database is backed by the OPFS sahpool VFS, whose
// FileSystemSyncAccessHandles exist only on a dedicated Worker — not the main
// thread. So the `CovenStore` (the wasm facade) and every call into it run
// here, and the page talks to this Worker by message:
//
//   page → worker:  { type: "open",     config }   (open the store)
//                   { type: "addNote",  id, body }  (insert a note + sync now)
//                   { type: "list" }                (read all notes)
//                   { type: "syncNow" }             (force a sync cycle)
//   worker → page:  { type: "opened" }
//                   { type: "notes",  rows }
//                   { type: "syncing", value }      (sync loop running?)
//                   { type: "error",  message }
//
// The pkg/ directory is the coven-wasm wasm-pack output (see README).

import init, { CovenStore } from "../../pkg/coven_wasm.js";

let store = null;

const migrations = [
  {
    version: 1,
    name: "notes-schema",
    sql: `CREATE TABLE IF NOT EXISTS notes (
      id TEXT PRIMARY KEY,
      body TEXT NOT NULL,
      _updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL
    );`,
  },
];

const syncedTables = [{ name: "notes" }];

async function handle(message) {
  const data = message.data;
  switch (data.type) {
    case "open": {
      // Initialize the wasm module, then open the store against the config the
      // page collected. `CovenStore.open` installs the OPFS VFS, opens the
      // database, and builds the sync runtime; `start_sync` begins the loop.
      await init();
      store = await CovenStore.open(data.config, migrations, syncedTables);
      store.start_sync();
      self.postMessage({ type: "opened" });
      self.postMessage({ type: "syncing", value: store.is_syncing() });
      await sendNotes();
      break;
    }
    case "addNote": {
      const updatedAt = store.stamp();
      const createdAt = new Date().toISOString();
      // Parameterized through coven's `sql`. The values are escaped here for the
      // demo's single-quote-free inputs; a real app would pass bound parameters.
      const id = sqlString(data.id);
      const body = sqlString(data.body);
      store.sql(
        `INSERT INTO notes (id, body, _updated_at, created_at) ` +
          `VALUES (${id}, ${body}, ${sqlString(updatedAt)}, ${sqlString(createdAt)}) ` +
          `ON CONFLICT(id) DO UPDATE SET body = excluded.body, _updated_at = excluded._updated_at`,
      );
      // Push this write now rather than waiting out the idle interval.
      store.sync_now();
      await sendNotes();
      break;
    }
    case "list": {
      await sendNotes();
      break;
    }
    case "syncNow": {
      store.sync_now();
      break;
    }
    default:
      self.postMessage({ type: "error", message: `unknown message: ${data.type}` });
  }
}

async function sendNotes() {
  const rows = await store.sqlRead(
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
