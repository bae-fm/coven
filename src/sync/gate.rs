//! Row-level sync gating.
//!
//! A host declares a boolean **gate** column on a *root* synced table (via
//! [`SyncedTable::gated_by`](super::session::SyncedTable::gated_by)). A root row
//! is shared — i.e. it syncs to peers — iff its gate column is true. The gate
//! flows down *declared foreign keys*: a child row is shared iff the row at the
//! top of its FK chain (its gated-ancestor root) is shared. Rows that are not
//! gated and not FK-descendants of a gated root always sync.
//!
//! The gate also flows **up** for declared *ancestors*
//! ([`SyncedTable::gated_by_descendants`](super::session::SyncedTable::gated_by_descendants)).
//! An ancestor is an always-shared FK *parent* of gated rows (e.g. an album is
//! the FK parent of releases). Left alone it would sync even when its whole gated
//! subtree is cut, landing on peers as an orphan with zero children. A
//! gated-by-descendants ancestor is shared iff some inferred child table still
//! holds a kept row referencing it; the keep composes recursively up the FK chain
//! to the gated roots at the bottom. The keep-children are inferred from the live
//! FK graph, never declared.
//!
//! [`gate_outbound`] is the one entry point. Given the changeset a cycle
//! captured, it returns a new changeset with gated-false rows cut, plus — when a
//! root's gate flips false→true this cycle — full-state INSERTs for that root's
//! whole now-visible subtree (peers never saw it while it was private), so the
//! promotion lands as a complete consistent subtree on every peer.
//!
//! Revoke (gate true→false) is a *retract*: when a previously-shared root flips
//! true→false this cycle, the rows that leave the shared set are emitted as
//! DELETEs so peers remove them — the exact mirror of the false→true re-emit. The
//! flipping device keeps its rows locally (now gated-false = local-only); retract
//! writes only to the outbound changeset, never to the live tables, and fires once
//! on the flip cycle. A root that was never shared has nothing on peers to retract
//! and emits nothing.
//!
//! ## How it is built
//!
//! - **Cut / keep** uses `sqlite3changegroup_add_change`: we walk the captured
//!   changeset and, at each kept row's iterator position, append the change
//!   verbatim into a changegroup, then `sqlite3changegroup_output` the result.
//!   Kept rows keep their exact binary form; nothing is reconstructed.
//! - **Re-emit on flip** uses `sqlite3session_diff`: we attach an empty,
//!   schema-identical in-memory database, create a session on it, diff each
//!   gated table against `main` (empty vs. populated yields a full-state INSERT
//!   per current row), then scope those INSERTs through the same keep-filter,
//!   restricted to the roots that flipped this cycle, and merge them into the
//!   output. The changegroup dedups by primary key, so a row already present
//!   from the captured changeset is not duplicated.
//! - **Retract on flip** is the reverse `sqlite3session_diff`: we create the
//!   session on the *empty* clone and diff `from = "main"` (populated → empty
//!   yields a full-state DELETE per current row), then scope those DELETEs to the
//!   rows leaving the shared set — the structural connected component of the roots
//!   that flipped true→false this cycle, minus the rows still kept by another
//!   managed root — and merge them in.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;

// Reach the SQLite C FFI through rusqlite so it resolves to the right backend
// per target: libsqlite3-sys natively, sqlite-wasm-rs on wasm32.
use rusqlite::ffi;
use rusqlite::Connection;
use tracing::{debug, warn};

use super::session::{quote_ident, SyncedTable};

/// Read a changeset/sqlite value as a text string, or `None` for NULL. Mirrors
/// `sqlite3_value_text` so gate reads match the rest of the engine.
unsafe fn value_to_string(val: *mut ffi::sqlite3_value) -> Option<String> {
    let vtype = ffi::sqlite3_value_type(val);
    if vtype == ffi::SQLITE_NULL as c_int {
        return None;
    }
    let text = ffi::sqlite3_value_text(val);
    if text.is_null() {
        return None;
    }
    Some(
        CStr::from_ptr(text as *const c_char)
            .to_string_lossy()
            .into_owned(),
    )
}

/// The new value at `col` for the change at the iterator's current position
/// (`None` if absent — e.g. an unchanged column in an update — or NULL).
unsafe fn extract_new_value(iter: *mut ffi::sqlite3_changeset_iter, col: c_int) -> Option<String> {
    let mut val: *mut ffi::sqlite3_value = ptr::null_mut();
    let rc = ffi::sqlite3changeset_new(iter, col, &mut val);
    if rc != ffi::SQLITE_OK as c_int || val.is_null() {
        return None;
    }
    value_to_string(val)
}

/// The old value at `col` for the change at the iterator's current position
/// (`None` if absent — e.g. an unchanged column in an update — or NULL).
unsafe fn extract_old_value(iter: *mut ffi::sqlite3_changeset_iter, col: c_int) -> Option<String> {
    let mut val: *mut ffi::sqlite3_value = ptr::null_mut();
    let rc = ffi::sqlite3changeset_old(iter, col, &mut val);
    if rc != ffi::SQLITE_OK as c_int || val.is_null() {
        return None;
    }
    value_to_string(val)
}

/// A changegroup: accumulates changes (by iterator position or whole changeset)
/// and concatenates/dedups them into one output changeset.
struct Changegroup {
    raw: *mut ffi::sqlite3_changegroup,
}

impl Changegroup {
    fn new() -> Result<Self, GateError> {
        let mut raw: *mut ffi::sqlite3_changegroup = ptr::null_mut();
        let rc = unsafe { ffi::sqlite3changegroup_new(&mut raw) };
        if rc != ffi::SQLITE_OK as c_int {
            return Err(GateError::Ffi("sqlite3changegroup_new", rc));
        }
        Ok(Changegroup { raw })
    }

    /// Tell the changegroup the schema of `db` so it can dedup rows by primary
    /// key across `add_change` calls from differently-sourced changesets.
    ///
    /// # Safety
    /// `db` must be a valid, open sqlite3 connection.
    unsafe fn set_schema(&self, db: *mut ffi::sqlite3) -> Result<(), GateError> {
        let main = CString::new("main").unwrap();
        let rc = ffi::sqlite3changegroup_schema(self.raw, db, main.as_ptr());
        if rc != ffi::SQLITE_OK as c_int {
            return Err(GateError::Ffi("sqlite3changegroup_schema", rc));
        }
        Ok(())
    }

    /// Append the change at the iterator's current position.
    ///
    /// # Safety
    /// `iter` must point at a valid current change (a `SQLITE_ROW` step).
    unsafe fn add_change(&self, iter: *mut ffi::sqlite3_changeset_iter) -> Result<(), GateError> {
        let rc = ffi::sqlite3changegroup_add_change(self.raw, iter);
        if rc != ffi::SQLITE_OK as c_int {
            return Err(GateError::Ffi("sqlite3changegroup_add_change", rc));
        }
        Ok(())
    }

    /// Concatenate everything added so far into one changeset's bytes.
    fn output(&self) -> Result<Vec<u8>, GateError> {
        let mut len: c_int = 0;
        let mut buf: *mut c_void = ptr::null_mut();
        let rc = unsafe { ffi::sqlite3changegroup_output(self.raw, &mut len, &mut buf) };
        if rc != ffi::SQLITE_OK as c_int {
            return Err(GateError::Ffi("sqlite3changegroup_output", rc));
        }
        // `output` hands us sqlite3-managed memory; copy it out then free it.
        let bytes = if buf.is_null() || len == 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(buf as *const u8, len as usize).to_vec() }
        };
        if !buf.is_null() {
            unsafe { ffi::sqlite3_free(buf) };
        }
        Ok(bytes)
    }
}

impl Drop for Changegroup {
    fn drop(&mut self) {
        unsafe { ffi::sqlite3changegroup_delete(self.raw) };
    }
}

/// A raw-FFI session wrapper used only by [`full_state_diff`] (the re-emit
/// INSERTs and the retract DELETEs), which runs entirely against the raw
/// `*mut sqlite3` the gate
/// already holds (alongside the changegroup, also raw FFI). The capture session
/// that records host writes lives in [`crate::database`] on `rusqlite::session`;
/// this is a throwaway diff session, not that one.
struct DiffSession {
    raw: *mut ffi::sqlite3_session,
}

impl DiffSession {
    /// Create a diff session bound to the schema named `schema` (`"main"` for the
    /// re-emit direction, the empty-clone alias for the retract direction). The
    /// diff transforms the `from_db` table into this bound schema's table, so
    /// which schema the session binds to picks the direction: bound to `main` with
    /// `from = empty` yields INSERTs; bound to `empty` with `from = "main"` yields
    /// DELETEs.
    unsafe fn new(db: *mut ffi::sqlite3, schema: &str) -> Result<Self, GateError> {
        let db_name = CString::new(schema).unwrap();
        let mut raw: *mut ffi::sqlite3_session = ptr::null_mut();
        let rc = ffi::sqlite3session_create(db, db_name.as_ptr(), &mut raw);
        if rc != ffi::SQLITE_OK as c_int {
            return Err(GateError::SessionCreate(rc));
        }
        Ok(DiffSession { raw })
    }

    unsafe fn attach(&self, table: &str) -> Result<(), GateError> {
        let c_table = CString::new(table).unwrap();
        let rc = ffi::sqlite3session_attach(self.raw, c_table.as_ptr());
        if rc != ffi::SQLITE_OK as c_int {
            return Err(GateError::SessionCreate(rc));
        }
        Ok(())
    }

    /// Record the changes that would transform `from_db.tbl` into the table of the
    /// schema this session is bound to. Bound to `main` with an empty `from_db`,
    /// that is a full-state INSERT per current row; bound to the empty clone with
    /// `from_db = "main"`, it is a full-state DELETE per current row.
    unsafe fn diff(&self, from_db: &str, tbl: &str) -> Result<(), GateError> {
        let from = CString::new(from_db).unwrap();
        let table = CString::new(tbl).unwrap();
        let mut errmsg: *mut c_char = ptr::null_mut();
        let rc = ffi::sqlite3session_diff(self.raw, from.as_ptr(), table.as_ptr(), &mut errmsg);
        if rc != ffi::SQLITE_OK as c_int {
            let detail = if errmsg.is_null() {
                None
            } else {
                let s = CStr::from_ptr(errmsg).to_string_lossy().into_owned();
                ffi::sqlite3_free(errmsg as *mut c_void);
                Some(s)
            };
            return Err(GateError::Diff(tbl.to_string(), rc, detail));
        }
        Ok(())
    }

    /// Extract the recorded changeset bytes.
    unsafe fn changeset(&self) -> Result<Vec<u8>, GateError> {
        let mut len: c_int = 0;
        let mut buf: *mut c_void = ptr::null_mut();
        let rc = ffi::sqlite3session_changeset(self.raw, &mut len, &mut buf);
        if rc != ffi::SQLITE_OK as c_int {
            return Err(GateError::ChangesetExtract(rc));
        }
        let bytes = if buf.is_null() || len == 0 {
            Vec::new()
        } else {
            std::slice::from_raw_parts(buf as *const u8, len as usize).to_vec()
        };
        if !buf.is_null() {
            ffi::sqlite3_free(buf);
        }
        Ok(bytes)
    }
}

impl Drop for DiffSession {
    fn drop(&mut self) {
        unsafe { ffi::sqlite3session_delete(self.raw) };
    }
}

/// How a synced table relates to the gate.
enum TableGate {
    /// A gated root: the boolean gate lives at this column index.
    Root { gate_col: usize },
    /// A child whose gate is inherited from `parent` via the FK column at
    /// `fk_col` (column index in *this* table), holding the parent's id.
    Child { fk_col: usize, parent: String },
    /// An always-shared ancestor kept alive by its gated subtree: shared iff
    /// some inferred child still has a kept row referencing it. Each entry is a
    /// `(child table, FK column index in that child)` pair, where the FK column
    /// holds this table's id. The children are inferred from the live FK graph,
    /// never declared.
    Parent { children: Vec<(String, usize)> },
}

/// The gate model for a sync cycle, computed once from the live schema.
///
/// Maps each gated-or-inheriting synced table to how it resolves its gate. A
/// synced table absent from this map is ungated and unconditionally shared.
pub struct Gates {
    tables: HashMap<String, TableGate>,
}

impl Gates {
    /// Build the gate model from the declared [`SyncedTable`]s and the live
    /// schema (`PRAGMA table_info` for gate-column indices, `PRAGMA
    /// foreign_key_list` for FK edges).
    ///
    /// Runs on the connection coven owns; the session FFI the gate uses needs the
    /// raw handle, so this borrows it once via [`Connection::handle`].
    pub fn from_tables(conn: &Connection, tables: &[SyncedTable]) -> Result<Self, GateError> {
        unsafe { Self::from_tables_raw(conn.handle(), tables) }
    }

    /// # Safety
    /// `db` must be a valid, open sqlite3 connection holding the synced schema.
    unsafe fn from_tables_raw(
        db: *mut ffi::sqlite3,
        tables: &[SyncedTable],
    ) -> Result<Self, GateError> {
        let ancestors: HashSet<&str> = tables
            .iter()
            .filter(|t| t.is_gated_by_descendants())
            .map(|t| t.name())
            .collect();
        let mut gate_map = HashMap::new();

        // Classify each table's downward gate-parent. Roots and ancestors are
        // termini; a plain table inherits from the FK parent picked by
        // `select_parent_fk` (which considers ALL its synced-parent FKs, prefers
        // a parent that reaches a gated root, then the most-specific ancestor,
        // then lexicographic). Ancestors are deferred: their upward keep-children
        // are built below, once every plain table's downward parent is known, so
        // an ancestor is inserted already complete — never empty-then-filled.
        for t in tables {
            if t.is_gated_by_descendants() {
                continue;
            }

            let cols = column_names(db, t.name())?;

            if let Some(gate) = t.gate_column() {
                let idx = cols.iter().position(|c| c == gate).ok_or_else(|| {
                    GateError::MissingGateColumn(t.name().to_string(), gate.to_string())
                })?;
                gate_map.insert(t.name().to_string(), TableGate::Root { gate_col: idx });
                continue;
            }

            // A plain table inherits the gate downward from its selected FK
            // parent. Inheritance flows ONLY through declared FKs, toward synced
            // parents, and (for a multi-FK join row) toward the gated side, never
            // up an ancestor back-edge.
            if let Some((fk_name, parent)) = select_parent_fk(db, t.name(), tables, &ancestors)? {
                let fk_col = cols.iter().position(|c| c == &fk_name).ok_or_else(|| {
                    GateError::MissingFkColumn(t.name().to_string(), fk_name.clone())
                })?;
                gate_map.insert(t.name().to_string(), TableGate::Child { fk_col, parent });
            }
            // else: ungated, unconditionally shared — not in the map.
        }

        // Children are filled once all downward parents are known. A keep-child
        // of ancestor P is any synced table with an FK referencing P, MINUS any
        // table whose chosen downward gate-parent IS P (the join-table back-edge:
        // a child cannot keep its own parent alive — that is the circular fixpoint
        // that would keep an empty album alive forever). An ancestor that infers
        // no children is a host error (the keep would be vacuously false). The
        // children are computed first, so the `Parent` is inserted fully formed.
        for &ancestor in &ancestors {
            let mut children = Vec::new();
            for t in tables {
                if t.name() == ancestor {
                    continue;
                }
                // Skip the back-edge: a table whose downward gate-parent is this
                // ancestor is NOT a keep-child of it.
                if let Some(TableGate::Child { parent, .. }) = gate_map.get(t.name()) {
                    if parent == ancestor {
                        continue;
                    }
                }
                // Otherwise, if this table has an FK referencing the ancestor, it
                // is a keep-child: record the FK column index in that child.
                if let Some(fk_col) = fk_col_referencing(db, t.name(), ancestor)? {
                    children.push((t.name().to_string(), fk_col));
                }
            }
            if children.is_empty() {
                return Err(GateError::NoGatedDescendants(ancestor.to_string()));
            }
            children.sort();
            gate_map.insert(ancestor.to_string(), TableGate::Parent { children });
        }

        // Prune children whose FK chain never reaches a gate terminus (a
        // gated root or an ancestor): they are effectively ungated. Roots and
        // ancestors are themselves termini and are always retained.
        let reaches_gate: HashSet<String> = gate_map
            .keys()
            .filter(|name| chain_to_gate_depth(&gate_map, name).is_some())
            .cloned()
            .collect();
        gate_map.retain(|name, tg| match tg {
            TableGate::Root { .. } | TableGate::Parent { .. } => true,
            TableGate::Child { .. } => reaches_gate.contains(name),
        });

        Ok(Gates { tables: gate_map })
    }

    /// Every table governed by the gate, in FK-topological order: a table comes
    /// after every gated table it has a foreign key to (e.g. artists, albums,
    /// album_artists, releases, tracks).
    ///
    /// [`delete_gated_false`](Self::delete_gated_false) needs this so it can
    /// delete *child-first* — its reverse — without an FK rejecting the deletion
    /// of a parent a child still references under `foreign_keys=ON`. The re-emit
    /// changeset uses the same order for a deterministic, FK-sensible layout; the
    /// changeset *apply* itself tolerates any order, since
    /// `sqlite3changeset_apply` defers FK enforcement to the end of its savepoint.
    ///
    /// A chain-depth sort does not suffice: the gate graph spans both directions
    /// — an ancestor (album) is the FK *parent* of gated rows (releases) yet is
    /// itself kept *by* them — so only a real topological sort over the FK edges
    /// among the gated tables produces a valid order.
    ///
    /// # Safety
    /// `db` must be a valid, open sqlite3 connection holding the synced schema.
    unsafe fn gated_tables_parent_first(
        &self,
        db: *mut ffi::sqlite3,
    ) -> Result<Vec<&str>, GateError> {
        fk_topological_order(db, &self.tables)
    }

    /// Delete from `db` every row the gate excludes: each gated root row whose
    /// gate is false, plus its FK-descendants. This is the same exclusion the
    /// outbound changeset gate applies (a root shares iff its gate is true; a
    /// descendant shares iff its gated-ancestor root does), expressed as SQL
    /// `DELETE`s over the live tables rather than as a changeset filter.
    ///
    /// Both channels a row can use to cross devices — the changeset
    /// ([`gate_outbound`]) and the snapshot — must honor the same gate, so the
    /// snapshot calls this on its VACUUM'd copy to strip gated-false subtrees
    /// before the bytes leave the device. Sharing this method (not a parallel
    /// FK model) keeps a single definition of what the gate excludes.
    ///
    /// Each gated table — root, descendant, or ancestor — resolves its own keep
    /// by a fully-inlined clause that bottoms out at root truthy columns. The
    /// prune is monotonic: a `DELETE` removes only rows that fail their *own*
    /// keep, and a kept row's keep references only rows that are themselves kept
    /// (never deleted), so deleting gated-false rows can never flip a kept row to
    /// not-kept. The final row set is therefore independent of deletion order.
    ///
    /// Runs on the snapshot copy's owned connection; borrows the raw handle once
    /// for the FK-walk SQL.
    pub fn delete_gated_false(&self, conn: &Connection) -> Result<(), GateError> {
        unsafe { self.delete_gated_false_raw(conn.handle()) }
    }

    /// # Safety
    /// `db` must be a valid, open sqlite3 connection holding the synced schema.
    unsafe fn delete_gated_false_raw(&self, db: *mut ffi::sqlite3) -> Result<(), GateError> {
        // The final row set is order-independent (the prune is monotonic, above).
        // The only caller is the snapshot scope, whose copy connection opens with
        // `foreign_keys` OFF, so no FK would reject deleting a parent before its
        // child here. We still delete child-first — the reverse of the
        // FK-topological apply order — so this stays correct under
        // `foreign_keys=ON` too: a parent FK without `ON DELETE CASCADE` would
        // otherwise reject deleting a parent a child still references. Child-first
        // is order-safe regardless of the copy's FK setting.
        let mut order = self.gated_tables_parent_first(db)?;
        order.reverse();
        for tbl in order {
            let keep = self.keep_clause(db, tbl)?;
            let sql = format!("DELETE FROM {} WHERE NOT ({keep})", quote_ident(tbl));
            exec_sql(db, &sql)?;
        }
        Ok(())
    }

    /// A SQL boolean that is true for rows of `tbl` the gate keeps. The shape
    /// depends on how `tbl` relates to the gate:
    ///
    /// - **Root**: the root's own gate column, tested truthy.
    /// - **Child**: a correlated `EXISTS` joining up the FK to the parent's
    ///   keep-clause, so the gate flows *down* the chain to the root truthy test.
    /// - **Parent** (ancestor): a disjunction of correlated `EXISTS`, one per
    ///   inferred child, so the keep flows *up* — the ancestor is kept iff some
    ///   child has a kept row referencing it.
    ///
    /// Built inside-out and fully inlined down to the root truthy columns. A
    /// dangling FK anywhere makes its `EXISTS` false (not shared), matching
    /// `resolve_root`'s treatment of a missing ancestor. The recursion is
    /// cycle-guarded by `visiting`: a `Parent` references its children and a
    /// `Child` references its parent, so a malformed declaration could otherwise
    /// loop. Revisiting a table in the current path yields `FALSE` rather than
    /// recursing again.
    ///
    /// # Safety
    /// `db` must be a valid, open sqlite3 connection holding the synced schema.
    unsafe fn keep_clause(&self, db: *mut ffi::sqlite3, tbl: &str) -> Result<String, GateError> {
        self.keep_clause_guarded(db, tbl, &mut HashSet::new())
    }

    unsafe fn keep_clause_guarded(
        &self,
        db: *mut ffi::sqlite3,
        tbl: &str,
        visiting: &mut HashSet<String>,
    ) -> Result<String, GateError> {
        if !visiting.insert(tbl.to_string()) {
            // Already on the current recursion path: refuse to loop. A row kept
            // only via a cycle is treated as not kept.
            return Ok("FALSE".to_string());
        }
        let clause = match self.tables.get(tbl) {
            Some(TableGate::Root { gate_col }) => {
                let gate = nth_column_name(db, tbl, *gate_col)?;
                truthy_sql(&format!("{}.{}", quote_ident(tbl), quote_ident(&gate)))
            }
            Some(TableGate::Child { fk_col, parent }) => {
                let fk = nth_column_name(db, tbl, *fk_col)?;
                let inner = self.keep_clause_guarded(db, parent, visiting)?;
                // Join up the FK to the parent's keep: parent.id = child.fk.
                fk_exists_clause(parent, "id", tbl, &fk, &inner)
            }
            Some(TableGate::Parent { children }) => {
                if children.is_empty() {
                    // `from_tables` rejects an ancestor with no inferred children
                    // at construction, so a `Parent` reaching here always has at
                    // least one.
                    unreachable!("Parent {tbl} has empty children, rejected by from_tables");
                }
                let mut disjuncts = Vec::with_capacity(children.len());
                for (child, fk_col) in children {
                    let fk = nth_column_name(db, child, *fk_col)?;
                    let inner = self.keep_clause_guarded(db, child, visiting)?;
                    // Join down to each child's keep: child.fk = parent.id.
                    disjuncts.push(fk_exists_clause(child, &fk, tbl, "id", &inner));
                }
                format!("({})", disjuncts.join(" OR "))
            }
            // Unreachable: callers pass table names straight from `self.tables`,
            // and the recursion descends only to parents/children that
            // `from_tables` proved are in the map. A table outside the map never
            // reaches this match.
            None => unreachable!("keep_clause called for {tbl}, absent from the gate map"),
        };
        visiting.remove(tbl);
        Ok(clause)
    }

    /// Whether the live row (`tbl`, `id`) is currently kept by the gate, by
    /// evaluating `tbl`'s keep-clause against the live db for that one row. Used
    /// to resolve an ancestor's share decision (an album is kept iff it has a
    /// kept child) — a property of the live child tables, not of the ancestor
    /// row's own columns.
    ///
    /// # Safety
    /// `db` must be a valid, open sqlite3 connection holding the synced schema.
    unsafe fn row_kept(
        &self,
        db: *mut ffi::sqlite3,
        tbl: &str,
        id: &str,
    ) -> Result<bool, GateError> {
        let keep = self.keep_clause(db, tbl)?;
        let sql = format!(
            "SELECT 1 FROM {t} WHERE {t}.{id_col} = ? AND ({keep})",
            t = quote_ident(tbl),
            id_col = quote_ident("id"),
        );
        let stmt = prepare(db, &sql)?;
        bind_text(stmt, 1, id);
        let present = ffi::sqlite3_step(stmt) == ffi::SQLITE_ROW as c_int;
        ffi::sqlite3_finalize(stmt);
        Ok(present)
    }
}

/// The SQL form of [`truthy`]: a predicate that is true for `expr` exactly when
/// [`truthy`] would return true for the same value. [`truthy`] owns the single
/// definition of gate-truth; this realizes it in SQL — the `CAST` collapses to 0
/// for NULL and non-numeric text, so only a genuine nonzero integer passes.
/// Keep the two in lockstep: a change to the gate-truth rule changes both.
fn truthy_sql(expr: &str) -> String {
    format!("({expr} IS NOT NULL AND CAST({expr} AS INTEGER) <> 0)")
}

/// A correlated `EXISTS` that follows one FK edge to a related table's keep:
/// true for a row of `self_t` when some row of `other_t` joins to it on
/// `other_t.other_col = self_t.self_col` and itself satisfies `inner`. The Child
/// keep (join *up* to the parent: `parent.id = child.fk`) and the Parent keep
/// (join *down* to a child: `child.fk = parent.id`) are the same shape with the
/// join direction swapped.
fn fk_exists_clause(
    other_t: &str,
    other_col: &str,
    self_t: &str,
    self_col: &str,
    inner: &str,
) -> String {
    format!(
        "EXISTS (SELECT 1 FROM {other} \
           WHERE {other}.{other_col} = {this}.{self_col} AND ({inner}))",
        other = quote_ident(other_t),
        other_col = quote_ident(other_col),
        this = quote_ident(self_t),
        self_col = quote_ident(self_col),
    )
}

/// Walk `gate_map` from `name` up its declared-FK chain to a gate terminus — a
/// gated root or an ancestor: `Some(0)` for a terminus itself, `Some(n)` for an
/// n-hop descendant of one, `None` if the chain never reaches a terminus (the
/// table is effectively ungated) or loops. Only `Child` links are followed
/// upward; a terminus stops the walk (a `Parent`'s upward keep over its own
/// children is a separate relation, not part of this downward chain).
fn chain_to_gate_depth(gate_map: &HashMap<String, TableGate>, name: &str) -> Option<usize> {
    let mut depth = 0;
    let mut cur = name;
    let mut seen = HashSet::new();
    loop {
        if !seen.insert(cur.to_string()) {
            return None; // cycle, defensive
        }
        match gate_map.get(cur) {
            Some(TableGate::Root { .. }) | Some(TableGate::Parent { .. }) => return Some(depth),
            Some(TableGate::Child { parent, .. }) => {
                depth += 1;
                cur = parent.as_str();
            }
            None => return None,
        }
    }
}

/// The gated FK edges of the schema, as `parent table -> [(child table, child's
/// FK column name)]`: for every gated table, each of its FKs that points at
/// another gated table contributes an edge under the *target* (the parent). The
/// fixpoint walk in [`connected_component`] follows these down-edges
/// directly; [`fk_topological_order`] uses the same edges (discarding the FK
/// column) so the parent-first order is derived from one definition, not a second
/// parallel FK scan.
///
/// # Safety
/// `db` must be a valid, open sqlite3 connection holding the synced schema.
unsafe fn gated_fk_child_edges(
    db: *mut ffi::sqlite3,
    gate_map: &HashMap<String, TableGate>,
) -> Result<HashMap<String, Vec<(String, String)>>, GateError> {
    let mut edges: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for referrer in gate_map.keys() {
        for (fk_col, target) in foreign_keys(db, referrer)? {
            // Self-FKs and FKs to ungated tables are not cross-table gate edges.
            if target == *referrer {
                continue;
            }
            if gate_map.contains_key(&target) {
                edges
                    .entry(target)
                    .or_default()
                    .push((referrer.clone(), fk_col));
            }
        }
    }
    Ok(edges)
}

/// The gated tables in FK-topological order: every table comes after every gated
/// table it has a foreign key to (e.g. artists, albums, album_artists, releases,
/// tracks). The snapshot prune deletes in the reverse (child-first) order, which
/// must not reject a parent's deletion under `foreign_keys=ON`; the re-emit
/// changeset reuses this order for a deterministic, FK-sensible layout.
///
/// A chain-depth sort does not suffice: the gate graph spans both directions — an
/// ancestor (album) is the FK *parent* of gated rows (releases) yet is itself
/// kept *by* them — so only a real topological sort over the FK edges produces a
/// valid order. Deterministic Kahn: among the ready (zero-indegree) tables,
/// always take the lexicographically smallest, so the order is stable.
///
/// # Safety
/// `db` must be a valid, open sqlite3 connection holding the synced schema.
unsafe fn fk_topological_order(
    db: *mut ffi::sqlite3,
    gate_map: &HashMap<String, TableGate>,
) -> Result<Vec<&str>, GateError> {
    // Edge parent -> child means "parent must precede child". A table's FK to a
    // gated table makes that table its prerequisite (it points at the parent's
    // id), so the FK target is the parent of the edge and the referrer the child.
    // Only edges between two gated tables matter.
    let names: Vec<&str> = gate_map.keys().map(String::as_str).collect();
    let mut indegree: HashMap<&str, usize> = names.iter().map(|&n| (n, 0)).collect();
    let mut edges: HashMap<&str, Vec<&str>> = names.iter().map(|&n| (n, Vec::new())).collect();

    let child_edges = gated_fk_child_edges(db, gate_map)?;
    for (parent, children) in &child_edges {
        // Resolve the owned names back to the gate_map's `&str` keys so the
        // returned order borrows from the map.
        let (parent_key, _) = gate_map.get_key_value(parent).unwrap();
        for (child, _fk_col) in children {
            let (child_key, _) = gate_map.get_key_value(child).unwrap();
            edges.get_mut(parent_key.as_str()).unwrap().push(child_key);
            *indegree.get_mut(child_key.as_str()).unwrap() += 1;
        }
    }

    // Kahn with a deterministic tie-break: a min-heap of the ready
    // (zero-indegree) tables, so equal-rank tables always emit smallest-first.
    let mut ready: BinaryHeap<Reverse<&str>> = indegree
        .iter()
        .filter(|(_, &d)| d == 0)
        .map(|(&n, _)| Reverse(n))
        .collect();

    let mut order = Vec::with_capacity(names.len());
    while let Some(Reverse(next)) = ready.pop() {
        order.push(next);
        for &child in &edges[next] {
            let d = indegree.get_mut(child).unwrap();
            *d -= 1;
            if *d == 0 {
                ready.push(Reverse(child));
            }
        }
    }

    if order.len() != names.len() {
        let mut remaining: Vec<String> = names
            .iter()
            .filter(|n| !order.contains(*n))
            .map(|n| n.to_string())
            .collect();
        remaining.sort();
        return Err(GateError::FkCycle(remaining));
    }
    Ok(order)
}

/// Gate a captured outbound changeset: cut gated-false rows (and their gated
/// descendants), re-emit the full subtree of any root that flipped false→true
/// this cycle as INSERTs, and emit DELETEs for the rows that leave the shared set
/// when a root flips true→false this cycle (retract).
///
/// Runs on the connection coven owns; gating reads current row state from the
/// live tables (capture stays enabled here, disabled only around the pull's
/// apply). The changegroup and changeset-iteration FFI need the raw handle,
/// borrowed once here.
pub fn gate_outbound(
    conn: &Connection,
    changeset: &[u8],
    gates: &Gates,
) -> Result<Vec<u8>, GateError> {
    unsafe { gate_outbound_raw(conn.handle(), changeset, gates) }
}

/// # Safety
/// `db` must be the valid, open connection the changeset was captured on, with
/// no live session attached (gating reads current row state from it).
unsafe fn gate_outbound_raw(
    db: *mut ffi::sqlite3,
    changeset: &[u8],
    gates: &Gates,
) -> Result<Vec<u8>, GateError> {
    let group = Changegroup::new()?;
    group.set_schema(db)?;

    // Roots that flip false→true this cycle need their whole current connected
    // component re-emitted (peers never had it while private) — descendants AND
    // always-shared ancestors. Keyed by `(root table, root id)`.
    let mut flipped_roots: HashSet<(String, String)> = HashSet::new();

    // Roots that flip true→false this cycle were shared and must be retracted: the
    // rows leaving their shared set are emitted as DELETEs below (pass 2), so peers
    // remove them. Keyed by `(root table, root id)`. The mirror of flipped_roots.
    let mut retracted_roots: HashSet<(String, String)> = HashSet::new();

    // Gated parents a kept row's UPDATE repoints an FK onto. The new parent may
    // be a subtree peers never received (cut until this reparent made it
    // relevant), so it seeds the re-emit alongside the flipped roots. Without it,
    // a peer applies the bare FK-change against a parent it doesn't have.
    let mut reparent_seeds: HashSet<(String, String)> = HashSet::new();

    // Every deleted row's old values, so a DELETE's keep test can resolve the gate
    // against the row's pre-deletion state (its terminus may be gone from the live
    // db). Memo + cycle guard span the whole pass.
    let deleted = collect_deletes(changeset)?;
    let mut shared_memo: HashMap<(String, String), bool> = HashMap::new();
    let mut shared_visiting: HashSet<(String, String)> = HashSet::new();

    // Pass 1: walk the captured changeset, keep gated-true rows, note flips.
    for_each_change(changeset, |iter, row| {
        // A root whose gate flips false→true this cycle has its whole now-visible
        // subtree re-emitted as full-state INSERTs below. Record it and skip the
        // captured row: an UPDATE(false→true) is wrong for a peer that never had
        // the row (it would apply as a NOTFOUND no-op), and an INSERT is reproduced
        // identically by the re-emit. Letting re-emit be the single source avoids
        // an UPDATE/INSERT dedup clash.
        if let Some(TableGate::Root { gate_col }) = gates.tables.get(&row.table) {
            let flips = match row.op {
                x if x == ffi::SQLITE_UPDATE => {
                    row.old_truth(*gate_col) == Some(false)
                        && row.new_truth(*gate_col) == Some(true)
                }
                x if x == ffi::SQLITE_INSERT => row.new_truth(*gate_col) == Some(true),
                _ => false,
            };
            if flips {
                match row.pk() {
                    Some(pk) => {
                        flipped_roots.insert((row.table.clone(), pk.to_string()));
                    }
                    None => debug!(
                        table = %row.table,
                        "gate: flipped root row has no primary key; its subtree cannot be re-emitted"
                    ),
                }
                return Ok(());
            }

            // A gated root flipping true→false this cycle was shared; emit DELETEs
            // for the rows leaving its shared set (pass 2). Skip the captured
            // UPDATE-to-false: a peer applying it would freeze a zombie (the row
            // stops updating but is never removed); the synthetic DELETE replaces
            // it. A root that was never shared has no true→false transition and is
            // handled by the ordinary cut path below (it emits nothing).
            let retracts = row.op == ffi::SQLITE_UPDATE
                && row.old_truth(*gate_col) == Some(true)
                && row.new_truth(*gate_col) == Some(false);
            if retracts {
                match row.pk() {
                    Some(pk) => {
                        retracted_roots.insert((row.table.clone(), pk.to_string()));
                    }
                    None => debug!(
                        table = %row.table,
                        "gate: retracted root row has no primary key; its subtree cannot be retracted"
                    ),
                }
                return Ok(());
            }
        }

        // A DELETE propagates iff the row was shared before this changeset removed
        // it — resolved against its pre-deletion state, since the live db (and thus
        // `effective_gate`) no longer holds its gate terminus. An insert/update
        // keeps the live-state gate.
        let keep = if row.op == ffi::SQLITE_DELETE {
            match row.pk() {
                Some(pk) => was_shared(
                    db,
                    gates,
                    &deleted,
                    &row.table,
                    pk,
                    &mut shared_memo,
                    &mut shared_visiting,
                )?,
                None => {
                    debug!(table = %row.table, "gate: delete row has no primary key; treating as not shared");
                    false
                }
            }
        } else {
            effective_gate(db, gates, &row)?
        };
        if keep {
            group.add_change(iter)?;
            // A kept row that repoints an FK onto a gated parent drags that parent's
            // (possibly never-shared) subtree into visibility.
            reparent_seeds.extend(reparent_targets(db, gates, &row)?);
        }
        Ok(())
    })?;

    // Pass 2: re-emit full subtrees for flipped roots and reparent targets.
    if !flipped_roots.is_empty() || !reparent_seeds.is_empty() {
        reemit_subtrees(db, gates, &flipped_roots, &reparent_seeds, &group)?;
    }

    // Pass 2 (retract): emit DELETEs for the rows leaving the shared set of any
    // root that flipped true→false this cycle. The mirror of reemit_subtrees.
    if !retracted_roots.is_empty() {
        reemit_retract_deletes(db, gates, &retracted_roots, &group)?;
    }

    group.output()
}

/// New gated parents a kept row's UPDATE repoints a foreign key onto. When a kept
/// row's FK to a gated table changes (e.g. a managed release moves to another
/// album that had no managed release before, so peers never saw it), the new
/// parent's subtree must be re-emitted or the peer applies the FK change against
/// a missing parent. Returns `(parent_table, new_parent_id)` per changed FK to a
/// gated table; the caller seeds the re-emit with them.
///
/// # Safety
/// `db` must be the valid, open connection the changeset was captured on.
unsafe fn reparent_targets(
    db: *mut ffi::sqlite3,
    gates: &Gates,
    row: &ChangeRow,
) -> Result<Vec<(String, String)>, GateError> {
    // Only an UPDATE repoints an existing row's FK. An INSERT of a managed root is
    // already re-emitted via the gate flip; a new child under an already-shared
    // parent needs nothing extra.
    if row.op != ffi::SQLITE_UPDATE {
        return Ok(Vec::new());
    }
    let cols = column_names(db, &row.table)?;
    let mut out = Vec::new();
    for (fk_col, parent) in foreign_keys(db, &row.table)? {
        if parent == row.table || !gates.tables.contains_key(&parent) {
            continue;
        }
        let Some(idx) = cols.iter().position(|c| c == &fk_col) else {
            warn!(
                table = %row.table,
                fk_col,
                "reparent_targets: FK column absent from table columns; skipping"
            );
            continue;
        };
        // A session UPDATE records a column only when it changed, so an old AND a
        // new value both present means this FK was repointed this cycle.
        let old = row.old.get(idx).and_then(|v| v.as_deref());
        let new = row.new.get(idx).and_then(|v| v.as_deref());
        if let (Some(old), Some(new)) = (old, new) {
            if old != new {
                out.push((parent, new.to_string()));
            }
        }
    }
    Ok(out)
}

/// Re-emit the whole connected component of currently-kept gated rows reachable
/// from each flipped root as full-state INSERTs: the root's *descendants* (rows
/// whose gated-ancestor root is a flipped root) AND the transitive closure of the
/// kept rows around it — its always-shared *ancestors* up the FK chain (album,
/// artist), the kept *children* of those ancestors (album_artists, sibling
/// already-managed releases), the *ancestors of those kept children* (a featured
/// artist credited via a join row, off the flipped row's own lineage), and so on
/// to a fixpoint. A peer that never saw the now-public root needs the entire
/// component to land, exactly the row set the snapshot `keep_clause` would keep.
///
/// Over-emitting is safe: the apply conflict handler resolves a duplicate-PK
/// INSERT by `_updated_at` LWW (never aborts), so re-sending a row a peer already
/// has is idempotent. Under-emitting is the only failure, so the closure is
/// computed in full rather than as a fixed up-then-one-level-down pass.
unsafe fn reemit_subtrees(
    db: *mut ffi::sqlite3,
    gates: &Gates,
    flipped_roots: &HashSet<(String, String)>,
    reparent_seeds: &HashSet<(String, String)>,
    group: &Changegroup,
) -> Result<(), GateError> {
    // Compute the whole connected kept component of every flipped root: its
    // ancestors (album, artist), the kept children of those ancestors
    // (album_artists, sibling releases), and — transitively — the ancestors of
    // those kept children (a featured artist credited via a join row) and so on.
    // These are re-emitted by explicit `(table, id)` membership; the flipped
    // root's own descendants are re-emitted by the scoping test below. Both feed
    // the same diff. The reparent targets seed the walk too, so a newly-referenced
    // parent's whole kept component lands on peers.
    let mut seeds = flipped_roots.clone();
    seeds.extend(reparent_seeds.iter().cloned());
    let reemit_ids = connected_component(db, gates, &seeds, true)?;

    let diff_bytes = full_state_diff(db, gates, FullStateDirection::Inserts)?;
    if diff_bytes.is_empty() {
        return Ok(());
    }

    for_each_change(&diff_bytes, |iter, row| {
        let in_descendants =
            gated_root_id(db, gates, &row)?.is_some_and(|key| flipped_roots.contains(&key));
        let in_kept_component = row
            .pk()
            .is_some_and(|pk| reemit_ids.contains(&(row.table.clone(), pk.to_string())));
        if in_descendants || in_kept_component {
            group.add_change(iter)?;
        }
        Ok(())
    })
}

/// Emit DELETEs for the rows that leave the shared set when each retracted root's
/// gate flips true→false this cycle — the exact mirror of [`reemit_subtrees`].
///
/// The candidate set is the *structural* connected component of the retracted
/// roots ([`connected_component`] with `restrict_to_kept = false`): the same
/// bidirectional FK closure the re-emit path walks, except the down-walk follows
/// live FK edges WITHOUT a kept-filter. The rows still physically exist locally —
/// only the root's gate column changed — so a kept-filter would wrongly exclude
/// the root's own descendants (they inherit the now-false gate).
///
/// From that candidate set we keep only the rows NO LONGER kept under the
/// post-flip live state (`!gates.row_kept`). That single filter does both jobs:
/// it SPARES a sibling still held by another managed root sharing an album/artist
/// ancestor (that sibling is still kept), and it INCLUDES a now-childless
/// `gated_by_descendants` ancestor (album/artist) so it is DELETEd too (it is no
/// longer kept).
///
/// The DELETEs are synthesized by [`full_state_diff`] with
/// [`FullStateDirection::Deletes`] (a DELETE per live gated row, scoped here to
/// the to-delete set). The changegroup dedups by primary key, so a row both
/// locally deleted this cycle and synthetically retracted resolves to a single
/// DELETE. That diff only carries rows still present in `main`, so a row the
/// captured changeset already removed never collides here.
unsafe fn reemit_retract_deletes(
    db: *mut ffi::sqlite3,
    gates: &Gates,
    retracted_roots: &HashSet<(String, String)>,
    group: &Changegroup,
) -> Result<(), GateError> {
    let component = connected_component(db, gates, retracted_roots, false)?;

    // Keep only the rows no longer kept under the post-flip live state. The live db
    // already reflects the gate flip when gate_outbound runs, so the retracted
    // root and its now-orphaned descendants/ancestors read not-kept, while a
    // sibling still held by another managed root reads kept and is spared.
    let mut to_delete: HashSet<(String, String)> = HashSet::new();
    for (table, id) in component {
        if !gates.row_kept(db, &table, &id)? {
            to_delete.insert((table, id));
        }
    }
    if to_delete.is_empty() {
        return Ok(());
    }

    let delete_bytes = full_state_diff(db, gates, FullStateDirection::Deletes)?;
    if delete_bytes.is_empty() {
        return Ok(());
    }

    for_each_change(&delete_bytes, |iter, row| {
        let in_to_delete = row
            .pk()
            .is_some_and(|pk| to_delete.contains(&(row.table.clone(), pk.to_string())));
        if in_to_delete {
            group.add_change(iter)?;
        }
        Ok(())
    })
}

/// The whole connected component of gated rows reachable from `seeds`, walking the
/// live FK graph in BOTH directions: *up* to gated ancestors (release → album →
/// artist) and *down* to gated children (album → its releases; artist → its join
/// rows). Crucially, a child pulled in *down* has its own ancestors walked *up* in
/// turn — a join row (album_artists) reached as a child of an album drags in the
/// second artist it credits (a featured artist who does not own the album), which
/// the snapshot `keep_clause` also keeps. The result is the transitive closure,
/// cycle-guarded by the visited set.
///
/// `restrict_to_kept` governs only the *down*-walk (the *up*-walk is unconditional
/// either way, so an ancestor is always reached and the caller can make its share
/// decision):
///
/// - `true` (re-emit, false→true): descend only into currently-*kept* children, so
///   the component is exactly the row set the snapshot `keep_clause` keeps — the
///   rows a fresh peer must materialize. Reconstructs in row-walk form the same
///   relation `keep_clause` expresses recursively.
/// - `false` (retract, true→false): descend *structurally* into every child by
///   live FK, no kept-filter. At retract time the root's gate column has already
///   flipped false, so its descendants are no longer kept; a kept-filtered walk
///   would never reach them. The retract caller filters the structural component
///   by post-flip `row_kept` to decide which rows actually leave the shared set.
///
/// Over-collecting is safe for both callers (re-emit dedups by PK and resolves a
/// duplicate INSERT by LWW; retract filters by `row_kept` before emitting); only
/// under-collecting fails, so the closure is computed in full rather than as a
/// fixed up-then-one-level-down pass.
unsafe fn connected_component(
    db: *mut ffi::sqlite3,
    gates: &Gates,
    seeds: &HashSet<(String, String)>,
    restrict_to_kept: bool,
) -> Result<HashSet<(String, String)>, GateError> {
    // Down-edges: for each gated table, the gated tables that hold an FK
    // referencing it, paired with the referrer's FK column name. Built once from
    // the shared FK-edge scan so the per-row down-expansion is a map lookup, not a
    // schema scan, and the same edges drive `fk_topological_order`.
    let down_edges = gated_fk_child_edges(db, &gates.tables)?;

    let mut out: HashSet<(String, String)> = HashSet::new();
    let mut work: Vec<(String, String)> = seeds.iter().cloned().collect();
    while let Some((table, id)) = work.pop() {
        if !out.insert((table.clone(), id.clone())) {
            continue; // already visited: cycle-guard and dedup.
        }
        // Up: every gated FK parent of this row.
        for (fk_col_name, parent) in foreign_keys(db, &table)? {
            if parent == table || !gates.tables.contains_key(&parent) {
                continue;
            }
            if let Some(parent_id) = query_column_text(db, &table, &fk_col_name, &id)? {
                work.push((parent, parent_id));
            }
        }
        // Down: each gated child referencing this row — filtered to kept children
        // for re-emit, taken structurally (every live FK edge) for retract.
        if let Some(children) = down_edges.get(table.as_str()) {
            for (child_table, fk) in children {
                for child_id in rows_referencing(db, child_table, fk, &id)? {
                    if !restrict_to_kept || gates.row_kept(db, child_table, &child_id)? {
                        work.push((child_table.clone(), child_id));
                    }
                }
            }
        }
    }
    Ok(out)
}

/// The ids of rows in `table` whose `fk` column equals `value`.
unsafe fn rows_referencing(
    db: *mut ffi::sqlite3,
    table: &str,
    fk: &str,
    value: &str,
) -> Result<Vec<String>, GateError> {
    let sql = format!(
        "SELECT {id} FROM {t} WHERE {fk} = ?",
        id = quote_ident("id"),
        t = quote_ident(table),
        fk = quote_ident(fk),
    );
    let stmt = prepare(db, &sql)?;
    bind_text(stmt, 1, value);
    let mut ids = Vec::new();
    while ffi::sqlite3_step(stmt) == ffi::SQLITE_ROW as c_int {
        let p = ffi::sqlite3_column_text(stmt, 0);
        if p.is_null() {
            // `id` is a NOT NULL primary key, so a NULL here is a genuine schema
            // anomaly, not a row we may quietly drop from the kept component.
            warn!("gate: row in {table} referencing {fk}={value} has a NULL id; skipping it from the kept component");
            continue;
        }
        ids.push(
            CStr::from_ptr(p as *const c_char)
                .to_string_lossy()
                .into_owned(),
        );
    }
    ffi::sqlite3_finalize(stmt);
    Ok(ids)
}

/// Attach a fresh empty in-memory db, recreate each gated table's schema in it
/// (copied verbatim from `sqlite_master` so a diff sees identical tables), run
/// `f` against the clone, and always detach afterward. Both full-state diff
/// directions share this setup; they differ only in which schema the diff session
/// binds to. `f` receives the clone alias and the gated tables in parent-first
/// order. A unique alias avoids colliding with any host-attached db.
unsafe fn with_empty_clone<R>(
    db: *mut ffi::sqlite3,
    gates: &Gates,
    f: impl FnOnce(&str, &[&str]) -> Result<R, GateError>,
) -> Result<R, GateError> {
    let alias = "coven_gate_empty";
    let attach = format!("ATTACH DATABASE ':memory:' AS {alias}");
    exec_sql(db, &attach)?;

    let tables = gates.gated_tables_parent_first(db)?;
    let result = (|| {
        for tbl in &tables {
            let create = create_table_sql(db, tbl)?;
            // The CREATE statement names the bare table; run it in the attached
            // db by qualifying via the schema-aware exec on the alias.
            let in_alias = rewrite_create_into_schema(&create, tbl, alias);
            exec_sql(db, &in_alias)?;
        }
        f(alias, &tables)
    })();

    // Always detach, even on error. A failed detach leaves the clone attached
    // under `alias`, which would make next cycle's ATTACH collide — surface it.
    let detach = format!("DETACH DATABASE {alias}");
    if let Err(e) = exec_sql(db, &detach) {
        warn!("gate: failed to detach the temporary clone db ({alias}): {e}");
    }

    result
}

/// Which direction a full-state diff against the empty clone runs. The two
/// directions are exact mirrors: `sqlite3session_diff` records the changes that
/// transform the `from_db` table into the session-bound table, so the
/// bind/`from` pairing — not a flag inside SQLite — is what sets the direction
/// (verified by the retract peer-apply tests).
enum FullStateDirection {
    /// Bind the session to `main`, diff `from = empty`: empty → main yields a
    /// full-state INSERT per current row (the re-emit, false→true).
    Inserts,
    /// Bind the session to the empty clone, diff `from = "main"`: main → empty
    /// yields a full-state DELETE per current row (present in `from`, absent in
    /// the session db). The retract path (true→false) scopes these to the rows
    /// leaving the shared set.
    Deletes,
}

/// Diff every gated table against an empty schema-identical clone, producing a
/// full-state changeset for all currently-present rows of those tables —
/// [`FullStateDirection::Inserts`] for the re-emit, [`FullStateDirection::Deletes`]
/// for the retract.
unsafe fn full_state_diff(
    db: *mut ffi::sqlite3,
    gates: &Gates,
    direction: FullStateDirection,
) -> Result<Vec<u8>, GateError> {
    with_empty_clone(db, gates, |alias, tables| {
        let (session_schema, from_schema) = match direction {
            FullStateDirection::Inserts => ("main", alias),
            FullStateDirection::Deletes => (alias, "main"),
        };
        let session = DiffSession::new(db, session_schema)?;
        for tbl in tables {
            session.attach(tbl)?;
            session.diff(from_schema, tbl)?;
        }
        session.changeset()
    })
}

/// Whether `row`'s effective gate is true (it should be kept/shared).
unsafe fn effective_gate(
    db: *mut ffi::sqlite3,
    gates: &Gates,
    row: &ChangeRow,
) -> Result<bool, GateError> {
    match gates.tables.get(&row.table) {
        None => Ok(true), // ungated table: always shared.
        Some(TableGate::Root { gate_col }) => match row.effective_truth(*gate_col) {
            Some(t) => Ok(t),
            // Gate unchanged in an UPDATE (omitted from the changeset): read the
            // current value from the live row. A delete with no old gate value
            // resolves the same way (the row may still exist as old-state).
            None => match row.pk() {
                Some(pk) => {
                    let col = nth_column_name(db, &row.table, *gate_col)?;
                    match query_truth(db, &row.table, &col, pk)? {
                        Some(t) => Ok(t),
                        None => {
                            warn!(
                                "gate: root {}.{pk} absent from live db while resolving an \
                                 unchanged gate column; treating as not-shared",
                                row.table
                            );
                            Ok(false)
                        }
                    }
                }
                None => {
                    debug!(
                        "gate: root row in {} has no primary key; treating as not-shared",
                        row.table
                    );
                    Ok(false)
                }
            },
        },
        Some(TableGate::Child { fk_col, parent }) => {
            let parent_id = match row.fk_value(*fk_col) {
                Some(id) => id.to_string(),
                None => match row.pk() {
                    // FK unchanged in an UPDATE: read it from the live row.
                    Some(pk) => match lookup_fk_in_db(db, &row.table, *fk_col, pk)? {
                        Some(id) => id,
                        None => {
                            warn!(
                                "gate: child {}.{pk} has no FK target in live db; \
                                 treating as not-shared",
                                row.table
                            );
                            return Ok(false);
                        }
                    },
                    None => {
                        debug!(
                            "gate: child row in {} has no primary key; treating as not-shared",
                            row.table
                        );
                        return Ok(false);
                    }
                },
            };
            Ok(resolve_root(db, gates, parent, &parent_id)?
                .map(|r| r.kept)
                .unwrap_or(false))
        }
        Some(TableGate::Parent { .. }) => {
            // An ancestor (album) is shared iff it currently has a kept child
            // referencing it. The keep is a property of the *live* child tables,
            // not of the ancestor row's own columns, so we evaluate the ancestor's
            // keep-clause against the live db for this row's pk. An album in the
            // changeset with no managed release is thereby cut.
            match row.pk() {
                Some(pk) => gates.row_kept(db, &row.table, pk),
                None => {
                    warn!(
                        "gate: ancestor row in {} has no primary key; treating as not-shared",
                        row.table
                    );
                    Ok(false)
                }
            }
        }
    }
}

/// Walk `changeset`, reading each change as a [`ChangeRow`] and handing it — with
/// the live iterator, which the caller needs for `add_change` — to `f`. Owns the
/// `start`/`next`/`finalize` FFI boilerplate so each caller writes only its
/// per-row action; `f` returning `Ok(())` early is this walk's "skip this row".
/// A finalize failure surfaces only when the walk itself succeeded — a walk error
/// is the more specific cause and takes precedence.
unsafe fn for_each_change(
    changeset: &[u8],
    mut f: impl FnMut(*mut ffi::sqlite3_changeset_iter, ChangeRow) -> Result<(), GateError>,
) -> Result<(), GateError> {
    if changeset.is_empty() {
        return Ok(());
    }
    let mut iter: *mut ffi::sqlite3_changeset_iter = ptr::null_mut();
    let rc = ffi::sqlite3changeset_start(
        &mut iter,
        changeset.len() as c_int,
        changeset.as_ptr() as *mut c_void,
    );
    if rc != ffi::SQLITE_OK as c_int {
        return Err(GateError::Ffi("sqlite3changeset_start", rc));
    }
    let walk = loop {
        let step = ffi::sqlite3changeset_next(iter);
        if step == ffi::SQLITE_DONE as c_int {
            break Ok(());
        }
        if step != ffi::SQLITE_ROW as c_int {
            break Err(GateError::Ffi("sqlite3changeset_next", step));
        }
        let row = ChangeRow::read(iter);
        if let Err(e) = f(iter, row) {
            break Err(e);
        }
    };
    let fin = ffi::sqlite3changeset_finalize(iter);
    match walk {
        // Clean walk, failed finalize: the finalize failure is the cycle's outcome.
        Ok(()) if fin != ffi::SQLITE_OK as c_int => {
            Err(GateError::Ffi("sqlite3changeset_finalize", fin))
        }
        Ok(()) => Ok(()),
        // The walk already failed (the more specific cause): return it, but don't
        // swallow a finalize failure silently — log it alongside.
        Err(e) => {
            if fin != ffi::SQLITE_OK as c_int {
                warn!(
                    rc = fin,
                    "gate: changeset finalize failed after a walk error"
                );
            }
            Err(e)
        }
    }
}

/// Every DELETE in the changeset, keyed by `(table, primary key)`, holding the
/// row's old column values. [`was_shared`] reads these to resolve a deleted row's
/// pre-deletion gate state — its gate terminus is gone from the live db, so the
/// old values in the changeset are the only record that it was shared.
unsafe fn collect_deletes(
    changeset: &[u8],
) -> Result<HashMap<(String, String), ChangeRow>, GateError> {
    let mut deleted = HashMap::new();
    for_each_change(changeset, |_iter, row| {
        if row.op == ffi::SQLITE_DELETE {
            match row.pk() {
                Some(pk) => {
                    deleted.insert((row.table.clone(), pk.to_string()), row);
                }
                None => debug!(
                    table = %row.table,
                    "gate: delete row has no primary key; not tracked for pre-delete resolution"
                ),
            }
        }
        Ok(())
    })?;
    Ok(deleted)
}

/// Whether the row `(table, id)` was shared to peers *before* this changeset's
/// deletions — the keep test for a DELETE. The gate evaluates "shared" against the
/// live db, but a deleted row's gate terminus is gone from it (an album whose last
/// release was deleted, a track whose release was deleted), so a live evaluation
/// always reads "not shared" and the deletion is wrongly cut, stranding a phantom
/// on every peer. This resolves the gate against the row's pre-deletion state: the
/// changeset's old values for rows it deleted, falling back to the live db for
/// rows the changeset left in place (a descendant deleted under a surviving root).
///
/// - A root was shared iff its old gate value is truthy.
/// - A child was shared iff its FK parent (old FK for a deleted child, live FK
///   otherwise) was shared — recursively to the gated terminus.
/// - An ancestor was shared iff it still has a live kept child, or a kept child of
///   it is being deleted in this same changeset. A never-shared ancestor (only
///   unmanaged children) stays cut, so its DELETE never leaks old column values to
///   peers that never had it.
///
/// Memoized and cycle-guarded on `(table, id)`.
unsafe fn was_shared(
    db: *mut ffi::sqlite3,
    gates: &Gates,
    deleted: &HashMap<(String, String), ChangeRow>,
    table: &str,
    id: &str,
    memo: &mut HashMap<(String, String), bool>,
    visiting: &mut HashSet<(String, String)>,
) -> Result<bool, GateError> {
    let key = (table.to_string(), id.to_string());
    if let Some(&v) = memo.get(&key) {
        return Ok(v);
    }
    if !visiting.insert(key.clone()) {
        // A declared-FK cycle is not a path to a gated terminus. Defensive: the
        // schema's gated FK graph is a DAG, so this should never fire.
        debug!(
            table,
            id, "gate: FK cycle while resolving pre-delete share; treating as not shared"
        );
        return Ok(false);
    }

    let shared = match gates.tables.get(table) {
        // Ungated tables always sync, so their deletes always propagate.
        None => true,
        // A truthy old gate value means the root was shared. A present-but-NULL
        // gate is a genuine not-shared value (a gated-false root), not masked data;
        // a gate column missing from the delete row's old image is malformed.
        Some(TableGate::Root { gate_col }) => match deleted.get(&key) {
            Some(row) => match row.old.get(*gate_col) {
                Some(Some(v)) => truthy(v),
                Some(None) => false,
                None => {
                    warn!(table, id, "gate: deleted root's old gate value absent from the changeset row; treating as not shared");
                    false
                }
            },
            None => {
                let col = nth_column_name(db, table, *gate_col)?;
                match query_truth(db, table, &col, id)? {
                    Some(t) => t,
                    None => {
                        warn!(table, id, "gate: live root absent while resolving a descendant's pre-delete share; treating as not shared");
                        false
                    }
                }
            }
        },
        Some(TableGate::Child { fk_col, parent }) => {
            let parent_id = match deleted.get(&key) {
                Some(row) => row.fk_value(*fk_col).map(str::to_string),
                None => lookup_fk_in_db(db, table, *fk_col, id)?,
            };
            match parent_id {
                Some(pid) => was_shared(db, gates, deleted, parent, &pid, memo, visiting)?,
                None => {
                    warn!(table, id, "gate: child has no FK parent while resolving pre-delete share; treating as not shared");
                    false
                }
            }
        }
        Some(TableGate::Parent { children }) => {
            // A live kept child keeps a surviving ancestor shared (a descendant was
            // deleted but the ancestor and a sibling remain). For a deleted ancestor
            // the cascade leaves no live child, so the kept child is found among the
            // changeset's deletes instead.
            if gates.row_kept(db, table, id)? {
                true
            } else {
                let mut found = false;
                'children: for (child_table, child_fk_col) in children {
                    for ((dt, dpk), drow) in deleted {
                        if dt == child_table
                            && drow.fk_value(*child_fk_col) == Some(id)
                            && was_shared(db, gates, deleted, child_table, dpk, memo, visiting)?
                        {
                            found = true;
                            break 'children;
                        }
                    }
                }
                found
            }
        }
    };

    visiting.remove(&key);
    memo.insert(key, shared);
    Ok(shared)
}

/// The flipped-root key this row belongs to (for re-emit scoping): the
/// `(root table, root id)` of its gated-ancestor root, or `None` if the row is
/// ungated/unrooted or not shared.
unsafe fn gated_root_id(
    db: *mut ffi::sqlite3,
    gates: &Gates,
    row: &ChangeRow,
) -> Result<Option<(String, String)>, GateError> {
    match gates.tables.get(&row.table) {
        None => Ok(None),
        Some(TableGate::Root { gate_col }) => {
            if row.effective_truth(*gate_col) == Some(true) {
                Ok(row.pk().map(|pk| (row.table.clone(), pk.to_string())))
            } else {
                Ok(None)
            }
        }
        Some(TableGate::Child { fk_col, parent }) => {
            let parent_id = match row.fk_value(*fk_col) {
                Some(id) => id.to_string(),
                None => match row.pk() {
                    Some(pk) => match lookup_fk_in_db(db, &row.table, *fk_col, pk)? {
                        Some(id) => id,
                        None => {
                            warn!(
                                "gate: child {}.{pk} has no FK target in live db during re-emit; \
                                 skipping",
                                row.table
                            );
                            return Ok(None);
                        }
                    },
                    None => {
                        debug!(
                            "gate: child row in {} has no primary key during re-emit; skipping",
                            row.table
                        );
                        return Ok(None);
                    }
                },
            };
            Ok(resolve_root(db, gates, parent, &parent_id)?
                .filter(|r| r.kept)
                .map(|r| (r.terminus_table, r.terminus_id)))
        }
        // An ancestor has no gated-ancestor root in the downward sense this
        // scoping uses; its re-emit is driven by the kept-component closure in
        // `connected_component`, not by the flipped-root descendant test.
        Some(TableGate::Parent { .. }) => Ok(None),
    }
}

/// The gate terminus a row resolves to: the gated table at the top of its FK
/// chain (a gated root, or an ancestor when the chain inherits upward from one),
/// its id, and whether the gate keeps it.
struct ResolvedGate {
    terminus_table: String,
    terminus_id: String,
    kept: bool,
}

/// Walk the live-db FK chain from (`table`, `id`) up to its gate terminus,
/// returning that terminus and its keep truth. `None` if the chain never reaches
/// a terminus, or a row along it is missing from the live db (an anomaly the
/// caller treats as not-shared).
unsafe fn resolve_root(
    db: *mut ffi::sqlite3,
    gates: &Gates,
    table: &str,
    id: &str,
) -> Result<Option<ResolvedGate>, GateError> {
    match gates.tables.get(table) {
        Some(TableGate::Root { gate_col }) => {
            let col = nth_column_name(db, table, *gate_col)?;
            match query_truth(db, table, &col, id)? {
                Some(truth) => Ok(Some(ResolvedGate {
                    terminus_table: table.to_string(),
                    terminus_id: id.to_string(),
                    kept: truth,
                })),
                None => {
                    warn!("gate: gated root {table}.{id} absent from live db; cannot resolve gate");
                    Ok(None)
                }
            }
        }
        Some(TableGate::Child { fk_col, parent }) => {
            let col = nth_column_name(db, table, *fk_col)?;
            match query_column_text(db, table, &col, id)? {
                Some(parent_id) => resolve_root(db, gates, parent, &parent_id),
                None => {
                    warn!("gate: {table}.{id} has no FK parent in live db; cannot resolve gate");
                    Ok(None)
                }
            }
        }
        // A child whose parent is an ancestor (album_artists → albums) inherits
        // the ancestor's keep: shared iff the ancestor itself is kept by one of
        // *its* children. The ancestor is the terminus.
        Some(TableGate::Parent { .. }) => Ok(Some(ResolvedGate {
            terminus_table: table.to_string(),
            terminus_id: id.to_string(),
            kept: gates.row_kept(db, table, id)?,
        })),
        // A child whose parent is not itself gated/inheriting was pruned from the
        // map, so this is unreachable for retained tables; treat as ungated.
        None => Ok(None),
    }
}

// ---- one change extracted from a changeset iterator ------------------------

/// A single change at a changeset iterator's current position, with its table,
/// op, and the columns needed for gating. We read columns eagerly so the
/// iterator can advance.
struct ChangeRow {
    table: String,
    op: c_int,
    /// New values (insert/update); `None` per column = absent or NULL.
    new: Vec<Option<String>>,
    /// Old values (delete/update); `None` per column = absent or NULL.
    old: Vec<Option<String>>,
}

impl ChangeRow {
    /// Read the current change. Does not advance the iterator.
    unsafe fn read(iter: *mut ffi::sqlite3_changeset_iter) -> Self {
        let mut table_ptr: *const c_char = ptr::null();
        let mut ncol: c_int = 0;
        let mut op: c_int = 0;
        let mut indirect: c_int = 0;
        ffi::sqlite3changeset_op(iter, &mut table_ptr, &mut ncol, &mut op, &mut indirect);
        let table = CStr::from_ptr(table_ptr)
            .to_str()
            .expect("SQLite table names are always UTF-8")
            .to_string();

        let mut new = Vec::with_capacity(ncol as usize);
        let mut old = Vec::with_capacity(ncol as usize);
        for c in 0..ncol {
            new.push(extract_new_value(iter, c));
            old.push(extract_old_value(iter, c));
        }
        ChangeRow {
            table,
            op,
            new,
            old,
        }
    }

    /// Primary key (column 0), following op semantics.
    fn pk(&self) -> Option<&str> {
        self.col0()
    }

    fn col0(&self) -> Option<&str> {
        match self.op {
            x if x == ffi::SQLITE_DELETE => self.old.first().and_then(|v| v.as_deref()),
            _ => self
                .new
                .first()
                .and_then(|v| v.as_deref())
                .or_else(|| self.old.first().and_then(|v| v.as_deref())),
        }
    }

    /// The FK value at `col`, following op semantics (new for insert/update,
    /// old for delete). `None` if absent (e.g. unchanged in an update).
    fn fk_value(&self, col: usize) -> Option<&str> {
        match self.op {
            x if x == ffi::SQLITE_DELETE => self.old.get(col).and_then(|v| v.as_deref()),
            _ => self
                .new
                .get(col)
                .and_then(|v| v.as_deref())
                .or_else(|| self.old.get(col).and_then(|v| v.as_deref())),
        }
    }

    fn new_truth(&self, col: usize) -> Option<bool> {
        self.new.get(col).and_then(|v| v.as_deref()).map(truthy)
    }

    fn old_truth(&self, col: usize) -> Option<bool> {
        self.old.get(col).and_then(|v| v.as_deref()).map(truthy)
    }

    /// Effective gate truth for the row, following op semantics. For an update
    /// where the gate column is unchanged, the changeset omits it from both
    /// old and new; we treat absence as "unknown" → caller resolves from db.
    fn effective_truth(&self, col: usize) -> Option<bool> {
        match self.op {
            x if x == ffi::SQLITE_DELETE => self.old_truth(col),
            _ => self.new_truth(col).or_else(|| self.old_truth(col)),
        }
    }
}

/// The single definition of gate-truth, evaluated in Rust over a gate value read
/// as text: a nonzero integer is true; `0`/empty/non-integer is false.
/// [`truthy_sql`] is the SQL realization of this same rule for the snapshot path;
/// changing the rule here means changing it there too.
fn truthy(s: &str) -> bool {
    s.trim().parse::<i64>().map(|n| n != 0).unwrap_or(false)
}

// ---- small schema/query helpers (FFI) --------------------------------------

/// Column names of `table`, in declared order, via `PRAGMA table_info`.
unsafe fn column_names(db: *mut ffi::sqlite3, table: &str) -> Result<Vec<String>, GateError> {
    let sql = format!("PRAGMA table_info({})", quote_ident(table));
    let stmt = prepare(db, &sql)?;
    let mut names = Vec::new();
    while ffi::sqlite3_step(stmt) == ffi::SQLITE_ROW as c_int {
        let name_ptr = ffi::sqlite3_column_text(stmt, 1);
        if !name_ptr.is_null() {
            names.push(
                CStr::from_ptr(name_ptr as *const c_char)
                    .to_string_lossy()
                    .into_owned(),
            );
        }
    }
    ffi::sqlite3_finalize(stmt);
    Ok(names)
}

/// The name of column `idx` of `table`.
unsafe fn nth_column_name(
    db: *mut ffi::sqlite3,
    table: &str,
    idx: usize,
) -> Result<String, GateError> {
    column_names(db, table)?
        .into_iter()
        .nth(idx)
        .ok_or_else(|| GateError::MissingFkColumn(table.to_string(), format!("col#{idx}")))
}

/// Every foreign key on `table`, as `(child column name, parent table name)`
/// pairs, via `PRAGMA foreign_key_list`. Composite keys contribute one pair per
/// row; the gate only ever uses the column, so the granularity matches.
unsafe fn foreign_keys(
    db: *mut ffi::sqlite3,
    table: &str,
) -> Result<Vec<(String, String)>, GateError> {
    let sql = format!("PRAGMA foreign_key_list({})", quote_ident(table));
    let stmt = prepare(db, &sql)?;
    let mut fks = Vec::new();
    while ffi::sqlite3_step(stmt) == ffi::SQLITE_ROW as c_int {
        // columns: id, seq, table(parent), from(child col), to, on_update, ...
        let parent_ptr = ffi::sqlite3_column_text(stmt, 2);
        let from_ptr = ffi::sqlite3_column_text(stmt, 3);
        if parent_ptr.is_null() || from_ptr.is_null() {
            continue;
        }
        let parent = CStr::from_ptr(parent_ptr as *const c_char)
            .to_string_lossy()
            .into_owned();
        let from = CStr::from_ptr(from_ptr as *const c_char)
            .to_string_lossy()
            .into_owned();
        fks.push((from, parent));
    }
    ffi::sqlite3_finalize(stmt);
    Ok(fks)
}

/// The index in `child`'s column list of the FK column that references `parent`,
/// or `None` if `child` has no FK to `parent`. Used to wire an ancestor to a
/// keep-child: the inference names the child *table*, and this resolves which of
/// its columns holds the ancestor's id.
unsafe fn fk_col_referencing(
    db: *mut ffi::sqlite3,
    child: &str,
    parent: &str,
) -> Result<Option<usize>, GateError> {
    let from = foreign_keys(db, child)?
        .into_iter()
        .find(|(_, p)| p == parent)
        .map(|(from, _)| from);
    match from {
        Some(from) => {
            let cols = column_names(db, child)?;
            cols.iter()
                .position(|c| c == &from)
                .map(Some)
                .ok_or_else(|| GateError::MissingFkColumn(child.to_string(), from))
        }
        None => Ok(None),
    }
}

/// Pick `table`'s single DOWNWARD gate-parent among ALL its synced-parent FKs —
/// not just the first PRAGMA row, whose order is non-deterministic w.r.t.
/// declaration (SQLite numbers FKs in reverse). Returns `(child FK column name,
/// parent table)`, or `None` if no synced-parent FK exists.
///
/// A join row (e.g. `album_artists` → albums, artists) must inherit downward from
/// the right parent, so the choice follows a deterministic preference:
///
/// 1. **Prefer a parent that reaches a gated root downward** — a Root, or a plain
///    table whose own chosen FK chain reaches a Root. So `release_files` →
///    releases (a Root), not `release_files` → audio_formats (a lookup ancestor).
/// 2. **Else, among ancestor parents, pick the most-specific** — the candidate
///    that is itself an FK-descendant of the other candidates (deepest in the
///    containment DAG). So `album_artists` → albums, since albums is a descendant
///    of artists (albums.artist_id → artists).
/// 3. **Else break ties lexicographically** by parent name.
unsafe fn select_parent_fk(
    db: *mut ffi::sqlite3,
    table: &str,
    tables: &[SyncedTable],
    ancestors: &HashSet<&str>,
) -> Result<Option<(String, String)>, GateError> {
    let synced: HashSet<&str> = tables.iter().map(|t| t.name()).collect();
    let candidates: Vec<(String, String)> = foreign_keys(db, table)?
        .into_iter()
        .filter(|(_, parent)| synced.contains(parent.as_str()))
        .collect();
    if candidates.is_empty() {
        return Ok(None);
    }

    // Rank each candidate `(fk, parent)` by the preference and pick the smallest:
    //   tier 0  parent reaches a gated root downward (a Root, or a plain chain to
    //           one) — the gated side of a join row;
    //   tier 1  parent is an ancestor, ranked most-specific first (a deeper
    //           ancestor sorts before a shallower one, so albums beats artists);
    //   tier 2  some other synced parent (neither).
    // The lexicographic parent name is the final tie-break. A stable key makes
    // the choice deterministic regardless of PRAGMA row order. The ranking probes
    // the FK graph (fallible), so build each key before sorting rather than inside
    // the sort comparator.
    //
    // `ParentRank`'s field order is its comparison order (derived `Ord`): tier,
    // then specificity, then name.
    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    struct ParentRank {
        tier: u8,
        specificity: isize,
        name: String,
    }
    let mut keyed = Vec::with_capacity(candidates.len());
    for (fk, parent) in candidates {
        let tier = if parent_reaches_root(db, tables, ancestors, &parent, &mut HashSet::new())? {
            0u8
        } else if ancestors.contains(parent.as_str()) {
            1
        } else {
            2
        };
        let specificity = if tier == 1 {
            -(ancestor_depth(db, ancestors, &parent, &mut HashSet::new())? as isize)
        } else {
            0
        };
        let rank = ParentRank {
            tier,
            specificity,
            name: parent.clone(),
        };
        keyed.push((rank, (fk, parent)));
    }
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(keyed.into_iter().next().map(|(_, candidate)| candidate))
}

/// Whether `parent`'s own gate eventually reaches a gated *root* downward, so a
/// child inheriting from it lands on a real root rather than on an ancestor or
/// nothing. A gated root is the terminus; a plain table reaches one iff its own
/// selected parent FK does; an ancestor is NOT a downward root path (its keep is
/// the separate upward relation). Cycle-guarded by `visiting`.
fn parent_reaches_root(
    db: *mut ffi::sqlite3,
    tables: &[SyncedTable],
    ancestors: &HashSet<&str>,
    parent: &str,
    visiting: &mut HashSet<String>,
) -> Result<bool, GateError> {
    if !visiting.insert(parent.to_string()) {
        return Ok(false); // a cycle is not a path to a real root.
    }
    let decl = tables.iter().find(|t| t.name() == parent);
    let reaches = match decl {
        Some(t) if t.gate_column().is_some() => true,
        // An ancestor is not a downward root path.
        Some(t) if t.is_gated_by_descendants() => false,
        // A plain (or unknown) parent reaches a root iff its own chain does.
        _ => match unsafe { select_parent_fk(db, parent, tables, ancestors)? } {
            Some((_, grandparent)) => {
                parent_reaches_root(db, tables, ancestors, &grandparent, visiting)?
            }
            // No synced-parent FK: the chain ends here without a root.
            None => false,
        },
    };
    visiting.remove(parent);
    Ok(reaches)
}

/// How deep `ancestor` sits in the containment DAG of ancestor tables: 0 if it
/// references no other ancestor, else 1 + the max depth of the ancestors it has
/// an FK to. A deeper ancestor is more specific (e.g. albums references artists,
/// so albums is depth 1 and artists depth 0). Cycle-guarded by `visiting`.
unsafe fn ancestor_depth(
    db: *mut ffi::sqlite3,
    ancestors: &HashSet<&str>,
    ancestor: &str,
    visiting: &mut HashSet<String>,
) -> Result<usize, GateError> {
    if !visiting.insert(ancestor.to_string()) {
        return Ok(0); // defensive against a malformed ancestor cycle.
    }
    let mut depth = 0;
    for (_, parent) in foreign_keys(db, ancestor)? {
        if parent != ancestor && ancestors.contains(parent.as_str()) {
            depth = depth.max(1 + ancestor_depth(db, ancestors, &parent, visiting)?);
        }
    }
    visiting.remove(ancestor);
    Ok(depth)
}

/// `CREATE TABLE` text for `table` from `sqlite_master`.
unsafe fn create_table_sql(db: *mut ffi::sqlite3, table: &str) -> Result<String, GateError> {
    let sql = format!(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='{}'",
        table.replace('\'', "''")
    );
    let stmt = prepare(db, &sql)?;
    let step = ffi::sqlite3_step(stmt);
    if step != ffi::SQLITE_ROW as c_int {
        ffi::sqlite3_finalize(stmt);
        return Err(GateError::NoSchema(table.to_string()));
    }
    let text_ptr = ffi::sqlite3_column_text(stmt, 0);
    let create = if text_ptr.is_null() {
        ffi::sqlite3_finalize(stmt);
        return Err(GateError::NoSchema(table.to_string()));
    } else {
        CStr::from_ptr(text_ptr as *const c_char)
            .to_string_lossy()
            .into_owned()
    };
    ffi::sqlite3_finalize(stmt);
    Ok(create)
}

/// Qualify a `CREATE TABLE <name> ...` statement so it builds the table inside
/// the attached schema `alias`, by replacing the first occurrence of the table
/// name token with `alias."name"`.
fn rewrite_create_into_schema(create: &str, table: &str, alias: &str) -> String {
    // sqlite_master stores: CREATE TABLE "name" (...) or CREATE TABLE name (...).
    // Replace the first occurrence of the bare/quoted name after "TABLE".
    let qualified = format!("{alias}.{}", quote_ident(table));
    let needle_quoted = format!("\"{table}\"");
    if let Some(pos) = create.find(&needle_quoted) {
        let mut out = String::with_capacity(create.len() + alias.len() + 4);
        out.push_str(&create[..pos]);
        out.push_str(&qualified);
        out.push_str(&create[pos + needle_quoted.len()..]);
        return out;
    }
    // Unquoted: replace the bare token right after "TABLE ".
    if let Some(tpos) = create.find("TABLE ") {
        let after = tpos + "TABLE ".len();
        if create[after..].starts_with(table) {
            let mut out = String::with_capacity(create.len() + alias.len() + 4);
            out.push_str(&create[..after]);
            out.push_str(&qualified);
            out.push_str(&create[after + table.len()..]);
            return out;
        }
    }
    // Fallback: prepend the alias-qualified create assuming standard prefix.
    create.replacen(
        &format!("CREATE TABLE {table}"),
        &format!("CREATE TABLE {qualified}"),
        1,
    )
}

/// Query a single text column value for the row with id `id`.
unsafe fn query_column_text(
    db: *mut ffi::sqlite3,
    table: &str,
    column: &str,
    id: &str,
) -> Result<Option<String>, GateError> {
    let sql = format!(
        "SELECT {} FROM {} WHERE {} = ?",
        quote_ident(column),
        quote_ident(table),
        quote_ident("id"),
    );
    let stmt = prepare(db, &sql)?;
    bind_text(stmt, 1, id);
    let step = ffi::sqlite3_step(stmt);
    let out = if step == ffi::SQLITE_ROW as c_int {
        let p = ffi::sqlite3_column_text(stmt, 0);
        if p.is_null() {
            None
        } else {
            Some(
                CStr::from_ptr(p as *const c_char)
                    .to_string_lossy()
                    .into_owned(),
            )
        }
    } else {
        None
    };
    ffi::sqlite3_finalize(stmt);
    Ok(out)
}

/// Query a single boolean gate column for the row with id `id`.
unsafe fn query_truth(
    db: *mut ffi::sqlite3,
    table: &str,
    column: &str,
    id: &str,
) -> Result<Option<bool>, GateError> {
    Ok(query_column_text(db, table, column, id)?.map(|s| truthy(&s)))
}

/// Read the FK value (`column` at index `fk_col`) for the live row `pk`.
unsafe fn lookup_fk_in_db(
    db: *mut ffi::sqlite3,
    table: &str,
    fk_col: usize,
    pk: &str,
) -> Result<Option<String>, GateError> {
    let column = nth_column_name(db, table, fk_col)?;
    query_column_text(db, table, &column, pk)
}

unsafe fn prepare(db: *mut ffi::sqlite3, sql: &str) -> Result<*mut ffi::sqlite3_stmt, GateError> {
    let c_sql = CString::new(sql).map_err(|_| GateError::BadSql(sql.to_string()))?;
    let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
    let rc = ffi::sqlite3_prepare_v2(db, c_sql.as_ptr(), -1, &mut stmt, ptr::null_mut());
    if rc != ffi::SQLITE_OK as c_int {
        return Err(GateError::Prepare(sql.to_string(), rc));
    }
    Ok(stmt)
}

unsafe fn bind_text(stmt: *mut ffi::sqlite3_stmt, idx: c_int, val: &str) {
    // SQLITE_TRANSIENT tells SQLite to copy the bytes; they outlive this call.
    let transient = std::mem::transmute::<isize, ffi::sqlite3_destructor_type>(-1isize);
    ffi::sqlite3_bind_text(
        stmt,
        idx,
        val.as_ptr() as *const c_char,
        val.len() as c_int,
        transient,
    );
}

unsafe fn exec_sql(db: *mut ffi::sqlite3, sql: &str) -> Result<(), GateError> {
    let c_sql = CString::new(sql).map_err(|_| GateError::BadSql(sql.to_string()))?;
    let rc = ffi::sqlite3_exec(db, c_sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut());
    if rc != ffi::SQLITE_OK as c_int {
        return Err(GateError::Exec(sql.to_string(), rc));
    }
    Ok(())
}

#[derive(Debug)]
pub enum GateError {
    Ffi(&'static str, c_int),
    Diff(String, c_int, Option<String>),
    SessionCreate(i32),
    ChangesetExtract(i32),
    MissingGateColumn(String, String),
    MissingFkColumn(String, String),
    /// A `gated_by_descendants` ancestor (the table) has no inferred gated
    /// descendant — no synced table has a foreign key into it after the
    /// join-table back-edge is excluded. The keep would be vacuously false, so
    /// the declaration is a host error rather than a silent always-share.
    NoGatedDescendants(String),
    /// The gated tables form an FK cycle, so no parent-first apply order exists.
    FkCycle(Vec<String>),
    NoSchema(String),
    BadSql(String),
    Prepare(String, c_int),
    Exec(String, c_int),
}

impl std::fmt::Display for GateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GateError::Ffi(func, rc) => write!(f, "{func} failed (rc={rc})"),
            GateError::Diff(tbl, rc, msg) => match msg {
                Some(m) => write!(f, "session_diff failed for {tbl} (rc={rc}): {m}"),
                None => write!(f, "session_diff failed for {tbl} (rc={rc})"),
            },
            GateError::SessionCreate(rc) => write!(f, "session create failed (rc={rc})"),
            GateError::ChangesetExtract(rc) => write!(f, "changeset extract failed (rc={rc})"),
            GateError::MissingGateColumn(tbl, col) => {
                write!(f, "gated table {tbl} has no gate column {col}")
            }
            GateError::MissingFkColumn(tbl, col) => {
                write!(f, "table {tbl} has no FK column {col}")
            }
            GateError::NoGatedDescendants(tbl) => {
                write!(
                    f,
                    "gated_by_descendants ancestor {tbl} has no inferred gated descendant: no \
                     synced table references it"
                )
            }
            GateError::FkCycle(tables) => {
                write!(f, "gated tables form an FK cycle: {}", tables.join(", "))
            }
            GateError::NoSchema(tbl) => write!(f, "no CREATE TABLE schema for {tbl}"),
            GateError::BadSql(sql) => write!(f, "SQL not representable as a C string: {sql}"),
            GateError::Prepare(sql, rc) => write!(f, "prepare failed (rc={rc}): {sql}"),
            GateError::Exec(sql, rc) => write!(f, "exec failed (rc={rc}): {sql}"),
        }
    }
}

impl std::error::Error for GateError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changeset::{walk, ChangeOp};
    use crate::sync::apply::apply_changeset_lww;
    use rusqlite::session::Session as RqSession;

    /// A throwaway in-memory connection with `foreign_keys=ON`, for the gate
    /// tests' bespoke schemas. The gate's public API takes `&Connection`.
    fn conn() -> Connection {
        let c = Connection::open_in_memory().expect("open in-memory");
        c.execute_batch("PRAGMA foreign_keys = ON").expect("fk on");
        c
    }

    fn exec(c: &Connection, sql: &str) {
        c.execute_batch(sql)
            .unwrap_or_else(|e| panic!("exec failed for {sql}: {e}"));
    }

    fn query_text(c: &Connection, sql: &str) -> String {
        c.query_row(sql, [], |r| r.get::<_, String>(0))
            .unwrap_or_else(|e| panic!("query_text failed for {sql}: {e}"))
    }

    fn query_int(c: &Connection, sql: &str) -> i64 {
        c.query_row(sql, [], |r| r.get::<_, i64>(0))
            .unwrap_or_else(|e| panic!("query_int failed for {sql}: {e}"))
    }

    fn row_exists(c: &Connection, sql: &str) -> bool {
        use rusqlite::OptionalExtension;
        c.query_row(sql, [], |_| Ok(()))
            .optional()
            .unwrap_or_else(|e| panic!("row_exists failed for {sql}: {e}"))
            .is_some()
    }

    /// Capture a changeset over `tables` while running `stmts`. Returns the
    /// recorded changeset bytes.
    fn capture(c: &Connection, tables: &[SyncedTable], stmts: &[&str]) -> Vec<u8> {
        let mut session = RqSession::new(c).expect("session");
        for t in tables {
            session.attach(Some(t.name())).expect("attach");
        }
        for s in stmts {
            exec(c, s);
        }
        let mut buf = Vec::new();
        session.changeset_strm(&mut buf).expect("changeset");
        buf
    }

    /// Capture, then gate against `tables`' gate model. Returns gated bytes.
    fn capture_and_gate(c: &Connection, tables: &[SyncedTable], stmts: &[&str]) -> Vec<u8> {
        let bytes = capture(c, tables, stmts);
        let gates = Gates::from_tables(c, tables).expect("build gates");
        gate_outbound(c, &bytes, &gates).expect("gate outbound")
    }

    fn test_synced_tables() -> Vec<SyncedTable> {
        vec![
            SyncedTable::new("notes").gated_by("shared"),
            SyncedTable::new("note_tags"),
            SyncedTable::new("note_photos"),
        ]
    }

    /// The synthetic notes/note_tags/note_photos schema, built directly on `c`.
    fn create_synced_schema(c: &Connection) {
        exec(
            c,
            "CREATE TABLE notes (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                body TEXT,
                shared INTEGER NOT NULL DEFAULT 0,
                _updated_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE note_tags (
                id TEXT PRIMARY KEY,
                note_id TEXT NOT NULL,
                tag TEXT NOT NULL,
                _updated_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
            );
            CREATE TABLE note_photos (
                id TEXT PRIMARY KEY,
                note_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                _updated_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
            );",
        );
    }

    fn has_row(changes: &[crate::changeset::RowChange], table: &str, pk: &str) -> bool {
        changes
            .iter()
            .any(|c| c.table == table && c.pk() == Some(pk))
    }

    #[test]
    fn gated_false_root_is_cut() {
        let c = conn();
        create_synced_schema(&c);
        let out = capture_and_gate(
            &c,
            &test_synced_tables(),
            &[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
               VALUES ('n1', 'Private', NULL, 0, '0000000001000-0000-dev1', '2026-01-01')",
            ],
        );
        let changes = walk(&out).expect("walk");
        assert!(
            !has_row(&changes, "notes", "n1"),
            "a gated-false root must be cut from the outbound changeset"
        );
    }

    #[test]
    fn gated_true_root_passes_through() {
        let c = conn();
        create_synced_schema(&c);
        let out = capture_and_gate(
            &c,
            &test_synced_tables(),
            &[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
               VALUES ('n1', 'Public', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            ],
        );
        let changes = walk(&out).expect("walk");
        assert!(
            has_row(&changes, "notes", "n1"),
            "a gated-true root must pass through"
        );
    }

    #[test]
    fn child_cut_because_parent_gated_false() {
        let c = conn();
        create_synced_schema(&c);
        let out = capture_and_gate(
            &c,
            &test_synced_tables(),
            &[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Private', NULL, 0, '0000000001000-0000-dev1', '2026-01-01')",
                "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                 VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
            ],
        );
        let changes = walk(&out).expect("walk");
        assert!(!has_row(&changes, "notes", "n1"), "parent cut");
        assert!(
            !has_row(&changes, "note_tags", "t1"),
            "child must be cut because its parent is gated-false"
        );
    }

    #[test]
    fn child_passes_when_parent_gated_true() {
        let c = conn();
        create_synced_schema(&c);
        let out = capture_and_gate(
            &c,
            &test_synced_tables(),
            &[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Public', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
                "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                 VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
                "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
                 VALUES ('p1', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
            ],
        );
        let changes = walk(&out).expect("walk");
        assert!(has_row(&changes, "notes", "n1"));
        assert!(
            has_row(&changes, "note_tags", "t1"),
            "child inherits parent's true gate"
        );
        assert!(
            has_row(&changes, "note_photos", "p1"),
            "FK child inherits parent's true gate"
        );
    }

    #[test]
    fn ungated_table_always_passes() {
        let c = conn();
        exec(
            &c,
            "CREATE TABLE notes (id TEXT PRIMARY KEY, shared INTEGER NOT NULL DEFAULT 0, \
             _updated_at TEXT NOT NULL)",
        );
        exec(
            &c,
            "CREATE TABLE settings (id TEXT PRIMARY KEY, val TEXT, _updated_at TEXT NOT NULL)",
        );
        let tables = vec![
            SyncedTable::new("notes").gated_by("shared"),
            SyncedTable::new("settings"),
        ];
        let out = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO notes (id, shared, _updated_at) VALUES ('n1', 0, '0000000001000-0000-dev1')",
                "INSERT INTO settings (id, val, _updated_at) VALUES ('s1', 'x', '0000000001000-0000-dev1')",
            ],
        );
        let changes = walk(&out).expect("walk");
        assert!(!has_row(&changes, "notes", "n1"), "gated-false note is cut");
        assert!(
            has_row(&changes, "settings", "s1"),
            "ungated table always passes through"
        );
    }

    #[test]
    fn flip_false_to_true_reemits_full_subtree() {
        let c = conn();
        create_synced_schema(&c);
        let tables = test_synced_tables();

        // Cycle 1: create a private note with children. All cut.
        let out1 = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Private', 'b', 0, '0000000001000-0000-dev1', '2026-01-01')",
                "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                 VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
                "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
                 VALUES ('p1', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
            ],
        );
        assert!(
            walk(&out1).expect("walk").is_empty(),
            "cycle 1 emits nothing while private"
        );

        // Cycle 2: flip the gate true. Only the note UPDATE is captured; the
        // children are re-emitted by the flip logic.
        let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE notes SET shared = 1, _updated_at = '0000000002000-0000-dev1' WHERE id = 'n1'"],
        );
        let c2 = walk(&out2).expect("walk");
        assert!(has_row(&c2, "notes", "n1"), "promoted note is emitted");
        assert!(
            has_row(&c2, "note_tags", "t1"),
            "re-emit includes the pre-existing tag child"
        );
        assert!(
            has_row(&c2, "note_photos", "p1"),
            "re-emit includes the pre-existing photo child"
        );

        // Apply cycle 2's output to a fresh peer: complete consistent subtree.
        let peer = conn();
        create_synced_schema(&peer);
        apply_changeset_lww(&peer, &out2, &tables, crate::sync::hlc::now_wall_ms())
            .expect("apply to peer");
        assert!(
            row_exists(&peer, "SELECT 1 FROM notes WHERE id = 'n1'"),
            "peer has the note"
        );
        assert_eq!(
            query_text(&peer, "SELECT title FROM notes WHERE id = 'n1'"),
            "Private"
        );
        assert!(
            row_exists(&peer, "SELECT 1 FROM note_tags WHERE id = 't1'"),
            "peer has the tag"
        );
        assert!(
            row_exists(&peer, "SELECT 1 FROM note_photos WHERE id = 'p1'"),
            "peer has the photo"
        );
    }

    #[test]
    fn post_promotion_edit_is_single_update_not_reemit() {
        let c = conn();
        create_synced_schema(&c);
        let tables = test_synced_tables();

        // Create + promote in cycle 1 (note shared=1 immediately).
        let _ = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Public', 'b', 1, '0000000001000-0000-dev1', '2026-01-01')",
                "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                 VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
            ],
        );

        // Cycle 2: an ordinary edit to the already-shared note. Not a flip, so it
        // must emit exactly one UPDATE for the note and nothing else.
        let out = capture_and_gate(
            &c,
            &tables,
            &["UPDATE notes SET title = 'Renamed', _updated_at = '0000000002000-0000-dev1' WHERE id = 'n1'"],
        );
        let changes = walk(&out).expect("walk");
        assert_eq!(changes.len(), 1, "exactly one change");
        assert_eq!(changes[0].table, "notes");
        assert_eq!(changes[0].op, ChangeOp::Update);
        assert_eq!(changes[0].pk(), Some("n1"));
    }

    #[test]
    fn multi_hop_fk_inheritance() {
        // grandchild -> child -> root(gated). The gate must flow two hops.
        let c = conn();
        exec(
            &c,
            "CREATE TABLE albums (id TEXT PRIMARY KEY, shared INTEGER NOT NULL DEFAULT 0, \
             _updated_at TEXT NOT NULL)",
        );
        exec(
            &c,
            "CREATE TABLE photos (id TEXT PRIMARY KEY, album_id TEXT NOT NULL, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (album_id) REFERENCES albums (id) ON DELETE CASCADE)",
        );
        exec(
            &c,
            "CREATE TABLE comments (id TEXT PRIMARY KEY, photo_id TEXT NOT NULL, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (photo_id) REFERENCES photos (id) ON DELETE CASCADE)",
        );
        let tables = vec![
            SyncedTable::new("albums").gated_by("shared"),
            SyncedTable::new("photos"),
            SyncedTable::new("comments"),
        ];

        // Private album with a 2-level subtree: all cut.
        let out = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO albums (id, shared, _updated_at) VALUES ('a1', 0, '0000000001000-0000-dev1')",
                "INSERT INTO photos (id, album_id, _updated_at) VALUES ('p1', 'a1', '0000000001000-0000-dev1')",
                "INSERT INTO comments (id, photo_id, _updated_at) VALUES ('c1', 'p1', '0000000001000-0000-dev1')",
            ],
        );
        assert!(
            walk(&out).expect("walk").is_empty(),
            "private 2-level subtree fully cut"
        );

        // Flip the album true: re-emit must reach the grandchild comment.
        let out = capture_and_gate(
            &c,
            &tables,
            &["UPDATE albums SET shared = 1, _updated_at = '0000000002000-0000-dev1' WHERE id = 'a1'"],
        );
        let changes = walk(&out).expect("walk");
        assert!(has_row(&changes, "albums", "a1"));
        assert!(
            has_row(&changes, "photos", "p1"),
            "one-hop child re-emitted"
        );
        assert!(
            has_row(&changes, "comments", "c1"),
            "two-hop grandchild re-emitted"
        );
    }

    #[test]
    fn delete_gated_false_strips_private_subtrees_in_place() {
        let c = conn();
        exec(
            &c,
            "CREATE TABLE albums (id TEXT PRIMARY KEY, shared INTEGER NOT NULL DEFAULT 0, \
             _updated_at TEXT NOT NULL)",
        );
        exec(
            &c,
            "CREATE TABLE photos (id TEXT PRIMARY KEY, album_id TEXT NOT NULL, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (album_id) REFERENCES albums (id) ON DELETE CASCADE)",
        );
        exec(
            &c,
            "CREATE TABLE comments (id TEXT PRIMARY KEY, photo_id TEXT NOT NULL, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (photo_id) REFERENCES photos (id) ON DELETE CASCADE)",
        );
        exec(
            &c,
            "CREATE TABLE settings (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL)",
        );
        let tables = vec![
            SyncedTable::new("albums").gated_by("shared"),
            SyncedTable::new("photos"),
            SyncedTable::new("comments"),
            SyncedTable::new("settings"),
        ];

        exec(&c, "INSERT INTO albums (id, shared, _updated_at) VALUES ('priv', 0, '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO photos (id, album_id, _updated_at) VALUES ('priv_p', 'priv', '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO comments (id, photo_id, _updated_at) VALUES ('priv_c', 'priv_p', '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO albums (id, shared, _updated_at) VALUES ('pub', 1, '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO photos (id, album_id, _updated_at) VALUES ('pub_p', 'pub', '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO comments (id, photo_id, _updated_at) VALUES ('pub_c', 'pub_p', '0000000001000-0000-dev1')");
        exec(
            &c,
            "INSERT INTO settings (id, _updated_at) VALUES ('s1', '0000000001000-0000-dev1')",
        );

        let gates = Gates::from_tables(&c, &tables).expect("gates");
        gates.delete_gated_false(&c).expect("delete gated-false");

        assert!(!row_exists(&c, "SELECT 1 FROM albums WHERE id = 'priv'"));
        assert!(!row_exists(&c, "SELECT 1 FROM photos WHERE id = 'priv_p'"));
        assert!(!row_exists(
            &c,
            "SELECT 1 FROM comments WHERE id = 'priv_c'"
        ));
        assert!(row_exists(&c, "SELECT 1 FROM albums WHERE id = 'pub'"));
        assert!(row_exists(&c, "SELECT 1 FROM photos WHERE id = 'pub_p'"));
        assert!(row_exists(&c, "SELECT 1 FROM comments WHERE id = 'pub_c'"));
        assert!(row_exists(&c, "SELECT 1 FROM settings WHERE id = 's1'"));
    }

    // ---- upward gate (gated_by_descendants) ----------------------------------

    fn album_tables() -> Vec<SyncedTable> {
        vec![
            SyncedTable::new("releases").gated_by("managed"),
            SyncedTable::new("albums").gated_by_descendants(),
            SyncedTable::new("artists").gated_by_descendants(),
            SyncedTable::new("album_artists"),
            SyncedTable::new("tracks"),
        ]
    }

    fn create_album_schema(c: &Connection) {
        exec(
            c,
            "CREATE TABLE artists (id TEXT PRIMARY KEY, name TEXT, _updated_at TEXT NOT NULL)",
        );
        exec(
            c,
            "CREATE TABLE albums (id TEXT PRIMARY KEY, artist_id TEXT, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (artist_id) REFERENCES artists (id))",
        );
        exec(
            c,
            "CREATE TABLE album_artists (id TEXT PRIMARY KEY, album_id TEXT NOT NULL, \
             artist_id TEXT NOT NULL, _updated_at TEXT NOT NULL, \
             FOREIGN KEY (album_id) REFERENCES albums (id) ON DELETE CASCADE, \
             FOREIGN KEY (artist_id) REFERENCES artists (id) ON DELETE CASCADE)",
        );
        exec(
            c,
            "CREATE TABLE releases (id TEXT PRIMARY KEY, album_id TEXT NOT NULL, \
             managed INTEGER NOT NULL DEFAULT 0, _updated_at TEXT NOT NULL, \
             FOREIGN KEY (album_id) REFERENCES albums (id) ON DELETE CASCADE)",
        );
        exec(
            c,
            "CREATE TABLE tracks (id TEXT PRIMARY KEY, release_id TEXT NOT NULL, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (release_id) REFERENCES releases (id) ON DELETE CASCADE)",
        );
    }

    /// Apply a changeset with the production LWW path, scoped to the album set.
    fn apply_album(c: &Connection, bytes: &[u8]) {
        apply_changeset_lww(c, bytes, &album_tables(), crate::sync::hlc::now_wall_ms())
            .expect("apply album changeset");
    }

    /// The inferred keep-children of `tbl`, as `(child, fk column name)`, sorted.
    fn inferred_children(c: &Connection, gates: &Gates, tbl: &str) -> Vec<(String, String)> {
        match gates.tables.get(tbl) {
            Some(TableGate::Parent { children }) => {
                let mut out: Vec<(String, String)> = children
                    .iter()
                    .map(|(ch, idx)| {
                        (
                            ch.clone(),
                            unsafe { nth_column_name(c.handle(), ch, *idx) }.expect("fk col"),
                        )
                    })
                    .collect();
                out.sort();
                out
            }
            Some(_) => panic!("{tbl} is in the gate map but not modeled as a Parent"),
            None => panic!("{tbl} is absent from the gate map; expected a Parent"),
        }
    }

    /// The downward gate-parent `from_tables` chose for `tbl`, as `(parent, fk
    /// column name)`. Panics if `tbl` is not modeled as an inheriting `Child`.
    fn downward_parent(c: &Connection, gates: &Gates, tbl: &str) -> (String, String) {
        match gates.tables.get(tbl) {
            Some(TableGate::Child { fk_col, parent }) => (
                parent.clone(),
                unsafe { nth_column_name(c.handle(), tbl, *fk_col) }.expect("fk col"),
            ),
            other => panic!(
                "{tbl} must be an inheriting Child, got present={}",
                other.is_some()
            ),
        }
    }

    #[test]
    fn inference_resolves_children_and_join_parent() {
        let c = conn();
        create_album_schema(&c);
        let gates = Gates::from_tables(&c, &album_tables()).expect("gates");

        assert_eq!(
            inferred_children(&c, &gates, "albums"),
            vec![("releases".to_string(), "album_id".to_string())],
            "albums is kept only by releases (the album_artists back-edge is excluded)"
        );
        assert_eq!(
            inferred_children(&c, &gates, "artists"),
            vec![
                ("album_artists".to_string(), "artist_id".to_string()),
                ("albums".to_string(), "artist_id".to_string()),
            ],
            "artists is kept by albums OR album_artists"
        );
        assert_eq!(
            downward_parent(&c, &gates, "album_artists"),
            ("albums".to_string(), "album_id".to_string()),
        );
    }

    #[test]
    fn downward_parent_is_most_specific_not_lexicographic() {
        let c = conn();
        exec(
            &c,
            "CREATE TABLE aouter (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL)",
        );
        exec(
            &c,
            "CREATE TABLE zinner (id TEXT PRIMARY KEY, aouter_id TEXT, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (aouter_id) REFERENCES aouter (id))",
        );
        exec(
            &c,
            "CREATE TABLE zgated (id TEXT PRIMARY KEY, zinner_id TEXT NOT NULL, \
             shared INTEGER NOT NULL DEFAULT 0, _updated_at TEXT NOT NULL, \
             FOREIGN KEY (zinner_id) REFERENCES zinner (id))",
        );
        exec(
            &c,
            "CREATE TABLE joiner (id TEXT PRIMARY KEY, aouter_id TEXT NOT NULL, \
             zinner_id TEXT NOT NULL, _updated_at TEXT NOT NULL, \
             FOREIGN KEY (aouter_id) REFERENCES aouter (id), \
             FOREIGN KEY (zinner_id) REFERENCES zinner (id))",
        );
        let tables = vec![
            SyncedTable::new("aouter").gated_by_descendants(),
            SyncedTable::new("zinner").gated_by_descendants(),
            SyncedTable::new("zgated").gated_by("shared"),
            SyncedTable::new("joiner"),
        ];
        let gates = Gates::from_tables(&c, &tables).expect("gates");
        assert_eq!(
            downward_parent(&c, &gates, "joiner"),
            ("zinner".to_string(), "zinner_id".to_string()),
            "the most-specific (deeper) ancestor wins even though it sorts \
             lexicographically later than `aouter`"
        );
    }

    #[test]
    fn fk_topological_order_is_parent_first() {
        let c = conn();
        create_album_schema(&c);
        let gates = Gates::from_tables(&c, &album_tables()).expect("gates");
        let order = unsafe { gates.gated_tables_parent_first(c.handle()) }.expect("topo order");
        let pos = |t: &str| order.iter().position(|x| *x == t).unwrap();
        assert!(pos("artists") < pos("albums"), "artist before album");
        assert!(pos("albums") < pos("releases"), "album before release");
        assert!(pos("releases") < pos("tracks"), "release before track");
        assert!(
            pos("albums") < pos("album_artists"),
            "album before album_artists"
        );
        assert!(
            pos("artists") < pos("album_artists"),
            "artist before album_artists"
        );
    }

    #[test]
    fn delete_gated_false_prunes_empty_ancestors() {
        let c = conn();
        create_album_schema(&c);

        exec(
            &c,
            "INSERT INTO artists (id, _updated_at) VALUES ('A1', '0000000001000-0000-dev1')",
        );
        exec(
            &c,
            "INSERT INTO artists (id, _updated_at) VALUES ('A2', '0000000001000-0000-dev1')",
        );
        exec(&c, "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL_EMPTY', 'A1', '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL_MIXED', 'A2', '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA_EMPTY', 'AL_EMPTY', 'A1', '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA_MIXED', 'AL_MIXED', 'A2', '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R_UNMAN', 'AL_EMPTY', 0, '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R_MAN', 'AL_MIXED', 1, '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R_UNMAN2', 'AL_MIXED', 0, '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T_UNMAN', 'R_UNMAN', '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T_MAN', 'R_MAN', '0000000001000-0000-dev1')");

        let gates = Gates::from_tables(&c, &album_tables()).expect("gates");
        gates.delete_gated_false(&c).expect("delete gated-false");

        assert!(
            !row_exists(&c, "SELECT 1 FROM albums WHERE id = 'AL_EMPTY'"),
            "empty album pruned"
        );
        assert!(
            !row_exists(&c, "SELECT 1 FROM releases WHERE id = 'R_UNMAN'"),
            "unmanaged release gone"
        );
        assert!(
            !row_exists(&c, "SELECT 1 FROM tracks WHERE id = 'T_UNMAN'"),
            "track of unmanaged release gone"
        );
        assert!(
            !row_exists(&c, "SELECT 1 FROM album_artists WHERE id = 'AA_EMPTY'"),
            "album_artists of pruned album gone"
        );
        assert!(
            !row_exists(&c, "SELECT 1 FROM artists WHERE id = 'A1'"),
            "artist with no kept album pruned"
        );
        assert!(
            row_exists(&c, "SELECT 1 FROM albums WHERE id = 'AL_MIXED'"),
            "mixed album survives"
        );
        assert!(
            row_exists(&c, "SELECT 1 FROM releases WHERE id = 'R_MAN'"),
            "managed release survives"
        );
        assert!(
            row_exists(&c, "SELECT 1 FROM tracks WHERE id = 'T_MAN'"),
            "track of managed release survives"
        );
        assert!(
            !row_exists(&c, "SELECT 1 FROM releases WHERE id = 'R_UNMAN2'"),
            "the unmanaged sibling release is still cut"
        );
        assert!(
            row_exists(&c, "SELECT 1 FROM album_artists WHERE id = 'AA_MIXED'"),
            "album_artists of surviving album kept"
        );
        assert!(
            row_exists(&c, "SELECT 1 FROM artists WHERE id = 'A2'"),
            "artist kept via a surviving album"
        );
    }

    #[test]
    fn changeset_cut_drops_orphan_ancestor() {
        let c = conn();
        create_album_schema(&c);
        let out = capture_and_gate(
            &c,
            &album_tables(),
            &[
                "INSERT INTO albums (id, _updated_at) VALUES ('AL', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R', 'AL', 0, '0000000001000-0000-dev1')",
                "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T', 'R', '0000000001000-0000-dev1')",
            ],
        );
        let changes = walk(&out).expect("walk");
        assert!(
            !has_row(&changes, "albums", "AL"),
            "orphan album cut (no kept release)"
        );
        assert!(!has_row(&changes, "releases", "R"), "unmanaged release cut");
        assert!(
            !has_row(&changes, "tracks", "T"),
            "track of unmanaged release cut"
        );
    }

    #[test]
    fn deleting_a_shared_album_with_its_release_propagates_the_album_delete() {
        let c = conn();
        create_album_schema(&c);

        // A shared album: a managed release under it makes the album sync to peers.
        exec(&c, "INSERT INTO artists (id, name, _updated_at) VALUES ('ar1', 'Artist', '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('al1', 'ar1', '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('re1', 'al1', 1, '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('tr1', 're1', '0000000001000-0000-dev1')");

        // Deleting the album cascades the release and track; the capture records the
        // album DELETE plus the cascaded child DELETEs.
        let out = capture_and_gate(
            &c,
            &album_tables(),
            &["DELETE FROM albums WHERE id = 'al1'"],
        );
        let changes = walk(&out).expect("walk");

        assert!(
            has_row(&changes, "releases", "re1"),
            "the managed release's removal propagates"
        );
        assert!(
            has_row(&changes, "albums", "al1"),
            "the album's own removal must propagate so a peer drops the now-empty \
             album instead of keeping a phantom"
        );
        assert!(
            has_row(&changes, "tracks", "tr1"),
            "the track's removal must propagate too — apply does not cascade, so a \
             cut track would orphan under the removed release on every peer"
        );
    }

    #[test]
    fn deleting_one_release_keeps_the_surviving_shared_album() {
        let c = conn();
        create_album_schema(&c);

        // An album with two managed releases; deleting one leaves the album shared.
        exec(
            &c,
            "INSERT INTO albums (id, _updated_at) VALUES ('al1', '0000000001000-0000-dev1')",
        );
        exec(&c, "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('re1', 'al1', 1, '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('re2', 'al1', 1, '0000000001000-0000-dev1')");

        let out = capture_and_gate(
            &c,
            &album_tables(),
            &["DELETE FROM releases WHERE id = 're1'"],
        );
        let changes = walk(&out).expect("walk");

        assert!(
            has_row(&changes, "releases", "re1"),
            "the deleted managed release's removal propagates"
        );
        assert!(
            !has_row(&changes, "albums", "al1"),
            "the album survives (it still has a managed release), so its row is not \
             emitted as a deletion"
        );
    }

    #[test]
    fn deleting_a_private_album_does_not_propagate_its_delete() {
        let c = conn();
        create_album_schema(&c);

        // A private album: only an unmanaged release, so it never synced to peers.
        exec(
            &c,
            "INSERT INTO albums (id, _updated_at) VALUES ('al1', '0000000001000-0000-dev1')",
        );
        exec(&c, "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('re1', 'al1', 0, '0000000001000-0000-dev1')");

        let out = capture_and_gate(
            &c,
            &album_tables(),
            &["DELETE FROM albums WHERE id = 'al1'"],
        );
        let changes = walk(&out).expect("walk");

        assert!(
            !has_row(&changes, "releases", "re1"),
            "the unmanaged release was never shared; its removal is cut"
        );
        assert!(
            !has_row(&changes, "albums", "al1"),
            "a never-shared album's removal must not propagate — its DELETE carries \
             old column values a peer should never receive"
        );
    }

    #[test]
    fn deleting_a_shared_artist_with_its_album_propagates_the_artist_delete() {
        let c = conn();
        create_album_schema(&c);

        exec(&c, "INSERT INTO artists (id, name, _updated_at) VALUES ('ar1', 'Artist', '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('al1', 'ar1', '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('aa1', 'al1', 'ar1', '0000000001000-0000-dev1')");
        exec(&c, "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('re1', 'al1', 1, '0000000001000-0000-dev1')");

        // album.artist_id has no ON DELETE CASCADE, so an artist is removed by
        // deleting its albums (cascading releases/album_artists) then the artist.
        let out = capture_and_gate(
            &c,
            &album_tables(),
            &[
                "DELETE FROM albums WHERE id = 'al1'",
                "DELETE FROM artists WHERE id = 'ar1'",
            ],
        );
        let changes = walk(&out).expect("walk");

        assert!(
            has_row(&changes, "albums", "al1"),
            "the shared album's removal propagates"
        );
        assert!(
            has_row(&changes, "artists", "ar1"),
            "the artist's removal must propagate up the chain once its last kept \
             album is being removed"
        );
    }

    #[test]
    fn flip_reemits_whole_connected_component_to_peer() {
        let c = conn();
        create_album_schema(&c);
        let tables = album_tables();

        // Cycle 1: build the private graph. Nothing should escape.
        let out1 = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA', 'AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R', 'AL', 0, '0000000001000-0000-dev1')",
                "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T', 'R', '0000000001000-0000-dev1')",
            ],
        );
        assert!(
            walk(&out1).expect("walk").is_empty(),
            "private graph emits nothing"
        );

        // Cycle 2: flip the release managed. Re-emit the whole component.
        let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET managed = 1, _updated_at = '0000000002000-0000-dev1' WHERE id = 'R'"],
        );
        let changes = walk(&out2).expect("walk");
        assert!(
            has_row(&changes, "releases", "R"),
            "promoted release emitted"
        );
        assert!(
            has_row(&changes, "albums", "AL"),
            "ancestor album re-emitted"
        );
        assert!(
            has_row(&changes, "artists", "AR"),
            "ancestor artist re-emitted"
        );
        assert!(
            has_row(&changes, "tracks", "T"),
            "descendant track re-emitted"
        );
        assert!(
            has_row(&changes, "album_artists", "AA"),
            "kept child of ancestor re-emitted"
        );

        let peer = conn();
        create_album_schema(&peer);
        apply_album(&peer, &out2);
        assert!(
            row_exists(&peer, "SELECT 1 FROM artists WHERE id = 'AR'"),
            "peer has artist"
        );
        assert!(
            row_exists(&peer, "SELECT 1 FROM albums WHERE id = 'AL'"),
            "peer has album"
        );
        assert!(
            row_exists(&peer, "SELECT 1 FROM album_artists WHERE id = 'AA'"),
            "peer has album_artists"
        );
        assert!(
            row_exists(&peer, "SELECT 1 FROM releases WHERE id = 'R'"),
            "peer has release"
        );
        assert!(
            row_exists(&peer, "SELECT 1 FROM tracks WHERE id = 'T'"),
            "peer has track"
        );
    }

    #[test]
    fn reparent_onto_a_cut_ancestor_reemits_it_to_peer() {
        let c = conn();
        create_album_schema(&c);
        let tables = album_tables();

        // Cycle 1: an artist, two albums under it, and a managed release under
        // AL1. AL2 has no managed release, so the gate cuts it — the peer never
        // receives it.
        let out1 = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL1', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL2', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL1', 1, '0000000001000-0000-dev1')",
            ],
        );
        let peer = conn();
        create_album_schema(&peer);
        apply_album(&peer, &out1);
        assert!(
            row_exists(&peer, "SELECT 1 FROM albums WHERE id = 'AL1'"),
            "peer has the shared album AL1"
        );
        assert!(
            !row_exists(&peer, "SELECT 1 FROM albums WHERE id = 'AL2'"),
            "AL2 was cut (no managed release) — the peer never received it"
        );

        // Cycle 2: reparent the managed release onto AL2 (previously cut). The
        // gate must re-emit AL2 (and its component), or the peer applies the bare
        // FK change against a missing album and is left with a dangling release.
        let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET album_id = 'AL2', _updated_at = '0000000002000-0000-dev1' WHERE id = 'R1'"],
        );
        let changes = walk(&out2).expect("walk");
        assert!(
            has_row(&changes, "albums", "AL2"),
            "the newly-referenced album AL2 must be re-emitted"
        );

        apply_album(&peer, &out2);
        assert!(
            row_exists(&peer, "SELECT 1 FROM albums WHERE id = 'AL2'"),
            "peer now has AL2"
        );
        assert_eq!(
            query_text(&peer, "SELECT album_id FROM releases WHERE id = 'R1'"),
            "AL2",
            "R1 now points at AL2 on the peer"
        );
        assert!(
            !row_exists(
                &peer,
                "SELECT 1 FROM releases r WHERE NOT EXISTS \
                 (SELECT 1 FROM albums a WHERE a.id = r.album_id)"
            ),
            "no release on the peer points at a missing album"
        );
    }

    #[test]
    fn flip_reemits_sideways_featured_artist() {
        let c = conn();
        create_album_schema(&c);
        let tables = album_tables();

        // AR1 owns AL1; AR2 is featured via AA; release R1 unmanaged.
        let out1 = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR1', 'Owner', '0000000001000-0000-dev1')",
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR2', 'Featured', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL1', 'AR1', '0000000001000-0000-dev1')",
                "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA', 'AL1', 'AR2', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL1', 0, '0000000001000-0000-dev1')",
            ],
        );
        assert!(
            walk(&out1).expect("walk").is_empty(),
            "private graph emits nothing"
        );

        // Flip R1 managed.
        let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET managed = 1, _updated_at = '0000000002000-0000-dev1' WHERE id = 'R1'"],
        );
        let changes = walk(&out2).expect("walk");
        assert!(
            has_row(&changes, "album_artists", "AA"),
            "featured join row re-emitted"
        );
        assert!(
            has_row(&changes, "artists", "AR2"),
            "featured artist re-emitted"
        );

        let peer = conn();
        create_album_schema(&peer);
        apply_album(&peer, &out2);
        assert!(
            row_exists(&peer, "SELECT 1 FROM album_artists WHERE id = 'AA'"),
            "peer has join row"
        );
        assert!(
            row_exists(&peer, "SELECT 1 FROM artists WHERE id = 'AR2'"),
            "peer has featured artist"
        );
    }

    #[test]
    fn second_flip_is_idempotent_under_lww() {
        let c = conn();
        create_album_schema(&c);
        let tables = album_tables();

        // Cycle 1: an album with one managed release, synced to the peer.
        let out1 = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL', 1, '0000000001000-0000-dev1')",
            ],
        );

        let peer = conn();
        create_album_schema(&peer);
        apply_album(&peer, &out1);
        assert!(
            row_exists(&peer, "SELECT 1 FROM albums WHERE id = 'AL'"),
            "peer has the album after cycle 1"
        );

        // Cycle 2a: insert a second release unmanaged (stays private, cut).
        let _ = capture_and_gate(
            &c,
            &tables,
            &["INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R2', 'AL', 0, '0000000002000-0000-dev1')"],
        );

        // Cycle 2b: flip the second release managed. Re-emit re-sends the album.
        let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET managed = 1, _updated_at = '0000000003000-0000-dev1' WHERE id = 'R2'"],
        );
        let changes = walk(&out2).expect("walk");
        assert!(
            has_row(&changes, "albums", "AL"),
            "album re-emitted on the second flip"
        );
        assert!(
            has_row(&changes, "releases", "R2"),
            "second release emitted"
        );

        // Applying the duplicate album INSERT must not error; peer consistent.
        apply_album(&peer, &out2);
        assert!(
            row_exists(&peer, "SELECT 1 FROM albums WHERE id = 'AL'"),
            "album still present"
        );
        assert!(
            row_exists(&peer, "SELECT 1 FROM releases WHERE id = 'R1'"),
            "first release still present"
        );
        assert!(
            row_exists(&peer, "SELECT 1 FROM releases WHERE id = 'R2'"),
            "second release now present"
        );
        assert_eq!(
            query_int(&peer, "SELECT COUNT(*) FROM albums WHERE id = 'AL'"),
            1,
            "the duplicate INSERT did not create a second album row"
        );
    }

    // ---- retract (gate true→false) -------------------------------------------

    fn has_op(
        changes: &[crate::changeset::RowChange],
        table: &str,
        pk: &str,
        op: ChangeOp,
    ) -> bool {
        changes
            .iter()
            .any(|c| c.table == table && c.pk() == Some(pk) && c.op == op)
    }

    #[test]
    fn retract_emits_deletes_and_local_rows_remain() {
        let c = conn();
        create_synced_schema(&c);
        let tables = test_synced_tables();

        // Cycle 1: a shared note with children (inserted shared=1 → flip emits the
        // subtree as INSERTs).
        let _ = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Public', 'b', 1, '0000000001000-0000-dev1', '2026-01-01')",
                "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                 VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
                "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
                 VALUES ('p1', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
            ],
        );

        // Cycle 2: flip the gate false. The captured UPDATE-to-false is skipped;
        // the gate synthesizes DELETEs for the root and its now-private subtree.
        let out = capture_and_gate(
            &c,
            &tables,
            &["UPDATE notes SET shared = 0, _updated_at = '0000000002000-0000-dev1' WHERE id = 'n1'"],
        );
        let changes = walk(&out).expect("walk");
        assert!(
            has_op(&changes, "notes", "n1", ChangeOp::Delete),
            "the retracted root is emitted as a DELETE"
        );
        assert!(
            has_op(&changes, "note_tags", "t1", ChangeOp::Delete),
            "the tag child leaves the shared set as a DELETE"
        );
        assert!(
            has_op(&changes, "note_photos", "p1", ChangeOp::Delete),
            "the photo child leaves the shared set as a DELETE"
        );
        assert!(
            changes.iter().all(|c| c.op == ChangeOp::Delete),
            "retract emits only DELETEs (verifies the reverse session_diff direction)"
        );

        // The local rows stay — retract is peer-only, never a local DELETE FROM.
        assert!(row_exists(&c, "SELECT 1 FROM notes WHERE id = 'n1'"));
        assert!(row_exists(&c, "SELECT 1 FROM note_tags WHERE id = 't1'"));
        assert!(row_exists(&c, "SELECT 1 FROM note_photos WHERE id = 'p1'"));
    }

    #[test]
    fn peer_applies_retract_and_subtree_is_removed() {
        let c = conn();
        create_synced_schema(&c);
        let tables = test_synced_tables();

        // Share the subtree to a peer.
        let out1 = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Public', 'b', 1, '0000000001000-0000-dev1', '2026-01-01')",
                "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                 VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
                "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
                 VALUES ('p1', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
            ],
        );
        let peer = conn();
        create_synced_schema(&peer);
        apply_changeset_lww(&peer, &out1, &tables, crate::sync::hlc::now_wall_ms())
            .expect("apply share");
        assert!(row_exists(&peer, "SELECT 1 FROM notes WHERE id = 'n1'"));
        assert!(row_exists(&peer, "SELECT 1 FROM note_tags WHERE id = 't1'"));
        assert!(row_exists(
            &peer,
            "SELECT 1 FROM note_photos WHERE id = 'p1'"
        ));

        // Retract and apply: the whole subtree is gone on the peer.
        let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE notes SET shared = 0, _updated_at = '0000000002000-0000-dev1' WHERE id = 'n1'"],
        );
        apply_changeset_lww(&peer, &out2, &tables, crate::sync::hlc::now_wall_ms())
            .expect("apply retract");
        assert!(
            !row_exists(&peer, "SELECT 1 FROM notes WHERE id = 'n1'"),
            "peer drops the retracted root"
        );
        assert!(
            !row_exists(&peer, "SELECT 1 FROM note_tags WHERE id = 't1'"),
            "peer drops the tag child"
        );
        assert!(
            !row_exists(&peer, "SELECT 1 FROM note_photos WHERE id = 'p1'"),
            "peer drops the photo child"
        );
    }

    #[test]
    fn retract_one_of_two_managed_roots_spares_sibling_and_ancestor() {
        let c = conn();
        create_album_schema(&c);
        let tables = album_tables();

        // Two managed releases under one album/artist, with a track each.
        let out1 = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA', 'AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL', 1, '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R2', 'AL', 1, '0000000001000-0000-dev1')",
                "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T1', 'R1', '0000000001000-0000-dev1')",
                "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T2', 'R2', '0000000001000-0000-dev1')",
            ],
        );
        let peer = conn();
        create_album_schema(&peer);
        apply_album(&peer, &out1);

        // Retract only R1.
        let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET managed = 0, _updated_at = '0000000002000-0000-dev1' WHERE id = 'R1'"],
        );
        let changes = walk(&out2).expect("walk");
        assert!(
            has_op(&changes, "releases", "R1", ChangeOp::Delete),
            "R1 deleted"
        );
        assert!(
            has_op(&changes, "tracks", "T1", ChangeOp::Delete),
            "T1 deleted"
        );
        assert!(
            !has_row(&changes, "releases", "R2"),
            "the sibling managed release is spared"
        );
        assert!(
            !has_row(&changes, "tracks", "T2"),
            "the sibling's track is spared"
        );
        assert!(
            !has_row(&changes, "albums", "AL"),
            "the album is spared (still has a managed release)"
        );
        assert!(!has_row(&changes, "artists", "AR"), "the artist is spared");
        assert!(
            !has_row(&changes, "album_artists", "AA"),
            "the join row is spared"
        );

        apply_album(&peer, &out2);
        assert!(
            !row_exists(&peer, "SELECT 1 FROM releases WHERE id = 'R1'"),
            "R1 gone on peer"
        );
        assert!(
            !row_exists(&peer, "SELECT 1 FROM tracks WHERE id = 'T1'"),
            "T1 gone on peer"
        );
        assert!(
            row_exists(&peer, "SELECT 1 FROM releases WHERE id = 'R2'"),
            "R2 kept on peer"
        );
        assert!(
            row_exists(&peer, "SELECT 1 FROM tracks WHERE id = 'T2'"),
            "T2 kept on peer"
        );
        assert!(
            row_exists(&peer, "SELECT 1 FROM albums WHERE id = 'AL'"),
            "album kept on peer"
        );
        assert!(
            row_exists(&peer, "SELECT 1 FROM artists WHERE id = 'AR'"),
            "artist kept on peer"
        );
        assert!(
            row_exists(&peer, "SELECT 1 FROM album_artists WHERE id = 'AA'"),
            "join row kept on peer"
        );
    }

    #[test]
    fn retract_last_root_under_ancestor_deletes_ancestor() {
        let c = conn();
        create_album_schema(&c);
        let tables = album_tables();

        // A single managed release under an album/artist.
        let out1 = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA', 'AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL', 1, '0000000001000-0000-dev1')",
                "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T1', 'R1', '0000000001000-0000-dev1')",
            ],
        );
        let peer = conn();
        create_album_schema(&peer);
        apply_album(&peer, &out1);

        // Retract the last managed release: the now-childless album AND artist are
        // deleted too.
        let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET managed = 0, _updated_at = '0000000002000-0000-dev1' WHERE id = 'R1'"],
        );
        let changes = walk(&out2).expect("walk");
        for (table, pk) in [
            ("releases", "R1"),
            ("tracks", "T1"),
            ("albums", "AL"),
            ("artists", "AR"),
            ("album_artists", "AA"),
        ] {
            assert!(
                has_op(&changes, table, pk, ChangeOp::Delete),
                "{table}.{pk} is deleted when the last managed root is retracted"
            );
        }

        apply_album(&peer, &out2);
        assert!(!row_exists(&peer, "SELECT 1 FROM releases WHERE id = 'R1'"));
        assert!(!row_exists(&peer, "SELECT 1 FROM tracks WHERE id = 'T1'"));
        assert!(
            !row_exists(&peer, "SELECT 1 FROM albums WHERE id = 'AL'"),
            "the now-childless album is deleted on the peer"
        );
        assert!(
            !row_exists(&peer, "SELECT 1 FROM artists WHERE id = 'AR'"),
            "the now-childless artist is deleted on the peer"
        );
        assert!(!row_exists(
            &peer,
            "SELECT 1 FROM album_artists WHERE id = 'AA'"
        ));
    }

    #[test]
    fn gated_false_root_from_start_emits_no_deletes() {
        let c = conn();
        create_synced_schema(&c);
        let tables = test_synced_tables();

        // Cycle 1: a private note (never shared). Nothing emitted.
        let out1 = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Private', 'b', 0, '0000000001000-0000-dev1', '2026-01-01')",
            ],
        );
        assert!(
            walk(&out1).expect("walk").is_empty(),
            "a gated-false root inserted private emits nothing — no DELETE"
        );

        // Cycle 2: edit the still-private note (no gate transition). The update is
        // cut as before; no retract fires (there was never a true→false flip).
        let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE notes SET body = 'edited', _updated_at = '0000000002000-0000-dev1' WHERE id = 'n1'"],
        );
        assert!(
            walk(&out2).expect("walk").is_empty(),
            "editing a never-shared gated-false root emits nothing — retract only fires on a true→false transition"
        );
    }

    #[test]
    fn reshare_after_retract_reemits_inserts() {
        let c = conn();
        create_album_schema(&c);
        let tables = album_tables();

        // Cycle 1: a private subtree (release unmanaged). Nothing escapes.
        let _ = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA', 'AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL', 0, '0000000001000-0000-dev1')",
                "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T1', 'R1', '0000000001000-0000-dev1')",
            ],
        );
        let peer = conn();
        create_album_schema(&peer);

        // Cycle 2: share (false→true). Peer gets the whole component.
        let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET managed = 1, _updated_at = '0000000002000-0000-dev1' WHERE id = 'R1'"],
        );
        apply_album(&peer, &out2);
        assert!(
            row_exists(&peer, "SELECT 1 FROM releases WHERE id = 'R1'"),
            "shared to peer"
        );

        // Cycle 3: retract (true→false). Peer loses the component.
        let out3 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET managed = 0, _updated_at = '0000000003000-0000-dev1' WHERE id = 'R1'"],
        );
        apply_album(&peer, &out3);
        assert!(
            !row_exists(&peer, "SELECT 1 FROM releases WHERE id = 'R1'"),
            "retracted from peer"
        );
        assert!(
            !row_exists(&peer, "SELECT 1 FROM albums WHERE id = 'AL'"),
            "album retracted"
        );

        // Cycle 4: re-share (false→true) re-emits full INSERTs — round-trip.
        let out4 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET managed = 1, _updated_at = '0000000004000-0000-dev1' WHERE id = 'R1'"],
        );
        let changes = walk(&out4).expect("walk");
        for (table, pk) in [
            ("releases", "R1"),
            ("tracks", "T1"),
            ("albums", "AL"),
            ("artists", "AR"),
            ("album_artists", "AA"),
        ] {
            assert!(
                has_op(&changes, table, pk, ChangeOp::Insert),
                "{table}.{pk} re-emitted as an INSERT on re-share"
            );
        }
        apply_album(&peer, &out4);
        assert!(
            row_exists(&peer, "SELECT 1 FROM releases WHERE id = 'R1'"),
            "re-shared to peer"
        );
        assert!(
            row_exists(&peer, "SELECT 1 FROM tracks WHERE id = 'T1'"),
            "track back on peer"
        );
        assert!(
            row_exists(&peer, "SELECT 1 FROM albums WHERE id = 'AL'"),
            "album back on peer"
        );
        assert!(
            row_exists(&peer, "SELECT 1 FROM artists WHERE id = 'AR'"),
            "artist back on peer"
        );
        assert!(
            row_exists(&peer, "SELECT 1 FROM album_artists WHERE id = 'AA'"),
            "join row back"
        );
    }

    #[test]
    fn multi_device_share_then_retract() {
        // Device A shares a subtree (false→true), device B applies it; A retracts
        // (true→false), B applies and the subtree is gone on B.
        let a = conn();
        create_album_schema(&a);
        let tables = album_tables();
        let b = conn();
        create_album_schema(&b);

        // A builds a private subtree, then flips it shared.
        let _ = capture_and_gate(
            &a,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist', '0000000001000-0000-devA')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-devA')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL', 0, '0000000001000-0000-devA')",
                "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T1', 'R1', '0000000001000-0000-devA')",
            ],
        );
        let share = capture_and_gate(
            &a,
            &tables,
            &["UPDATE releases SET managed = 1, _updated_at = '0000000002000-0000-devA' WHERE id = 'R1'"],
        );
        apply_album(&b, &share);
        assert!(
            row_exists(&b, "SELECT 1 FROM releases WHERE id = 'R1'"),
            "B has the release"
        );
        assert!(
            row_exists(&b, "SELECT 1 FROM tracks WHERE id = 'T1'"),
            "B has the track"
        );
        assert!(
            row_exists(&b, "SELECT 1 FROM albums WHERE id = 'AL'"),
            "B has the album"
        );

        // A retracts; B applies; the subtree is gone on B while A keeps it locally.
        let retract = capture_and_gate(
            &a,
            &tables,
            &["UPDATE releases SET managed = 0, _updated_at = '0000000003000-0000-devA' WHERE id = 'R1'"],
        );
        apply_album(&b, &retract);
        assert!(
            !row_exists(&b, "SELECT 1 FROM releases WHERE id = 'R1'"),
            "B drops the release"
        );
        assert!(
            !row_exists(&b, "SELECT 1 FROM tracks WHERE id = 'T1'"),
            "B drops the track"
        );
        assert!(
            !row_exists(&b, "SELECT 1 FROM albums WHERE id = 'AL'"),
            "B drops the album"
        );

        // A keeps the rows locally — retract is peer-only.
        assert!(
            row_exists(&a, "SELECT 1 FROM releases WHERE id = 'R1'"),
            "A keeps the release"
        );
        assert!(
            row_exists(&a, "SELECT 1 FROM tracks WHERE id = 'T1'"),
            "A keeps the track"
        );
        assert!(
            row_exists(&a, "SELECT 1 FROM albums WHERE id = 'AL'"),
            "A keeps the album"
        );
    }
}
