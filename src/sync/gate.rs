//! Row-level sync gating.
//!
//! A host declares a boolean **gate** column on a *root* synced table (via
//! [`SyncedTable::gated_by`](super::session::SyncedTable::gated_by)). A root row
//! is shared — i.e. it syncs to peers — iff its gate column is true. The gate
//! flows down *declared foreign keys*: a child row is shared iff the row at the
//! top of its FK chain (its gated-ancestor root) is shared. Rows that are not
//! gated and not FK-descendants of a gated root always sync.
//!
//! [`gate_outbound`] is the one entry point. Given the changeset a cycle
//! captured, it returns a new changeset with gated-false rows cut, plus — when a
//! root's gate flips false→true this cycle — full-state INSERTs for that root's
//! whole now-visible subtree (peers never saw it while it was private), so the
//! promotion lands as a complete consistent subtree on every peer.
//!
//! Revoke (gate true→false) is a *freeze*: the row simply stops being emitted.
//! coven does not retract already-synced rows from peers.
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

use std::collections::{HashMap, HashSet};
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;

use libsqlite3_sys as ffi;
use tracing::{debug, warn};

use crate::changeset::{extract_new_value, extract_old_value};

use super::session::{synced_tables, SyncedTable};
use super::session_ext::{quote_ident, Changeset, Session};

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

    /// Concatenate everything added so far into one changeset.
    fn output(&self) -> Result<Changeset, GateError> {
        let mut len: c_int = 0;
        let mut buf: *mut c_void = ptr::null_mut();
        let rc = unsafe { ffi::sqlite3changegroup_output(self.raw, &mut len, &mut buf) };
        if rc != ffi::SQLITE_OK as c_int {
            return Err(GateError::Ffi("sqlite3changegroup_output", rc));
        }
        // `output` hands us sqlite3-managed memory; wrap it so Drop frees it.
        let bytes = if buf.is_null() || len == 0 {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(buf as *const u8, len as usize) }
        };
        let cs = Changeset::from_bytes(bytes);
        if !buf.is_null() {
            unsafe { ffi::sqlite3_free(buf) };
        }
        Ok(cs)
    }
}

impl Drop for Changegroup {
    fn drop(&mut self) {
        unsafe { ffi::sqlite3changegroup_delete(self.raw) };
    }
}

impl Session {
    /// Record into this session the changes that would transform table `tbl` in
    /// the attached database `from_db` into `tbl` in this session's `main`.
    ///
    /// With an empty `from_db.tbl`, the recorded changeset is a full-state INSERT
    /// for every current row of `main.tbl`.
    ///
    /// # Safety
    /// `from_db` must name a database attached to this session's connection, and
    /// `from_db.tbl` must have a schema identical to `main.tbl`.
    pub unsafe fn diff(&self, from_db: &str, tbl: &str) -> Result<(), GateError> {
        let from = CString::new(from_db).unwrap();
        let table = CString::new(tbl).unwrap();
        let mut errmsg: *mut c_char = ptr::null_mut();
        let rc =
            ffi::sqlite3session_diff(self.raw_ptr(), from.as_ptr(), table.as_ptr(), &mut errmsg);
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
}

/// How a synced table relates to the gate.
enum TableGate {
    /// A gated root: the boolean gate lives at this column index.
    Root { gate_col: usize },
    /// A child whose gate is inherited from `parent` via the FK column at
    /// `fk_col` (column index in *this* table), holding the parent's id.
    Child { fk_col: usize, parent: String },
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
    /// # Safety
    /// `db` must be a valid, open sqlite3 connection holding the synced schema.
    pub unsafe fn from_db(db: *mut ffi::sqlite3) -> Result<Self, GateError> {
        Self::from_tables(db, synced_tables())
    }

    /// Build from an explicit table set (the production path passes
    /// [`synced_tables`]; tests pass their own).
    ///
    /// # Safety
    /// `db` must be a valid, open sqlite3 connection holding the synced schema.
    pub unsafe fn from_tables(
        db: *mut ffi::sqlite3,
        tables: &[SyncedTable],
    ) -> Result<Self, GateError> {
        let synced_names: HashSet<&str> = tables.iter().map(|t| t.name()).collect();
        let mut gate_map = HashMap::new();

        for t in tables {
            let cols = column_names(db, t.name())?;

            if let Some(gate) = t.gate_column() {
                let idx = cols.iter().position(|c| c == gate).ok_or_else(|| {
                    GateError::MissingGateColumn(t.name().to_string(), gate.to_string())
                })?;
                gate_map.insert(t.name().to_string(), TableGate::Root { gate_col: idx });
                continue;
            }

            // Not a declared root: does it have an FK to another synced table?
            // If so it inherits that parent's gate. Gate inheritance flows ONLY
            // through declared FKs, and only toward synced parents.
            if let Some((fk_name, parent)) = first_synced_fk(db, t.name(), &synced_names)? {
                let fk_col = cols.iter().position(|c| c == &fk_name).ok_or_else(|| {
                    GateError::MissingFkColumn(t.name().to_string(), fk_name.clone())
                })?;
                gate_map.insert(t.name().to_string(), TableGate::Child { fk_col, parent });
            }
            // else: ungated, unconditionally shared — not in the map.
        }

        // Prune children whose FK chain never reaches a gated root: they are
        // effectively ungated. (A child of an ungated parent inherits nothing.)
        let reaches_root: HashSet<String> = gate_map
            .keys()
            .filter(|name| chain_to_root_depth(&gate_map, name).is_some())
            .cloned()
            .collect();
        gate_map.retain(|name, tg| match tg {
            TableGate::Root { .. } => true,
            TableGate::Child { .. } => reaches_root.contains(name),
        });

        Ok(Gates { tables: gate_map })
    }

    /// Every table governed by the gate (roots and inheriting children), in FK
    /// order: a parent always precedes its children. Re-emitted full-state
    /// INSERTs must apply parent-first or a peer with `foreign_keys=ON` rejects
    /// the child before its parent row exists.
    fn gated_tables_parent_first(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.tables.keys().map(String::as_str).collect();
        // Every retained table reaches a root (children that don't were pruned),
        // so the depth is always `Some`; a root sorts first at depth 0.
        names.sort_by_key(|n| chain_to_root_depth(&self.tables, n).unwrap_or(0));
        names
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
    /// Each table — root or descendant — resolves its own gate truth by walking
    /// its declared FK chain up to the root and testing that root's gate, so the
    /// result is independent of deletion order and does not rely on the schema
    /// declaring `ON DELETE CASCADE`. Roots are deleted last only so a
    /// descendant's chain walk still sees its (gated-false) root row.
    ///
    /// # Safety
    /// `db` must be a valid, open sqlite3 connection holding the synced schema.
    pub unsafe fn delete_gated_false(&self, db: *mut ffi::sqlite3) -> Result<(), GateError> {
        // Descendants first, roots last: each descendant's keep-test joins up to
        // its root row, which must still exist while the descendant is pruned.
        let mut tables = self.gated_tables_parent_first();
        tables.reverse();

        for tbl in tables {
            let keep = self.keep_clause(db, tbl)?;
            let sql = format!("DELETE FROM {} WHERE NOT ({keep})", quote_ident(tbl));
            exec_sql(db, &sql)?;
        }
        Ok(())
    }

    /// A SQL boolean that is true for rows of `tbl` the gate keeps: `tbl`'s gate,
    /// resolved by walking its declared FK chain up to the gated root and testing
    /// that root's gate column. Built inside-out — the innermost fragment is the
    /// root gate test; each child wraps its parent's clause in a correlated
    /// `EXISTS` joined on the FK. A dangling FK anywhere on the chain makes the
    /// `EXISTS` false (the row is not shared), matching `resolve_root`'s
    /// treatment of a missing ancestor as not-shared.
    ///
    /// # Safety
    /// `db` must be a valid, open sqlite3 connection holding the synced schema.
    unsafe fn keep_clause(&self, db: *mut ffi::sqlite3, tbl: &str) -> Result<String, GateError> {
        match self.tables.get(tbl) {
            Some(TableGate::Root { gate_col }) => {
                let gate = nth_column_name(db, tbl, *gate_col)?;
                Ok(truthy_sql(&format!(
                    "{}.{}",
                    quote_ident(tbl),
                    quote_ident(&gate)
                )))
            }
            Some(TableGate::Child { fk_col, parent }) => {
                let fk = nth_column_name(db, tbl, *fk_col)?;
                let inner = self.keep_clause(db, parent)?;
                Ok(format!(
                    "EXISTS (SELECT 1 FROM {parent_t} \
                       WHERE {parent_t}.{id} = {child}.{fk} AND ({inner}))",
                    parent_t = quote_ident(parent),
                    id = quote_ident("id"),
                    child = quote_ident(tbl),
                    fk = quote_ident(&fk),
                ))
            }
            // Not in the gate map: ungated, always kept.
            None => Ok("1".to_string()),
        }
    }
}

/// SQL predicate that is true exactly when `expr` is a gate-true value, matching
/// [`truthy`]: a nonzero integer. NULL, `0`, and non-integers are false. The
/// `CAST` collapses to 0 for non-numeric text, so only a genuine nonzero integer
/// passes — the same rule the changeset gate applies in Rust.
fn truthy_sql(expr: &str) -> String {
    format!("({expr} IS NOT NULL AND CAST({expr} AS INTEGER) <> 0)")
}

/// Walk `gate_map` from `name` up its FK chain to its gated root: `Some(0)` for
/// a root, `Some(n)` for an n-hop descendant of a root, `None` if the chain
/// never reaches a gated root (the table is effectively ungated) or loops.
fn chain_to_root_depth(gate_map: &HashMap<String, TableGate>, name: &str) -> Option<usize> {
    let mut depth = 0;
    let mut cur = name;
    let mut seen = HashSet::new();
    loop {
        if !seen.insert(cur.to_string()) {
            return None; // cycle, defensive
        }
        match gate_map.get(cur) {
            Some(TableGate::Root { .. }) => return Some(depth),
            Some(TableGate::Child { parent, .. }) => {
                depth += 1;
                cur = parent.as_str();
            }
            None => return None,
        }
    }
}

/// Gate a captured outbound changeset: cut gated-false rows (and their gated
/// descendants), and re-emit the full subtree of any root that flipped
/// false→true this cycle.
///
/// # Safety
/// `db` must be the valid, open connection the changeset was captured on, with
/// no live session attached (gating reads current row state from it).
pub unsafe fn gate_outbound(
    db: *mut ffi::sqlite3,
    changeset: &Changeset,
    gates: &Gates,
) -> Result<Changeset, GateError> {
    let group = Changegroup::new()?;
    group.set_schema(db)?;

    // Roots that flip false→true this cycle need their whole current subtree
    // re-emitted (peers never had it while private).
    let mut flipped_roots: HashSet<String> = HashSet::new();

    // Pass 1: walk the captured changeset, keep gated-true rows, note flips.
    let bytes = changeset.as_bytes();
    if !bytes.is_empty() {
        let mut iter: *mut ffi::sqlite3_changeset_iter = ptr::null_mut();
        let rc = ffi::sqlite3changeset_start(
            &mut iter,
            bytes.len() as c_int,
            bytes.as_ptr() as *mut c_void,
        );
        if rc != ffi::SQLITE_OK as c_int {
            return Err(GateError::Ffi("sqlite3changeset_start", rc));
        }

        loop {
            let step = ffi::sqlite3changeset_next(iter);
            if step == ffi::SQLITE_DONE as c_int {
                break;
            }
            if step != ffi::SQLITE_ROW as c_int {
                ffi::sqlite3changeset_finalize(iter);
                return Err(GateError::Ffi("sqlite3changeset_next", step));
            }

            let row = ChangeRow::read(iter);

            // A root whose gate flips false→true this cycle has its whole now-
            // visible subtree re-emitted as full-state INSERTs below. Record it
            // and skip the captured row: an UPDATE(false→true) is wrong for a
            // peer that never had the row (it would apply as a NOTFOUND no-op),
            // and an INSERT is reproduced identically by the re-emit. Letting
            // re-emit be the single source avoids an UPDATE/INSERT dedup clash.
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
                    if let Some(pk) = row.pk() {
                        flipped_roots.insert(pk.to_string());
                    }
                    continue;
                }
            }

            if effective_gate(db, gates, &row)? {
                group.add_change(iter)?;
            }
        }

        let rc = ffi::sqlite3changeset_finalize(iter);
        if rc != ffi::SQLITE_OK as c_int {
            return Err(GateError::Ffi("sqlite3changeset_finalize", rc));
        }
    }

    // Pass 2: re-emit full subtrees for flipped roots, if any.
    if !flipped_roots.is_empty() {
        reemit_subtrees(db, gates, &flipped_roots, &group)?;
    }

    group.output()
}

/// For each flipped root id, diff every gated table against an empty clone to
/// obtain full-state INSERTs, then keep only rows whose gated-ancestor is a
/// flipped root, merging them into `group`.
unsafe fn reemit_subtrees(
    db: *mut ffi::sqlite3,
    gates: &Gates,
    flipped_roots: &HashSet<String>,
    group: &Changegroup,
) -> Result<(), GateError> {
    // The flipped roots, keyed by which root table they belong to, so re-emit
    // scoping can identify "row r belongs to flipped root x".
    let diff_cs = full_state_changeset(db, gates)?;
    let bytes = diff_cs.as_bytes();
    if bytes.is_empty() {
        return Ok(());
    }

    let mut iter: *mut ffi::sqlite3_changeset_iter = ptr::null_mut();
    let rc = ffi::sqlite3changeset_start(
        &mut iter,
        bytes.len() as c_int,
        bytes.as_ptr() as *mut c_void,
    );
    if rc != ffi::SQLITE_OK as c_int {
        return Err(GateError::Ffi("sqlite3changeset_start", rc));
    }

    loop {
        let step = ffi::sqlite3changeset_next(iter);
        if step == ffi::SQLITE_DONE as c_int {
            break;
        }
        if step != ffi::SQLITE_ROW as c_int {
            ffi::sqlite3changeset_finalize(iter);
            return Err(GateError::Ffi("sqlite3changeset_next", step));
        }

        let row = ChangeRow::read(iter);
        if let Some(root_id) = gated_root_id(db, gates, &row)? {
            if flipped_roots.contains(&root_id) {
                group.add_change(iter)?;
            }
        }
    }

    let rc = ffi::sqlite3changeset_finalize(iter);
    if rc != ffi::SQLITE_OK as c_int {
        return Err(GateError::Ffi("sqlite3changeset_finalize", rc));
    }
    Ok(())
}

/// Diff every gated table (`main`) against an empty schema-identical clone,
/// producing full-state INSERTs for all currently-present rows of those tables.
unsafe fn full_state_changeset(
    db: *mut ffi::sqlite3,
    gates: &Gates,
) -> Result<Changeset, GateError> {
    // Attach a fresh empty in-memory db and recreate each gated table's schema
    // in it, copied verbatim from sqlite_master so the diff sees identical
    // tables. A unique alias avoids colliding with any host-attached db.
    let alias = "coven_gate_empty";
    let attach = format!("ATTACH DATABASE ':memory:' AS {alias}");
    exec_sql(db, &attach)?;

    let tables = gates.gated_tables_parent_first();
    let result = (|| {
        for tbl in &tables {
            let create = create_table_sql(db, tbl)?;
            // The CREATE statement names the bare table; run it in the attached
            // db by qualifying via the schema-aware exec on the alias.
            let in_alias = rewrite_create_into_schema(&create, tbl, alias);
            exec_sql(db, &in_alias)?;
        }

        let session = Session::new(db).map_err(GateError::SessionCreate)?;
        for tbl in &tables {
            session
                .attach(Some(tbl))
                .map_err(GateError::SessionCreate)?;
            session.diff(alias, tbl)?;
        }
        session.changeset().map_err(GateError::ChangesetExtract)
    })();

    // Always detach, even on error. A failed detach leaves the clone attached
    // under `alias`, which would make next cycle's ATTACH collide — surface it.
    let detach = format!("DETACH DATABASE {alias}");
    if let Err(e) = exec_sql(db, &detach) {
        warn!("gate: failed to detach the temporary clone db ({alias}): {e}");
    }

    result
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
                .map(|(_, truth)| truth)
                .unwrap_or(false))
        }
    }
}

/// The flipped-root id this row belongs to (for re-emit scoping): the id of the
/// gated-ancestor root, or `None` if the row is ungated/unrooted or not shared.
unsafe fn gated_root_id(
    db: *mut ffi::sqlite3,
    gates: &Gates,
    row: &ChangeRow,
) -> Result<Option<String>, GateError> {
    match gates.tables.get(&row.table) {
        None => Ok(None),
        Some(TableGate::Root { gate_col }) => {
            if row.effective_truth(*gate_col) == Some(true) {
                Ok(row.pk().map(str::to_string))
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
                .filter(|(_, truth)| *truth)
                .map(|(root_id, _)| root_id))
        }
    }
}

/// Walk the live-db FK chain from (`table`, `id`) up to its gated root,
/// returning the root's id and its gate truth. `None` if the chain never
/// reaches a gated root, or a row along it is missing from the live db (an
/// anomaly the caller treats as not-shared).
unsafe fn resolve_root(
    db: *mut ffi::sqlite3,
    gates: &Gates,
    table: &str,
    id: &str,
) -> Result<Option<(String, bool)>, GateError> {
    match gates.tables.get(table) {
        Some(TableGate::Root { gate_col }) => {
            let col = nth_column_name(db, table, *gate_col)?;
            match query_truth(db, table, &col, id)? {
                Some(truth) => Ok(Some((id.to_string(), truth))),
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

/// SQLite boolean truth for a gate value read as text: a nonzero integer is
/// true; `0`/empty/non-integer is false.
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

/// The first FK on `table` whose parent is a synced table: returns
/// (child FK column name, parent table). Via `PRAGMA foreign_key_list`.
unsafe fn first_synced_fk(
    db: *mut ffi::sqlite3,
    table: &str,
    synced: &HashSet<&str>,
) -> Result<Option<(String, String)>, GateError> {
    let sql = format!("PRAGMA foreign_key_list({})", quote_ident(table));
    let stmt = prepare(db, &sql)?;
    let mut found = None;
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
        if synced.contains(parent.as_str()) {
            found = Some((from, parent));
            break;
        }
    }
    ffi::sqlite3_finalize(stmt);
    Ok(found)
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
    use crate::sync::session::SyncSession;
    use crate::sync::session_ext::Changeset;
    use crate::sync::test_helpers::*;

    /// Capture a changeset over `stmts`, then gate it against the test schema's
    /// gate model. Returns the gated output changeset.
    unsafe fn capture_and_gate(db: *mut ffi::sqlite3, stmts: &[&str]) -> Changeset {
        let session = SyncSession::start(db).expect("start session");
        for s in stmts {
            exec(db, s);
        }
        let cs = session
            .changeset()
            .expect("changeset")
            .expect("non-empty changeset");
        drop(session);
        let gates = Gates::from_tables(db, &test_synced_tables()).expect("build gates");
        gate_outbound(db, &cs, &gates).expect("gate outbound")
    }

    fn has_row(changes: &[crate::changeset::RowChange], table: &str, pk: &str) -> bool {
        changes
            .iter()
            .any(|c| c.table == table && c.pk() == Some(pk))
    }

    #[test]
    fn gated_false_root_is_cut() {
        unsafe {
            init_synced_tables();
            let db = open_memory_db();
            create_synced_schema(db);

            let out = capture_and_gate(
                db,
                &[
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                     VALUES ('n1', 'Private', NULL, 0, '0000000001000-0000-dev1', '2026-01-01')",
                ],
            );

            let changes = walk(out.as_bytes()).expect("walk");
            assert!(
                !has_row(&changes, "notes", "n1"),
                "a gated-false root must be cut from the outbound changeset"
            );

            ffi::sqlite3_close(db);
        }
    }

    #[test]
    fn gated_true_root_passes_through() {
        unsafe {
            init_synced_tables();
            let db = open_memory_db();
            create_synced_schema(db);

            let out = capture_and_gate(
                db,
                &[
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                     VALUES ('n1', 'Public', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
                ],
            );

            let changes = walk(out.as_bytes()).expect("walk");
            assert!(
                has_row(&changes, "notes", "n1"),
                "a gated-true root must pass through"
            );

            ffi::sqlite3_close(db);
        }
    }

    #[test]
    fn child_cut_because_parent_gated_false() {
        unsafe {
            init_synced_tables();
            let db = open_memory_db();
            create_synced_schema(db);

            let out = capture_and_gate(
                db,
                &[
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                     VALUES ('n1', 'Private', NULL, 0, '0000000001000-0000-dev1', '2026-01-01')",
                    "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                     VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
                ],
            );

            let changes = walk(out.as_bytes()).expect("walk");
            assert!(!has_row(&changes, "notes", "n1"), "parent cut");
            assert!(
                !has_row(&changes, "note_tags", "t1"),
                "child must be cut because its parent is gated-false"
            );

            ffi::sqlite3_close(db);
        }
    }

    #[test]
    fn child_passes_when_parent_gated_true() {
        unsafe {
            init_synced_tables();
            let db = open_memory_db();
            create_synced_schema(db);

            let out = capture_and_gate(
                db,
                &[
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                     VALUES ('n1', 'Public', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
                    "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                     VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
                    "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
                     VALUES ('p1', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
                ],
            );

            let changes = walk(out.as_bytes()).expect("walk");
            assert!(has_row(&changes, "notes", "n1"));
            assert!(
                has_row(&changes, "note_tags", "t1"),
                "child inherits parent's true gate"
            );
            assert!(
                has_row(&changes, "note_photos", "p1"),
                "FK child inherits parent's true gate"
            );

            ffi::sqlite3_close(db);
        }
    }

    #[test]
    fn ungated_table_always_passes() {
        // A synced table that is neither gated nor an FK-descendant of a gated
        // root always syncs, regardless of any gate state.
        unsafe {
            let db = open_memory_db();
            exec(db, "PRAGMA foreign_keys = ON");
            exec(
                db,
                "CREATE TABLE notes (id TEXT PRIMARY KEY, shared INTEGER NOT NULL DEFAULT 0, \
                 _updated_at TEXT NOT NULL)",
            );
            exec(
                db,
                "CREATE TABLE settings (id TEXT PRIMARY KEY, val TEXT, _updated_at TEXT NOT NULL)",
            );

            let tables = vec![
                SyncedTable::new("notes").gated_by("shared"),
                SyncedTable::new("settings"),
            ];

            let session = Session::new(db).expect("session");
            session.attach(Some("notes")).expect("attach notes");
            session.attach(Some("settings")).expect("attach settings");
            exec(
                db,
                "INSERT INTO notes (id, shared, _updated_at) VALUES ('n1', 0, '0000000001000-0000-dev1')",
            );
            exec(
                db,
                "INSERT INTO settings (id, val, _updated_at) VALUES ('s1', 'x', '0000000001000-0000-dev1')",
            );
            let cs = session.changeset().expect("cs");
            drop(session);

            let gates = Gates::from_tables(db, &tables).expect("gates");
            let out = gate_outbound(db, &cs, &gates).expect("gate");
            let changes = walk(out.as_bytes()).expect("walk");

            assert!(!has_row(&changes, "notes", "n1"), "gated-false note is cut");
            assert!(
                has_row(&changes, "settings", "s1"),
                "ungated table always passes through"
            );

            ffi::sqlite3_close(db);
        }
    }

    #[test]
    fn flip_false_to_true_reemits_full_subtree() {
        unsafe {
            init_synced_tables();
            let db = open_memory_db();
            create_synced_schema(db);

            // Cycle 1: create a private note with children. All cut.
            let out1 = capture_and_gate(
                db,
                &[
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                     VALUES ('n1', 'Private', 'b', 0, '0000000001000-0000-dev1', '2026-01-01')",
                    "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                     VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
                    "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
                     VALUES ('p1', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
                ],
            );
            let c1 = walk(out1.as_bytes()).expect("walk");
            assert!(c1.is_empty(), "cycle 1 emits nothing while private");

            // Cycle 2: flip the gate true. The note row itself is the UPDATE in
            // this changeset; the children are NOT (they were inserted earlier).
            let out2 = capture_and_gate(
                db,
                &[
                    "UPDATE notes SET shared = 1, _updated_at = '0000000002000-0000-dev1' \
                     WHERE id = 'n1'",
                ],
            );
            let c2 = walk(out2.as_bytes()).expect("walk");
            assert!(has_row(&c2, "notes", "n1"), "promoted note is emitted");
            assert!(
                has_row(&c2, "note_tags", "t1"),
                "re-emit includes the pre-existing tag child"
            );
            assert!(
                has_row(&c2, "note_photos", "p1"),
                "re-emit includes the pre-existing photo child"
            );

            // Apply cycle 2's output to a fresh peer: it must land as a complete
            // consistent subtree.
            let peer = open_memory_db();
            create_synced_schema(peer);
            apply_changeset_lww(peer, &out2).expect("apply to peer");

            assert!(
                row_exists(peer, "SELECT 1 FROM notes WHERE id = 'n1'"),
                "peer has the note"
            );
            assert_eq!(
                query_text(peer, "SELECT title FROM notes WHERE id = 'n1'"),
                "Private"
            );
            assert!(
                row_exists(peer, "SELECT 1 FROM note_tags WHERE id = 't1'"),
                "peer has the tag"
            );
            assert!(
                row_exists(peer, "SELECT 1 FROM note_photos WHERE id = 'p1'"),
                "peer has the photo"
            );

            ffi::sqlite3_close(db);
            ffi::sqlite3_close(peer);
        }
    }

    #[test]
    fn post_promotion_edit_is_single_update_not_reemit() {
        unsafe {
            init_synced_tables();
            let db = open_memory_db();
            create_synced_schema(db);

            // Create + promote in cycle 1 (note shared=1 immediately).
            let _ = capture_and_gate(
                db,
                &[
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                     VALUES ('n1', 'Public', 'b', 1, '0000000001000-0000-dev1', '2026-01-01')",
                    "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                     VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
                ],
            );

            // Cycle 2: an ordinary edit to the already-shared note. Not a flip,
            // so it must emit exactly one UPDATE for the note and nothing else.
            let out = capture_and_gate(
                db,
                &[
                    "UPDATE notes SET title = 'Renamed', _updated_at = '0000000002000-0000-dev1' \
                     WHERE id = 'n1'",
                ],
            );
            let changes = walk(out.as_bytes()).expect("walk");
            assert_eq!(changes.len(), 1, "exactly one change");
            assert_eq!(changes[0].table, "notes");
            assert_eq!(changes[0].op, ChangeOp::Update);
            assert_eq!(changes[0].pk(), Some("n1"));

            ffi::sqlite3_close(db);
        }
    }

    #[test]
    fn multi_hop_fk_inheritance() {
        // grandchild -> child -> root(gated). The gate must flow two hops.
        unsafe {
            let db = open_memory_db();
            exec(db, "PRAGMA foreign_keys = ON");
            exec(
                db,
                "CREATE TABLE albums (id TEXT PRIMARY KEY, shared INTEGER NOT NULL DEFAULT 0, \
                 _updated_at TEXT NOT NULL)",
            );
            exec(
                db,
                "CREATE TABLE photos (id TEXT PRIMARY KEY, album_id TEXT NOT NULL, \
                 _updated_at TEXT NOT NULL, \
                 FOREIGN KEY (album_id) REFERENCES albums (id) ON DELETE CASCADE)",
            );
            exec(
                db,
                "CREATE TABLE comments (id TEXT PRIMARY KEY, photo_id TEXT NOT NULL, \
                 _updated_at TEXT NOT NULL, \
                 FOREIGN KEY (photo_id) REFERENCES photos (id) ON DELETE CASCADE)",
            );

            let tables = vec![
                SyncedTable::new("albums").gated_by("shared"),
                SyncedTable::new("photos"),
                SyncedTable::new("comments"),
            ];

            let attach = |db: *mut ffi::sqlite3| {
                let s = Session::new(db).expect("session");
                s.attach(Some("albums")).unwrap();
                s.attach(Some("photos")).unwrap();
                s.attach(Some("comments")).unwrap();
                s
            };

            // Private album with a 2-level subtree: all cut.
            let s = attach(db);
            exec(db, "INSERT INTO albums (id, shared, _updated_at) VALUES ('a1', 0, '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO photos (id, album_id, _updated_at) VALUES ('p1', 'a1', '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO comments (id, photo_id, _updated_at) VALUES ('c1', 'p1', '0000000001000-0000-dev1')");
            let cs = s.changeset().expect("cs");
            drop(s);
            let gates = Gates::from_tables(db, &tables).expect("gates");
            let out = gate_outbound(db, &cs, &gates).expect("gate");
            let changes = walk(out.as_bytes()).expect("walk");
            assert!(changes.is_empty(), "private 2-level subtree fully cut");

            // Flip the album true: re-emit must reach the grandchild comment.
            let s = attach(db);
            exec(db, "UPDATE albums SET shared = 1, _updated_at = '0000000002000-0000-dev1' WHERE id = 'a1'");
            let cs = s.changeset().expect("cs");
            drop(s);
            let out = gate_outbound(db, &cs, &gates).expect("gate");
            let changes = walk(out.as_bytes()).expect("walk");
            assert!(has_row(&changes, "albums", "a1"));
            assert!(
                has_row(&changes, "photos", "p1"),
                "one-hop child re-emitted"
            );
            assert!(
                has_row(&changes, "comments", "c1"),
                "two-hop grandchild re-emitted via multi-hop FK inheritance"
            );

            ffi::sqlite3_close(db);
        }
    }

    #[test]
    fn delete_gated_false_strips_private_subtrees_in_place() {
        // The snapshot path: `delete_gated_false` removes gated-false roots and
        // their FK-descendants from a live DB, keeping gated-true subtrees and
        // ungated tables. Exercises a two-hop chain so the FK walk is tested
        // past the single hop the snapshot integration test reaches.
        unsafe {
            let db = open_memory_db();
            exec(db, "PRAGMA foreign_keys = ON");
            exec(
                db,
                "CREATE TABLE albums (id TEXT PRIMARY KEY, shared INTEGER NOT NULL DEFAULT 0, \
                 _updated_at TEXT NOT NULL)",
            );
            exec(
                db,
                "CREATE TABLE photos (id TEXT PRIMARY KEY, album_id TEXT NOT NULL, \
                 _updated_at TEXT NOT NULL, \
                 FOREIGN KEY (album_id) REFERENCES albums (id) ON DELETE CASCADE)",
            );
            exec(
                db,
                "CREATE TABLE comments (id TEXT PRIMARY KEY, photo_id TEXT NOT NULL, \
                 _updated_at TEXT NOT NULL, \
                 FOREIGN KEY (photo_id) REFERENCES photos (id) ON DELETE CASCADE)",
            );
            exec(
                db,
                "CREATE TABLE settings (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL)",
            );

            let tables = vec![
                SyncedTable::new("albums").gated_by("shared"),
                SyncedTable::new("photos"),
                SyncedTable::new("comments"),
                SyncedTable::new("settings"),
            ];

            // A private album with a 2-level subtree, a shared album with its
            // own subtree, and an ungated settings row.
            exec(db, "INSERT INTO albums (id, shared, _updated_at) VALUES ('priv', 0, '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO photos (id, album_id, _updated_at) VALUES ('priv_p', 'priv', '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO comments (id, photo_id, _updated_at) VALUES ('priv_c', 'priv_p', '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO albums (id, shared, _updated_at) VALUES ('pub', 1, '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO photos (id, album_id, _updated_at) VALUES ('pub_p', 'pub', '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO comments (id, photo_id, _updated_at) VALUES ('pub_c', 'pub_p', '0000000001000-0000-dev1')");
            exec(
                db,
                "INSERT INTO settings (id, _updated_at) VALUES ('s1', '0000000001000-0000-dev1')",
            );

            let gates = Gates::from_tables(db, &tables).expect("gates");
            gates.delete_gated_false(db).expect("delete gated-false");

            // Private subtree gone at every level.
            assert!(!row_exists(db, "SELECT 1 FROM albums WHERE id = 'priv'"));
            assert!(!row_exists(db, "SELECT 1 FROM photos WHERE id = 'priv_p'"));
            assert!(!row_exists(
                db,
                "SELECT 1 FROM comments WHERE id = 'priv_c'"
            ));

            // Shared subtree intact at every level.
            assert!(row_exists(db, "SELECT 1 FROM albums WHERE id = 'pub'"));
            assert!(row_exists(db, "SELECT 1 FROM photos WHERE id = 'pub_p'"));
            assert!(row_exists(db, "SELECT 1 FROM comments WHERE id = 'pub_c'"));

            // Ungated table untouched.
            assert!(row_exists(db, "SELECT 1 FROM settings WHERE id = 's1'"));

            ffi::sqlite3_close(db);
        }
    }
}
