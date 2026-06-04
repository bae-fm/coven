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

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
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
    /// # Safety
    /// `db` must be a valid, open sqlite3 connection holding the synced schema.
    pub unsafe fn delete_gated_false(&self, db: *mut ffi::sqlite3) -> Result<(), GateError> {
        // The final row set is order-independent (the prune is monotonic, above),
        // but the DELETEs run under `foreign_keys=ON`: a parent FK without
        // `ON DELETE CASCADE` rejects deleting a parent while a child still
        // references it. So delete child-first — the reverse of the FK-topological
        // apply order — which is safe regardless of whether the schema cascades.
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
/// fixpoint walk in [`connected_kept_component`] follows these down-edges
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

    // Roots that flip false→true this cycle need their whole current connected
    // component re-emitted (peers never had it while private) — descendants AND
    // always-shared ancestors. Keyed by `(root table, root id)`.
    let mut flipped_roots: HashSet<(String, String)> = HashSet::new();

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
                        flipped_roots.insert((row.table.clone(), pk.to_string()));
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
    group: &Changegroup,
) -> Result<(), GateError> {
    // Compute the whole connected kept component of every flipped root: its
    // ancestors (album, artist), the kept children of those ancestors
    // (album_artists, sibling releases), and — transitively — the ancestors of
    // those kept children (a featured artist credited via a join row) and so on.
    // These are re-emitted by explicit `(table, id)` membership; the flipped
    // root's own descendants are re-emitted by the scoping test below. Both feed
    // the same diff.
    let reemit_ids = connected_kept_component(db, gates, flipped_roots)?;

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
        let in_descendants =
            gated_root_id(db, gates, &row)?.is_some_and(|key| flipped_roots.contains(&key));
        let in_kept_component = row
            .pk()
            .is_some_and(|pk| reemit_ids.contains(&(row.table.clone(), pk.to_string())));
        if in_descendants || in_kept_component {
            group.add_change(iter)?;
        }
    }

    let rc = ffi::sqlite3changeset_finalize(iter);
    if rc != ffi::SQLITE_OK as c_int {
        return Err(GateError::Ffi("sqlite3changeset_finalize", rc));
    }
    Ok(())
}

/// The whole connected component of currently-kept gated rows reachable from the
/// flipped roots, walking the live FK graph in BOTH directions: *up* to gated
/// ancestors (release → album → artist) and *down* to currently-kept children
/// (album → its kept releases; artist → its kept join rows). Crucially, a kept
/// child pulled in *down* has its own ancestors walked *up* in turn — a join row
/// (album_artists) reached as a kept child of an album drags in the second artist
/// it credits (a featured artist who does not own the album), which the snapshot
/// `keep_clause` also keeps. The result is the transitive closure, so a fresh
/// peer materializes the same connected graph the snapshot prune would.
///
/// This reconstructs in row-walk form the same relation `keep_clause` expresses
/// recursively; computing it as a fixed up-then-one-level-down pass under-emits
/// any kept row reachable only sideways. A worklist to a fixpoint, cycle-guarded
/// by the visited set, is the only correct shape.
///
/// The seed is the flipped roots themselves. Re-walking their descendants here is
/// harmless: the changegroup dedups by primary key and the apply conflict handler
/// resolves a duplicate INSERT by LWW, so over-emitting never corrupts a peer;
/// only under-emitting does.
unsafe fn connected_kept_component(
    db: *mut ffi::sqlite3,
    gates: &Gates,
    flipped_roots: &HashSet<(String, String)>,
) -> Result<HashSet<(String, String)>, GateError> {
    // Down-edges: for each gated table, the gated tables that hold an FK
    // referencing it, paired with the referrer's FK column name. Built once from
    // the shared FK-edge scan so the per-row down-expansion is a map lookup, not a
    // schema scan, and the same edges drive `fk_topological_order`.
    let down_edges = gated_fk_child_edges(db, &gates.tables)?;

    let mut out: HashSet<(String, String)> = HashSet::new();
    let mut work: Vec<(String, String)> = flipped_roots.iter().cloned().collect();
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
        // Down: every currently-kept gated child referencing this row.
        if let Some(children) = down_edges.get(table.as_str()) {
            for (child_table, fk) in children {
                for child_id in rows_referencing(db, child_table, fk, &id)? {
                    if gates.row_kept(db, child_table, &child_id)? {
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

    let tables = gates.gated_tables_parent_first(db)?;
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
        // `connected_kept_component`, not by the flipped-root descendant test.
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
        // ungated tables. Exercises a two-hop FK chain (root → child →
        // grandchild) so the recursive keep-clause walk is tested across more
        // than one hop.
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

    // ---- upward gate (gated_by_descendants) ----------------------------------
    //
    // Synthetic schema exercising an always-shared ancestor (`albums`) kept alive
    // by its gated subtree (`releases` gated by `managed`), a two-level ancestor
    // (`artists`, kept by albums and album_artists), and a multi-parent join row
    // (`album_artists` → albums AND artists) whose downward gate-parent must be
    // inferred as the more-specific ancestor (albums), not artists. The gate
    // flows up: an album/artist whose gated subtree is empty is cut.

    /// Declared synced set for the upward-gate tests. `albums` and `artists` are
    /// ancestors (no child list — children are inferred); `album_artists` and
    /// `tracks` are plain and inherit their gate downward.
    fn album_tables() -> Vec<SyncedTable> {
        vec![
            SyncedTable::new("releases").gated_by("managed"),
            SyncedTable::new("albums").gated_by_descendants(),
            SyncedTable::new("artists").gated_by_descendants(),
            SyncedTable::new("album_artists"),
            SyncedTable::new("tracks"),
        ]
    }

    /// Build the album/artist schema on `db`. `album_artists` declares its
    /// `album_id` FK *before* its `artist_id` FK on purpose: SQLite numbers FKs
    /// in reverse, so `PRAGMA foreign_key_list` lists `artists` first — the case
    /// that breaks a naive "first FK wins" parent selection.
    unsafe fn create_album_schema(db: *mut ffi::sqlite3) {
        exec(db, "PRAGMA foreign_keys = ON");
        exec(
            db,
            "CREATE TABLE artists (id TEXT PRIMARY KEY, name TEXT, \
             _updated_at TEXT NOT NULL)",
        );
        exec(
            db,
            "CREATE TABLE albums (id TEXT PRIMARY KEY, artist_id TEXT, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (artist_id) REFERENCES artists (id))",
        );
        exec(
            db,
            "CREATE TABLE album_artists (id TEXT PRIMARY KEY, album_id TEXT NOT NULL, \
             artist_id TEXT NOT NULL, _updated_at TEXT NOT NULL, \
             FOREIGN KEY (album_id) REFERENCES albums (id) ON DELETE CASCADE, \
             FOREIGN KEY (artist_id) REFERENCES artists (id) ON DELETE CASCADE)",
        );
        exec(
            db,
            "CREATE TABLE releases (id TEXT PRIMARY KEY, album_id TEXT NOT NULL, \
             managed INTEGER NOT NULL DEFAULT 0, _updated_at TEXT NOT NULL, \
             FOREIGN KEY (album_id) REFERENCES albums (id) ON DELETE CASCADE)",
        );
        exec(
            db,
            "CREATE TABLE tracks (id TEXT PRIMARY KEY, release_id TEXT NOT NULL, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (release_id) REFERENCES releases (id) ON DELETE CASCADE)",
        );
    }

    /// Attach a session over every album-schema table.
    unsafe fn album_session(db: *mut ffi::sqlite3) -> Session {
        let s = Session::new(db).expect("session");
        for t in album_tables() {
            s.attach(Some(t.name())).expect("attach");
        }
        s
    }

    /// Apply a changeset with the production LWW path, scoped to the album table
    /// set rather than the process-global synced tables (which the other tests fix
    /// to the notes schema). Calls the real `apply_changeset_lww_for`, so the
    /// conflict handler is exercised, not re-implemented.
    unsafe fn apply_album(db: *mut ffi::sqlite3, cs: &Changeset) {
        crate::sync::apply::apply_changeset_lww_for(db, cs, &album_tables())
            .expect("apply album changeset");
    }

    /// The inferred keep-children of `tbl` as `(child table, fk column name)`
    /// pairs, sorted — for asserting the inferred gate map.
    unsafe fn inferred_children(
        db: *mut ffi::sqlite3,
        gates: &Gates,
        tbl: &str,
    ) -> Vec<(String, String)> {
        match gates.tables.get(tbl) {
            Some(TableGate::Parent { children }) => {
                let mut out: Vec<(String, String)> = children
                    .iter()
                    .map(|(c, idx)| (c.clone(), nth_column_name(db, c, *idx).expect("fk col")))
                    .collect();
                out.sort();
                out
            }
            Some(_) => panic!("{tbl} is in the gate map but not modeled as a Parent"),
            None => panic!("{tbl} is absent from the gate map; expected a Parent"),
        }
    }

    /// The downward gate-parent `from_tables` chose for `tbl`, as `(parent,
    /// child's FK column name)`, read out of the resulting gate map. Panics if
    /// `tbl` is not modeled as an inheriting `Child`.
    unsafe fn downward_parent(db: *mut ffi::sqlite3, gates: &Gates, tbl: &str) -> (String, String) {
        match gates.tables.get(tbl) {
            Some(TableGate::Child { fk_col, parent }) => (
                parent.clone(),
                nth_column_name(db, tbl, *fk_col).expect("fk col"),
            ),
            other => panic!(
                "{tbl} must be an inheriting Child, got present={}",
                other.is_some()
            ),
        }
    }

    #[test]
    fn inference_resolves_children_and_join_parent() {
        // The crux. From the schema alone (no declared child lists), assert the
        // inferred gate map `from_tables` produces: albums.children = [releases];
        // artists.children = [albums, album_artists] (album_artists IS a
        // keep-child of artists because its downward parent is albums, not
        // artists); and album_artists's downward gate-parent resolves to albums
        // (not artists) with the album_id FK, regardless of FK declaration order.
        unsafe {
            let db = open_memory_db();
            create_album_schema(db);
            let gates = Gates::from_tables(db, &album_tables()).expect("gates");

            assert_eq!(
                inferred_children(db, &gates, "albums"),
                vec![("releases".to_string(), "album_id".to_string())],
                "albums is kept only by releases (the album_artists back-edge is excluded)"
            );
            assert_eq!(
                inferred_children(db, &gates, "artists"),
                vec![
                    ("album_artists".to_string(), "artist_id".to_string()),
                    ("albums".to_string(), "artist_id".to_string()),
                ],
                "artists is kept by albums OR album_artists"
            );

            // The join row inherits downward from the more-specific ancestor
            // (albums) via its album_id FK, independent of which FK PRAGMA lists
            // first — observed through the gate map `from_tables` built, not a
            // private selection call.
            assert_eq!(
                downward_parent(db, &gates, "album_artists"),
                ("albums".to_string(), "album_id".to_string()),
            );

            ffi::sqlite3_close(db);
        }
    }

    #[test]
    fn downward_parent_is_most_specific_not_lexicographic() {
        // Isolate the "most-specific ancestor" rule (`ancestor_depth`) from the
        // lexicographic fallback by making them DISAGREE: a join row references two
        // ancestors where the more-specific (deeper) one sorts lexicographically
        // LATER. `zinner` is an FK-descendant of `aouter` (depth 1 vs 0), so it is
        // the most-specific parent — yet "aouter" < "zinner", so a lexicographic
        // tie-break alone would pick `aouter`. Only the depth signal yields the
        // right answer, driven through `from_tables` and read out of the gate map
        // (the album-schema test above can't catch this: there `albums` wins both
        // rules). Each ancestor is given its own gated descendant (`zgated` under
        // `zinner`; `zinner`/`joiner` under `aouter`) so `from_tables` accepts the
        // schema rather than rejecting an empty-keep ancestor.
        unsafe {
            let db = open_memory_db();
            exec(db, "PRAGMA foreign_keys = ON");
            exec(
                db,
                "CREATE TABLE aouter (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL)",
            );
            exec(
                db,
                "CREATE TABLE zinner (id TEXT PRIMARY KEY, aouter_id TEXT, \
                 _updated_at TEXT NOT NULL, \
                 FOREIGN KEY (aouter_id) REFERENCES aouter (id))",
            );
            // A gated root under `zinner`, so `zinner` has a kept descendant.
            exec(
                db,
                "CREATE TABLE zgated (id TEXT PRIMARY KEY, zinner_id TEXT NOT NULL, \
                 shared INTEGER NOT NULL DEFAULT 0, _updated_at TEXT NOT NULL, \
                 FOREIGN KEY (zinner_id) REFERENCES zinner (id))",
            );
            // The join row declares its FK to `aouter` first, so PRAGMA lists the
            // `zinner` FK first — independent of the chosen ranking.
            exec(
                db,
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
            let gates = Gates::from_tables(db, &tables).expect("gates");

            assert_eq!(
                downward_parent(db, &gates, "joiner"),
                ("zinner".to_string(), "zinner_id".to_string()),
                "the most-specific (deeper) ancestor wins even though it sorts \
                 lexicographically later than `aouter`"
            );

            ffi::sqlite3_close(db);
        }
    }

    #[test]
    fn fk_topological_order_is_parent_first() {
        // The re-emit apply order must place every table after every gated table
        // it has an FK to: artists, albums, album_artists, releases, tracks.
        unsafe {
            let db = open_memory_db();
            create_album_schema(db);
            let gates = Gates::from_tables(db, &album_tables()).expect("gates");
            let order = gates.gated_tables_parent_first(db).expect("topo order");
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

            ffi::sqlite3_close(db);
        }
    }

    #[test]
    fn delete_gated_false_prunes_empty_ancestors() {
        // An album whose only release is unmanaged is deleted (orphan ancestor
        // pruned); an album with a managed release survives with only the managed
        // release; an artist kept only via a deleted album is deleted; an artist
        // kept via a surviving album survives.
        unsafe {
            let db = open_memory_db();
            create_album_schema(db);

            // Artist A1: album AL_EMPTY with one unmanaged release -> all gone.
            // Artist A2: album AL_MIXED (one managed + one unmanaged release) and
            //            an album_artists row -> survives with only the managed.
            exec(
                db,
                "INSERT INTO artists (id, _updated_at) VALUES ('A1', '0000000001000-0000-dev1')",
            );
            exec(
                db,
                "INSERT INTO artists (id, _updated_at) VALUES ('A2', '0000000001000-0000-dev1')",
            );
            exec(db, "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL_EMPTY', 'A1', '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL_MIXED', 'A2', '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA_EMPTY', 'AL_EMPTY', 'A1', '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA_MIXED', 'AL_MIXED', 'A2', '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R_UNMAN', 'AL_EMPTY', 0, '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R_MAN', 'AL_MIXED', 1, '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R_UNMAN2', 'AL_MIXED', 0, '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T_UNMAN', 'R_UNMAN', '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T_MAN', 'R_MAN', '0000000001000-0000-dev1')");

            let gates = Gates::from_tables(db, &album_tables()).expect("gates");
            gates.delete_gated_false(db).expect("delete gated-false");

            // Empty album (only an unmanaged release) and its whole subtree gone.
            assert!(
                !row_exists(db, "SELECT 1 FROM albums WHERE id = 'AL_EMPTY'"),
                "empty album pruned"
            );
            assert!(
                !row_exists(db, "SELECT 1 FROM releases WHERE id = 'R_UNMAN'"),
                "unmanaged release gone"
            );
            assert!(
                !row_exists(db, "SELECT 1 FROM tracks WHERE id = 'T_UNMAN'"),
                "track of unmanaged release gone"
            );
            assert!(
                !row_exists(db, "SELECT 1 FROM album_artists WHERE id = 'AA_EMPTY'"),
                "album_artists of pruned album gone"
            );
            assert!(
                !row_exists(db, "SELECT 1 FROM artists WHERE id = 'A1'"),
                "artist with no kept album pruned"
            );

            // Mixed album survives with ONLY the managed release.
            assert!(
                row_exists(db, "SELECT 1 FROM albums WHERE id = 'AL_MIXED'"),
                "mixed album survives"
            );
            assert!(
                row_exists(db, "SELECT 1 FROM releases WHERE id = 'R_MAN'"),
                "managed release survives"
            );
            assert!(
                row_exists(db, "SELECT 1 FROM tracks WHERE id = 'T_MAN'"),
                "track of managed release survives"
            );
            assert!(
                !row_exists(db, "SELECT 1 FROM releases WHERE id = 'R_UNMAN2'"),
                "the unmanaged sibling release is still cut"
            );
            assert!(
                row_exists(db, "SELECT 1 FROM album_artists WHERE id = 'AA_MIXED'"),
                "album_artists of surviving album kept"
            );
            assert!(
                row_exists(db, "SELECT 1 FROM artists WHERE id = 'A2'"),
                "artist kept via a surviving album"
            );

            ffi::sqlite3_close(db);
        }
    }

    #[test]
    fn changeset_cut_drops_orphan_ancestor() {
        // A changeset inserting an album + an unmanaged release + its tracks
        // emits NONE of them — the album is cut because it has no kept child.
        unsafe {
            let db = open_memory_db();
            create_album_schema(db);

            let s = album_session(db);
            exec(
                db,
                "INSERT INTO albums (id, _updated_at) VALUES ('AL', '0000000001000-0000-dev1')",
            );
            exec(db, "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R', 'AL', 0, '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T', 'R', '0000000001000-0000-dev1')");
            let cs = s.changeset().expect("cs");
            drop(s);

            let gates = Gates::from_tables(db, &album_tables()).expect("gates");
            let out = gate_outbound(db, &cs, &gates).expect("gate");
            let changes = walk(out.as_bytes()).expect("walk");

            assert!(
                !has_row(&changes, "albums", "AL"),
                "orphan album cut (no kept release)"
            );
            assert!(!has_row(&changes, "releases", "R"), "unmanaged release cut");
            assert!(
                !has_row(&changes, "tracks", "T"),
                "track of unmanaged release cut"
            );

            ffi::sqlite3_close(db);
        }
    }

    #[test]
    fn flip_reemits_whole_connected_component_to_peer() {
        // An album with an unmanaged release lives locally (never synced).
        // Flipping the release managed false->true must re-emit the WHOLE
        // connected component — album, release, tracks, album_artists, artist —
        // so a fresh peer materializes the complete graph.
        //
        // This tests COMPLETENESS (every component row reaches the peer), not the
        // FK-topological apply order: `sqlite3changeset_apply` defers foreign-key
        // enforcement to the end of its internal savepoint, so the changeset's
        // table order does not gate the apply (a child row may precede its parent
        // and still land). The apply order is guarded structurally by
        // `fk_topological_order_is_parent_first`, not by this peer-apply check.
        unsafe {
            let db = open_memory_db();
            create_album_schema(db);

            // Cycle 1: build the private graph. Nothing should escape.
            let s = album_session(db);
            exec(db, "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist', '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA', 'AL', 'AR', '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R', 'AL', 0, '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T', 'R', '0000000001000-0000-dev1')");
            let cs1 = s.changeset().expect("cs");
            drop(s);
            let gates = Gates::from_tables(db, &album_tables()).expect("gates");
            let out1 = gate_outbound(db, &cs1, &gates).expect("gate");
            assert!(
                walk(out1.as_bytes()).expect("walk").is_empty(),
                "private graph emits nothing"
            );

            // Cycle 2: flip the release managed. Re-emit the whole component.
            let s = album_session(db);
            exec(db, "UPDATE releases SET managed = 1, _updated_at = '0000000002000-0000-dev1' WHERE id = 'R'");
            let cs2 = s.changeset().expect("cs");
            drop(s);
            let out2 = gate_outbound(db, &cs2, &gates).expect("gate");
            let changes = walk(out2.as_bytes()).expect("walk");
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

            // Apply to a fresh peer (foreign_keys = ON): the whole graph lands.
            // (Completeness only — the apply defers FK checks to its savepoint
            // end, so this does not exercise the re-emit table order.)
            let peer = open_memory_db();
            create_album_schema(peer);
            apply_album(peer, &out2);
            assert!(
                row_exists(peer, "SELECT 1 FROM artists WHERE id = 'AR'"),
                "peer has artist"
            );
            assert!(
                row_exists(peer, "SELECT 1 FROM albums WHERE id = 'AL'"),
                "peer has album"
            );
            assert!(
                row_exists(peer, "SELECT 1 FROM album_artists WHERE id = 'AA'"),
                "peer has album_artists"
            );
            assert!(
                row_exists(peer, "SELECT 1 FROM releases WHERE id = 'R'"),
                "peer has release"
            );
            assert!(
                row_exists(peer, "SELECT 1 FROM tracks WHERE id = 'T'"),
                "peer has track"
            );

            ffi::sqlite3_close(db);
            ffi::sqlite3_close(peer);
        }
    }

    #[test]
    fn flip_reemits_sideways_featured_artist() {
        // The flip re-emit must close over the FULL connected kept component, not
        // just the flipped row's own lineage. A featured artist (AR2) credited via
        // album_artists who does NOT own the album sits *sideways* off the
        // release→album→owner walk: the upward walk reaches the owner (AR1), never
        // AR2. Only the transitive closure — walking the kept join row's own
        // ancestors up — pulls AR2 and AA in, matching the snapshot prune
        // (`keep_clause(artists)` keeps AR2 via the album_artists disjunct).
        unsafe {
            let db = open_memory_db();
            create_album_schema(db);
            let gates = Gates::from_tables(db, &album_tables()).expect("gates");

            // AR1 owns AL1; AR2 is featured via AA; release R1 unmanaged.
            let s = album_session(db);
            exec(db, "INSERT INTO artists (id, name, _updated_at) VALUES ('AR1', 'Owner', '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO artists (id, name, _updated_at) VALUES ('AR2', 'Featured', '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL1', 'AR1', '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA', 'AL1', 'AR2', '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL1', 0, '0000000001000-0000-dev1')");
            let cs1 = s.changeset().expect("cs");
            drop(s);
            assert!(
                walk(gate_outbound(db, &cs1, &gates).expect("gate").as_bytes())
                    .expect("walk")
                    .is_empty(),
                "private graph emits nothing"
            );

            // Flip R1 managed.
            let s = album_session(db);
            exec(db, "UPDATE releases SET managed = 1, _updated_at = '0000000002000-0000-dev1' WHERE id = 'R1'");
            let cs2 = s.changeset().expect("cs");
            drop(s);
            let out2 = gate_outbound(db, &cs2, &gates).expect("gate");
            let changes = walk(out2.as_bytes()).expect("walk");

            assert!(
                has_row(&changes, "album_artists", "AA"),
                "featured join row re-emitted"
            );
            assert!(
                has_row(&changes, "artists", "AR2"),
                "featured artist re-emitted"
            );

            let peer = open_memory_db();
            create_album_schema(peer);
            apply_album(peer, &out2);
            assert!(
                row_exists(peer, "SELECT 1 FROM album_artists WHERE id = 'AA'"),
                "peer has join row"
            );
            assert!(
                row_exists(peer, "SELECT 1 FROM artists WHERE id = 'AR2'"),
                "peer has featured artist"
            );

            ffi::sqlite3_close(db);
            ffi::sqlite3_close(peer);
        }
    }

    #[test]
    fn second_flip_is_idempotent_under_lww() {
        // An album already visible on a peer (one managed release synced)
        // flips a SECOND release managed. The re-emit re-sends the album INSERT
        // the peer already has; LWW resolves the duplicate-PK INSERT without
        // error and the peer stays consistent.
        unsafe {
            let db = open_memory_db();
            create_album_schema(db);
            let gates = Gates::from_tables(db, &album_tables()).expect("gates");

            // Cycle 1: an album with one managed release, synced to the peer.
            let s = album_session(db);
            exec(db, "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist', '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-dev1')");
            exec(db, "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL', 1, '0000000001000-0000-dev1')");
            let cs1 = s.changeset().expect("cs");
            drop(s);
            let out1 = gate_outbound(db, &cs1, &gates).expect("gate");

            let peer = open_memory_db();
            create_album_schema(peer);
            apply_album(peer, &out1);
            assert!(
                row_exists(peer, "SELECT 1 FROM albums WHERE id = 'AL'"),
                "peer has the album after cycle 1"
            );

            // Cycle 2a: insert a second release unmanaged (stays private, cut).
            let s = album_session(db);
            exec(db, "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R2', 'AL', 0, '0000000002000-0000-dev1')");
            let cs2a = s.changeset().expect("cs");
            drop(s);
            let _ = gate_outbound(db, &cs2a, &gates).expect("gate");

            // Cycle 2b: flip the second release managed. Re-emit re-sends the
            // album the peer already has (over-emit), plus the new release.
            let s = album_session(db);
            exec(db, "UPDATE releases SET managed = 1, _updated_at = '0000000003000-0000-dev1' WHERE id = 'R2'");
            let cs2b = s.changeset().expect("cs");
            drop(s);
            let out2 = gate_outbound(db, &cs2b, &gates).expect("gate");
            let changes = walk(out2.as_bytes()).expect("walk");
            assert!(
                has_row(&changes, "albums", "AL"),
                "album re-emitted on the second flip"
            );
            assert!(
                has_row(&changes, "releases", "R2"),
                "second release emitted"
            );

            // Applying the duplicate album INSERT must not error; peer consistent.
            apply_album(peer, &out2);
            assert!(
                row_exists(peer, "SELECT 1 FROM albums WHERE id = 'AL'"),
                "album still present"
            );
            assert!(
                row_exists(peer, "SELECT 1 FROM releases WHERE id = 'R1'"),
                "first release still present"
            );
            assert!(
                row_exists(peer, "SELECT 1 FROM releases WHERE id = 'R2'"),
                "second release now present"
            );
            assert_eq!(
                query_int(peer, "SELECT COUNT(*) FROM albums WHERE id = 'AL'"),
                1,
                "the duplicate INSERT did not create a second album row"
            );

            ffi::sqlite3_close(db);
            ffi::sqlite3_close(peer);
        }
    }
}
