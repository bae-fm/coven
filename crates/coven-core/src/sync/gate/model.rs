//! Gate-model construction: classify each synced table against the gate (root,
//! remote root, inheriting child, or kept-by-descendants ancestor), infer the
//! keep-children from the live FK graph, and answer share/keep/subtree queries
//! against the live database.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

use rusqlite::Connection;
use tracing::warn;

use super::outbound::resolve_root;
use super::{execute_batch, query_mapped_rows, query_row_optional, row_value_to_string, GateError};
use crate::sync::session::{quote_ident, table_columns, SyncedTable};

/// How a synced table relates to the gate.
pub(super) enum TableGate {
    /// A gated root: the boolean gate lives at this column.
    Root { gate_col: GateColumn },
    /// A root whose rows sync unconditionally and whose blob subtree is always
    /// Remote.
    RemoteRoot,
    /// A child whose gate is inherited from `parent` via the FK column at
    /// `fk_col` (in *this* table), holding the parent's id.
    Child { fk_col: GateColumn, parent: String },
    /// An always-shared ancestor kept alive by its gated subtree: shared iff
    /// some inferred child still has a kept row referencing it. Each entry is a
    /// `(child table, FK column in that child)` pair, where the FK column holds
    /// this table's id. The children are inferred from the live FK graph, never
    /// declared.
    Parent { children: Vec<(String, GateColumn)> },
}

/// A gate column as both a changeset position and a SQL column name.
#[derive(Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct GateColumn {
    pub(super) index: usize,
    pub(super) name: String,
}

/// The gate model for a database handle, computed from the live schema at open.
///
/// Maps each gated-or-inheriting synced table to how it resolves its gate. A
/// synced table absent from this map is ungated and unconditionally shared.
pub struct Gates {
    pub(super) tables: HashMap<String, TableGate>,
}

#[cfg(test)]
thread_local! {
    static FROM_TABLES_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_from_tables_call_count() {
    FROM_TABLES_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
pub(crate) fn from_tables_call_count() -> usize {
    FROM_TABLES_CALLS.with(std::cell::Cell::get)
}

impl Gates {
    /// Build the gate model from the declared [`SyncedTable`]s and the live
    /// schema (`PRAGMA table_info` for gate-column indices, `PRAGMA
    /// foreign_key_list` for FK edges).
    ///
    pub fn from_tables(conn: &Connection, tables: &[SyncedTable]) -> Result<Self, GateError> {
        #[cfg(test)]
        FROM_TABLES_CALLS.with(|calls| calls.set(calls.get() + 1));

        Self::from_tables_conn(conn, tables)
    }

    fn from_tables_conn(conn: &Connection, tables: &[SyncedTable]) -> Result<Self, GateError> {
        let ancestors: HashSet<&str> = tables
            .iter()
            .filter(|t| t.is_gated_by_descendants())
            .map(|t| t.name())
            .collect();
        // Asset tables ride their FK subject's gate as inherited children but are
        // never keep-reasons: excluded from every ancestor's keep-children below.
        let assets: HashSet<&str> = tables
            .iter()
            .filter(|t| t.is_asset())
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

            let cols = table_columns(conn, t.name())
                .map_err(|e| GateError::Sql(format!("read columns of {}", t.name()), e))?;

            if t.is_remote_root() {
                gate_map.insert(t.name().to_string(), TableGate::RemoteRoot);
                continue;
            }

            if let Some(gate) = t.gate_column() {
                let gate_col = gate_column(&cols, t.name(), gate)?;
                gate_map.insert(t.name().to_string(), TableGate::Root { gate_col });
                continue;
            }

            // A plain table inherits the gate downward from its selected FK
            // parent. Inheritance flows ONLY through declared FKs, toward synced
            // parents, and (for a multi-FK join row) toward the gated side, never
            // up an ancestor back-edge.
            if let Some((fk_name, parent)) = select_parent_fk(conn, t.name(), tables, &ancestors)? {
                let fk_col = fk_column(&cols, t.name(), &fk_name)?;
                gate_map.insert(t.name().to_string(), TableGate::Child { fk_col, parent });
            }
            // else: ungated, unconditionally shared — not in the map.
        }

        // Children are filled once all downward parents are known. A keep-child
        // of ancestor P is any synced table with an FK referencing P, MINUS two
        // kinds: an *asset* (a host-declared decoration that rides P's gate but
        // never keeps it alive — e.g. an artist image keeping its artist), and a
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
                // Skip an asset: it inherits the gate downward as a child but is
                // never a keep-reason. Excluding it also keeps the asset-rides-gate
                // vs. ancestor-kept-by-children relation acyclic.
                if assets.contains(t.name()) {
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
                // is a keep-child: record the FK column in that child.
                if let Some(fk_name) = fk_col_referencing(conn, t.name(), ancestor)? {
                    let cols = table_columns(conn, t.name())
                        .map_err(|e| GateError::Sql(format!("read columns of {}", t.name()), e))?;
                    let fk_col = fk_column(&cols, t.name(), &fk_name)?;
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
            .filter(|name| reaches_gate_terminus(&gate_map, name))
            .cloned()
            .collect();
        gate_map.retain(|name, tg| match tg {
            TableGate::Root { .. } | TableGate::RemoteRoot | TableGate::Parent { .. } => true,
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
    pub(super) fn gated_tables_parent_first(
        &self,
        conn: &Connection,
    ) -> Result<Vec<String>, GateError> {
        fk_topological_order(conn, &self.tables)
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
    pub fn delete_gated_false(&self, conn: &Connection) -> Result<(), GateError> {
        self.delete_gated_false_conn(conn)
    }

    fn delete_gated_false_conn(&self, conn: &Connection) -> Result<(), GateError> {
        // The final row set is order-independent (the prune is monotonic, above).
        // The only caller is the snapshot scope, whose copy connection opens with
        // `foreign_keys` OFF, so no FK would reject deleting a parent before its
        // child here. We still delete child-first — the reverse of the
        // FK-topological apply order — so this stays correct under
        // `foreign_keys=ON` too: a parent FK without `ON DELETE CASCADE` would
        // otherwise reject deleting a parent a child still references. Child-first
        // is order-safe regardless of the copy's FK setting.
        let mut order = self.gated_tables_parent_first(conn)?;
        order.reverse();
        for tbl in order {
            let keep = self.keep_clause(&tbl)?;
            let sql = format!("DELETE FROM {} WHERE NOT ({keep})", quote_ident(&tbl));
            execute_batch(conn, &sql)?;
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
    fn keep_clause(&self, tbl: &str) -> Result<String, GateError> {
        self.keep_clause_guarded(tbl, &mut HashSet::new())
    }

    fn keep_clause_guarded(
        &self,
        tbl: &str,
        visiting: &mut HashSet<String>,
    ) -> Result<String, GateError> {
        if !visiting.insert(tbl.to_string()) {
            // Already on the current recursion path: refuse to loop. A row kept
            // only via a cycle is treated as not kept.
            return Ok("FALSE".to_string());
        }
        let clause = match self.tables.get(tbl) {
            Some(TableGate::Root { gate_col }) => truthy_sql(&format!(
                "{}.{}",
                quote_ident(tbl),
                quote_ident(&gate_col.name)
            )),
            Some(TableGate::RemoteRoot) => "TRUE".to_string(),
            Some(TableGate::Child { fk_col, parent }) => {
                let inner = self.keep_clause_guarded(parent, visiting)?;
                // Join up the FK to the parent's keep: parent.id = child.fk.
                fk_exists_clause(parent, "id", tbl, &fk_col.name, &inner)
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
                    let inner = self.keep_clause_guarded(child, visiting)?;
                    // Join down to each child's keep: child.fk = parent.id.
                    disjuncts.push(fk_exists_clause(child, &fk_col.name, tbl, "id", &inner));
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
    pub(super) fn row_kept(
        &self,
        conn: &Connection,
        tbl: &str,
        id: &str,
    ) -> Result<bool, GateError> {
        let keep = self.keep_clause(tbl)?;
        let sql = format!(
            "SELECT 1 FROM {t} WHERE {t}.{id_col} = ? AND ({keep})",
            t = quote_ident(tbl),
            id_col = quote_ident("id"),
        );
        let present = query_row_optional(conn, &sql, [id], |_| Ok(()))?.is_some();
        Ok(present)
    }

    /// The locality terminus the live row `(table, id)` resolves to by walking up
    /// its declared-FK chain — the gated root, remote root, or inheriting ancestor at
    /// the top — as `(terminus_table, terminus_id)`, regardless of whether a gated
    /// terminus currently keeps it. `None` if the row is ungated/unrooted, or a row
    /// along the chain is absent from the live db.
    ///
    /// The blob-transition drain uses this to map a just-uploaded blob's row to the
    /// gated root a make_remote tracks: a `release_files` row resolves up to its
    /// `releases` root, whose `blob_make_remote_intents` row the completion check reads.
    pub(crate) fn resolve_root_of(
        &self,
        conn: &Connection,
        table: &str,
        id: &str,
    ) -> Result<Option<(String, String)>, GateError> {
        Ok(resolve_root(conn, self, table, id)?.map(|r| (r.terminus_table, r.terminus_id)))
    }

    /// Whether the blob-bearing row `(table, id)` resolves to Remote locality:
    /// `Some(true)` is Remote (shared, bytes in the cloud), `Some(false)` is Local
    /// (bytes on-device). The same FK up-walk as
    /// [`resolve_root_of`](Self::resolve_root_of), returning the locality truth that
    /// walk already reads (a gated root's own column, a remote root's declared Remote
    /// state, or a `gated_by_descendants` ancestor's keep), so the read path dispatches
    /// on this rather than probing every store. `None` when the chain reaches no
    /// locality terminus (the row is ungated/unrooted) or a row along it is missing —
    /// an unresolvable locality the read path fails loud on rather than guessing a
    /// source.
    pub(crate) fn root_kept_of(
        &self,
        conn: &Connection,
        table: &str,
        id: &str,
    ) -> Result<Option<bool>, GateError> {
        Ok(resolve_root(conn, self, table, id)?.map(|r| r.kept))
    }

    /// Every row in the gated subtree rooted at `(root_table, root_id)`: the root
    /// itself plus the transitive closure of its gated FK-*descendants*, as
    /// `(table, primary key)` pairs. A pure down-walk over the gated FK edges — it
    /// does NOT climb to ancestors or cross to sibling roots, so a release's subtree
    /// is exactly that release and its own files, never another release sharing an
    /// album. Structural (no kept-filter): a managed or managing root's whole
    /// subtree is returned whatever its gate currently reads.
    ///
    /// [`crate::blob::decl::BlobDecls::refs_for_root`] maps these rows to the blobs
    /// a transition uploads (make_remote) or materializes (make_local).
    pub(crate) fn subtree_rows(
        &self,
        conn: &Connection,
        root_table: &str,
        root_id: &str,
    ) -> Result<HashSet<(String, String)>, GateError> {
        self.subtree_rows_conn(conn, root_table, root_id)
    }

    fn subtree_rows_conn(
        &self,
        conn: &Connection,
        root_table: &str,
        root_id: &str,
    ) -> Result<HashSet<(String, String)>, GateError> {
        // The down-edges (parent table -> its gated children + FK column), the same
        // map the re-emit/retract closure walks; here we follow only this map (down,
        // never up) from the single root so the result is one subtree.
        let down_edges = gated_fk_child_edges(conn, &self.tables)?;
        let mut out: HashSet<(String, String)> = HashSet::new();
        let mut work = vec![(root_table.to_string(), root_id.to_string())];
        while let Some((table, id)) = work.pop() {
            if !out.insert((table.clone(), id.clone())) {
                continue; // already visited: cycle-guard and dedup.
            }
            if let Some(children) = down_edges.get(table.as_str()) {
                for (child_table, fk) in children {
                    for child_id in rows_referencing(conn, child_table, fk, &id)? {
                        work.push((child_table.clone(), child_id));
                    }
                }
            }
        }
        Ok(out)
    }
}

/// Write a gated root's gate column on (`true`) or off (`false`), stamping
/// `_updated_at` so the flip sorts causally and is captured into this cycle's
/// changeset. The single place the transition commits flip a gate — make_remote
/// completion (on) and make_local (off) — so the write shape lives here once. Runs on
/// the caller's connection/transaction.
pub(crate) fn write_gate(
    conn: &Connection,
    root_table: &str,
    gate_col: &str,
    on: bool,
    stamp: &str,
    root_id: &str,
) -> Result<(), rusqlite::Error> {
    conn.execute(
        &format!(
            "UPDATE {} SET {} = ?1, _updated_at = ?2 WHERE id = ?3",
            quote_ident(root_table),
            quote_ident(gate_col),
        ),
        (on as i64, stamp, root_id),
    )?;
    Ok(())
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

/// Whether walking `gate_map` from `name` up its declared-FK chain reaches a
/// gate terminus: a gated root, remote root, or ancestor. Only `Child` links are
/// followed upward; a terminus stops the walk (a `Parent`'s upward keep over its
/// own children is a separate relation, not part of this downward chain).
fn reaches_gate_terminus(gate_map: &HashMap<String, TableGate>, name: &str) -> bool {
    let mut cur = name;
    let mut seen = HashSet::new();
    loop {
        if !seen.insert(cur.to_string()) {
            return false; // cycle, defensive
        }
        match gate_map.get(cur) {
            Some(TableGate::Root { .. })
            | Some(TableGate::RemoteRoot)
            | Some(TableGate::Parent { .. }) => return true,
            Some(TableGate::Child { parent, .. }) => {
                cur = parent.as_str();
            }
            None => return false,
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
pub(super) fn gated_fk_child_edges(
    conn: &Connection,
    gate_map: &HashMap<String, TableGate>,
) -> Result<HashMap<String, Vec<(String, String)>>, GateError> {
    let mut edges: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for referrer in gate_map.keys() {
        for (fk_col, target) in foreign_keys(conn, referrer)? {
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
fn fk_topological_order(
    conn: &Connection,
    gate_map: &HashMap<String, TableGate>,
) -> Result<Vec<String>, GateError> {
    // Edge parent -> child means "parent must precede child". A table's FK to a
    // gated table makes that table its prerequisite (it points at the parent's
    // id), so the FK target is the parent of the edge and the referrer the child.
    // Only edges between two gated tables matter.
    let names: Vec<String> = gate_map.keys().cloned().collect();
    let mut indegree: HashMap<String, usize> = names.iter().map(|n| (n.clone(), 0)).collect();
    let mut edges: HashMap<String, Vec<String>> =
        names.iter().map(|n| (n.clone(), Vec::new())).collect();

    let child_edges = gated_fk_child_edges(conn, gate_map)?;
    for (parent, children) in &child_edges {
        for (child, _fk_col) in children {
            edges.get_mut(parent).unwrap().push(child.clone());
            *indegree.get_mut(child).unwrap() += 1;
        }
    }

    // Kahn with a deterministic tie-break: a min-heap of the ready
    // (zero-indegree) tables, so equal-rank tables always emit smallest-first.
    let mut ready: BinaryHeap<Reverse<String>> = indegree
        .iter()
        .filter(|(_, &d)| d == 0)
        .map(|(n, _)| Reverse(n.clone()))
        .collect();

    let mut order = Vec::with_capacity(names.len());
    while let Some(Reverse(next)) = ready.pop() {
        for child in &edges[&next] {
            let d = indegree.get_mut(child).unwrap();
            *d -= 1;
            if *d == 0 {
                ready.push(Reverse(child.clone()));
            }
        }
        order.push(next);
    }

    if order.len() != names.len() {
        let mut remaining: Vec<String> = names
            .iter()
            .filter(|n| !order.contains(n))
            .cloned()
            .collect();
        remaining.sort();
        return Err(GateError::FkCycle(remaining));
    }
    Ok(order)
}
/// The ids of rows in `table` whose `fk` column equals `value`.
pub(super) fn rows_referencing(
    conn: &Connection,
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
    let mut ids = Vec::new();
    for id in query_mapped_rows(conn, &sql, [value], |row| row_value_to_string(row, 0))? {
        let Some(id) = id else {
            // `id` is a NOT NULL primary key, so a NULL here is a genuine schema
            // anomaly, not a row we may quietly drop from the kept component.
            warn!("gate: row in {table} referencing {fk}={value} has a NULL id; skipping it from the kept component");
            continue;
        };
        ids.push(id);
    }
    Ok(ids)
}
/// The single definition of gate-truth, evaluated in Rust over a gate value read
/// as text: a nonzero integer is true; `0`/empty/non-integer is false.
/// [`truthy_sql`] is the SQL realization of this same rule for the snapshot path;
/// changing the rule here means changing it there too.
pub(super) fn truthy(s: &str) -> bool {
    s.trim().parse::<i64>().map(|n| n != 0).unwrap_or(false)
}

// ---- small schema/query helpers -------------------------------------------

fn gate_column(cols: &[String], table: &str, name: &str) -> Result<GateColumn, GateError> {
    column_ref_or(cols, table, name, GateError::MissingGateColumn)
}

fn fk_column(cols: &[String], table: &str, name: &str) -> Result<GateColumn, GateError> {
    column_ref_or(cols, table, name, GateError::MissingFkColumn)
}

fn column_ref_or(
    cols: &[String],
    table: &str,
    name: &str,
    err: impl FnOnce(String, String) -> GateError,
) -> Result<GateColumn, GateError> {
    cols.iter()
        .position(|c| c == name)
        .map(|index| GateColumn {
            index,
            name: name.to_string(),
        })
        .ok_or_else(|| err(table.to_string(), name.to_string()))
}

/// Every foreign key on `table`, as `(child column name, parent table name)`
/// pairs, via `PRAGMA foreign_key_list`. Composite keys contribute one pair per
/// row; the gate only ever uses the column, so the granularity matches.
pub(super) fn foreign_keys(
    conn: &Connection,
    table: &str,
) -> Result<Vec<(String, String)>, GateError> {
    let sql = format!("PRAGMA foreign_key_list({})", quote_ident(table));
    let rows = query_mapped_rows(conn, &sql, [], |row| {
        Ok((
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(2)?,
        ))
    })?;
    let mut fks = Vec::new();
    for (from, parent) in rows {
        let Some(from) = from else {
            warn!(
                table,
                "gate: foreign_key_list row has no child column; skipping it"
            );
            continue;
        };
        let Some(parent) = parent else {
            warn!(
                table,
                from, "gate: foreign_key_list row has no parent table; skipping it"
            );
            continue;
        };
        fks.push((from, parent));
    }
    Ok(fks)
}
/// The FK column in `child` that references `parent`, or `None` if `child` has
/// no FK to `parent`. Used to wire an ancestor to a keep-child: the inference
/// names the child *table*, and this resolves which of its columns holds the
/// ancestor's id.
fn fk_col_referencing(
    conn: &Connection,
    child: &str,
    parent: &str,
) -> Result<Option<String>, GateError> {
    Ok(foreign_keys(conn, child)?
        .into_iter()
        .find(|(_, p)| p == parent)
        .map(|(from, _)| from))
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
fn select_parent_fk(
    conn: &Connection,
    table: &str,
    tables: &[SyncedTable],
    ancestors: &HashSet<&str>,
) -> Result<Option<(String, String)>, GateError> {
    let synced: HashSet<&str> = tables.iter().map(|t| t.name()).collect();
    let candidates: Vec<(String, String)> = foreign_keys(conn, table)?
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
        let tier = if parent_reaches_root(conn, tables, ancestors, &parent, &mut HashSet::new())? {
            0u8
        } else if ancestors.contains(parent.as_str()) {
            1
        } else {
            2
        };
        let specificity = if tier == 1 {
            -(ancestor_depth(conn, ancestors, &parent, &mut HashSet::new())? as isize)
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

/// Whether `parent`'s own gate eventually reaches a locality root downward, so a
/// child inheriting from it lands on a real root rather than on an ancestor or
/// nothing. A gated root or remote root is the terminus; a plain table reaches one
/// iff its own selected parent FK does; an ancestor is NOT a downward root path (its
/// keep is the separate upward relation). Cycle-guarded by `visiting`.
fn parent_reaches_root(
    conn: &Connection,
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
        Some(t) if t.gate_column().is_some() || t.is_remote_root() => true,
        // An ancestor is not a downward root path.
        Some(t) if t.is_gated_by_descendants() => false,
        // A plain (or unknown) parent reaches a root iff its own chain does.
        _ => match select_parent_fk(conn, parent, tables, ancestors)? {
            Some((_, grandparent)) => {
                parent_reaches_root(conn, tables, ancestors, &grandparent, visiting)?
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
fn ancestor_depth(
    conn: &Connection,
    ancestors: &HashSet<&str>,
    ancestor: &str,
    visiting: &mut HashSet<String>,
) -> Result<usize, GateError> {
    if !visiting.insert(ancestor.to_string()) {
        return Ok(0); // defensive against a malformed ancestor cycle.
    }
    let mut depth = 0;
    for (_, parent) in foreign_keys(conn, ancestor)? {
        if parent != ancestor && ancestors.contains(parent.as_str()) {
            depth = depth.max(1 + ancestor_depth(conn, ancestors, &parent, visiting)?);
        }
    }
    visiting.remove(ancestor);
    Ok(depth)
}
