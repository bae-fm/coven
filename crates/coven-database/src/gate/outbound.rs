//! The outbound gating passes: cut gated-false rows from a captured changeset,
//! re-emit the full subtree of a root that flips false->true this cycle, and emit
//! DELETEs for the rows that leave the shared set when a root flips true->false.

use std::collections::{HashMap, HashSet};

use rusqlite::ffi;
use rusqlite::Connection;
use tracing::{debug, warn};

use super::audience::live_row_audience;
use super::ffi::{collect_deletes, for_each_change, ChangeRow, Changegroup};
use super::model::{
    child_rows, foreign_keys, gated_fk_child_edges, truthy, GateColumn, Gates, TableGate,
};
use super::{execute_batch, query_row_optional, row_value_to_string, GateError};
use crate::{create_table_sql, quote_ident, rewrite_create_into_schema};
use coven_protocol::circle::Audience;

#[derive(Clone, Copy)]
enum OutboundScope {
    #[cfg(test)]
    EntireGate,
    Store,
}

impl OutboundScope {
    fn contains(self, gates: &Gates, table: &str) -> bool {
        match self {
            #[cfg(test)]
            Self::EntireGate => true,
            Self::Store => !gates.table_is_scoped(table),
        }
    }
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
#[cfg(test)]
pub(crate) fn gate_outbound(
    conn: &Connection,
    changeset: &[u8],
    gates: &Gates,
) -> Result<Vec<u8>, GateError> {
    unsafe { gate_outbound_raw(conn, changeset, gates, OutboundScope::EntireGate) }
}

pub(crate) fn gate_store_outbound(
    conn: &Connection,
    changeset: &[u8],
    gates: &Gates,
) -> Result<Vec<u8>, GateError> {
    unsafe { gate_outbound_raw(conn, changeset, gates, OutboundScope::Store) }
}

/// # Safety
/// `conn` must be the valid, open connection the changeset was captured on, with
/// no live session attached (gating reads current row state from it).
unsafe fn gate_outbound_raw(
    conn: &Connection,
    changeset: &[u8],
    gates: &Gates,
    scope: OutboundScope,
) -> Result<Vec<u8>, GateError> {
    let db = conn.handle();
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
    // db). The resolution's memo + cycle guard span the whole pass.
    let deleted = collect_deletes(changeset)?;
    let mut resolution = DeletedAudiences::new(conn, gates, &deleted, UnresolvedAudience::Local);

    // Pass 1: walk the captured changeset, keep gated-true rows, note flips.
    for_each_change(changeset, |iter, row| {
        if !scope.contains(gates, &row.table) {
            return Ok(());
        }
        // A root whose gate flips false→true this cycle has its whole now-visible
        // subtree re-emitted as full-state INSERTs below. Record it and skip the
        // captured row: an UPDATE(false→true) is wrong for a peer that never had
        // the row (it would apply as a NOTFOUND no-op), and an INSERT is reproduced
        // identically by the re-emit. Letting re-emit be the single source avoids
        // an UPDATE/INSERT dedup clash.
        if let Some(TableGate::Root { gate_col }) = gates.tables.get(&row.table) {
            let flips = match row.op {
                x if x == ffi::SQLITE_UPDATE => {
                    row.old_truth(gate_col.index) == Some(false)
                        && row.new_truth(gate_col.index) == Some(true)
                }
                x if x == ffi::SQLITE_INSERT => row.new_truth(gate_col.index) == Some(true),
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
                && row.old_truth(gate_col.index) == Some(true)
                && row.new_truth(gate_col.index) == Some(false);
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
                Some(pk) => {
                    resolution.audience(&(row.table.clone(), pk.to_string()))? != Audience::Local
                }
                None => {
                    debug!(table = %row.table, "gate: delete row has no primary key; treating as not shared");
                    false
                }
            }
        } else {
            effective_gate(conn, gates, &row)?
        };
        if keep {
            group.add_change(iter)?;
            // A kept row that repoints an FK onto a gated parent drags that parent's
            // (possibly never-shared) subtree into visibility.
            reparent_seeds.extend(reparent_targets(conn, gates, &row, scope)?);
        }
        Ok(())
    })?;

    let reemission = OutboundReemission::new(conn, gates, &group, scope);

    // Pass 2: re-emit full subtrees for flipped roots and reparent targets.
    if !flipped_roots.is_empty() || !reparent_seeds.is_empty() {
        reemission.reemit_subtrees(&flipped_roots, &reparent_seeds)?;
    }

    // Pass 2 (retract): emit DELETEs for the rows leaving the shared set of any
    // root that flipped true→false this cycle. The mirror of reemit_subtrees.
    if !retracted_roots.is_empty() {
        reemission.reemit_retract_deletes(&retracted_roots)?;
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
fn reparent_targets(
    conn: &Connection,
    gates: &Gates,
    row: &ChangeRow,
    scope: OutboundScope,
) -> Result<Vec<(String, String)>, GateError> {
    // Only an UPDATE repoints an existing row's FK. An INSERT of a managed root is
    // already re-emitted via the gate flip; a new child under an already-shared
    // parent needs nothing extra.
    if row.op != ffi::SQLITE_UPDATE {
        return Ok(Vec::new());
    }
    let cols = super::gate_table_columns(conn, &row.table)?;
    let mut out = Vec::new();
    for (fk_col, parent, parent_col) in foreign_keys(conn, &row.table)? {
        if parent == row.table
            || !gates.tables.contains_key(&parent)
            || !scope.contains(gates, &parent)
        {
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
                if let Some(parent_id) = row_id_for_column_value(conn, &parent, &parent_col, new)? {
                    out.push((parent, parent_id));
                }
            }
        }
    }
    Ok(out)
}

struct OutboundReemission<'a> {
    connection: &'a Connection,
    gates: &'a Gates,
    group: &'a Changegroup,
    scope: OutboundScope,
}

impl<'a> OutboundReemission<'a> {
    fn new(
        connection: &'a Connection,
        gates: &'a Gates,
        group: &'a Changegroup,
        scope: OutboundScope,
    ) -> Self {
        Self {
            connection,
            gates,
            group,
            scope,
        }
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
        &self,
        flipped_roots: &HashSet<(String, String)>,
        reparent_seeds: &HashSet<(String, String)>,
    ) -> Result<(), GateError> {
        let conn = self.connection;
        let gates = self.gates;
        let group = self.group;
        let scope = self.scope;
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
        let component = connected_component(conn, gates, &seeds, true, scope)?;

        // Filter the component by the live kept-state, mirroring the retract path's
        // symmetric `!row_kept` filter. `connected_component` inserts every seed
        // unconditionally, so a root whose gate was captured flipping false→true but
        // flipped back false before this runs (a host write landing between the
        // batch load and the gate) would otherwise re-emit its now-private row (and
        // any now-childless kept ancestor) as a full-state INSERT to every peer.
        // Over-emitting a kept row is safe (LWW dedup on apply); emitting an unkept
        // row is the leak this closes.
        let mut reemit_ids: HashSet<(String, String)> = HashSet::new();
        for (table, id) in component {
            if gates.row_kept(conn, &table, &id)? {
                reemit_ids.insert((table, id));
            }
        }

        let diff_bytes = full_state_diff(conn, gates, FullStateDirection::Inserts)?;
        if diff_bytes.is_empty() {
            return Ok(());
        }

        for_each_change(&diff_bytes, |iter, row| {
            if !scope.contains(gates, &row.table) {
                return Ok(());
            }
            let in_descendants =
                gated_root_id(conn, gates, &row)?.is_some_and(|key| flipped_roots.contains(&key));
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
        &self,
        retracted_roots: &HashSet<(String, String)>,
    ) -> Result<(), GateError> {
        let conn = self.connection;
        let gates = self.gates;
        let group = self.group;
        let scope = self.scope;
        let component = connected_component(conn, gates, retracted_roots, false, scope)?;

        // Keep only the rows no longer kept under the post-flip live state. The live db
        // already reflects the gate flip when gate_outbound runs, so the retracted
        // root and its now-orphaned descendants/ancestors read not-kept, while a
        // sibling still held by another managed root reads kept and is spared.
        let mut to_delete: HashSet<(String, String)> = HashSet::new();
        for (table, id) in component {
            if !gates.row_kept(conn, &table, &id)? {
                to_delete.insert((table, id));
            }
        }
        if to_delete.is_empty() {
            return Ok(());
        }

        let delete_bytes = full_state_diff(conn, gates, FullStateDirection::Deletes)?;
        if delete_bytes.is_empty() {
            return Ok(());
        }

        for_each_change(&delete_bytes, |iter, row| {
            if !scope.contains(gates, &row.table) {
                return Ok(());
            }
            let in_to_delete = row
                .pk()
                .is_some_and(|pk| to_delete.contains(&(row.table.clone(), pk.to_string())));
            if in_to_delete {
                group.add_change(iter)?;
            }
            Ok(())
        })
    }
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
fn connected_component(
    conn: &Connection,
    gates: &Gates,
    seeds: &HashSet<(String, String)>,
    restrict_to_kept: bool,
    scope: OutboundScope,
) -> Result<HashSet<(String, String)>, GateError> {
    // Down-edges: for each gated table, the gated tables that hold an FK
    // referencing it, paired with the referrer's FK column name. Built once from
    // the shared FK-edge scan so the per-row down-expansion is a map lookup, not a
    // schema scan, and the same edges drive `fk_topological_order`.
    let down_edges = gated_fk_child_edges(conn, &gates.tables)?;

    let mut out: HashSet<(String, String)> = HashSet::new();
    let mut work: Vec<(String, String)> = seeds.iter().cloned().collect();
    while let Some((table, id)) = work.pop() {
        if !scope.contains(gates, &table) {
            continue;
        }
        if !out.insert((table.clone(), id.clone())) {
            continue; // already visited: cycle-guard and dedup.
        }
        // Up: every gated FK parent of this row.
        for (fk_col_name, parent, parent_col) in foreign_keys(conn, &table)? {
            if parent == table
                || !gates.tables.contains_key(&parent)
                || !scope.contains(gates, &parent)
            {
                continue;
            }
            if let FkParentRow::Found(parent_id) =
                fk_parent_row(conn, &table, &id, &fk_col_name, &parent, &parent_col)?
            {
                work.push((parent, parent_id));
            }
        }
        // Down: each gated child referencing this row — filtered to kept children
        // for re-emit, taken structurally (every live FK edge) for retract.
        if let Some(edges) = down_edges.get(table.as_str()) {
            for (child_table, child_id) in child_rows(conn, edges, &table, &id)? {
                if !scope.contains(gates, &child_table) {
                    continue;
                }
                if !restrict_to_kept || gates.row_kept(conn, &child_table, &child_id)? {
                    work.push((child_table, child_id));
                }
            }
        }
    }
    Ok(out)
}
/// Attach a fresh empty in-memory db, recreate each gated table's schema in it
/// (copied verbatim from `sqlite_master` so a diff sees identical tables), run
/// `f` against the clone, and always detach afterward. Both full-state diff
/// directions share this setup; they differ only in which schema the diff session
/// binds to. `f` receives the clone alias and the gated tables in parent-first
/// order. A unique alias avoids colliding with any host-attached db.
pub(crate) fn with_empty_clone<R>(
    conn: &Connection,
    gates: &Gates,
    f: impl FnOnce(&str, &[String]) -> Result<R, GateError>,
) -> Result<R, GateError> {
    let alias = "coven_gate_empty";
    let owns_clone = !empty_clone_attached(conn)?;
    let tables = if owns_clone {
        attach_empty_clone(conn, gates)?
    } else {
        gates.gated_tables_parent_first(conn)?
    };
    let result = f(alias, &tables);

    // A clone created by this call is detached before returning. A borrowed clone
    // remains owned by the surrounding host transaction.
    if owns_clone {
        if let Err(detach_err) = detach_empty_clone(conn) {
            if result.is_ok() {
                return Err(detach_err);
            }
            warn!("gate: failed to detach the temporary clone db ({alias}): {detach_err}");
        }
    }

    result
}

pub(crate) fn attach_empty_clone(
    conn: &Connection,
    gates: &Gates,
) -> Result<Vec<String>, GateError> {
    let alias = "coven_gate_empty";
    if empty_clone_attached(conn)? {
        return Err(GateError::Sql(
            "attach transaction gate clone".to_string(),
            rusqlite::Error::InvalidQuery,
        ));
    }
    execute_batch(conn, &format!("ATTACH DATABASE ':memory:' AS {alias}"))?;
    let prepared = (|| {
        let tables = gates.gated_tables_parent_first(conn)?;
        for table in &tables {
            let create = create_table_sql(conn, table)?;
            let in_alias = rewrite_create_into_schema(&create, table, alias)?;
            execute_batch(conn, &in_alias)?;
        }
        Ok(tables)
    })();
    match prepared {
        Ok(tables) => Ok(tables),
        Err(operation) => match detach_empty_clone(conn) {
            Ok(()) => Err(operation),
            Err(cleanup) => Err(GateError::Cleanup {
                operation: Box::new(operation),
                cleanup: Box::new(cleanup),
            }),
        },
    }
}

fn detach_empty_clone(conn: &Connection) -> Result<(), GateError> {
    execute_batch(conn, "DETACH DATABASE coven_gate_empty")
}

fn empty_clone_attached(conn: &Connection) -> Result<bool, GateError> {
    let mut statement = conn
        .prepare("PRAGMA database_list")
        .map_err(|source| GateError::Sql("prepare database list".to_string(), source))?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|source| GateError::Sql("read database list".to_string(), source))?;
    for row in rows {
        if row.map_err(|source| GateError::Sql("read database name".to_string(), source))?
            == "coven_gate_empty"
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Which direction a full-state diff against the empty clone runs. The two
/// directions are exact mirrors: `sqlite3session_diff` records the changes that
/// transform the `from_db` table into the session-bound table, so the
/// bind/`from` pairing — not a flag inside SQLite — is what sets the direction
/// (verified by the retract peer-apply tests).
pub(crate) enum FullStateDirection {
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
pub(crate) fn full_state_diff(
    conn: &Connection,
    gates: &Gates,
    direction: FullStateDirection,
) -> Result<Vec<u8>, GateError> {
    with_empty_clone(conn, gates, |alias, tables| {
        let (session_schema, from_schema) = match direction {
            FullStateDirection::Inserts => ("main", alias),
            FullStateDirection::Deletes => (alias, "main"),
        };
        let mut session = rusqlite::session::Session::new_with_name(conn, session_schema).map_err(
            session_error(format!("create diff session on schema {session_schema}")),
        )?;
        for tbl in tables {
            session
                .attach(Some(tbl.as_str()))
                .map_err(session_error(format!(
                    "attach table {session_schema}.{tbl}"
                )))?;
            session
                .diff::<&str, &str>(from_schema, tbl.as_str())
                .map_err(session_error(format!(
                    "diff {from_schema}.{tbl} into {session_schema}.{tbl}"
                )))?;
        }
        let mut out = Vec::new();
        session
            .changeset_strm(&mut out)
            .map_err(session_error(format!(
                "extract changeset from diff session on schema {session_schema}"
            )))?;
        Ok(out)
    })
}

fn session_error(operation: String) -> impl FnOnce(rusqlite::Error) -> GateError {
    |source| GateError::Session { operation, source }
}

/// Whether `row`'s effective gate is true (it should be kept/shared).
pub(crate) fn effective_gate(
    conn: &Connection,
    gates: &Gates,
    row: &ChangeRow,
) -> Result<bool, GateError> {
    match gates.tables.get(&row.table) {
        None => Ok(true), // ungated table: always shared.
        Some(TableGate::Root { gate_col }) => match row.effective_truth(gate_col.index) {
            Some(t) => Ok(t),
            // Gate unchanged in an UPDATE (omitted from the changeset): read the
            // current value from the live row. A delete with no old gate value
            // resolves the same way (the row may still exist as old-state).
            None => match row.pk() {
                Some(pk) => match query_truth(conn, &row.table, &gate_col.name, pk)? {
                    Some(t) => Ok(t),
                    None => {
                        warn!(
                            "gate: root {}.{pk} absent from live db while resolving an \
                                 unchanged gate column; treating as not-shared",
                            row.table
                        );
                        Ok(false)
                    }
                },
                None => {
                    debug!(
                        "gate: root row in {} has no primary key; treating as not-shared",
                        row.table
                    );
                    Ok(false)
                }
            },
        },
        Some(TableGate::ScopedRoot { .. }) => Err(GateError::ScopedOutboundRequiresPartitioning {
            table: row.table.clone(),
        }),
        Some(TableGate::RemoteRoot) => Ok(true),
        Some(TableGate::Child {
            fk_col,
            parent,
            parent_col,
        }) => {
            let Some(parent_key) =
                changeset_child_parent_id(conn, row, fk_col, ChildParentResolution::ShareDecision)?
            else {
                return Ok(false);
            };
            let Some(parent_id) =
                row_id_for_column_value(conn, parent, &parent_col.name, &parent_key)?
            else {
                return Ok(false);
            };
            Ok(resolve_root(conn, gates, parent, &parent_id)?
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
                Some(pk) => gates.row_kept(conn, &row.table, pk),
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
/// What a resolution answers for a deleted row whose pre-deletion audience
/// neither the changeset nor the live db establishes — a foreign-key cycle, a
/// root the live db no longer holds, a parent nothing carries.
#[derive(Clone, Copy)]
pub(crate) enum UnresolvedAudience {
    /// Treat the row as never shared. The outbound gate cuts a deletion it
    /// cannot prove was shared rather than sending peers a row they never had.
    Local,
    /// Refuse. Routing a deletion to the wrong audience strands or leaks it, so
    /// an audience that cannot be established is an error.
    Rejected,
}

/// Resolves what the rows a captured changeset deletes were before it deleted
/// them, against that changeset's old images and the live db behind them.
///
/// Holds the rows it has already answered and the chain it is currently walking,
/// so a shared parent is resolved once and a foreign-key cycle is caught rather
/// than followed. One resolution spans a whole pass.
pub(crate) struct DeletedAudiences<'a> {
    conn: &'a Connection,
    gates: &'a Gates,
    deleted: &'a HashMap<(String, String), ChangeRow>,
    unresolved: UnresolvedAudience,
    resolved: HashMap<(String, String), Audience>,
    visiting: HashSet<(String, String)>,
}

impl<'a> DeletedAudiences<'a> {
    pub(crate) fn new(
        conn: &'a Connection,
        gates: &'a Gates,
        deleted: &'a HashMap<(String, String), ChangeRow>,
        unresolved: UnresolvedAudience,
    ) -> Self {
        Self {
            conn,
            gates,
            deleted,
            unresolved,
            resolved: HashMap::new(),
            visiting: HashSet::new(),
        }
    }

    /// The audience the row `(table, id)` was in *before* this changeset deleted
    /// it.
    ///
    /// The gate resolves an audience against the live db, but a deleted row's
    /// terminus may be gone from it (an album whose last release was deleted, a
    /// track whose release was deleted), so a live resolution always reads
    /// Local: the deletion is wrongly cut and a phantom is stranded on every
    /// peer. This resolves each step against the row's pre-deletion state
    /// instead — the changeset's old values for rows it deletes, falling back to
    /// the live db for rows the changeset left in place (a descendant deleted
    /// under a surviving root).
    ///
    /// - A gated root was in Store iff its old gate value is truthy.
    /// - A scoped root was in the audience its old audience column names.
    /// - A child was in its foreign-key parent's audience — the old foreign key
    ///   for a deleted child, the live one otherwise — recursively to the
    ///   terminus.
    /// - An ancestor is in Store iff it still has a live kept child, or a kept
    ///   child of it is being deleted in this same changeset. An ancestor that
    ///   was never shared (only unmanaged children) stays Local, so its deletion
    ///   never leaks old column values to peers that never had it.
    pub(crate) fn audience(&mut self, key: &(String, String)) -> Result<Audience, GateError> {
        if let Some(audience) = self.resolved.get(key) {
            return Ok(audience.clone());
        }
        if !self.visiting.insert(key.clone()) {
            // The gated foreign-key graph is a DAG, so a cycle here is a
            // malformed declaration, not a shape the walk is meant to handle.
            return self.unresolved.answer(
                key,
                "its foreign-key chain loops",
                GateError::FkCycle(vec![key.0.clone()]),
            );
        }
        let resolved = self.dispatch(key);
        self.visiting.remove(key);
        let audience = resolved?;
        self.resolved.insert(key.clone(), audience.clone());
        Ok(audience)
    }

    fn dispatch(&mut self, key: &(String, String)) -> Result<Audience, GateError> {
        // Copies of the borrowed context, so reading it does not hold a borrow
        // of `self` across the recursive calls below.
        let (conn, gates, deleted) = (self.conn, self.gates, self.deleted);
        let (table, id) = (key.0.as_str(), key.1.as_str());
        let row = deleted.get(key);
        match gates.tables.get(table) {
            // An ungated table syncs unconditionally, so its rows are in Store.
            None | Some(TableGate::RemoteRoot) => Ok(Audience::Store),
            Some(TableGate::Root { gate_col }) => {
                let kept = match row {
                    Some(row) => match row.old.get(gate_col.index) {
                        Some(value) => value.as_deref().is_some_and(truthy),
                        None => {
                            warn!(table, id, "gate: deleted root's old gate value absent from the changeset row; treating as never shared");
                            false
                        }
                    },
                    None => match query_truth(conn, table, &gate_col.name, id)? {
                        Some(truth) => truth,
                        None => {
                            return self.unresolved.answer(
                                key,
                                "the live root is absent",
                                GateError::MissingAudienceRow {
                                    table: key.0.clone(),
                                    row_id: key.1.clone(),
                                },
                            )
                        }
                    },
                };
                Ok(if kept {
                    Audience::Store
                } else {
                    Audience::Local
                })
            }
            Some(TableGate::ScopedRoot { audience_col }) => match row {
                Some(row) => {
                    let value = row
                        .old
                        .get(audience_col.index)
                        .ok_or_else(|| GateError::MissingAudienceRow {
                            table: key.0.clone(),
                            row_id: key.1.clone(),
                        })?
                        .clone();
                    Audience::from_column(value.as_deref()).map_err(|error| {
                        GateError::InvalidAudience {
                            table: key.0.clone(),
                            value,
                            reason: error.to_string(),
                        }
                    })
                }
                None => live_row_audience(conn, gates, table, id),
            },
            Some(TableGate::Child {
                fk_col,
                parent,
                parent_col,
            }) => {
                let parent_key = match row {
                    Some(row) => row.fk_value(fk_col.index).map(str::to_string),
                    None => query_column_text(conn, table, &fk_col.name, id)?,
                };
                let parent_row = match &parent_key {
                    Some(parent_key) => {
                        deleted_or_live_parent(conn, deleted, parent, parent_col, parent_key)?
                    }
                    None => None,
                };
                match parent_row {
                    Some(parent_row) => {
                        self.audience(&(parent.clone(), parent_row.id().to_string()))
                    }
                    None => self.unresolved.answer(
                        key,
                        "it names no foreign-key parent",
                        GateError::MissingAudienceParent {
                            table: key.0.clone(),
                            row_id: Some(key.1.clone()),
                            parent: parent.clone(),
                        },
                    ),
                }
            }
            Some(TableGate::Parent { children }) => {
                // A live kept child keeps a surviving ancestor shared (a
                // descendant was deleted but the ancestor and a sibling remain).
                // For a deleted ancestor the cascade leaves no live child, so
                // the kept child is found among the deletions instead.
                if gates.row_kept(conn, table, id)? {
                    return Ok(Audience::Store);
                }
                for (child_table, child_fk_col, parent_col) in children {
                    let parent_key = row
                        .and_then(|row| row.old.get(parent_col.index))
                        .and_then(|value| value.as_deref())
                        .map(str::to_string)
                        .or(query_column_text(conn, table, &parent_col.name, id)?);
                    let Some(parent_key) = parent_key else {
                        continue;
                    };
                    for child_key in deleted.iter().filter_map(|(child_key, child_row)| {
                        (&child_key.0 == child_table
                            && child_row.fk_value(child_fk_col.index) == Some(parent_key.as_str()))
                        .then_some(child_key)
                    }) {
                        if self.audience(child_key)? != Audience::Local {
                            return Ok(Audience::Store);
                        }
                    }
                }
                Ok(Audience::Local)
            }
        }
    }
}

impl UnresolvedAudience {
    /// The audience a row whose pre-deletion state is unestablished takes, or
    /// `refusal` when the caller refuses to guess one.
    fn answer(
        self,
        key: &(String, String),
        reason: &str,
        refusal: GateError,
    ) -> Result<Audience, GateError> {
        match self {
            Self::Local => {
                warn!(
                    table = key.0,
                    id = key.1,
                    reason,
                    "gate: cannot establish a pre-delete audience; treating as never shared"
                );
                Ok(Audience::Local)
            }
            Self::Rejected => Err(refusal),
        }
    }
}

/// The flipped-root key this row belongs to (for re-emit scoping): the
/// `(root table, root id)` of its gated-ancestor root, or `None` if the row is
/// ungated/unrooted or not shared.
unsafe fn gated_root_id(
    conn: &Connection,
    gates: &Gates,
    row: &ChangeRow,
) -> Result<Option<(String, String)>, GateError> {
    match gates.tables.get(&row.table) {
        None => Ok(None),
        Some(TableGate::Root { gate_col }) => {
            if row.effective_truth(gate_col.index) == Some(true) {
                Ok(row.pk().map(|pk| (row.table.clone(), pk.to_string())))
            } else {
                Ok(None)
            }
        }
        Some(TableGate::ScopedRoot { .. }) => Err(GateError::ScopedOutboundRequiresPartitioning {
            table: row.table.clone(),
        }),
        Some(TableGate::RemoteRoot) => Ok(row.pk().map(|pk| (row.table.clone(), pk.to_string()))),
        Some(TableGate::Child {
            fk_col,
            parent,
            parent_col,
        }) => {
            let Some(parent_key) =
                changeset_child_parent_id(conn, row, fk_col, ChildParentResolution::ReemitScope)?
            else {
                return Ok(None);
            };
            let Some(parent_id) =
                row_id_for_column_value(conn, parent, &parent_col.name, &parent_key)?
            else {
                return Ok(None);
            };
            Ok(resolve_root(conn, gates, parent, &parent_id)?
                .filter(|r| r.kept)
                .map(|r| (r.terminus_table, r.terminus_id)))
        }
        // An ancestor has no gated-ancestor root in the downward sense this
        // scoping uses; its re-emit is driven by the kept-component closure in
        // `connected_component`, not by the flipped-root descendant test.
        Some(TableGate::Parent { .. }) => Ok(None),
    }
}

enum ChildParentResolution {
    ShareDecision,
    ReemitScope,
}

/// The parent id for a child changeset row. The changeset carries the FK when it
/// changed; when it omits the FK, read the live row by primary key.
fn changeset_child_parent_id(
    conn: &Connection,
    row: &ChangeRow,
    fk_col: &GateColumn,
    resolution: ChildParentResolution,
) -> Result<Option<String>, GateError> {
    if let Some(id) = row.fk_value(fk_col.index) {
        return Ok(Some(id.to_string()));
    }

    let Some(pk) = row.pk() else {
        match resolution {
            ChildParentResolution::ShareDecision => debug!(
                "gate: child row in {} has no primary key; treating as not-shared",
                row.table
            ),
            ChildParentResolution::ReemitScope => debug!(
                "gate: child row in {} has no primary key during re-emit; skipping",
                row.table
            ),
        }
        return Ok(None);
    };

    match query_column_text(conn, &row.table, &fk_col.name, pk)? {
        Some(id) => Ok(Some(id)),
        None => {
            match resolution {
                ChildParentResolution::ShareDecision => warn!(
                    "gate: child {}.{pk} has no FK target in live db; treating as not-shared",
                    row.table
                ),
                ChildParentResolution::ReemitScope => warn!(
                    "gate: child {}.{pk} has no FK target in live db during re-emit; skipping",
                    row.table
                ),
            }
            Ok(None)
        }
    }
}

/// The locality terminus a row resolves to: the table at the top of its FK chain
/// (a gated root, a remote root, or an ancestor when the chain inherits upward from
/// one), its id, and whether it gives the row Remote locality.
pub(crate) struct ResolvedGate {
    pub terminus_table: String,
    pub terminus_id: String,
    pub kept: bool,
}

/// Walk the live-db FK chain from (`table`, `id`) up to its locality terminus,
/// returning that terminus and its keep truth. `None` if the chain never reaches a
/// terminus, or a row along it is missing from the live db (an anomaly the caller
/// treats as not-shared).
pub(crate) fn resolve_root(
    conn: &Connection,
    gates: &Gates,
    table: &str,
    id: &str,
) -> Result<Option<ResolvedGate>, GateError> {
    match gates.tables.get(table) {
        Some(TableGate::Root { gate_col }) => match query_truth(conn, table, &gate_col.name, id)? {
            Some(truth) => Ok(Some(ResolvedGate {
                terminus_table: table.to_string(),
                terminus_id: id.to_string(),
                kept: truth,
            })),
            None => {
                warn!("gate: gated root {table}.{id} absent from live db; cannot resolve gate");
                Ok(None)
            }
        },
        Some(TableGate::ScopedRoot { .. }) => Err(GateError::ScopedOutboundRequiresPartitioning {
            table: table.to_string(),
        }),
        Some(TableGate::RemoteRoot) => match query_column_text(conn, table, "id", id)? {
            Some(_) => Ok(Some(ResolvedGate {
                terminus_table: table.to_string(),
                terminus_id: id.to_string(),
                kept: true,
            })),
            None => {
                warn!("gate: remote root {table}.{id} absent from live db; cannot resolve gate");
                Ok(None)
            }
        },
        Some(TableGate::Child {
            fk_col,
            parent,
            parent_col,
        }) => match fk_parent_row(conn, table, id, &fk_col.name, parent, &parent_col.name)? {
            FkParentRow::Found(parent_id) => resolve_root(conn, gates, parent, &parent_id),
            FkParentRow::ParentAbsent => {
                warn!("gate: {table}.{id} names a {parent} row absent from the live db; cannot resolve gate");
                Ok(None)
            }
            FkParentRow::RowAbsent | FkParentRow::NullForeignKey => {
                warn!("gate: {table}.{id} has no FK parent in live db; cannot resolve gate");
                Ok(None)
            }
        },
        // A child whose parent is an ancestor (album_artists → albums) inherits
        // the ancestor's keep: shared iff the ancestor itself is kept by one of
        // *its* children. The ancestor is the terminus.
        Some(TableGate::Parent { .. }) => Ok(Some(ResolvedGate {
            terminus_table: table.to_string(),
            terminus_id: id.to_string(),
            kept: gates.row_kept(conn, table, id)?,
        })),
        // A child whose parent is not itself gated/inheriting was pruned from the
        // map, so this is unreachable for retained tables; treat as ungated.
        None => Ok(None),
    }
}
/// Query a single column value for the row with id `id`, keeping the two ways it
/// can read empty apart: the outer `None` is a row absent from the live db, the
/// inner `None` a row whose column is NULL.
pub(crate) fn query_column_present(
    conn: &Connection,
    table: &str,
    column: &str,
    id: &str,
) -> Result<Option<Option<String>>, GateError> {
    let sql = format!(
        "SELECT {} FROM {} WHERE {} = ?",
        quote_ident(column),
        quote_ident(table),
        quote_ident("id"),
    );
    query_row_optional(conn, &sql, [id], |row| row_value_to_string(row, 0))
}

/// Query a single text column value for the row with id `id`, for callers that
/// treat an absent row and a NULL column alike.
pub(crate) fn query_column_text(
    conn: &Connection,
    table: &str,
    column: &str,
    id: &str,
) -> Result<Option<String>, GateError> {
    Ok(query_column_present(conn, table, column, id)?.flatten())
}

/// How one hop up a declared foreign key came out — see [`fk_parent_row`].
pub(crate) enum FkParentRow {
    /// The parent row's id.
    Found(String),
    /// The row the hop started from is absent from the live db.
    RowAbsent,
    /// The row's foreign-key column is NULL: it names no parent.
    NullForeignKey,
    /// The foreign key names a key no row in the parent table carries.
    ParentAbsent,
}

/// One hop up a declared foreign key: the row in `parent` that the live row
/// `(table, id)` names through `fk_col`, matched on the parent's `parent_col`.
///
/// The three ways the hop comes up empty stay apart because callers answer them
/// differently: a NULL foreign key names no parent by design, while a key no
/// parent carries — or a missing row — is an anomaly some callers refuse.
pub(crate) fn fk_parent_row(
    conn: &Connection,
    table: &str,
    id: &str,
    fk_col: &str,
    parent: &str,
    parent_col: &str,
) -> Result<FkParentRow, GateError> {
    let Some(parent_key) = query_column_present(conn, table, fk_col, id)? else {
        return Ok(FkParentRow::RowAbsent);
    };
    let Some(parent_key) = parent_key else {
        return Ok(FkParentRow::NullForeignKey);
    };
    match row_id_for_column_value(conn, parent, parent_col, &parent_key)? {
        Some(parent_id) => Ok(FkParentRow::Found(parent_id)),
        None => Ok(FkParentRow::ParentAbsent),
    }
}

/// Which row of a parent table carries a foreign key's value once a changeset's
/// deletions are taken into account — see [`deleted_or_live_parent`].
pub(crate) enum DeletedParent {
    /// A parent row this changeset deletes, keyed `(table, id)`.
    Deleted((String, String)),
    /// A parent row still present in the live db.
    Live(String),
}

impl DeletedParent {
    pub(crate) fn id(&self) -> &str {
        match self {
            Self::Deleted((_, id)) => id,
            Self::Live(id) => id,
        }
    }
}

/// The row of `parent` whose `parent_col` holds `parent_key`, searching the
/// changeset's deleted rows before the live db: a parent deleted in the same
/// changeset has no live row left to find, and resolving a deleted child against
/// it is the only way to read the pre-deletion state of a whole removed subtree.
pub(crate) fn deleted_or_live_parent(
    conn: &Connection,
    deleted: &HashMap<(String, String), ChangeRow>,
    parent: &str,
    parent_col: &GateColumn,
    parent_key: &str,
) -> Result<Option<DeletedParent>, GateError> {
    let deleted_parent = deleted.iter().find(|((table, _), row)| {
        table == parent
            && row
                .old
                .get(parent_col.index)
                .and_then(|value| value.as_deref())
                == Some(parent_key)
    });
    if let Some((key, _)) = deleted_parent {
        return Ok(Some(DeletedParent::Deleted(key.clone())));
    }
    Ok(
        row_id_for_column_value(conn, parent, &parent_col.name, parent_key)?
            .map(DeletedParent::Live),
    )
}

pub(crate) fn row_id_for_column_value(
    conn: &Connection,
    table: &str,
    column: &str,
    value: &str,
) -> Result<Option<String>, GateError> {
    let sql = format!(
        "SELECT {} FROM {} WHERE {} = ?",
        quote_ident("id"),
        quote_ident(table),
        quote_ident(column),
    );
    query_row_optional(conn, &sql, [value], |row| row_value_to_string(row, 0))
        .map(|row| row.flatten())
}

/// Query a single boolean gate column for the row with id `id`. `Some` is the
/// column's `truthy` reading; `None` when the row is absent or the gate column is
/// NULL — an unresolvable locality the caller fails loud on rather than guessing.
/// The single terminal gate-truth reader: `resolve_root` uses it for a gated root,
/// and the coven-owned transitions read a root's Local/Remote state through it so
/// there is one definition of a root's locality.
pub fn query_truth(
    conn: &Connection,
    table: &str,
    column: &str,
    id: &str,
) -> Result<Option<bool>, GateError> {
    Ok(query_column_text(conn, table, column, id)?.map(|s| truthy(&s)))
}
