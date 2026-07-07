//! The outbound gating passes: cut gated-false rows from a captured changeset,
//! re-emit the full subtree of a root that flips false->true this cycle, and emit
//! DELETEs for the rows that leave the shared set when a root flips true->false.

use std::collections::{HashMap, HashSet};

use rusqlite::ffi;
use rusqlite::Connection;
use tracing::{debug, warn};

use super::create_table::{create_table_sql, rewrite_create_into_schema};
use super::ffi::{collect_deletes, for_each_change, ChangeRow, Changegroup};
use super::model::{
    foreign_keys, gated_fk_child_edges, rows_referencing, truthy, GateColumn, Gates, TableGate,
};
use super::{execute_batch, query_row_optional, row_value_to_string, GateError};
use crate::sync::session::{quote_ident, table_columns};

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
    unsafe { gate_outbound_raw(conn, changeset, gates) }
}

pub(crate) fn combine_changesets(
    conn: &Connection,
    changesets: &[Vec<u8>],
) -> Result<Vec<u8>, GateError> {
    let group = Changegroup::new()?;
    unsafe {
        group.set_schema(conn.handle())?;
    }
    for changeset in changesets {
        group.add_changeset(changeset)?;
    }
    group.output()
}

/// # Safety
/// `conn` must be the valid, open connection the changeset was captured on, with
/// no live session attached (gating reads current row state from it).
unsafe fn gate_outbound_raw(
    conn: &Connection,
    changeset: &[u8],
    gates: &Gates,
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
                Some(pk) => was_shared(
                    conn,
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
            effective_gate(conn, gates, &row)?
        };
        if keep {
            group.add_change(iter)?;
            // A kept row that repoints an FK onto a gated parent drags that parent's
            // (possibly never-shared) subtree into visibility.
            reparent_seeds.extend(reparent_targets(conn, gates, &row)?);
        }
        Ok(())
    })?;

    // Pass 2: re-emit full subtrees for flipped roots and reparent targets.
    if !flipped_roots.is_empty() || !reparent_seeds.is_empty() {
        reemit_subtrees(conn, gates, &flipped_roots, &reparent_seeds, &group)?;
    }

    // Pass 2 (retract): emit DELETEs for the rows leaving the shared set of any
    // root that flipped true→false this cycle. The mirror of reemit_subtrees.
    if !retracted_roots.is_empty() {
        reemit_retract_deletes(conn, gates, &retracted_roots, &group)?;
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
) -> Result<Vec<(String, String)>, GateError> {
    // Only an UPDATE repoints an existing row's FK. An INSERT of a managed root is
    // already re-emitted via the gate flip; a new child under an already-shared
    // parent needs nothing extra.
    if row.op != ffi::SQLITE_UPDATE {
        return Ok(Vec::new());
    }
    let cols = table_columns(conn, &row.table)
        .map_err(|e| GateError::Sql(format!("read columns of {}", row.table), e))?;
    let mut out = Vec::new();
    for (fk_col, parent) in foreign_keys(conn, &row.table)? {
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
    conn: &Connection,
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
    let reemit_ids = connected_component(conn, gates, &seeds, true)?;

    let diff_bytes = full_state_diff(conn, gates, FullStateDirection::Inserts)?;
    if diff_bytes.is_empty() {
        return Ok(());
    }

    for_each_change(&diff_bytes, |iter, row| {
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
    conn: &Connection,
    gates: &Gates,
    retracted_roots: &HashSet<(String, String)>,
    group: &Changegroup,
) -> Result<(), GateError> {
    let component = connected_component(conn, gates, retracted_roots, false)?;

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
fn connected_component(
    conn: &Connection,
    gates: &Gates,
    seeds: &HashSet<(String, String)>,
    restrict_to_kept: bool,
) -> Result<HashSet<(String, String)>, GateError> {
    // Down-edges: for each gated table, the gated tables that hold an FK
    // referencing it, paired with the referrer's FK column name. Built once from
    // the shared FK-edge scan so the per-row down-expansion is a map lookup, not a
    // schema scan, and the same edges drive `fk_topological_order`.
    let down_edges = gated_fk_child_edges(conn, &gates.tables)?;

    let mut out: HashSet<(String, String)> = HashSet::new();
    let mut work: Vec<(String, String)> = seeds.iter().cloned().collect();
    while let Some((table, id)) = work.pop() {
        if !out.insert((table.clone(), id.clone())) {
            continue; // already visited: cycle-guard and dedup.
        }
        // Up: every gated FK parent of this row.
        for (fk_col_name, parent) in foreign_keys(conn, &table)? {
            if parent == table || !gates.tables.contains_key(&parent) {
                continue;
            }
            if let Some(parent_id) = query_column_text(conn, &table, &fk_col_name, &id)? {
                work.push((parent, parent_id));
            }
        }
        // Down: each gated child referencing this row — filtered to kept children
        // for re-emit, taken structurally (every live FK edge) for retract.
        if let Some(children) = down_edges.get(table.as_str()) {
            for (child_table, fk) in children {
                for child_id in rows_referencing(conn, child_table, fk, &id)? {
                    if !restrict_to_kept || gates.row_kept(conn, child_table, &child_id)? {
                        work.push((child_table.clone(), child_id));
                    }
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
pub(super) fn with_empty_clone<R>(
    conn: &Connection,
    gates: &Gates,
    f: impl FnOnce(&str, &[String]) -> Result<R, GateError>,
) -> Result<R, GateError> {
    let alias = "coven_gate_empty";
    let attach = format!("ATTACH DATABASE ':memory:' AS {alias}");
    execute_batch(conn, &attach)?;

    let tables = gates.gated_tables_parent_first(conn)?;
    let result = (|| {
        for tbl in &tables {
            let create = create_table_sql(conn, tbl)?;
            // The CREATE statement names the bare table; run it in the attached
            // db by qualifying via the schema-aware exec on the alias.
            let in_alias = rewrite_create_into_schema(&create, tbl, alias)?;
            execute_batch(conn, &in_alias)?;
        }
        f(alias, &tables)
    })();

    // Always detach, even on error. A failed detach leaves the clone attached
    // under `alias`, which would make next cycle's ATTACH collide — surface it.
    let detach = format!("DETACH DATABASE {alias}");
    if let Err(detach_err) = execute_batch(conn, &detach) {
        if result.is_ok() {
            return Err(detach_err);
        }
        warn!("gate: failed to detach the temporary clone db ({alias}): {detach_err}");
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
fn full_state_diff(
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
unsafe fn effective_gate(
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
        Some(TableGate::RemoteRoot) => Ok(true),
        Some(TableGate::Child { fk_col, parent }) => {
            let Some(parent_id) =
                changeset_child_parent_id(conn, row, fk_col, ChildParentResolution::ShareDecision)?
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
    conn: &Connection,
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
            Some(row) => match row.old.get(gate_col.index) {
                Some(Some(v)) => truthy(v),
                Some(None) => false,
                None => {
                    warn!(table, id, "gate: deleted root's old gate value absent from the changeset row; treating as not shared");
                    false
                }
            },
            None => match query_truth(conn, table, &gate_col.name, id)? {
                Some(t) => t,
                None => {
                    warn!(table, id, "gate: live root absent while resolving a descendant's pre-delete share; treating as not shared");
                    false
                }
            },
        },
        Some(TableGate::RemoteRoot) => true,
        Some(TableGate::Child { fk_col, parent }) => {
            let parent_id = match deleted.get(&key) {
                Some(row) => row.fk_value(fk_col.index).map(str::to_string),
                None => lookup_fk_in_db(conn, table, &fk_col.name, id)?,
            };
            match parent_id {
                Some(pid) => was_shared(conn, gates, deleted, parent, &pid, memo, visiting)?,
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
            if gates.row_kept(conn, table, id)? {
                true
            } else {
                let mut found = false;
                'children: for (child_table, child_fk_col) in children {
                    for ((dt, dpk), drow) in deleted {
                        if dt == child_table
                            && drow.fk_value(child_fk_col.index) == Some(id)
                            && was_shared(conn, gates, deleted, child_table, dpk, memo, visiting)?
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
        Some(TableGate::RemoteRoot) => Ok(row.pk().map(|pk| (row.table.clone(), pk.to_string()))),
        Some(TableGate::Child { fk_col, parent }) => {
            let Some(parent_id) =
                changeset_child_parent_id(conn, row, fk_col, ChildParentResolution::ReemitScope)?
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
unsafe fn changeset_child_parent_id(
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

    match lookup_fk_in_db(conn, &row.table, &fk_col.name, pk)? {
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
pub(super) struct ResolvedGate {
    pub(super) terminus_table: String,
    pub(super) terminus_id: String,
    pub(super) kept: bool,
}

/// Walk the live-db FK chain from (`table`, `id`) up to its locality terminus,
/// returning that terminus and its keep truth. `None` if the chain never reaches a
/// terminus, or a row along it is missing from the live db (an anomaly the caller
/// treats as not-shared).
pub(super) fn resolve_root(
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
        Some(TableGate::Child { fk_col, parent }) => {
            match query_column_text(conn, table, &fk_col.name, id)? {
                Some(parent_id) => resolve_root(conn, gates, parent, &parent_id),
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
            kept: gates.row_kept(conn, table, id)?,
        })),
        // A child whose parent is not itself gated/inheriting was pruned from the
        // map, so this is unreachable for retained tables; treat as ungated.
        None => Ok(None),
    }
}
/// Query a single text column value for the row with id `id`.
fn query_column_text(
    conn: &Connection,
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
    query_row_optional(conn, &sql, [id], |row| row_value_to_string(row, 0)).map(|row| row.flatten())
}

/// Query a single boolean gate column for the row with id `id`.
fn query_truth(
    conn: &Connection,
    table: &str,
    column: &str,
    id: &str,
) -> Result<Option<bool>, GateError> {
    Ok(query_column_text(conn, table, column, id)?.map(|s| truthy(&s)))
}

/// Read the FK value (`column`) for the live row `pk`.
fn lookup_fk_in_db(
    conn: &Connection,
    table: &str,
    column: &str,
    pk: &str,
) -> Result<Option<String>, GateError> {
    query_column_text(conn, table, column, pk)
}
