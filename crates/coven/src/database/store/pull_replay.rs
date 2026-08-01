use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::RetainedMergeMaterializationCache;
use crate::database::{
    BlobDecls, DbError, IncomingTimestampPolicy, MergeMaterializationTransaction, TableSchema,
    ValidatedChangeset,
};
use crate::protocol::store_commit::{CommitFrontier, StoreBatchCommitRef, StoreRootRef};
use crate::protocol::{circle, remote_object};
use crate::storage::ExactObjectRef;
use crate::sync::{
    activated_merge_membership_remote_objects, ApplyOutcome, HeldStorePositionReason,
    LocalStoreMembership, MembershipAuthorityBytes, PreparedMergeMaterialization,
    PreparedMergeMaterializationPackage, VerifiedCircleImage,
};
use crate::SyncedTable;

pub(crate) fn replay_retained_merge_projection_on(
    live: &rusqlite::Transaction<'_>,
    root: &StoreRootRef,
    retained_merge_materializations: &mut RetainedMergeMaterializationCache,
    blob_decls: &BlobDecls,
    gates: &crate::database::Gates,
    synced_tables: &[SyncedTable],
    routing_key: Option<&circle::RowRoutingKey>,
    retracted: &BTreeSet<StoreBatchCommitRef>,
    history_cut: Option<&CommitFrontier>,
    include_local_write_overlays: bool,
    local_store_membership: LocalStoreMembership,
) -> Result<rusqlite::Connection, DbError> {
    let baseline = crate::database::StoreDatabase::generation_zero_replay_baseline_on(live)?;
    let replay = baseline.open_image()?;
    replay
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(DbError::from)?;
    let schema = Arc::new(TableSchema::for_apply(&replay, synced_tables, gates)?);
    let circle_bootstraps =
        crate::database::StoreDatabase::circle_bootstrap_replay_inputs_on(live)?;
    let mut circle_bootstrap_cuts = BTreeMap::new();
    for (activation_commit, bootstrap) in &circle_bootstraps {
        crate::database::verify_circle_bootstrap_image(
            bootstrap.image_bytes(),
            bootstrap.reference(),
            bootstrap.circle_id(),
            synced_tables,
            routing_key,
        )
        .map_err(|error| {
            DbError::Message(format!(
                "verify retained Circle {} bootstrap: {error}",
                bootstrap.circle_id()
            ))
        })?;
        let tx = replay.unchecked_transaction().map_err(DbError::from)?;
        install_circle_bootstrap_image_on(&tx, synced_tables, activation_commit, bootstrap)?;
        tx.commit().map_err(DbError::from)?;
        if circle_bootstrap_cuts
            .insert(
                bootstrap.circle_id(),
                bootstrap.reference().coverage.clone(),
            )
            .is_some()
        {
            return Err(DbError::Message(format!(
                "retained replay has duplicate Circle {} bootstraps",
                bootstrap.circle_id()
            )));
        }
    }
    let retained = retained_merge_materializations.replay_inputs_on(live, root)?;
    let circle_epochs = retained_merge_materializations.circle_replay_epoch_index_on(live)?;
    let active_references = retained
        .iter()
        .filter(|materialization| {
            !retracted.contains(materialization.commit_ref())
                && history_cut
                    .is_none_or(|cutoff| cutoff.covers_commit(materialization.commit_ref()))
        })
        .map(|materialization| materialization.commit_ref().clone())
        .collect::<BTreeSet<_>>();
    for materialization in retained
        .iter()
        .filter(|materialization| active_references.contains(materialization.commit_ref()))
    {
        let mut dependencies = materialization
            .commit()
            .order
            .dependencies()
            .values()
            .cloned()
            .collect::<BTreeSet<_>>();
        if let Some(predecessor) = materialization.commit().order.predecessor() {
            dependencies.insert(predecessor.clone());
        }
        for dependency in dependencies {
            if retracted.contains(&dependency) {
                return Err(DbError::Message(format!(
                    "surviving retained Merge commit {:?} depends on retracted commit {:?}",
                    materialization.commit_ref(),
                    dependency
                )));
            }
            if !active_references.contains(&dependency)
                && !replay_dependency_is_baseline_covered(&dependency, &baseline.exact_cut)
            {
                return Err(DbError::Message(format!(
                    "surviving retained Merge commit {:?} has unretained dependency {:?}",
                    materialization.commit_ref(),
                    dependency
                )));
            }
        }
    }
    let active_accepted_writes = retained
        .iter()
        .filter(|materialization| active_references.contains(materialization.commit_ref()))
        .map(|materialization| materialization.commit().write_id.clone())
        .collect::<BTreeSet<_>>();
    let retracted_writes = retained
        .iter()
        .filter(|materialization| retracted.contains(materialization.commit_ref()))
        .map(|materialization| materialization.commit().write_id.clone())
        .collect::<BTreeSet<_>>();
    let write_overlays = if include_local_write_overlays {
        crate::database::StoreDatabase::load_merge_replay_write_overlays_on(
            live,
            &active_accepted_writes,
            &retracted_writes,
        )?
    } else {
        Vec::new()
    };
    let mut pending = retained
        .into_iter()
        .filter(|materialization| active_references.contains(materialization.commit_ref()))
        .map(|materialization| (materialization.commit_ref().clone(), materialization))
        .collect::<BTreeMap<_, _>>();
    let mut applied = BTreeSet::new();
    while !pending.is_empty() {
        let ready = pending
            .iter()
            .filter_map(|(reference, materialization)| {
                let predecessor_ready =
                    materialization
                        .commit()
                        .order
                        .predecessor()
                        .is_none_or(|predecessor| {
                            replay_dependency_is_settled(predecessor, &applied, &baseline.exact_cut)
                        });
                let dependencies_ready = materialization
                    .commit()
                    .order
                    .dependencies()
                    .values()
                    .all(|dependency| {
                        replay_dependency_is_settled(dependency, &applied, &baseline.exact_cut)
                    });
                (predecessor_ready && dependencies_ready).then(|| reference.clone())
            })
            .collect::<Vec<_>>();
        if ready.is_empty() {
            return Err(DbError::Message(
                "retained Merge replay is cyclic or has an unresolved dependency".to_string(),
            ));
        }
        let mut made_progress = false;
        for reference in ready {
            let materialization = pending
                .get(&reference)
                .expect("ready retained replay input remains pending")
                .clone();
            let timestamp_policy = match materialization.package_application() {
                None => IncomingTimestampPolicy::LocallyAuthored,
                Some(crate::database::RetainedPackageApplication::Received {
                    receiver_wall_ms,
                }) => IncomingTimestampPolicy::Received { receiver_wall_ms },
                Some(crate::database::RetainedPackageApplication::LocallyAuthored) => {
                    IncomingTimestampPolicy::LocallyAuthored
                }
            };
            let mut retained_packages = Vec::new();
            for package in materialization.packages() {
                if let crate::protocol::audience_package::PackageAudience::Circle {
                    circle_id,
                    control,
                    ..
                } = package.audience()
                {
                    if circle_bootstrap_cuts
                        .get(circle_id)
                        .is_some_and(|cut| cut.covers_commit(materialization.commit_ref()))
                    {
                        continue;
                    }
                    if !circle_epochs.permits(materialization.commit_ref(), *circle_id, control)? {
                        continue;
                    }
                    if !local_store_membership.retains_circle_rows() {
                        continue;
                    }
                }
                retained_packages.push(package.clone());
            }
            let package_application = if retained_packages.is_empty() {
                None
            } else {
                Some(materialization.package_application().ok_or_else(|| {
                    DbError::Message(
                        "retained Merge packages lack their application timestamp".to_string(),
                    )
                })?)
            };
            let packages = retained_packages
                .into_iter()
                .map(|package| {
                    let changeset =
                        ValidatedChangeset::new(package.changeset().to_vec(), schema.clone())
                            .map_err(|error| {
                                DbError::Message(format!(
                                    "retained Merge replay changeset: {error}"
                                ))
                            })?;
                    Ok(PreparedMergeMaterializationPackage { package, changeset })
                })
                .collect::<Result<Vec<_>, DbError>>()?;
            let membership_remote_objects = if let Some(objects) =
                materialization.membership_objects()
            {
                let retained_membership_bytes =
                    |object: &ExactObjectRef,
                     kind: &str|
                     -> Result<MembershipAuthorityBytes, DbError> {
                        let object_id = remote_object::remote_object_id(object);
                        let remote = crate::database::load_remote_object_on(live, object_id)
                                .map_err(|error| {
                                    DbError::Message(format!(
                                        "load retained Merge membership {kind} {object_id} for replay: {error}"
                                    ))
                                })?;
                        if remote.object() != object {
                            return Err(DbError::Message(format!(
                                    "retained Merge membership {kind} {object_id} has different exact object"
                                )));
                        }
                        let stored = remote
                                .bytes()
                                .stored()
                                .inline_bytes()
                                .ok_or_else(|| {
                                    DbError::Message(format!(
                                        "retained Merge membership {kind} {object_id} has no inline stored bytes"
                                    ))
                                })?
                                .to_vec();
                        Ok(MembershipAuthorityBytes::new(
                            remote.bytes().canonical_semantic_bytes().to_vec(),
                            stored,
                        ))
                    };
                let family = materialization.commit().candidate_family();
                let owner = materialization.commit_ref();
                let entry_bytes = retained_membership_bytes(&objects.entry().object, "entry")?;
                let head_bytes = retained_membership_bytes(&objects.head().object, "head")?;
                let resolution_bytes = objects
                    .resolution()
                    .map(|resolution| retained_membership_bytes(&resolution.object, "resolution"))
                    .transpose()?;
                activated_merge_membership_remote_objects(
                    family,
                    objects,
                    entry_bytes,
                    head_bytes,
                    resolution_bytes,
                    owner,
                )
                .map_err(|error| DbError::Message(error.to_string()))?
            } else {
                Vec::new()
            };
            let replay_materialization = PreparedMergeMaterialization {
                root: materialization.root().clone(),
                verified_commit: materialization.verified_commit().clone(),
                activation_head: materialization.activation_head().clone(),
                activation_head_object: materialization.activation_head_object().clone(),
                history_summary: materialization.history_summary().clone(),
                membership_objects: materialization.membership_objects().cloned(),
                membership_remote_objects,
                registrations: materialization.registrations().to_vec(),
                packages,
                device_operations: materialization.device_operations().clone(),
                circle_activations: materialization.circle_activations().clone(),
                package_application,
            };
            let tx = replay.unchecked_transaction().map_err(DbError::from)?;
            let outcome = MergeMaterializationTransaction::new(&tx)
                .apply_prepared_merge_materialization(
                    blob_decls,
                    gates,
                    synced_tables,
                    routing_key,
                    local_store_membership,
                    timestamp_policy,
                    Some(&circle_bootstrap_cuts),
                    replay_materialization,
                )
                .map_err(|error| {
                    DbError::Message(format!(
                    "apply retained Merge commit {reference:?} during canonical replay: {error}"
                ))
                })?;
            match outcome.outcome {
                ApplyOutcome::Applied(_) => {
                    tx.commit().map_err(DbError::from)?;
                    pending.remove(&reference);
                    applied.insert(reference);
                    made_progress = true;
                }
                ApplyOutcome::Held(HeldStorePositionReason::ForeignKeyDependency) => {
                    tx.rollback().map_err(DbError::from)?;
                }
                ApplyOutcome::Held(reason) => {
                    tx.rollback().map_err(DbError::from)?;
                    return Err(DbError::Message(format!(
                        "retained Merge replay held accepted commit {reference:?}: {reason:?}"
                    )));
                }
            }
        }
        if !made_progress {
            return Err(DbError::Message(
                "retained Merge replay has an unresolved foreign-key dependency".to_string(),
            ));
        }
    }
    for overlay in write_overlays {
        let tx = replay.unchecked_transaction().map_err(DbError::from)?;
        tx.pragma_update(None, "defer_foreign_keys", "ON")
            .map_err(DbError::from)?;
        let partitions = overlay
            .partitions
            .store
            .into_iter()
            .chain(overlay.partitions.circles)
            .chain(overlay.partitions.local);
        for partition in partitions {
            let changeset =
                ValidatedChangeset::new(partition.changeset, schema.clone()).map_err(|error| {
                    DbError::Message(format!(
                        "local replay write {} changeset: {error}",
                        overlay.write_id
                    ))
                })?;
            let applied = MergeMaterializationTransaction::new(&tx)
                .apply_changeset(changeset, IncomingTimestampPolicy::LocallyAuthored)?;
            if applied.had_fk_violations || !applied.constraint_conflict_tables.is_empty() {
                return Err(DbError::Message(format!(
                    "local replay write {} conflicts with accepted history",
                    overlay.write_id
                )));
            }
        }
        let violations: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        if violations {
            return Err(DbError::Message(format!(
                "local replay write {} violates foreign keys",
                overlay.write_id
            )));
        }
        tx.commit().map_err(DbError::from)?;
    }
    Ok(replay)
}

/// Install one verified Circle image's rows, routes, and blob graph onto `conn`
/// directly — no transaction of its own. `conn` is the caller's active
/// transaction: the pull replay wraps this in a fresh throwaway transaction; the
/// snapshot-restore installer runs it inside the single install transaction
/// alongside the Store image, so the whole set commits or rolls back together.
/// Foreign keys are deferred to that outer commit, matching the final
/// foreign-key validation the install runs over the installed union.
pub(crate) fn install_circle_bootstrap_image_on(
    conn: &rusqlite::Connection,
    synced_tables: &[SyncedTable],
    activation_commit: &StoreBatchCommitRef,
    bootstrap: &VerifiedCircleImage,
) -> Result<(), DbError> {
    let source =
        crate::database::open_database_image(bootstrap.image_bytes()).map_err(|error| {
            DbError::Message(format!("open retained Circle bootstrap image: {error}"))
        })?;
    let mut projection_tables = synced_tables
        .iter()
        .map(|table| table.name().to_string())
        .collect::<Vec<_>>();
    projection_tables.extend([
        "_coven_audience".to_string(),
        "_coven_row_routes".to_string(),
    ]);
    projection_tables.sort();
    projection_tables.dedup();
    conn.pragma_update(None, "defer_foreign_keys", "ON")
        .map_err(DbError::from)?;
    for table in &projection_tables {
        // The audience-routing tables are preserved wholesale by a Store image, so
        // a restore already carries their deterministic rows; skip a re-insert of a
        // row that is already present instead of failing on its unique key. A pull
        // installs onto an empty replay base, where nothing conflicts. Data tables
        // carry no circle rows on a Store image, so they insert exactly once.
        let ignore_existing = table == "_coven_audience" || table == "_coven_row_routes";
        crate::database::copy_table_with_conflicts(&source, conn, table, ignore_existing).map_err(
            |error| {
                DbError::Message(format!(
                    "install exact Circle {} bootstrap table {table}: {error}",
                    bootstrap.circle_id()
                ))
            },
        )?;
    }
    install_circle_bootstrap_remote_objects_on(conn, activation_commit, bootstrap)?;
    for binding in &bootstrap.reference().blobs {
        let stored = binding.stored().ok_or_else(|| {
            DbError::Message("Circle bootstrap row blob has no exact locator".to_string())
        })?;
        let object_id = remote_object::remote_object_id(stored.object());
        let crate::blob::RowBlobAuthority::Remote(authority) = binding.authority() else {
            return Err(DbError::Message(
                "Circle bootstrap row blob lacks remote package authority".to_string(),
            ));
        };
        let locator_hash = stored.locator().locator_hash().to_string();
        let locator_inserted = conn
            .execute(
                "INSERT INTO blob_locators (remote_object_id, locator_hash) VALUES (?1, ?2)
             ON CONFLICT(remote_object_id) DO NOTHING",
                rusqlite::params![object_id.to_string(), &locator_hash],
            )
            .map_err(DbError::from)?;
        if locator_inserted == 0 {
            let retained_locator_hash: String = conn
                .query_row(
                    "SELECT locator_hash FROM blob_locators WHERE remote_object_id = ?1",
                    [object_id.to_string()],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            if retained_locator_hash != locator_hash {
                return Err(DbError::Message(format!(
                    "Circle bootstrap blob locator conflicts for {object_id}"
                )));
            }
        }
        let encoded_authority = serde_json::to_string(authority).map_err(|error| {
            DbError::Message(format!(
                "serialize Circle bootstrap blob authority: {error}"
            ))
        })?;
        let binding_inserted = conn
            .execute(
                "INSERT INTO row_blob_locators
             (table_name, row_id, column_name, row_stamp, audience_authority, remote_object_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(table_name, row_id, column_name, row_stamp) DO NOTHING",
                rusqlite::params![
                    binding.table(),
                    binding.row_id(),
                    binding.column(),
                    binding.row_stamp(),
                    &encoded_authority,
                    object_id.to_string(),
                ],
            )
            .map_err(DbError::from)?;
        if binding_inserted == 0 {
            let (retained_authority, retained_object): (String, String) = conn
                .query_row(
                    "SELECT audience_authority, remote_object_id
                     FROM row_blob_locators
                     WHERE table_name = ?1 AND row_id = ?2
                       AND column_name = ?3 AND row_stamp = ?4",
                    rusqlite::params![
                        binding.table(),
                        binding.row_id(),
                        binding.column(),
                        binding.row_stamp(),
                    ],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(DbError::from)?;
            if retained_authority != encoded_authority || retained_object != object_id.to_string() {
                return Err(DbError::Message(format!(
                    "Circle bootstrap row blob binding conflicts for {}.{}.{} at {}",
                    binding.table(),
                    binding.row_id(),
                    binding.column(),
                    binding.row_stamp(),
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn install_circle_bootstrap_remote_objects_on(
    conn: &rusqlite::Connection,
    activation_commit: &StoreBatchCommitRef,
    bootstrap: &VerifiedCircleImage,
) -> Result<(), DbError> {
    for binding in &bootstrap.reference().blobs {
        let stored = binding.stored().ok_or_else(|| {
            DbError::Message("Circle bootstrap row blob has no exact locator".to_string())
        })?;
        let object_id = remote_object::remote_object_id(stored.object());
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                [object_id.to_string()],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        let remote = if exists {
            let mut remote = crate::database::load_remote_object_on(conn, object_id)?;
            remote
                .merge_blob_activation(stored, activation_commit)
                .map_err(|error| DbError::Message(error.to_string()))?;
            remote
        } else {
            remote_object::RemoteObjectRecord::activated_blob(stored, activation_commit.clone())
                .map_err(|error| DbError::Message(error.to_string()))?
        };
        conn.execute(
            "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)
             ON CONFLICT(object_id) DO UPDATE SET state = excluded.state",
            rusqlite::params![
                object_id.to_string(),
                serde_json::to_string(&remote).map_err(|error| {
                    DbError::Message(format!("serialize Circle bootstrap blob: {error}"))
                })?,
            ],
        )
        .map_err(DbError::from)?;
    }
    Ok(())
}

fn replay_dependency_is_settled(
    dependency: &StoreBatchCommitRef,
    applied: &BTreeSet<StoreBatchCommitRef>,
    baseline: &CommitFrontier,
) -> bool {
    if applied.contains(dependency) {
        return true;
    }
    replay_dependency_is_baseline_covered(dependency, baseline)
}

fn replay_dependency_is_baseline_covered(
    dependency: &StoreBatchCommitRef,
    baseline: &CommitFrontier,
) -> bool {
    baseline
        .0
        .get(&dependency.coord.stream_id)
        .is_some_and(|covered| {
            covered.coord.sequence() > dependency.coord.sequence
                || (covered.coord.sequence() == dependency.coord.sequence && covered == dependency)
        })
}
