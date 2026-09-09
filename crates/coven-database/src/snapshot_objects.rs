use crate::blob_records::live_blob_row;
use crate::blob_records::load_activated_registration_on;
use crate::blob_records::validate_live_blob_locator;
use crate::blob_records::validate_stored_row_binding_on;
use crate::remote_object_records::load_remote_object_on;
use crate::PreparedSnapshotBlob;

use super::*;

pub(crate) fn validate_snapshot_object_owners_on(
    conn: &Connection,
    root: &coven_protocol::store_commit::StoreRootRef,
    reference: &StoreSnapshotRef,
    meta: &SnapshotMeta,
) -> Result<(), DbError> {
    load_activated_registration_on(conn, root, &meta.author_registration)?;
    let expected = coven_protocol::remote_object::SnapshotObjectOwner::Store {
        metadata_slot: reference.object.slot().clone(),
    };
    validate_snapshot_object_owner_records_on(
        conn,
        &expected,
        &meta.history_summary.pending_device_join_snapshot_slots(),
    )
}

pub fn validate_snapshot_author(
    author: &StoreDeviceRegistrationRef,
    local: &StoreDeviceRegistrationRef,
    label: &str,
) -> Result<(), DbError> {
    if author == local {
        Ok(())
    } else {
        Err(DbError::Message(format!(
            "staged {label} snapshot author differs from local activation"
        )))
    }
}

pub fn validate_snapshot_image(
    image: &SnapshotImageRef,
    prepared: &PreparedExactObject,
    plaintext_hash: ObjectHash,
    stored_hash: ObjectHash,
    stored_size: u64,
    expected_slot: String,
    label: &str,
) -> Result<(), DbError> {
    if image.object == *prepared.reference()
        && plaintext_hash == image.image_hash
        && stored_hash == image.object.stored_hash()
        && stored_size == image.object.stored_size()
        && image.object.slot().logical_key() == expected_slot
    {
        Ok(())
    } else {
        Err(DbError::Message(format!(
            "staged {label} snapshot image differs from its exact reference"
        )))
    }
}

pub(crate) fn validate_snapshot_blob_plans_on(
    conn: &Connection,
    gates: &Gates,
    synced_tables: &[SyncedTable],
    owner: &coven_protocol::remote_object::SnapshotObjectOwner,
    blobs: &[PreparedSnapshotBlob],
) -> Result<(), DbError> {
    for blob in blobs {
        blob.remote
            .validate()
            .map_err(|error| DbError::context("snapshot remote blob", error))?;
        let owners = blob.remote.snapshot_owners().collect::<Vec<_>>();
        if owners != [owner] {
            return Err(DbError::Message(
                "snapshot blob owner differs from the prepared snapshot".to_string(),
            ));
        }
        if blob.bindings.is_empty()
            || blob.bindings.iter().any(|binding| {
                binding.blob().object() != blob.remote.object()
                    || binding.blob().locator().audience() != blob.authority.remote_audience()
            })
        {
            return Err(DbError::Message(
                "snapshot blob plan has inconsistent exact references".to_string(),
            ));
        }
        for binding in &blob.bindings {
            let table = synced_tables
                .iter()
                .find(|table| table.name() == binding.table())
                .ok_or_else(|| {
                    DbError::Message(format!(
                        "snapshot blob names undeclared table {:?}",
                        binding.table()
                    ))
                })?;
            let declaration = table.blob().ok_or_else(|| {
                DbError::Message(format!(
                    "snapshot blob names table {:?} without a blob declaration",
                    table.name()
                ))
            })?;
            let row = live_blob_row(conn, table.name(), binding.row_id(), declaration)?
                .ok_or_else(|| {
                    DbError::Message(format!(
                        "snapshot blob row {:?}/{:?} is absent",
                        table.name(),
                        binding.row_id()
                    ))
                })?;
            let audience = gate::live_row_audience(conn, gates, table.name(), binding.row_id())
                .map_err(DbError::from)?;
            let audience = RemoteAudience::try_from(audience).map_err(DbError::from)?;
            validate_live_blob_locator(
                binding.table(),
                binding.row_id(),
                binding.column(),
                binding.row_stamp(),
                binding.blob(),
                declaration,
                &row,
                &audience,
            )?;
        }
    }
    Ok(())
}

pub(crate) fn persist_snapshot_image_on(
    conn: &Connection,
    store_dir: &coven_foundation::store_dir::StoreDir,
    image: &SnapshotImageRef,
    owner: coven_protocol::remote_object::SnapshotObjectOwner,
    label: &str,
) -> Result<(), DbError> {
    let image = RemoteObjectRecord::snapshot_activated_image(image, owner)
        .map_err(|error| DbError::context(format!("{label} ownership"), error))?;
    persist_exact_remote_object_on(conn, store_dir, &image, label)
}

/// Persist this snapshot candidate's exact membership rollup ownership.
pub(crate) fn persist_membership_rollup_on(
    conn: &Connection,
    store_dir: &coven_foundation::store_dir::StoreDir,
    rollup: &coven_protocol::store_commit::MembershipRollupRef,
    owner: coven_protocol::remote_object::SnapshotObjectOwner,
    label: &str,
) -> Result<(), DbError> {
    let rollup = RemoteObjectRecord::snapshot_activated_membership_rollup(rollup, owner)
        .map_err(|error| DbError::context(format!("{label} ownership"), error))?;
    persist_exact_remote_object_on(conn, store_dir, &rollup, label)
}

pub fn snapshot_generation_as_i64(generation: u64, label: &str) -> Result<i64, DbError> {
    i64::try_from(generation)
        .map_err(|_| DbError::Message(format!("{label} generation exceeds SQLite INTEGER")))
}

pub(crate) fn validate_snapshot_object_owner_records_on(
    conn: &Connection,
    expected: &coven_protocol::remote_object::SnapshotObjectOwner,
    pending_store_snapshots: &BTreeSet<coven_protocol::objects::ObjectSlot>,
) -> Result<(), DbError> {
    let mut statement = conn
        .prepare("SELECT object_id FROM remote_objects ORDER BY object_id")
        .map_err(DbError::from)?;
    let object_ids = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(DbError::from)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(DbError::from)?;
    drop(statement);
    for object_id in object_ids {
        let parsed = object_id.parse().map_err(|error| {
            DbError::context(format!("snapshot remote object id {object_id:?}"), error)
        })?;
        let remote = load_remote_object_on(conn, parsed)?;
        for owner in remote.snapshot_owners() {
            let matches = match (owner, expected) {
                (
                    coven_protocol::remote_object::SnapshotObjectOwner::Store { metadata_slot },
                    coven_protocol::remote_object::SnapshotObjectOwner::Store { .. },
                ) => owner == expected || pending_store_snapshots.contains(metadata_slot),
                (
                    coven_protocol::remote_object::SnapshotObjectOwner::Circle {
                        activation,
                        generation,
                    },
                    coven_protocol::remote_object::SnapshotObjectOwner::Circle {
                        activation: expected_activation,
                        generation: expected_generation,
                    },
                ) => activation == expected_activation && generation <= expected_generation,
                _ => owner == expected,
            };
            if !matches {
                return Err(DbError::Message(format!(
                    "snapshot remote object {object_id} belongs to another snapshot"
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn replace_snapshot_object_owners_on(
    conn: &Connection,
    owner: &coven_protocol::remote_object::SnapshotObjectOwner,
    blobs: &[PreparedSnapshotBlob],
    pending_store_snapshots: &BTreeSet<coven_protocol::objects::ObjectSlot>,
) -> Result<(), DbError> {
    let mut statement = conn.prepare("SELECT object_id FROM remote_objects ORDER BY object_id")?;
    let object_ids = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    for object_id in object_ids {
        let object_id = object_id
            .parse()
            .map_err(|error| DbError::context("snapshot remote object id", error))?;
        let mut remote = load_remote_object_on(conn, object_id)?;
        let retained = blobs
            .iter()
            .any(|blob| blob.remote.object_id() == object_id);
        remote
            .replace_snapshot_owners_for_image(retained.then_some(owner), pending_store_snapshots)
            .map_err(|error| DbError::context("replace exported snapshot ownership", error))?;
        update_remote_object_on(conn, object_id, &remote)?;
    }
    Ok(())
}

fn retain_snapshot_blob_on(conn: &Connection, blob: &PreparedSnapshotBlob) -> Result<(), DbError> {
    let object_id = blob.remote.object_id();
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
            [object_id.to_string()],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    let merged = if exists {
        let mut existing = load_remote_object_on(conn, object_id)?;
        for owner in blob.remote.snapshot_owners() {
            existing
                .merge_snapshot_owner(blob.bindings[0].blob(), owner.clone())
                .map_err(|error| DbError::context("merge snapshot blob owner", error))?;
        }
        existing
    } else {
        blob.remote.clone()
    };
    let encoded = serde_json::to_string(&merged)
        .map_err(|error| DbError::context("serialize snapshot blob", error))?;
    conn.execute(
        "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)
         ON CONFLICT(object_id) DO UPDATE SET state = excluded.state",
        rusqlite::params![object_id.to_string(), encoded],
    )
    .map_err(DbError::from)?;
    crate::blob_records::record_stored_locator_on(conn, blob.bindings[0].blob())?;
    Ok(())
}

/// Install the snapshot's row bindings only into its own database image.
pub(crate) fn install_snapshot_blob_plan_on(
    conn: &Connection,
    blob: &PreparedSnapshotBlob,
) -> Result<(), DbError> {
    retain_snapshot_blob_on(conn, blob)?;
    let object_id = blob.remote.object_id();
    let authority = serde_json::to_string(&blob.authority)
        .map_err(|error| DbError::context("serialize snapshot blob authority", error))?;
    for binding in &blob.bindings {
        conn.execute(
            "INSERT INTO row_blob_locators
         (table_name, row_id, column_name, row_stamp, audience_authority, remote_object_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(table_name, row_id, column_name, row_stamp) DO NOTHING",
            rusqlite::params![
                binding.table(),
                binding.row_id(),
                binding.column(),
                binding.row_stamp(),
                authority,
                object_id.to_string(),
            ],
        )
        .map_err(DbError::from)?;
        validate_stored_row_binding_on(conn, binding, &blob.authority, object_id)?;
    }
    Ok(())
}

/// Publication retains the snapshot's payloads without changing current row
/// bindings: unpublished edits may have replaced or removed those rows.
pub(crate) fn install_snapshot_blob_plans_on(
    conn: &Connection,
    blobs: &[PreparedSnapshotBlob],
) -> Result<(), DbError> {
    for blob in blobs {
        retain_snapshot_blob_on(conn, blob)?;
    }
    Ok(())
}

/// A locally reconstructed cut retains only its live Store blobs for the
/// accepted snapshot. Current rows may include later edits, and importing the
/// cut's entire inventory would restore objects reclaimed after that cut.
pub(crate) fn retain_reconstructed_snapshot_blobs_on(
    conn: &Connection,
    image: &Connection,
    tables: &[SyncedTable],
    snapshot: &StoreSnapshotRef,
) -> Result<(), DbError> {
    let gates = Gates::from_tables(image, tables)?;
    let owner = coven_protocol::remote_object::SnapshotObjectOwner::Store {
        metadata_slot: snapshot.object.slot().clone(),
    };
    for encoded in crate::query_mapped_rows(
        image,
        "SELECT remote_object_id FROM blob_locators ORDER BY remote_object_id",
        [],
        |row| row.get::<_, String>(0),
    )? {
        let object_id = encoded.parse()?;
        let captured = load_remote_object_on(image, object_id)?;
        let locator = crate::blob_records::carried_blob_locator(
            &captured,
            "reconstructed snapshot inventory",
        )?;
        if locator.audience() != RemoteAudience::Store {
            continue;
        }
        let stored =
            coven_protocol::blob::locator::StoredBlobRef::new(locator, captured.object().clone())?;
        match Database::stored_blob_reference_state_on(image, &gates, tables, &stored)? {
            crate::StoredBlobReferenceState::NotLiveRemote => continue,
            crate::StoredBlobReferenceState::Unresolved => {
                return Err(DbError::Message(format!(
                    "reconstructed snapshot blob {object_id} has unresolved locality"
                )));
            }
            crate::StoredBlobReferenceState::LiveRemote => {}
        }
        let mut remote = load_remote_object_on(conn, object_id)?;
        remote.merge_snapshot_owner(&stored, owner.clone())?;
        update_remote_object_on(conn, object_id, &remote)?;
    }
    Ok(())
}
