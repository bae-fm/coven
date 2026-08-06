use crate::database::blob_records::live_blob_row;
use crate::database::blob_records::load_activated_registration_on;
use crate::database::blob_records::validate_live_blob_locator;
use crate::database::blob_records::validate_stored_locator_on;
use crate::database::blob_records::validate_stored_row_binding_on;
use crate::database::remote_object_records::load_remote_object_on;
use crate::database::PreparedSnapshotBlob;

use super::*;

pub(super) fn validate_snapshot_object_owners_on(
    conn: &Connection,
    root: &crate::protocol::store_commit::StoreRootRef,
    meta: &SnapshotMeta,
) -> Result<(), DbError> {
    let registration = load_activated_registration_on(conn, root, &meta.author_registration)?;
    let expected = crate::protocol::remote_object::SnapshotObjectOwner {
        activation: registration
            .store_snapshot_activation(&meta.author_registration)
            .map_err(|error| DbError::Message(error.to_string()))?
            .activation_id(),
        generation: meta.generation,
    };
    if meta.successor.activation != expected.activation {
        return Err(DbError::Message(
            "verified snapshot successor differs from its author stream activation".to_string(),
        ));
    }
    validate_snapshot_object_owner_records_on(conn, &expected)
}

pub(crate) async fn verify_snapshot_blob_spools(
    blobs: &[PreparedSnapshotBlob],
    label: &str,
) -> Result<(), DbError> {
    for blob in blobs {
        if let Some(spool_path) = &blob.spool_path {
            {
                let (size, digest) = coven_foundation::local_file::file_facts(spool_path)
                    .await
                    .map_err(|error| {
                        DbError::Message(format!("{label} snapshot blob spool: {error}"))
                    })?;
                blob.remote
                    .object()
                    .verify_stored_facts(
                        spool_path,
                        size,
                        crate::protocol::store_commit::ObjectHash::from_digest(digest),
                    )
                    .map_err(|error| {
                        DbError::context(format!("{label} snapshot blob spool"), error)
                    })?;
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_snapshot_author(
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

pub(crate) fn validate_snapshot_image(
    image: &SnapshotImageRef,
    prepared: &PreparedExactObject,
    image_bytes: &[u8],
    expected_slot: String,
    label: &str,
) -> Result<(), DbError> {
    if image.object == *prepared.reference()
        && ObjectHash::digest(image_bytes) == image.image_hash
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
    owner: &crate::protocol::remote_object::SnapshotObjectOwner,
    blobs: &[PreparedSnapshotBlob],
) -> Result<(), DbError> {
    for blob in blobs {
        blob.remote
            .validate()
            .map_err(|error| DbError::context("snapshot remote blob", error))?;
        let owners = blob.remote.snapshot_owners().collect::<Vec<_>>();
        if owners != [owner] {
            return Err(DbError::Message(
                "snapshot blob owner differs from the verified snapshot stream activation"
                    .to_string(),
            ));
        }
        if blob.bindings.is_empty()
            || blob.bindings.iter().any(|binding| {
                binding.blob().object() != blob.remote.object()
                    || binding.blob().locator().audience() != blob.authority.remote_audience()
            })
            || blob
                .spool_path
                .as_ref()
                .is_some_and(|path| !path.is_absolute())
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
                .map_err(|error| DbError::Message(error.to_string()))?;
            let audience = RemoteAudience::try_from(audience)
                .map_err(|error| DbError::Message(error.to_string()))?;
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
    image: &SnapshotImageRef,
    owner: crate::protocol::remote_object::SnapshotObjectOwner,
    label: &str,
) -> Result<(), DbError> {
    let image = RemoteObjectRecord::snapshot_activated_image(image, owner)
        .map_err(|error| DbError::context(format!("{label} ownership"), error))?;
    persist_exact_remote_object_on(conn, &image, label)
}

pub(crate) fn snapshot_generation_as_i64(generation: u64, label: &str) -> Result<i64, DbError> {
    i64::try_from(generation)
        .map_err(|_| DbError::Message(format!("{label} generation exceeds SQLite INTEGER")))
}

pub(super) fn validate_snapshot_object_owner_records_on(
    conn: &Connection,
    expected: &crate::protocol::remote_object::SnapshotObjectOwner,
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
            if owner.activation != expected.activation || owner.generation > expected.generation {
                return Err(DbError::Message(format!(
                    "snapshot remote object {object_id} belongs to another stream or a later generation"
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn install_snapshot_blob_plan_on(
    conn: &Connection,
    blob: &PreparedSnapshotBlob,
) -> Result<(), DbError> {
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
    conn.execute(
        "INSERT INTO blob_locators (remote_object_id, locator_hash) VALUES (?1, ?2)
         ON CONFLICT(remote_object_id) DO NOTHING",
        rusqlite::params![
            object_id.to_string(),
            blob.bindings[0].blob().locator().locator_hash().to_string(),
        ],
    )
    .map_err(DbError::from)?;
    validate_stored_locator_on(conn, blob.bindings[0].blob())?;
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

pub(crate) fn install_snapshot_blob_plans_on(
    conn: &Connection,
    blobs: &[PreparedSnapshotBlob],
) -> Result<(), DbError> {
    for blob in blobs {
        install_snapshot_blob_plan_on(conn, blob)?;
        if let Some(path) = &blob.spool_path {
            conn.execute(
                "INSERT INTO snapshot_blob_spool_cleanup (path) VALUES (?1)
                 ON CONFLICT(path) DO NOTHING",
                [path.to_string_lossy().as_ref()],
            )
            .map_err(DbError::from)?;
        }
    }
    Ok(())
}
