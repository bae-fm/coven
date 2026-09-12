use super::circle_bootstrap_rows::StagedCircleRows;
use crate::DbError;
use coven_protocol::circle_activation::VerifiedCircleImage;
use coven_protocol::remote_object;
use coven_protocol::store_commit::StoreBatchCommitRef;
use coven_protocol::synced_schema::SyncedTable;

/// Install one verified Circle bootstrap's rows and blob graph onto
/// `conn` directly — no transaction of its own. `conn` is the caller's active
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
    let staged = StagedCircleRows::stage(conn, bootstrap.image_bytes(), synced_tables)
        .map_err(|error| DbError::context("stage retained Circle bootstrap rows", error))?;
    staged.install_on(
        conn,
        synced_tables,
        activation_commit,
        bootstrap.circle_id(),
        bootstrap.reference(),
    )
}

pub(crate) fn install_circle_bootstrap_remote_objects_on(
    conn: &rusqlite::Connection,
    activation_commit: &StoreBatchCommitRef,
    bootstrap: &VerifiedCircleImage,
) -> Result<(), DbError> {
    install_circle_bootstrap_remote_objects_from_reference_on(
        conn,
        activation_commit,
        bootstrap.reference(),
    )
}

pub(super) fn install_circle_bootstrap_remote_objects_from_reference_on(
    conn: &rusqlite::Connection,
    activation_commit: &StoreBatchCommitRef,
    reference: &coven_protocol::circle::CircleBootstrapRef,
) -> Result<(), DbError> {
    for binding in &reference.blobs {
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
            let mut remote = crate::load_remote_object_on(conn, object_id)?;
            remote
                .merge_blob_activation(stored, activation_commit)
                .map_err(DbError::from)?;
            remote
        } else {
            remote_object::RemoteObjectRecord::activated_blob(stored, activation_commit.clone())
                .map_err(DbError::from)?
                .into_record()
        };
        conn.execute(
            "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)
             ON CONFLICT(object_id) DO UPDATE SET state = excluded.state",
            rusqlite::params![
                object_id.to_string(),
                serde_json::to_string(&remote).map_err(|error| {
                    DbError::context("serialize Circle bootstrap blob", error)
                })?,
            ],
        )
        .map_err(DbError::from)?;
        crate::blob_records::record_stored_locator_on(conn, stored)?;
    }
    Ok(())
}
