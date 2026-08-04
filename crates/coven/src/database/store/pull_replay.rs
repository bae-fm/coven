use crate::database::DbError;
use crate::protocol::remote_object;
use crate::protocol::store_commit::StoreBatchCommitRef;
use crate::sync::VerifiedCircleImage;
use crate::SyncedTable;

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
        let crate::protocol::blob::RowBlobAuthority::Remote(authority) = binding.authority() else {
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
