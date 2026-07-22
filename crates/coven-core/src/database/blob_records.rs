use crate::database::cloud_outbox_records::created_upload_handoff_on;
use crate::database::remote_object_records::load_remote_object_on;

use super::*;

pub(super) struct LiveBlobRow {
    pub(super) stamp: String,
    pub(super) blob_id: String,
    pub(super) plaintext_size: u64,
    pub(super) plaintext_hash: ObjectHash,
    pub(super) cloud_path: Option<String>,
}

pub(super) fn live_blob_row(
    conn: &Connection,
    table: &str,
    row_id: &str,
    declaration: &crate::sync::session::BlobDecl,
) -> Result<Option<LiveBlobRow>, DbError> {
    let cloud_path = declaration
        .cloud_path_column
        .as_deref()
        .map(quote_ident)
        .unwrap_or_else(|| "NULL".to_string());
    let sql = format!(
        "SELECT {}, {}, {}, {}, {} FROM {} WHERE {} = ?1",
        quote_ident(&declaration.id_column),
        quote_ident(&declaration.size_column),
        quote_ident(&declaration.hash_column),
        cloud_path,
        quote_ident("_updated_at"),
        quote_ident(table),
        quote_ident("id"),
    );
    let raw = conn
        .query_row(&sql, [row_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .optional()
        .map_err(DbError::from)?;
    let Some((blob_id, plaintext_size, plaintext_hash, cloud_path, stamp)) = raw else {
        return Ok(None);
    };
    let plaintext_size = u64::try_from(plaintext_size).map_err(|_| {
        DbError::Message(format!(
            "winning blob row {:?}/{:?} has negative plaintext size {plaintext_size}",
            table, row_id
        ))
    })?;
    let plaintext_hash = plaintext_hash.parse().map_err(|error| {
        DbError::Message(format!(
            "winning blob row {:?}/{:?} has invalid plaintext hash: {error}",
            table, row_id
        ))
    })?;
    Ok(Some(LiveBlobRow {
        stamp,
        blob_id,
        plaintext_size,
        plaintext_hash,
        cloud_path,
    }))
}

pub(super) fn validate_live_blob_row(
    binding: &RowBlobLocatorBinding,
    declaration: &crate::sync::session::BlobDecl,
    row: &LiveBlobRow,
    live_audience: &RemoteAudience,
) -> Result<(), DbError> {
    validate_live_blob_locator(
        binding.table(),
        binding.row_id(),
        binding.column(),
        binding.row_stamp(),
        binding.blob(),
        declaration,
        row,
        live_audience,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn validate_live_blob_locator(
    table: &str,
    row_id: &str,
    column: &str,
    row_stamp: &str,
    stored: &StoredBlobRef,
    declaration: &crate::sync::session::BlobDecl,
    row: &LiveBlobRow,
    live_audience: &RemoteAudience,
) -> Result<(), DbError> {
    let locator = stored.locator();
    let invalid = locator.namespace() != declaration.namespace
        || locator.blob_id() != row.blob_id
        || locator.plaintext_size() != row.plaintext_size
        || locator.plaintext_hash() != row.plaintext_hash
        || &locator.audience() != live_audience
        || locator
            .scope()
            .is_some_and(|scope| scope != &declaration.scope)
        || locator
            .cloud_path()
            .is_some_and(|path| row.cloud_path.as_deref() != Some(path));
    if invalid {
        return Err(DbError::Message(format!(
            "blob locator does not match winning row values for {:?}/{:?}/{:?} at {:?}",
            table, row_id, column, row_stamp
        )));
    }
    Ok(())
}

pub(super) fn validate_stored_locator_on(
    conn: &Connection,
    expected: &StoredBlobRef,
) -> Result<(), DbError> {
    let locator_hash = expected.locator().locator_hash().to_string();
    let expected_remote_object_id = remote_object_id(expected.object());
    let stored_locator_hash: String = conn
        .query_row(
            "SELECT locator_hash FROM blob_locators WHERE remote_object_id = ?1",
            [expected_remote_object_id.to_string()],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    if stored_locator_hash != locator_hash {
        return Err(DbError::Message(format!(
            "stored blob object {expected_remote_object_id} is indexed under locator {stored_locator_hash}, expected {locator_hash}"
        )));
    }
    let remote = load_remote_object_on(conn, expected_remote_object_id)?;
    if !remote.is_activated_stored_blob() {
        return Err(DbError::Message(format!(
            "stored blob locator {locator_hash} does not reference activated ownership"
        )));
    }
    let locator =
        BlobLocator::parse(remote.bytes().canonical_semantic_bytes()).map_err(|error| {
            DbError::Message(format!(
                "stored blob locator {locator_hash} is invalid: {error}"
            ))
        })?;
    let actual = StoredBlobRef::new(locator, remote.object().clone()).map_err(|error| {
        DbError::Message(format!(
            "stored blob reference {locator_hash} is invalid: {error}"
        ))
    })?;
    if &actual != expected {
        return Err(DbError::Message(format!(
            "blob object {expected_remote_object_id} differs from its exact stored reference"
        )));
    }
    Ok(())
}

pub(super) fn validate_stored_row_binding_on(
    conn: &Connection,
    binding: &RowBlobLocatorBinding,
    expected_authority: &crate::sync::audience_package::PackageAudience,
    expected_remote_object_id: ObjectHash,
) -> Result<(), DbError> {
    let (audience_authority, remote_object_id): (String, String) = conn
        .query_row(
            "SELECT audience_authority, remote_object_id FROM row_blob_locators
             WHERE table_name = ?1 AND row_id = ?2 AND column_name = ?3 AND row_stamp = ?4",
            rusqlite::params![
                binding.table(),
                binding.row_id(),
                binding.column(),
                binding.row_stamp(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(DbError::from)?;
    let actual_authority: crate::sync::audience_package::PackageAudience =
        serde_json::from_str(&audience_authority).map_err(|error| {
            DbError::Message(format!("parse stored row blob audience authority: {error}"))
        })?;
    if &actual_authority != expected_authority
        || remote_object_id != expected_remote_object_id.to_string()
    {
        return Err(DbError::Message(format!(
            "row blob binding {:?}/{:?}/{:?} at {:?} is already bound to different exact content",
            binding.table(),
            binding.row_id(),
            binding.column(),
            binding.row_stamp()
        )));
    }
    Ok(())
}

pub(super) fn load_prepared_audience_objects_on(
    conn: &Connection,
    write_id: &WriteId,
) -> Result<PreparedAudienceObjects, DbError> {
    let mut package_statement = conn
        .prepare(
            "SELECT remote_object_id FROM store_write_packages
             WHERE write_id = ?1 ORDER BY audience",
        )
        .map_err(DbError::from)?;
    let package_ids = package_statement
        .query_map([write_id.as_str()], |row| row.get::<_, String>(0))
        .map_err(DbError::from)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(DbError::from)?;
    let mut blob_statement = conn
        .prepare(
            "SELECT remote_object_id, audience, locator_hash, spool_path FROM store_write_blobs
             WHERE write_id = ?1 ORDER BY audience, remote_object_id",
        )
        .map_err(DbError::from)?;
    let blob_rows = blob_statement
        .query_map([write_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .map_err(DbError::from)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(DbError::from)?;
    let packages = package_ids
        .into_iter()
        .map(|encoded| {
            let object_id = encoded
                .parse()
                .map_err(|error| DbError::Message(format!("stored remote object id: {error}")))?;
            PreparedAudiencePackage::from_remote(load_remote_object_on(conn, object_id)?)
        })
        .collect::<Result<Vec<_>, DbError>>()?;
    let blobs = blob_rows
        .into_iter()
        .map(|(encoded, audience, locator_hash, spool_path)| {
            let object_id = encoded
                .parse()
                .map_err(|error| DbError::Message(format!("stored remote object id: {error}")))?;
            PreparedAudienceBlob::from_remote(
                parse_remote_audience_db(&audience)?,
                &locator_hash,
                load_remote_object_on(conn, object_id)?,
                spool_path.map(PathBuf::from),
            )
        })
        .collect::<Result<Vec<_>, DbError>>()?;
    Ok(PreparedAudienceObjects { packages, blobs })
}

pub(crate) fn load_activated_registration_on(
    conn: &Connection,
    root: &crate::sync::store_commit::StoreRootRef,
    reference: &StoreDeviceRegistrationRef,
) -> Result<StoreDeviceRegistration, DbError> {
    let (bytes, encoded): (Vec<u8>, String) = conn
        .query_row(
            "SELECT registration_bytes, registration_object \
             FROM store_device_registration_activations \
             WHERE device_id = ?1 AND registration_hash = ?2",
            (
                reference.device_id.to_string(),
                reference.registration_hash.to_string(),
            ),
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(DbError::from)?;
    let stored: StoreDeviceRegistrationRef = serde_json::from_str(&encoded)
        .map_err(|error| DbError::Message(format!("activated Store registration ref: {error}")))?;
    if stored != *reference {
        return Err(DbError::Message(
            "activated Store registration differs from its exact reference".to_string(),
        ));
    }
    let registration = StoreDeviceRegistration::parse_at(&bytes, root, reference.device_id)
        .map_err(|error| DbError::Message(format!("activated Store registration: {error}")))?;
    reference
        .verify_registration(&registration)
        .map_err(|error| DbError::Message(error.to_string()))?;
    Ok(registration)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn previous_row_blob_for_write_on(
    conn: &Connection,
    table: &str,
    row_id: &str,
    row_stamp: &str,
    column: &str,
    blob: &BlobRef,
    plaintext_size: u64,
    plaintext_hash: ObjectHash,
) -> Result<Option<StoreWriteRemoteBlob>, DbError> {
    if let Some(handoff) = created_upload_handoff_on(conn, table, row_id, column, row_stamp)? {
        let locator = handoff.stored.locator();
        if locator.namespace() != blob.namespace
            || locator.blob_id() != blob.id
            || locator.plaintext_size() != plaintext_size
            || locator.plaintext_hash() != plaintext_hash
            || locator.scope().is_some_and(|scope| scope != &blob.scope)
        {
            return Err(DbError::Message(format!(
                "created upload {table}/{row_id}/{column} at {row_stamp} differs from its captured row"
            )));
        }
        return Ok(Some(handoff));
    }
    let raw = conn
        .query_row(
            "SELECT row_blob_locators.audience_authority, blob_locators.remote_object_id
             FROM row_blob_locators
             JOIN blob_locators USING (remote_object_id)
             WHERE table_name = ?1 AND row_id = ?2 AND column_name = ?3
             ORDER BY row_stamp DESC LIMIT 1",
            rusqlite::params![table, row_id, column],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(DbError::from)?;
    let Some((authority, object_id)) = raw else {
        return Ok(None);
    };
    let authority: crate::sync::audience_package::PackageAudience =
        serde_json::from_str(&authority)
            .map_err(|error| DbError::Message(format!("prior row blob authority: {error}")))?;
    let object_id = object_id
        .parse()
        .map_err(|error| DbError::Message(format!("prior row blob object id: {error}")))?;
    let remote = load_remote_object_on(conn, object_id)?;
    if !remote.is_activated_stored_blob() {
        return Err(DbError::Message(format!(
            "prior row blob {table}/{row_id}/{column} is not activated"
        )));
    }
    let locator = BlobLocator::parse(remote.bytes().canonical_semantic_bytes())
        .map_err(|error| DbError::Message(format!("prior row blob locator: {error}")))?;
    if locator.namespace() != blob.namespace
        || locator.blob_id() != blob.id
        || locator.plaintext_size() != plaintext_size
        || locator.plaintext_hash() != plaintext_hash
        || locator.scope().is_some_and(|scope| scope != &blob.scope)
    {
        return Ok(None);
    }
    if locator.audience() != authority.remote_audience() {
        return Err(DbError::Message(format!(
            "prior row blob {table}/{row_id}/{column} authority differs from its locator"
        )));
    }
    let stored = StoredBlobRef::new(locator, remote.object().clone())
        .map_err(|error| DbError::Message(format!("prior row blob reference: {error}")))?;
    Ok(Some(StoreWriteRemoteBlob { authority, stored }))
}

pub(super) fn remote_audience_to_db(audience: &RemoteAudience) -> String {
    match audience {
        RemoteAudience::Store => "store".to_string(),
        RemoteAudience::Circle(circle_id) => circle_id.to_string(),
    }
}

pub(super) fn parse_remote_audience_db(value: &str) -> Result<RemoteAudience, DbError> {
    if value == "store" {
        return Ok(RemoteAudience::Store);
    }
    value.parse().map(RemoteAudience::Circle).map_err(|error| {
        DbError::Message(format!("invalid stored blob audience {value:?}: {error}"))
    })
}
