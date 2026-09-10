use super::*;

pub(crate) fn load_outbound_store_snapshot_on(
    conn: &Connection,
    store_dir: &coven_foundation::store_dir::StoreDir,
    authority: &coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
) -> Result<Option<DurableSnapshotPublication>, DbError> {
    conn.query_row(
        "SELECT snapshot_ref, meta_prepared, meta_bytes, blobs \
         FROM outbound_store_snapshot WHERE singleton = 1",
        [],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, String>(3)?,
            ))
        },
    )
    .optional()
    .map_err(DbError::from)?
    .map(|(reference, meta_prepared, meta_bytes, blobs)| {
        let reference: StoreSnapshotRef = serde_json::from_str(&reference)
            .map_err(|error| DbError::context("outbound Store snapshot ref", error))?;
        let meta_prepared: PreparedExactObject =
            serde_json::from_str(&meta_prepared).map_err(|error| {
                DbError::context("outbound prepared Store snapshot metadata", error)
            })?;
        let blobs: Vec<PreparedSnapshotBlob> = serde_json::from_str(&blobs)
            .map_err(|error| DbError::context("outbound prepared Store snapshot blobs", error))?;
        if meta_prepared.reference() != &reference.object {
            return Err(DbError::Message(
                "outbound Store snapshot exact references differ from prepared bytes".to_string(),
            ));
        }
        let author_ref = authority.reference();
        let author = authority.value();
        let meta = SnapshotMeta::parse_at(
            &meta_bytes,
            author.store_root.store_root_hash,
            &reference,
            author,
        )
        .map_err(|error| DbError::context("outbound Store snapshot", error))?;
        if &meta.author_registration != author_ref {
            return Err(DbError::Message(
                "outbound Store snapshot author differs from local activation".to_string(),
            ));
        }
        let active = crate::store::load_active_store_publication_on(conn)?.ok_or_else(|| {
            DbError::Message("outbound Store snapshot has no active publication".to_string())
        })?;
        if active.owner() != &ActiveStorePublicationOwner::Snapshot {
            return Err(DbError::Message(
                "outbound Store snapshot differs from the active publication owner".to_string(),
            ));
        }
        let publication = active.attempt()?.clone();
        publication
            .validate_snapshot_shape(&meta, &reference)
            .map_err(|error| DbError::context("outbound Store snapshot publication", error))?;
        let image =
            crate::snapshot_objects::load_snapshot_image_on(conn, store_dir, &meta.image, "Store")?;
        let rollup_reference = &meta.membership_rollup;
        let rollup_bytes = crate::payload_store::read_payload_blocking(
            conn,
            store_dir,
            rollup_reference.rollup_hash,
        )
        .map_err(|error| DbError::context("outbound membership rollup", error))?;
        let rollup_prepared = PreparedExactObject::new(
            rollup_reference.object.clone(),
            crate::payload_store::read_payload_blocking(
                conn,
                store_dir,
                rollup_reference.object.stored_hash(),
            )
            .map_err(|error| DbError::context("outbound prepared membership rollup", error))?,
        )
        .map_err(|error| DbError::context("outbound prepared membership rollup", error))?;
        let rollup = coven_protocol::store_commit::MembershipRollup::parse_at(
            &rollup_bytes,
            author.store_root.store_root_hash,
            rollup_reference,
            author,
        )
        .map_err(|error| DbError::context("outbound membership rollup", error))?;
        Ok(DurableSnapshotPublication {
            reference,
            publication,
            rollup: ExactProtocolObject {
                value: rollup,
                bytes: rollup_bytes,
                prepared: rollup_prepared,
            },
            meta: ExactProtocolObject {
                value: meta,
                bytes: meta_bytes,
                prepared: meta_prepared,
            },
            image,
            blobs,
        })
    })
    .transpose()
}
