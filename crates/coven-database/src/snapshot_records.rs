use super::*;

pub(crate) fn load_published_store_snapshot_on(
    conn: &Connection,
    authority: &coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
) -> Result<Option<PublishedStoreSnapshot>, DbError> {
    conn.query_row(
        "SELECT generation, snapshot_ref, successor_slot, meta_bytes \
         FROM published_store_snapshot ORDER BY generation DESC LIMIT 1",
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Vec<u8>>(3)?,
            ))
        },
    )
    .optional()
    .map_err(DbError::from)?
    .map(|(generation, reference, successor_slot, bytes)| {
        let generation = u64::try_from(generation).map_err(|_| {
            DbError::Message("published Store snapshot generation is negative".to_string())
        })?;
        let reference: StoreSnapshotRef = serde_json::from_str(&reference)
            .map_err(|error| DbError::context("published Store snapshot ref", error))?;
        if reference.generation != generation {
            return Err(DbError::Message(
                "published Store snapshot generation differs from its indexed generation"
                    .to_string(),
            ));
        }
        let successor_slot = serde_json::from_str(&successor_slot)
            .map_err(|error| DbError::context("published Store snapshot successor slot", error))?;
        let author_ref = authority.reference();
        let author = authority.value();
        let meta = SnapshotMeta::parse_at(
            &bytes,
            author.store_root.store_root_hash,
            &reference,
            author,
        )
        .map_err(|error| DbError::context("published Store snapshot", error))?;
        if &meta.author_registration != author_ref || meta.successor.next_slot != successor_slot {
            return Err(DbError::Message(
                "published Store snapshot differs from its local stream state".to_string(),
            ));
        }
        Ok(PublishedStoreSnapshot {
            reference,
            successor_slot,
            meta,
        })
    })
    .transpose()
}

pub(crate) fn load_outbound_store_snapshot_on(
    conn: &Connection,
    store_dir: &coven_foundation::store_dir::StoreDir,
    authority: &coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
) -> Result<Option<DurableSnapshotPublication>, DbError> {
    conn.query_row(
        "SELECT snapshot_ref, meta_prepared, image_ref, meta_bytes, blobs \
         FROM outbound_store_snapshot WHERE singleton = 1",
        [],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Vec<u8>>(3)?,
                row.get::<_, String>(4)?,
            ))
        },
    )
    .optional()
    .map_err(DbError::from)?
    .map(
        |(reference, meta_prepared, image_reference, meta_bytes, blobs)| {
            let reference: StoreSnapshotRef = serde_json::from_str(&reference)
                .map_err(|error| DbError::context("outbound Store snapshot ref", error))?;
            let meta_prepared: PreparedExactObject =
                serde_json::from_str(&meta_prepared).map_err(|error| {
                    DbError::context("outbound prepared Store snapshot metadata", error)
                })?;
            let image_reference: SnapshotImageRef = serde_json::from_str(&image_reference)
                .map_err(|error| DbError::context("outbound Store snapshot image ref", error))?;
            let image_bytes = crate::payload_store::read_payload_blocking(
                conn,
                store_dir,
                image_reference.image_hash,
            )
            .map_err(|error| DbError::context("outbound Store snapshot image", error))?;
            let image_prepared = PreparedExactObject::new(
                image_reference.object.clone(),
                crate::payload_store::read_payload_blocking(
                    conn,
                    store_dir,
                    image_reference.object.stored_hash(),
                )
                .map_err(|error| {
                    DbError::context("outbound prepared Store snapshot image", error)
                })?,
            )
            .map_err(|error| DbError::context("outbound prepared Store snapshot image", error))?;
            let blobs: Vec<PreparedSnapshotBlob> =
                serde_json::from_str(&blobs).map_err(|error| {
                    DbError::context("outbound prepared Store snapshot blobs", error)
                })?;
            if meta_prepared.reference() != &reference.object
                || image_prepared.reference() != &image_reference.object
                || ObjectHash::digest(&image_bytes) != image_reference.image_hash
            {
                return Err(DbError::Message(
                    "outbound Store snapshot exact references differ from prepared bytes"
                        .to_string(),
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
            if &meta.author_registration != author_ref || meta.image != image_reference {
                return Err(DbError::Message(
                    "outbound Store snapshot metadata differs from its exact image".to_string(),
                ));
            }
            Ok(DurableSnapshotPublication {
                reference,
                meta: ExactProtocolObject {
                    value: meta,
                    bytes: meta_bytes,
                    prepared: meta_prepared,
                },
                image: PreparedProtocolObject {
                    value: image_bytes,
                    prepared: image_prepared,
                },
                blobs,
            })
        },
    )
    .transpose()
}
