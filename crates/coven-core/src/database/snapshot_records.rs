use crate::database::local_store_identity::local_store_authority_on;

use super::*;

pub(super) fn load_published_store_snapshot_on(
    conn: &Connection,
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
            .map_err(|error| DbError::Message(format!("published Store snapshot ref: {error}")))?;
        if reference.generation != generation {
            return Err(DbError::Message(
                "published Store snapshot generation differs from its indexed generation"
                    .to_string(),
            ));
        }
        let successor_slot = serde_json::from_str(&successor_slot).map_err(|error| {
            DbError::Message(format!("published Store snapshot successor slot: {error}"))
        })?;
        let (root, author_ref, author) = local_store_authority_on(conn)?;
        let meta = SnapshotMeta::parse_at(&bytes, root.store_root_hash, &reference, &author)
            .map_err(|error| DbError::Message(format!("published Store snapshot: {error}")))?;
        if meta.author_registration != author_ref || meta.successor.next_slot != successor_slot {
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

pub(super) fn load_outbound_store_snapshot_on(
    conn: &Connection,
) -> Result<Option<DurableSnapshotPublication>, DbError> {
    conn.query_row(
        "SELECT snapshot_ref, meta_prepared, image_ref, image_prepared, image_bytes, meta_bytes, blobs \
         FROM outbound_store_snapshot WHERE singleton = 1",
        [],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Vec<u8>>(4)?,
                row.get::<_, Vec<u8>>(5)?,
                row.get::<_, String>(6)?,
            ))
        },
    )
    .optional()
    .map_err(DbError::from)?
    .map(
        |(reference, meta_prepared, image_reference, image_prepared, image_bytes, meta_bytes, blobs)| {
            let reference: StoreSnapshotRef =
                serde_json::from_str(&reference).map_err(|error| {
                    DbError::Message(format!("outbound Store snapshot ref: {error}"))
                })?;
            let meta_prepared: PreparedExactObject =
                serde_json::from_str(&meta_prepared).map_err(|error| {
                    DbError::Message(format!(
                        "outbound prepared Store snapshot metadata: {error}"
                    ))
                })?;
            let image_reference: SnapshotImageRef = serde_json::from_str(&image_reference)
                .map_err(|error| {
                    DbError::Message(format!("outbound Store snapshot image ref: {error}"))
                })?;
            let image_prepared: PreparedExactObject = serde_json::from_str(&image_prepared)
                .map_err(|error| {
                    DbError::Message(format!("outbound prepared Store snapshot image: {error}"))
                })?;
            let blobs: Vec<PreparedSnapshotBlob> = serde_json::from_str(&blobs).map_err(|error| {
                DbError::Message(format!("outbound prepared Store snapshot blobs: {error}"))
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
            let (root, author_ref, author) = local_store_authority_on(conn)?;
            let meta =
                SnapshotMeta::parse_at(&meta_bytes, root.store_root_hash, &reference, &author)
                    .map_err(|error| {
                        DbError::Message(format!("outbound Store snapshot: {error}"))
                    })?;
            if meta.author_registration != author_ref || meta.image != image_reference {
                return Err(DbError::Message(
                    "outbound Store snapshot metadata differs from its exact image".to_string(),
                ));
            }
            Ok(DurableSnapshotPublication {
                reference,
                meta: ExactProtocolObject {
                    value: meta,
                    bytes: meta_bytes,
                    object: meta_prepared.reference().clone(),
                    prepared: meta_prepared,
                },
                image: ExactProtocolObject {
                    value: image_bytes.clone(),
                    bytes: image_bytes,
                    object: image_prepared.reference().clone(),
                    prepared: image_prepared,
                },
                blobs,
            })
        },
    )
    .transpose()
}
