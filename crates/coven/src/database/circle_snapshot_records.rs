use crate::database::local_store_identity::local_store_authority_on;
use crate::protocol::circle::CircleId;
use crate::protocol::store_commit::{
    CircleSnapshotMeta, CircleSnapshotRef, ObjectHash, SnapshotImageRef,
};

use super::*;

pub(crate) fn load_published_circle_snapshot_on(
    conn: &Connection,
    circle_id: CircleId,
) -> Result<Option<PublishedCircleSnapshot>, DbError> {
    conn.query_row(
        "SELECT generation, snapshot_ref, successor_slot, cut, meta_bytes \
         FROM published_circle_snapshot WHERE circle_id = ?1 \
         ORDER BY generation DESC LIMIT 1",
        [circle_id.to_string()],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Vec<u8>>(4)?,
            ))
        },
    )
    .optional()
    .map_err(DbError::from)?
    .map(|(generation, reference, successor_slot, cut, bytes)| {
        let generation = u64::try_from(generation).map_err(|_| {
            DbError::Message("published Circle snapshot generation is negative".to_string())
        })?;
        let reference: CircleSnapshotRef = serde_json::from_str(&reference)
            .map_err(|error| DbError::context("published Circle snapshot ref", error))?;
        if reference.generation != generation {
            return Err(DbError::Message(
                "published Circle snapshot generation differs from its indexed generation"
                    .to_string(),
            ));
        }
        let successor_slot = serde_json::from_str(&successor_slot)
            .map_err(|error| DbError::context("published Circle snapshot successor slot", error))?;
        let authority = local_store_authority_on(conn)?;
        let author_ref = authority.reference();
        let author = authority.value();
        let meta = CircleSnapshotMeta::parse_at(
            &bytes,
            author.store_root.store_root_hash,
            &reference,
            author,
        )
        .map_err(|error| DbError::context("published Circle snapshot", error))?;
        let cut: crate::protocol::store_commit::CommitFrontier = serde_json::from_str(&cut)
            .map_err(|error| DbError::context("published Circle snapshot cut", error))?;
        if &meta.author_registration != author_ref
            || meta.circle_id != circle_id
            || meta.successor.next_slot != successor_slot
            || meta.bootstrap.coverage != cut
        {
            return Err(DbError::Message(
                "published Circle snapshot differs from its local stream state".to_string(),
            ));
        }
        Ok(PublishedCircleSnapshot {
            reference,
            successor_slot,
            cut,
        })
    })
    .transpose()
}

pub(crate) fn load_outbound_circle_snapshot_on(
    conn: &Connection,
    circle_id: CircleId,
) -> Result<Option<DurableCircleSnapshotPublication>, DbError> {
    conn.query_row(
        "SELECT snapshot_ref, meta_prepared, image_ref, image_prepared, image_bytes, meta_bytes, blobs \
         FROM outbound_circle_snapshot WHERE circle_id = ?1",
        [circle_id.to_string()],
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
            let reference: CircleSnapshotRef = serde_json::from_str(&reference).map_err(|error| {
                DbError::context("outbound Circle snapshot ref", error)
            })?;
            let meta_prepared: PreparedExactObject =
                serde_json::from_str(&meta_prepared).map_err(|error| {
                    DbError::context("outbound prepared Circle snapshot metadata", error)
                })?;
            let image_reference: SnapshotImageRef = serde_json::from_str(&image_reference)
                .map_err(|error| {
                    DbError::context("outbound Circle snapshot image ref", error)
                })?;
            let image_prepared: PreparedExactObject = serde_json::from_str(&image_prepared)
                .map_err(|error| {
                    DbError::context("outbound prepared Circle snapshot image", error)
                })?;
            let blobs: Vec<PreparedSnapshotBlob> = serde_json::from_str(&blobs).map_err(|error| {
                DbError::context("outbound prepared Circle snapshot blobs", error)
            })?;
            if meta_prepared.reference() != &reference.object
                || image_prepared.reference() != &image_reference.object
                || ObjectHash::digest(&image_bytes) != image_reference.image_hash
            {
                return Err(DbError::Message(
                    "outbound Circle snapshot exact references differ from prepared bytes"
                        .to_string(),
                ));
            }
            let authority = local_store_authority_on(conn)?;
            let author_ref = authority.reference();
            let author = authority.value();
            let meta = CircleSnapshotMeta::parse_at(
                &meta_bytes,
                author.store_root.store_root_hash,
                &reference,
                author,
            )
            .map_err(|error| DbError::context("outbound Circle snapshot", error))?;
            if &meta.author_registration != author_ref
                || meta.circle_id != circle_id
                || meta.bootstrap.image != image_reference
            {
                return Err(DbError::Message(
                    "outbound Circle snapshot metadata differs from its exact image".to_string(),
                ));
            }
            Ok(DurableCircleSnapshotPublication {
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
