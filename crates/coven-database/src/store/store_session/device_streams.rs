use crate::*;
use coven_protocol::store_commit::{SnapshotMeta, StoreAck, StoreAckRef, StoreSnapshotRef};

use super::*;

impl StoreDatabase {
    /// Record where the local device's published streams stand on the
    /// provider: the acknowledgement head the pulled history activated for it
    /// and the snapshot its stream ends on. A restore that adopts a
    /// registration the device registered in an earlier life finds the
    /// registration's own first slots already written, so its streams resume
    /// from these heads rather than restarting there.
    ///
    /// The acknowledgement only ever advances the recorded head; the snapshot
    /// is recorded when the local stream is empty and must match when it is
    /// not.
    pub async fn resume_local_device_streams(
        &self,
        latest_ack: (StoreAckRef, StoreAck),
        latest_snapshot: Option<(StoreSnapshotRef, SnapshotMeta)>,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.resume_local_device_streams(latest_ack, latest_snapshot)
        })
        .await
    }
}

impl StoreSession<'_> {
    fn resume_local_device_streams(
        &mut self,
        (latest_ack_ref, latest_ack): (StoreAckRef, StoreAck),
        latest_snapshot: Option<(StoreSnapshotRef, SnapshotMeta)>,
    ) -> Result<(), DbError> {
        let root = self.required_root_authority()?;
        let Some(registration_ref) = local_activated_registration_ref_on(self.conn)? else {
            return Err(DbError::Message(
                "resuming device streams requires a local activated registration".into(),
            ));
        };
        let activated = self.activated_registration(&registration_ref)?;
        if latest_ack_ref.registration != registration_ref
            || latest_ack.registration != registration_ref
            || latest_ack.sequence != latest_ack_ref.sequence
        {
            return Err(DbError::Message(
                "resumed acknowledgement head belongs to another registration".into(),
            ));
        }
        let verified = StoreAck::parse_at(
            &latest_ack.to_bytes(),
            &root,
            &latest_ack_ref,
            activated.value(),
        )
        .map_err(DbError::from)?;
        if verified != latest_ack {
            return Err(DbError::Message(
                "resumed acknowledgement head changed during exact verification".into(),
            ));
        }
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        let recorded = load_published_store_ack_on(&tx)?;
        if recorded
            .as_ref()
            .is_none_or(|recorded| recorded.reference.sequence < latest_ack_ref.sequence)
        {
            let ack_ref = serde_json::to_string(&latest_ack_ref)
                .map_err(|error| DbError::context("resumed acknowledgement head", error))?;
            let successor = serde_json::to_string(&latest_ack.successor.next_slot)
                .map_err(|error| DbError::context("resumed acknowledgement successor", error))?;
            tx.execute(
                "INSERT INTO published_store_acks (singleton, ack_ref, successor_slot) \
                     VALUES (1, ?1, ?2) \
                 ON CONFLICT (singleton) DO UPDATE SET \
                     ack_ref = excluded.ack_ref, successor_slot = excluded.successor_slot, \
                     standing = NULL",
                (&ack_ref, &successor),
            )
            .map_err(DbError::from)?;
        }
        let existing_snapshot = load_published_store_snapshot_on(&tx, &activated)?;
        match (existing_snapshot, latest_snapshot) {
            (None, None) => {}
            (None, Some((reference, meta))) => {
                let verified = SnapshotMeta::parse_stream_entry_at(
                    &meta.to_bytes(),
                    &root,
                    &registration_ref,
                    activated.value(),
                    &reference,
                )
                .map_err(DbError::from)?;
                if verified != meta {
                    return Err(DbError::Message(
                        "resumed snapshot head changed during exact verification".into(),
                    ));
                }
                let generation = i64::try_from(reference.generation).map_err(|_| {
                    DbError::Message("resumed snapshot generation exceeds SQLite INTEGER".into())
                })?;
                tx.execute(
                    "INSERT INTO published_store_snapshot \
                         (generation, snapshot_ref, successor_slot, meta_bytes) \
                         VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        generation,
                        serde_json::to_string(&reference)
                            .map_err(|error| DbError::context("resumed snapshot ref", error))?,
                        serde_json::to_string(&meta.successor.next_slot).map_err(|error| {
                            DbError::context("resumed snapshot successor", error)
                        })?,
                        meta.to_bytes(),
                    ],
                )
                .map_err(DbError::from)?;
            }
            (Some(existing), Some((reference, meta))) => {
                if existing.reference != reference || existing.meta != meta {
                    return Err(DbError::Message(
                        "local snapshot stream differs from the provider's head".into(),
                    ));
                }
            }
            (Some(_), None) => {
                return Err(DbError::Message(
                    "local snapshot stream names a snapshot the provider does not hold".into(),
                ));
            }
        }
        tx.commit().map_err(DbError::from)
    }
}
