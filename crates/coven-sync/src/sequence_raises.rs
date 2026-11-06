//! A breaking migration's raises: each audience it captured is raised to
//! its version through the operation path (§18.3, §18.5).
use crate::{
    sequence_sync::sequence_operations::{Request, SequenceOperation},
    sequence_sync::snapshot_capture::SnapshotRequest,
    sequence_sync::{SequenceSync, Session},
    SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::operation_kind;
use coven_format::sequence::Purpose;
use std::collections::BTreeSet;

impl SequenceSync {
    /// Supply to `DatabaseBuilder::migration_operation` at the composition
    /// root: the breaking migration's version commits with its schema, and
    /// passes raise each audience it captured to that version (§18.3).
    pub fn migration_operation(
        version: u32,
    ) -> Result<coven_database::NewOperation, coven_database::DbError> {
        Ok(coven_database::NewOperation {
            kind: operation_kind::SCHEMA_RAISE.into(),
            data: serde_json::to_vec(&version).map_err(coven_database::DbError::StateEncoding)?,
            started_by: operation_kind::COVEN.into(),
            pauses_writes: false,
        })
    }

    /// Raise every captured audience to its migration's version through the
    /// operation path, then finish the migration's journal. An audience
    /// another device already raised to that version, or a circle deleted
    /// since the capture, keeps no capture (§15.9, §18.5).
    pub(crate) async fn publish_raises(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        let records: Vec<_> = self
            .database
            .operations()
            .await?
            .into_iter()
            .filter(|record| record.kind == operation_kind::SCHEMA_RAISE)
            .collect();
        if records.is_empty() || !self.registered(session) {
            return Ok(());
        }
        let pending: BTreeSet<_> = self
            .database
            .operations()
            .await?
            .into_iter()
            .filter(|record| record.kind == operation_kind::SEQUENCE)
            .map(|record| serde_json::from_slice::<SequenceOperation>(&record.data))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter_map(|work| match work.request {
                Request::Snapshot(request) if matches!(request.purpose, Purpose::Raise(_)) => {
                    Some(request.audience)
                }
                _ => None,
            })
            .collect();
        for capture in self.database.raise_captures().await? {
            if !session.history.state.awaits_raise(&capture.audience, capture.version) {
                self.database.discard_raise_capture(capture.audience, capture.version).await?;
                continue;
            }
            if pending.contains(&capture.audience) {
                continue;
            }
            // The raise's snapshot covers the captured writes and the
            // marker, so it starts once they have landed. A write a bound
            // upload holds keeps it waiting until the file is stored, and
            // the upload's own record says why (§17.4, §18.5).
            if self.database.unconfirmed_own_prefix(capture.through).await?.is_some() {
                continue;
            }
            let result = self
                .start_request(
                    session,
                    ring,
                    member,
                    Request::Snapshot(SnapshotRequest {
                        audience: capture.audience.clone(),
                        purpose: Purpose::Raise(capture.version),
                    }),
                    operation_kind::COVEN,
                )
                .await
                .and_then(crate::operation_outcome::into_slot)
                .map(drop);
            self.own_work(result, |request| {
                matches!(request, Request::Snapshot(raise) if raise.audience == capture.audience)
            })
            .await?;
        }
        if self.database.raise_captures().await?.is_empty() {
            for record in records {
                self.database.finish_operation(record.id).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "sequence_raises_tests.rs"]
mod tests;
