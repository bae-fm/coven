use coven_protocol::circle_activation::VerifiedCircleReference;
use coven_protocol::hlc::{Timestamp, HIGHWATER_STATE_KEY};

use super::merge_materialization_transaction::IncomingTimestampPolicy;
use super::StoreTransaction;
use crate::DbError;

/// Collect at live acceptance, independently of whether the commit carries rows.
/// Replay must not reconsider a received timestamp against a later wall clock.
pub(super) fn observe_circle_metadata<'a>(
    floor: &mut Option<Timestamp>,
    activations: impl IntoIterator<Item = &'a VerifiedCircleReference>,
    policy: IncomingTimestampPolicy,
) -> Result<(), DbError> {
    for active in activations.into_iter().filter_map(|activation| {
        activation
            .local_access
            .as_ref()
            .and_then(|access| access.active.as_ref())
    }) {
        let raw = &active.metadata.metadata_stamp;
        let stamp = Timestamp::parse(raw).ok_or_else(|| {
            DbError::Message(format!("Circle metadata stamp is invalid: {raw:?}"))
        })?;
        if policy
            .received_wall_ms()
            .is_some_and(|wall| !stamp.is_within_future_bound(wall))
        {
            return Err(DbError::Message(format!(
                "Circle metadata stamp exceeds the receiver clock allowance: {raw:?}"
            )));
        }
        if floor.as_ref().is_none_or(|current| stamp > *current) {
            *floor = Some(stamp);
        }
    }
    Ok(())
}

impl StoreTransaction<'_, '_> {
    /// Every writer raises the same persisted floor. An older asynchronous flush
    /// cannot replace a newer floor recorded by an acceptance transaction.
    pub(super) fn raise_clock_floor(self, incoming: &Timestamp) -> Result<(), DbError> {
        let current = crate::get_protocol_state_on(self.transaction, HIGHWATER_STATE_KEY)?
            .map(|raw| {
                Timestamp::parse(&raw).ok_or_else(|| {
                    DbError::Message(format!("corrupt HLC high-water mark: {raw:?}"))
                })
            })
            .transpose()?;
        if current.as_ref().is_none_or(|current| incoming > current) {
            crate::set_protocol_state_on(
                self.transaction,
                HIGHWATER_STATE_KEY,
                &incoming.to_string(),
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::store_session::{StoreRecords, StoreTransactionOutcome};

    #[tokio::test]
    async fn delayed_clock_flush_cannot_lower_a_persisted_acceptance_floor() {
        let database = crate::Database::open(
            std::path::Path::new(":memory:"),
            Vec::new(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "clock-flush".to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            crate::CovenMigrationPolicy::ApplyPending,
            &[],
        )
        .expect("open clock database");
        let store = crate::StoreDatabase::new(&database);
        let captured = store.stamp();
        let mut accepted = Timestamp::parse(&captured).expect("parse captured timestamp");
        accepted.millis += 1;
        let expected = accepted.to_string();
        store
            .call_store(move |session| {
                StoreRecords::new(session.conn, session.store_dir).transaction(|transaction| {
                    transaction.raise_clock_floor(&accepted)?;
                    Ok(StoreTransactionOutcome::Commit(()))
                })
            })
            .await
            .expect("persist accepted floor");
        store
            .call_store(move |session| session.persist_clock_floor(&captured))
            .await
            .expect("finish the older captured flush");
        assert_eq!(
            store
                .get_protocol_state(HIGHWATER_STATE_KEY)
                .await
                .expect("read persisted floor"),
            Some(expected)
        );
    }
}
