use super::*;

use async_trait::async_trait;

use coven_protocol::objects::StorageError;
use coven_protocol::provider::{ProviderProbeId, ProviderProbeJournal, ProviderProbeJournalRecord};

impl StoreSession<'_> {
    fn load_provider_probe_journal(
        &self,
        key: &str,
    ) -> Result<Option<ProviderProbeJournalRecord>, DbError> {
        crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .protocol_state(key)?
            .map(|value| {
                serde_json::from_str(&value)
                    .map_err(|error| DbError::context("parse provider probe journal", error))
            })
            .transpose()
    }

    fn begin_provider_probe_journal(
        &self,
        key: &str,
        value: &str,
    ) -> Result<ProviderProbeJournalRecord, DbError> {
        let actual = crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .begin_protocol_state(key, value)?;
        serde_json::from_str(&actual)
            .map_err(|error| DbError::context("parse provider probe journal", error))
    }

    fn advance_provider_probe_journal(
        &self,
        key: &str,
        previous: &str,
        next: &str,
    ) -> Result<(), DbError> {
        if !crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .compare_exchange_protocol_state(key, previous, next)?
        {
            return Err(DbError::Message(
                "provider probe journal advance lost its exact predecessor".to_string(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl ProviderProbeJournal for StoreDatabase {
    async fn load(
        &self,
        probe_id: ProviderProbeId,
    ) -> Result<Option<ProviderProbeJournalRecord>, StorageError> {
        let key = format!("provider_probe/{}", hex::encode(probe_id.as_bytes()));
        self.call_store(move |session| session.load_provider_probe_journal(&key))
            .await
            .map_err(|error| StorageError::Storage(error.to_string()))
    }

    async fn begin(
        &self,
        prepared: ProviderProbeJournalRecord,
    ) -> Result<ProviderProbeJournalRecord, StorageError> {
        prepared.validate_begin().map_err(|error| {
            StorageError::Storage(format!("invalid provider probe journal beginning: {error}"))
        })?;
        let key = format!(
            "provider_probe/{}",
            hex::encode(prepared.probe_id().as_bytes())
        );
        let value = serde_json::to_string(&prepared).map_err(|error| {
            StorageError::Storage(format!("serialize provider probe journal: {error}"))
        })?;
        self.call_store(move |session| session.begin_provider_probe_journal(&key, &value))
            .await
            .map_err(|error| StorageError::Storage(error.to_string()))
    }

    async fn advance(
        &self,
        previous: &ProviderProbeJournalRecord,
        next: ProviderProbeJournalRecord,
    ) -> Result<(), StorageError> {
        previous.validate_transition(&next).map_err(|error| {
            StorageError::Storage(format!("invalid provider probe journal advance: {error}"))
        })?;
        let key = format!(
            "provider_probe/{}",
            hex::encode(previous.probe_id().as_bytes())
        );
        let previous = serde_json::to_string(previous).map_err(|error| {
            StorageError::Storage(format!("serialize provider probe journal: {error}"))
        })?;
        let next = serde_json::to_string(&next).map_err(|error| {
            StorageError::Storage(format!("serialize provider probe journal: {error}"))
        })?;
        self.call_store(move |session| {
            session.advance_provider_probe_journal(&key, &previous, &next)
        })
        .await
        .map_err(|error| StorageError::Storage(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coven_protocol::objects::ObjectSlot;
    use coven_protocol::provider::test_fixtures::{
        test_device_binding, test_exact_receipt, test_store_binding,
    };
    use coven_protocol::provider::{ExactProbeJournal, ExactProbeProgress};

    #[tokio::test]
    async fn database_probe_journal_rejects_a_skipped_progress_state() {
        let db = crate::synthetic_store::open_test_db();
        let journal = crate::StoreDatabase::new(db.database());
        let probe_id = ProviderProbeId::from_bytes([44; 32]);
        let binding = coven_protocol::objects::ResolvedProviderBinding {
            store: test_store_binding(),
            device: test_device_binding(),
        };
        let prepared = ProviderProbeJournalRecord::Exact(ExactProbeJournal {
            probe_id,
            binding,
            slot: ObjectSlot::logical("__coven_probe__/exact/journal".to_string()).unwrap(),
            lost_response_slot: ObjectSlot::logical(
                "__coven_probe__/lost-response/journal".to_string(),
            )
            .unwrap(),
            progress: ExactProbeProgress::Prepared,
        });
        assert_eq!(journal.begin(prepared.clone()).await.unwrap(), prepared);
        let ProviderProbeJournalRecord::Exact(mut final_record) = prepared.clone() else {
            unreachable!()
        };
        final_record.progress = ExactProbeProgress::ReceiptReady {
            receipt: test_exact_receipt(),
        };
        let final_record = ProviderProbeJournalRecord::Exact(final_record);
        assert!(journal.advance(&prepared, final_record).await.is_err());
        assert_eq!(journal.load(probe_id).await.unwrap(), Some(prepared));
    }
}
