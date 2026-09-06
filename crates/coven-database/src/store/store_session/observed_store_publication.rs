use crate::DbError;
use coven_protocol::objects::ExactObjectVersion;
use coven_protocol::store_commit::StoreCurrentPublicationRecord;
use rusqlite::OptionalExtension;

use super::StoreSession;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedStorePublication {
    record: StoreCurrentPublicationRecord,
    version: ExactObjectVersion,
}

impl ObservedStorePublication {
    pub fn verified_genesis(
        record: StoreCurrentPublicationRecord,
        version: ExactObjectVersion,
        expected_store_root_hash: coven_protocol::store_commit::ObjectHash,
        founder_pubkey: &str,
    ) -> Result<Self, coven_protocol::store_commit::StoreProtocolError> {
        record.verify_genesis(expected_store_root_hash, founder_pubkey)?;
        Ok(Self { record, version })
    }

    pub fn record(&self) -> &StoreCurrentPublicationRecord {
        &self.record
    }

    pub fn version(&self) -> &ExactObjectVersion {
        &self.version
    }

    pub fn verified_commit_successor(
        previous: &Self,
        record: StoreCurrentPublicationRecord,
        version: ExactObjectVersion,
        entry: &coven_protocol::store_commit::StorePublicationEntry,
        reference: &coven_protocol::store_commit::StorePublicationRef,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        publisher_signing_pubkey: &str,
    ) -> Result<Self, coven_protocol::store_commit::StoreProtocolError> {
        record.verify_commit_transition(
            &previous.record,
            entry,
            reference,
            commit,
            publisher_signing_pubkey,
        )?;
        Ok(Self { record, version })
    }
}

pub(super) fn load_store_current_publication_on(
    connection: &rusqlite::Connection,
) -> Result<ObservedStorePublication, DbError> {
    let (hash, bytes, version) = connection
        .query_row(
            "SELECT record_hash, record_bytes, provider_version
             FROM store_publication_current WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(DbError::from)?
        .ok_or_else(|| {
            DbError::Message("Store publication current record is absent".to_string())
        })?;
    let record: StoreCurrentPublicationRecord = serde_json::from_slice(&bytes)
        .map_err(|error| DbError::context("Store current publication record", error))?;
    if record.to_bytes() != bytes || record.record_hash().to_string() != hash {
        return Err(DbError::Message(
            "Store current publication row differs from its canonical record".to_string(),
        ));
    }
    Ok(ObservedStorePublication {
        record,
        version: ExactObjectVersion::from_provider(version).map_err(DbError::from)?,
    })
}

pub(super) fn install_genesis_store_publication_on(
    transaction: &rusqlite::Transaction<'_>,
    record: &StoreCurrentPublicationRecord,
    version: &ExactObjectVersion,
) -> Result<(), DbError> {
    if record.accepted().is_some() {
        return Err(DbError::Message(
            "initial Store publication record is not genesis".to_string(),
        ));
    }
    let bytes = record.to_bytes();
    let inserted = transaction
        .execute(
            "INSERT INTO store_publication_current
             (singleton, record_hash, record_bytes, provider_version)
             VALUES (1, ?1, ?2, ?3)
             ON CONFLICT(singleton) DO NOTHING",
            rusqlite::params![
                record.record_hash().to_string(),
                bytes,
                version.as_provider()
            ],
        )
        .map_err(DbError::from)?;
    if inserted == 0 {
        let current = load_store_current_publication_on(transaction)?;
        if current.record != *record || current.version != *version {
            return Err(DbError::Message(
                "Store publication genesis differs from installed current record".to_string(),
            ));
        }
    }
    Ok(())
}

pub(super) fn install_store_publication_successor_on(
    transaction: &rusqlite::Transaction<'_>,
    expected: &ObservedStorePublication,
    successor: &ObservedStorePublication,
) -> Result<(), DbError> {
    let successor_bytes = successor.record.to_bytes();
    let updated = transaction
        .execute(
            "UPDATE store_publication_current
             SET record_hash = ?1, record_bytes = ?2, provider_version = ?3
             WHERE singleton = 1 AND record_hash = ?4 AND record_bytes = ?5
               AND provider_version = ?6",
            rusqlite::params![
                successor.record.record_hash().to_string(),
                successor_bytes,
                successor.version.as_provider(),
                expected.record.record_hash().to_string(),
                expected.record.to_bytes(),
                expected.version.as_provider(),
            ],
        )
        .map_err(DbError::from)?;
    if updated != 1 {
        return Err(DbError::Message(
            "Store publication boundary changed before local completion".to_string(),
        ));
    }
    Ok(())
}

impl StoreSession<'_> {
    pub(crate) fn store_current_publication(&self) -> Result<ObservedStorePublication, DbError> {
        load_store_current_publication_on(self.conn)
    }
}
