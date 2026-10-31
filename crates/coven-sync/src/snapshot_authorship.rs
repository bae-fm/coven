//! Historical verification of the original write headers carried by snapshots.

use crate::history::History;
use coven_crypto::{AuthorshipHasher, StoreKeyring};
use coven_database::{Authorship, DbError, SnapshotError};
use coven_format::{
    authorship::WriteEvidence, sealed_write::WriteObjectPrefix, write_stream::WriteHeaderFrame,
};
use coven_foundation::id_source::{KeyId, StoreId};
use coven_merge::Audience;

pub(crate) struct SnapshotAuthorship {
    pub(crate) store: StoreId,
    pub(crate) history: History,
    pub(crate) ring: StoreKeyring,
}
impl SnapshotAuthorship {
    fn key(&self, audience: &Audience, key: KeyId) -> Result<coven_crypto::DerivedKeys, DbError> {
        crate::write_seal::derive(&self.ring, audience, key).map_err(|error| match error {
            crate::SyncError::KeyUnavailable(key) => {
                SnapshotError::KeyUnavailable { audience: audience.clone(), key }.into()
            }
            error => DbError::SyncStream(Box::new(error)),
        })
    }
}
impl Authorship for SnapshotAuthorship {
    fn header(&self, evidence: &WriteEvidence) -> Result<WriteHeaderFrame, DbError> {
        let prefix = WriteObjectPrefix::decode(&evidence.prefix)?;
        let identity = coven_format::writes_slot::identity(self.store, evidence.write)?;
        let sealed = WriteObjectPrefix::header_chunk(&evidence.header)?;
        let plaintext = self
            .key(&Audience::Store, prefix.store_key)?
            .open_object_chunk(&identity, &evidence.prefix, 0, 0, sealed)
            .map_err(|error| DbError::SyncStream(Box::new(error)))?;
        let header = WriteHeaderFrame::decode(&plaintext)?;
        let signer = self
            .history
            .write_signer(&header)
            .map_err(|error| DbError::SyncStream(Box::new(error)))?;
        signer
            .verify_write_header(
                &identity,
                &evidence.prefix,
                &evidence.header,
                &evidence.roots,
                &coven_crypto::Signature::from_bytes(evidence.signature),
            )
            .map_err(|error| DbError::SyncStream(Box::new(error)))?;
        Ok(header)
    }
    fn hasher(&self, audience: &Audience, key: KeyId) -> Result<AuthorshipHasher, DbError> {
        Ok(self.key(audience, key)?.authorship_hasher())
    }
}
