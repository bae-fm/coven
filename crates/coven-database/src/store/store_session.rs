use super::{StoreSession, VerifiedStoreAuthority};
use crate::DbError;
use coven_protocol::store_commit::{
    ReferencedStoreDeviceRegistration, StoreDeviceRegistrationRef, StoreRootRef,
};

impl<'session> StoreSession<'session> {
    pub(crate) fn new(
        conn: &'session rusqlite::Connection,
        store_dir: &'session coven_foundation::store_dir::StoreDir,
        verified_store_authority: &'session mut VerifiedStoreAuthority,
        gates: &'session crate::Gates,
        synced_tables: &'session [coven_protocol::synced_schema::SyncedTable],
        schema_version: u32,
        sync_routing_hash: coven_protocol::store_commit::ObjectHash,
        hlc: &'session std::sync::Arc<coven_protocol::hlc::Hlc>,
        blob_decls: &'session crate::BlobDecls,
        #[cfg(any(test, feature = "test-utils"))]
        merge_materialization_failure: &'session std::sync::Mutex<
            Option<crate::MergeMaterializationFailurePoint>,
        >,
    ) -> Self {
        Self {
            records: crate::store::StoreRecords::new(conn, store_dir),
            verified_store_authority,
            gates,
            synced_tables,
            schema_version,
            sync_routing_hash,
            hlc,
            blob_decls,
            #[cfg(any(test, feature = "test-utils"))]
            merge_materialization_failure,
        }
    }

    pub(super) fn required_root_authority(&mut self) -> Result<StoreRootRef, DbError> {
        self.verified_store_authority
            .required_root_authority_on(self.records)
    }

    pub(super) fn root_authority(
        &mut self,
    ) -> Result<
        Option<(
            StoreRootRef,
            coven_protocol::store_commit::StoreProtocolRoot,
        )>,
        DbError,
    > {
        self.verified_store_authority
            .root_authority_on(self.records)
    }

    pub(super) fn activated_registration(
        &mut self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<ReferencedStoreDeviceRegistration, DbError> {
        let root = self.required_root_authority()?;
        let registration = self.verified_store_authority.activated_registration_on(
            self.records,
            &root,
            reference,
        )?;
        ReferencedStoreDeviceRegistration::verified(reference.clone(), registration)
            .map_err(|error| DbError::Message(error.to_string()))
    }

    pub(super) fn local_store_authority(
        &mut self,
    ) -> Result<ReferencedStoreDeviceRegistration, DbError> {
        self.verified_store_authority
            .local_store_authority_on(self.records)
    }

    pub(super) fn verified_store_transaction<R>(
        &mut self,
        operation: impl FnOnce(
            &mut super::VerifiedStoreTransaction<'_, '_, '_>,
        ) -> Result<super::StoreTransactionOutcome<R>, DbError>,
    ) -> Result<R, DbError> {
        let transaction = self
            .records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let store = super::StoreTransaction::new(&transaction, self.records.store_dir);
        let mut authority =
            store.begin_verified_authority_transaction(self.verified_store_authority)?;
        let outcome = {
            let mut capability = super::VerifiedStoreTransaction {
                store,
                authority: &mut authority,
                gates: self.gates,
                synced_tables: self.synced_tables,
                blob_decls: self.blob_decls,
                #[cfg(any(test, feature = "test-utils"))]
                merge_materialization_failure: self.merge_materialization_failure,
            };
            operation(&mut capability)
        };
        match outcome {
            Ok(super::StoreTransactionOutcome::Commit(value)) => {
                transaction.commit().map_err(DbError::from)?;
                self.verified_store_authority.commit_transaction(authority);
                Ok(value)
            }
            Ok(super::StoreTransactionOutcome::Rollback(value)) => {
                transaction.rollback().map_err(DbError::from)?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }
}
