use super::{payload_spool::StoreRecords, StoreSession, VerifiedStoreAuthority};
use crate::DbError;
use coven_protocol::store_commit::{
    ReferencedStoreDeviceRegistration, StoreDeviceRegistrationRef, StoreRootRef,
};

impl<'session> StoreSession<'session> {
    pub(crate) fn new(
        records: StoreRecords<'session>,
        verified_store_authority: &'session mut VerifiedStoreAuthority,
        gates: &'session crate::Gates,
        synced_tables: &'session [coven_protocol::synced_schema::SyncedTable],
        hlc: &'session std::sync::Arc<coven_protocol::hlc::Hlc>,
        blob_decls: &'session crate::BlobDecls,
    ) -> Self {
        Self {
            records,
            verified_store_authority,
            gates,
            synced_tables,
            hlc,
            blob_decls,
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
}
