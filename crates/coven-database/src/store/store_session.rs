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
    ) -> Self {
        Self {
            conn,
            store_dir,
            verified_store_authority,
            gates,
            synced_tables,
            schema_version,
            sync_routing_hash,
            hlc,
            blob_decls,
        }
    }

    pub(super) fn required_root_authority(&mut self) -> Result<StoreRootRef, DbError> {
        self.verified_store_authority
            .required_root_authority_on(crate::store::StoreRecords::new(self.conn, self.store_dir))
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
            .root_authority_on(crate::store::StoreRecords::new(self.conn, self.store_dir))
    }

    pub(super) fn activated_registration(
        &mut self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<ReferencedStoreDeviceRegistration, DbError> {
        let root = self.required_root_authority()?;
        let registration = self.verified_store_authority.activated_registration_on(
            crate::store::StoreRecords::new(self.conn, self.store_dir),
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
            .local_store_authority_on(crate::store::StoreRecords::new(self.conn, self.store_dir))
    }
}
