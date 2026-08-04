use crate::database::StoreDatabase;
use crate::store_security::StoreSecurity;
use crate::store_sync::{StoreSync, SyncError};

#[derive(Clone)]
pub(crate) struct StoreRecovery {
    database: StoreDatabase,
    security: StoreSecurity,
    sync: StoreSync,
}

impl StoreRecovery {
    pub(crate) fn new(database: StoreDatabase, security: StoreSecurity, sync: StoreSync) -> Self {
        Self {
            database,
            security,
            sync,
        }
    }

    pub(crate) async fn generate_restore_code(&self) -> Result<String, SyncError> {
        if !self.sync.is_command_configured() {
            return Err(SyncError::NotConfigured);
        }
        let identity = self.security.established_identity()?;
        let restore_membership = self.sync.restore_membership().await?;
        let authority = crate::restoration::RestoreAuthority::ActivatedContinuation(
            identity
                .export_activated_device_continuation(&self.database)
                .await?,
        );
        self.security
            .generate_restore_code(
                &self.sync.command_config(),
                restore_membership.store_root,
                restore_membership.founder_pubkey,
                restore_membership.membership_floor,
                authority,
            )
            .map_err(SyncError::from)
    }
}
