//! Member S3 keys coven makes with admin access, one journaled step at a time
//! (§4, §18). The journal fixes the principal's name before the first provider
//! call; a failure that cannot finish rolls back what that name created and
//! reports both causes. Admin access stays in memory with its operation.

use super::StoreLogSync;
use crate::{operation_data::MintWork, *};
use coven_storage::{AccessKeyAdmin, S3MemberKey};
use std::sync::Arc;

/// Where one mint step left its work.
pub(crate) enum Minting {
    /// The key passed its member-side check and may be delivered.
    Ready(S3MemberKey),
    /// The work advanced; the caller commits it.
    Advanced,
    /// The step failed. When `reset` the provider state was rolled back and the
    /// caller commits the cleared work before reporting the error.
    Failed { error: SyncError, reset: bool },
}

impl StoreLogSync {
    /// Keep admin access for this operation only, replacing any earlier one.
    pub(crate) fn hold_admin_access(&mut self, id: OperationId, admin: Arc<dyn AccessKeyAdmin>) {
        self.admin_access.insert(id, admin);
    }

    /// Forget this operation's admin access once its provider work is done.
    pub(crate) fn release_admin_access(&mut self, id: OperationId) {
        self.admin_access.remove(&id);
    }

    pub(crate) fn admin_access(&self, id: OperationId) -> Option<Arc<dyn AccessKeyAdmin>> {
        self.admin_access.get(&id).cloned()
    }

    /// Mint, then check from the member's side, one provider step per call.
    pub(crate) async fn advance_mint(
        &self,
        admin: &dyn AccessKeyAdmin,
        work: &mut MintWork,
    ) -> Minting {
        let result = match &work.minted {
            None => admin.mint(&work.scope).await.map(|key| {
                work.minted = Some(key);
            }),
            Some(key) if !work.checked => admin
                .probe(&work.scope, &key.credentials)
                .await
                .map(|()| work.checked = true),
            Some(key) => return Minting::Ready(key.clone()),
        };
        match result {
            Ok(()) => Minting::Advanced,
            Err(error) => {
                let error = SyncError::Storage(error);
                match admin.roll_back(&work.scope).await {
                    Ok(()) => {
                        work.minted = None;
                        work.checked = false;
                        Minting::Failed { error, reset: true }
                    }
                    Err(cleanup) => Minting::Failed {
                        error: SyncError::Cleanup {
                            operation: Box::new(error),
                            cleanup: Box::new(SyncError::Storage(cleanup)),
                        },
                        reset: false,
                    },
                }
            }
        }
    }
}
