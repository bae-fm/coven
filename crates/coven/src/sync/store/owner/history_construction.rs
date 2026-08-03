use crate::protocol::store_commit::StoreRootRef;
use crate::storage::SyncStorage;

use super::pull::StorePullError;
use super::verification::StoreCommitVerifier;
use super::verified_history::MergeHistoryVerifier;
use crate::sync::store::protocol_root::VerifiedStoreRoot;

#[derive(Clone, Copy)]
pub(crate) struct HistoryConstructionAuthority(());

impl HistoryConstructionAuthority {
    pub(super) fn store() -> Self {
        Self(())
    }

    pub(crate) fn invitation() -> Self {
        Self(())
    }

    pub(super) fn founder() -> Self {
        Self(())
    }

    pub(crate) fn for_pending_device_join() -> Self {
        Self(())
    }

    pub(crate) fn for_snapshot() -> Self {
        Self(())
    }

    pub(crate) async fn open_pinned<'storage>(
        self,
        storage: &'storage dyn SyncStorage,
        root: &StoreRootRef,
    ) -> Result<MergeHistoryVerifier<'storage>, StorePullError> {
        let object =
            crate::sync::store::protocol_root::load_pinned_store_protocol_root(storage, root)
                .await
                .map_err(|error| StorePullError::Database(error.to_string()))?;
        let verified_root = VerifiedStoreRoot::from_verified_object(root.clone(), object)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        self.bind_verified(storage, verified_root).await
    }

    pub(super) async fn bind_verified<'storage>(
        self,
        storage: &'storage dyn SyncStorage,
        root: VerifiedStoreRoot,
    ) -> Result<MergeHistoryVerifier<'storage>, StorePullError> {
        let commit_verifier = StoreCommitVerifier::from_verified_root(self, storage, root.clone());
        MergeHistoryVerifier::from_commit_verifier(self, root, commit_verifier).await
    }
}
