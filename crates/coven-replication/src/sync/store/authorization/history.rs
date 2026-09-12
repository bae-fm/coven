use super::*;
use std::collections::BTreeMap;
use std::sync::Arc;

use super::authorized_store::LocalStoreDevice;
use crate::sync::store::commit_verification::commit::StoreMembershipObjectVerifier;
use crate::sync::store::commit_verification::merge_history::*;
use crate::sync::store::owner_role_promotion::OwnerPromotionHistory;
use crate::sync::store::pull;
use crate::sync::store::restore::RestoreHistory;

pub(crate) mod cleanup;
mod construction;
mod facades;
mod loading;
pub(crate) mod membership_publication;
pub(crate) mod publication;
pub(crate) mod retained;
mod test_support;

pub(crate) struct AuthorizedStoreHistory<'storage> {
    database: StoreDatabase,
    routing_encryption: Option<coven_keys::encryption::EncryptionService>,
    storage: &'storage Arc<dyn CloudSyncObjectStorage>,
    store_dir: &'storage coven_foundation::store_dir::StoreDir,
    blob_cache: crate::sync::store::blob::StoreBlobCache,
    history_verifier: MergeHistoryVerifier<'storage>,
    blob_source: crate::sync::store::blob::RemoteBlobSource<'storage>,
}

use coven_protocol::circle_control::StoreMembershipStateRef;
use coven_protocol::membership::{AuthorStreamId, MembershipChain, MembershipHeadRef};
use coven_protocol::store_commit::{
    ResolvedStoreDeviceState, StoreBatchCommitRef, StoreDeviceRegistrationRef, StoreDeviceStateRef,
    StoreRootRef,
};
