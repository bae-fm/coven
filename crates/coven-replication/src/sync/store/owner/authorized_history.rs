use super::*;
use coven_database::{PreparedMergeMaterialization, PreparedMergeMaterializationPackage};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::authorized_store::LocalStoreDevice;
use super::pull;
use super::verification::StoreMembershipObjectVerifier;
use super::verified_history::registration::RegistrationLoadError;
use super::verified_history::*;
use crate::sync::store::owner_role_promotion::OwnerPromotionHistory;
use crate::sync::store::restore::RestoreHistory;
use coven_protocol::store_commit::{StoreDeviceStatus, StreamActivation, StreamAnchorDomain};

mod cleanup;
mod construction;
mod facades;
mod loading;
mod pull_interface;
mod retained;
mod test_support;

pub(crate) struct AuthorizedStoreHistory<'storage> {
    database: StoreDatabase,
    storage: &'storage Arc<dyn SyncStorage>,
    store_dir: &'storage coven_foundation::store_dir::StoreDir,
    blob_cache: crate::sync::store::blob::StoreBlobCache,
    history_verifier: MergeHistoryVerifier<'storage>,
    blob_source: crate::sync::store::blob::RemoteBlobSource<'storage>,
    keyrings: Arc<super::keyring::StoreKeyrings<'storage>>,
}

impl<'storage> AuthorizedStoreHistory<'storage> {}

use coven_protocol::circle_control::StoreMembershipStateRef;
use coven_protocol::membership::{
    AuthorStreamId, MembershipChain, MembershipHeadRef, MembershipStatus,
};
use coven_protocol::store_commit::{
    CommitFrontier, OpenedRetainedMergeHistorySummary, ResolvedStoreDeviceState,
    StoreBatchCommitRef, StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreRootRef,
};
