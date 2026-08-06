use super::*;
use crate::database::{PreparedMergeMaterialization, PreparedMergeMaterializationPackage};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::authorized_store::LocalStoreDevice;
use super::history::{abandonment, OwnerPromotionHistory, RestoreHistory};
use super::pull;
use super::verification::StoreMembershipObjectVerifier;
use super::verified_history::registration::RegistrationLoadError;
use super::verified_history::*;
use crate::protocol::store_commit::{StoreDeviceStatus, StreamActivation, StreamAnchorDomain};

mod reclaim;

mod cleanup;
mod construction;
mod facades;
mod loading;
mod nonactivation;
mod pull_interface;
mod retained;
mod test_support;

pub(super) use nonactivation::MergeConflictResolutionAuthorization;
pub(super) use reclaim::{CircleSnapshotStream, ReclaimHistory, SelectedCircleSnapshot};

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

use crate::protocol::circle_control::StoreMembershipStateRef;
use crate::protocol::membership::{
    AuthorStreamId, MembershipChain, MembershipHeadRef, MembershipStatus,
};
use crate::protocol::store_commit::{
    CommitFrontier, OpenedRetainedMergeHistorySummary, ResolvedStoreDeviceState,
    StoreBatchCommitRef, StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreRootRef,
};
