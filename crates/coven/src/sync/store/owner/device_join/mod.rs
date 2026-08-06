//! Provider-aware Store device admission and cancellation protocol.
//!
//! The wire values in this module are the transfer boundaries of the join
//! exchange. Each value contains the exact signed value from the preceding
//! boundary. Durable role journals store one closed progress value and advance
//! only from the exact adjacent predecessor.

use super::verified_history::MergeHistoryVerifier;
use super::{
    prepare_registration_object, AuthorizedStoreHistory, AuthorizedWriterOperation,
    RegistrationOutbox, StoreKeyrings,
};
use crate::storage::SyncStorage;
use crate::sync::store::{Store, StoreDatabase};
use coven_database::DeviceJoinBootstrapPlan;
use coven_keys::keys::UserKeypair;
use coven_protocol::objects::ObjectSlot;
use coven_protocol::objects::{ProtocolObjectDomain, ProviderDeviceBinding, StoreProviderBinding};
use coven_protocol::provider::{
    ActivatedStoreMemberProviderAccessGrant, CrossPrincipalProbeChallenge,
    CrossPrincipalProbeResponse, DeviceJoinChallengePublicationAuthorization,
    ProviderAccessGrantId, ProviderAccessWithdrawal, ProviderAdminGrantId,
    ProviderAdminGrantRecord, StoreMemberProviderAccessGrantRef,
};
use coven_protocol::store_commit::{
    DeviceJoinAttempt, DeviceJoinAttemptDecisionRef, DeviceJoinAttemptId, DeviceJoinAttemptRef,
    DeviceJoinOutcomeRef, DeviceReadinessProof, ObjectHash, StoreBatchCommitRef,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreProtocolRoot, StoreRootRef,
};

mod authorized_join;
mod cleanup;
mod error;
pub(super) mod history;
mod joiner;
mod journal;

pub(super) use authorized_join::{AuthorizedJoin, AuthorizedProviderAdministratorJoin};

#[derive(Clone, Copy)]
pub(crate) struct PendingDeviceJoinHistoryConstruction;

pub use authorized_join::DeviceProviderAccessAdministrator;
pub use cleanup::*;
pub use coven_protocol::store_commit::device_join_exchange::*;
pub use error::*;
pub(crate) use joiner::*;
pub use journal::*;
