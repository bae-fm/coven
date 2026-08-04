//! Provider-aware Store device admission and cancellation protocol.
//!
//! The wire values in this module are the transfer boundaries of the join
//! exchange. Each value contains the exact signed value from the preceding
//! boundary. Durable role journals store one closed progress value and advance
//! only from the exact adjacent predecessor.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::pull::{DeviceJoinBootstrapPlan, StorePullError};
use super::verified_history::MergeHistoryVerifier;
use super::{
    prepare_registration_object, AuthorizedStoreHistory, AuthorizedWriterOperation,
    RegistrationOutbox, StoreKeyrings,
};
use crate::keys::{self, UserKeypair};
use crate::protocol::circle_control::StoreMembershipStateRef;
use crate::protocol::membership::MembershipGrantId;
use crate::protocol::provider::{
    ActivatedStoreMemberProviderAccessGrant, CrossPrincipalProbeChallenge,
    CrossPrincipalProbeReceipt, CrossPrincipalProbeResponse,
    DeviceJoinChallengePublicationAuthorization, ProviderAccessGrantId, ProviderAccessWithdrawal,
    ProviderAdminGrantId, ProviderAdminGrantRecord, StoreMemberProviderAccessGrant,
    StoreMemberProviderAccessGrantRef,
};
use crate::protocol::store_commit::{
    DeviceJoinAttempt, DeviceJoinAttemptDecisionRef, DeviceJoinAttemptId, DeviceJoinAttemptRef,
    DeviceJoinOutcomeRef, DeviceReadinessProof, ObjectHash, StoreBatchCommitRef,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreProtocolRoot, StoreRootRef,
    STORE_PROTOCOL_VERSION,
};
use crate::storage::cloud::ObjectSlot;
use crate::storage::{
    ExactObjectRef, ProtocolObjectDomain, ProviderDeviceBinding, StoreProviderBinding, SyncStorage,
};
use crate::sync::store::{Store, StoreDatabase};

mod authorized_join;
mod cleanup;
mod error;
mod exchange;
pub(super) mod history;
mod joiner;
mod journal;
mod provider_administrator;

pub(super) use authorized_join::AuthorizedJoin;
pub(super) use provider_administrator::AuthorizedProviderAdministratorJoin;

#[derive(Clone, Copy)]
pub(super) struct PendingDeviceJoinHistoryConstruction;

pub use cleanup::*;
pub use error::*;
pub use exchange::*;
pub(crate) use joiner::*;
pub use journal::*;
pub use provider_administrator::*;
