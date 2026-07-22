//! Provider-aware Store device admission and cancellation protocol.
//!
//! The wire values in this module are the transfer boundaries of the join
//! exchange. Each value contains the exact signed value from the preceding
//! boundary. Durable role journals store one closed progress value and advance
//! only from the exact adjacent predecessor.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::database::Database;
use crate::keys::{self, UserKeypair};
use crate::storage::cloud::{ExactSlotStorage, ObjectSlot};
use crate::sync::circle_control::StoreMembershipStateRef;
use crate::sync::membership::{MemberRole, MembershipChain, MembershipGrantId};
use crate::sync::provider::{
    ActivatedStoreMemberProviderAccessGrant, CrossPrincipalProbeChallenge,
    CrossPrincipalProbeReceipt, CrossPrincipalProbeResponse,
    DeviceJoinChallengePublicationAuthorization, ProviderAccessGrantId, ProviderAccessWithdrawal,
    ProviderAdminGrantId, ProviderAdminGrantRecord, StoreMemberProviderAccessGrant,
    StoreMemberProviderAccessGrantRef,
};
use crate::sync::storage::{
    ExactObjectRef, ProtocolObjectDomain, ProviderDeviceBinding, StoreProviderBinding, SyncStorage,
};
use crate::sync::store_commit::{
    DeviceJoinAttempt, DeviceJoinAttemptDecisionRef, DeviceJoinAttemptId, DeviceJoinAttemptRef,
    DeviceJoinOutcomeRef, DeviceReadinessProof, ObjectHash, StoreBatchCommitRef,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreProtocolRoot, StoreRootRef,
    STORE_PROTOCOL_VERSION,
};

mod authority;
mod cleanup;
mod error;
mod exchange;
mod joiner;
mod journal;
mod owner;
mod provider_administrator;

pub use authority::*;
pub use cleanup::*;
pub use error::*;
pub use exchange::*;
pub use joiner::*;
pub use journal::*;
pub use owner::*;
pub use provider_administrator::*;
