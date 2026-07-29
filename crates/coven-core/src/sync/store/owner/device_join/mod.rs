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

use super::AuthorizedWriterOperation;
use crate::database::Database;
use crate::keys::{self, UserKeypair};
use crate::storage::cloud::ObjectSlot;
use crate::sync::circle_control::StoreMembershipStateRef;
use crate::sync::membership::MembershipGrantId;
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
use crate::sync::store::{Store, StoreDatabase};
use crate::sync::store_commit::{
    DeviceJoinAttempt, DeviceJoinAttemptDecisionRef, DeviceJoinAttemptId, DeviceJoinAttemptRef,
    DeviceJoinOutcomeRef, DeviceReadinessProof, ObjectHash, StoreBatchCommitRef,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreProtocolRoot, StoreRootRef,
    STORE_PROTOCOL_VERSION,
};

mod cleanup;
mod error;
mod exchange;
mod joiner;
mod journal;
mod owner;
mod provider_administrator;

pub(super) struct AuthorizedJoin<'operation, 'storage> {
    writer: &'operation mut AuthorizedWriterOperation<'storage>,
}

pub(super) struct AuthorizedProviderAdministratorJoin<'operation, 'storage> {
    writer: &'operation mut AuthorizedWriterOperation<'storage>,
    grants: std::collections::BTreeMap<ProviderAdminGrantId, ProviderAdminGrantRecord>,
}

impl<'operation, 'storage> AuthorizedJoin<'operation, 'storage> {
    pub(super) fn new(writer: &'operation mut AuthorizedWriterOperation<'storage>) -> Self {
        Self { writer }
    }
}

impl<'operation, 'storage> AuthorizedProviderAdministratorJoin<'operation, 'storage> {
    pub(super) fn new(
        writer: &'operation mut AuthorizedWriterOperation<'storage>,
    ) -> Result<Self, DeviceJoinError> {
        let crate::sync::membership::MembershipStatus::Resolved(resolved) =
            writer.membership().status()
        else {
            return Err(DeviceJoinError::MembershipConflict);
        };
        let state = resolved.provider_admin.combined_state();
        let administrator = writer.registration().0;
        let grants = state
            .records()
            .iter()
            .filter(|(grant_id, record)| {
                &record.administrator == administrator
                    && state.authorizes(grant_id, &record.administrator)
            })
            .map(|(grant_id, record)| (grant_id.clone(), record.clone()))
            .collect::<std::collections::BTreeMap<_, _>>();
        if grants.is_empty() {
            return Err(DeviceJoinError::ProviderAdministratorRequired);
        }
        Ok(Self { writer, grants })
    }

    fn require_grant(
        &self,
        grant_id: &ProviderAdminGrantId,
    ) -> Result<&ProviderAdminGrantRecord, DeviceJoinError> {
        self.grants
            .get(grant_id)
            .ok_or(DeviceJoinError::ProviderAdministratorRequired)
    }
}

pub use cleanup::*;
pub use error::*;
pub use exchange::*;
pub use joiner::*;
pub use journal::*;
pub use provider_administrator::*;
