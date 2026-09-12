use super::*;
use crate::sync::store::commit_verification::commit::StoreMembershipObjectVerifier;
use crate::sync::store::membership::MembershipMutationError;
use coven_database::VerifiedMergeMembershipObjects;
use coven_protocol::membership::{
    self, MembershipChain, MembershipEntry, SealedStoreKey, StoreAuthorityChange,
};
use coven_protocol::membership_mutation::{
    PreparedMembershipPublication, PreparedMembershipTransition,
};
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{self, commit_semantic_prefix};
use std::collections::BTreeMap;
use std::sync::Arc;

mod blob_lifecycle;
pub(crate) use blob_lifecycle::TombstoneGcError;
mod blob_preparation;
mod blob_upload;
pub(crate) mod commit_plan;
pub(super) mod membership_mutation;
pub(super) mod membership_mutation_journal;
mod preparation;

mod commit_publication;
pub(crate) use commit_publication::StoreOperationPublicationOutcome;
mod facades;
mod membership_commands;
mod membership_publication;
mod operation_test_support;
mod signing;
mod store_writes;

pub(super) use blob_preparation::close_prepared_packages;

use membership_mutation_journal::{
    decode_membership_mutation, exact_owned_remote, AdmissionMutationPlan, MembershipMutationPlan,
    MembershipMutationProgress, MutationPersistence, RevokeMutationPlan,
};

#[derive(Debug, thiserror::Error)]
pub enum StoreWriterAuthorizationError {
    #[error("Store authority: {0}")]
    StoreAuthority(SyncCycleFailure),
    #[error("Store writer registration: {0}")]
    Registration(StoreRegistrationError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AuthorizationRefreshError {
    #[error("select this device's sealed-key authority: {0}")]
    Membership(#[source] coven_protocol::membership::MembershipError),
    #[error("open this device's sealed Store key: {0}")]
    SealedKey(#[source] crate::sync::store::membership::MembershipMutationError),
    #[error("rotation gate database state: {0}")]
    Database(#[source] coven_database::DbError),
    #[error("merge this device's live and selected keyrings: {0}")]
    InvalidKeyring(#[source] coven_keys::encryption::EncryptionError),
    #[error("adopt committed store-key rotation: {0}")]
    KeyAdoption(#[source] coven_keys::keys::KeyError),
}
