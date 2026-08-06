//! Resolution of Merge candidates that lost their activation slot: observing
//! who occupies a contested Store head, and proving that a candidate can never
//! activate.

mod abandonment;
mod nonactivation;
mod observation;

pub use abandonment::ExcludedCandidateHeadObservation;
pub use abandonment::MergeCandidateAbandonment;
pub(crate) use abandonment::VerifiedMergeWinner;
pub(crate) use nonactivation::validate_retained_membership_floors;
pub(crate) use nonactivation::MergeConflictResolutionAuthorization;
pub(crate) use nonactivation::TerminalNonactivationCandidate;

use coven_database::StoreDatabase;
use coven_storage::SyncStorage;

use crate::sync::store::owner::verified_history::MergeHistoryVerifier;

/// The Merge-conflict operations, holding exactly the capabilities they use:
/// the database that records candidate outcomes, the storage the contested
/// heads live in, and the verifier that authenticates them.
pub(crate) struct MergeConflictHistory<'operation, 'storage> {
    database: &'operation StoreDatabase,
    storage: &'storage dyn SyncStorage,
    history: &'operation mut MergeHistoryVerifier<'storage>,
}

impl<'operation, 'storage> MergeConflictHistory<'operation, 'storage> {
    pub(crate) fn new(
        database: &'operation StoreDatabase,
        storage: &'storage dyn SyncStorage,
        history: &'operation mut MergeHistoryVerifier<'storage>,
    ) -> Self {
        Self {
            database,
            storage,
            history,
        }
    }
}
