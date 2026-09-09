//! Memoize completed reclamation evaluations against their local inputs.
//!
//! A restart evaluates again. Failed evaluations are never recorded.

use std::sync::Mutex;

use coven_protocol::membership::MembershipHeadRef;
use coven_protocol::store_commit::{
    AcceptedStoreSnapshotRef, CommitFrontier, StoreDeviceRegistrationRef,
};

/// The local facts a provider-side evaluation depends on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CycleInputs {
    /// Every commit this device has materialized. Moves for a new package, a
    /// new acknowledgement, a membership control, a registration activation —
    /// every arrival that can change what may be reclaimed.
    frontier: CommitFrontier,
    /// Membership determines current reclamation authority.
    membership_heads: Vec<MembershipHeadRef>,
    /// Activated devices determine the remaining Circle acknowledgement requirements.
    registrations: Vec<StoreDeviceRegistrationRef>,
    /// The accepted snapshot can change without advancing a commit frontier,
    /// including when another owner publishes the same cut.
    accepted_snapshot: Option<AcceptedStoreSnapshotRef>,
}

impl CycleInputs {
    /// Read every input from this device's own database.
    pub(crate) async fn read(
        database: &coven_database::StoreDatabase,
        membership: &coven_protocol::membership::MembershipChain,
    ) -> Result<Self, coven_database::DbError> {
        let frontier = CommitFrontier(
            database
                .materialized_frontier()
                .await?
                .into_values()
                .map(|reference| (reference.coord.stream_id, reference))
                .collect(),
        );
        Ok(Self {
            frontier,
            membership_heads: membership.head_refs().to_vec(),
            registrations: database
                .activated_store_device_registration_records()
                .await?
                .iter()
                .map(|record| record.reference().clone())
                .collect(),
            accepted_snapshot: database
                .store_current_publication()
                .await?
                .record()
                .latest_snapshot()
                .cloned(),
        })
    }
}

/// What each of a cycle's provider-side evaluations last ran against.
#[derive(Default)]
pub(crate) struct SettledCycle {
    reclaim: Mutex<Option<CycleInputs>>,
}

impl SettledCycle {
    /// Whether reclaim has already been evaluated against exactly these inputs.
    pub(crate) fn reclaim_evaluated(&self, inputs: &CycleInputs) -> bool {
        self.locked().as_ref() == Some(inputs)
    }

    /// Record that a reclaim evaluation ran to completion against `inputs`.
    ///
    /// Recorded whatever it decided, including "nothing may be deleted": the
    /// decision cannot change while the inputs do not, and every way it can
    /// change moves one of them.
    pub(crate) fn record_reclaim_evaluated(&self, inputs: CycleInputs) {
        *self.locked() = Some(inputs);
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, Option<CycleInputs>> {
        self.reclaim.lock().expect("settled cycle memo poisoned")
    }
}
