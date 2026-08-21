//! What a cycle already knows the answer to.
//!
//! Two of a cycle's stages ask the provider a question whose answer is a
//! function of local facts: whether any package may be reclaimed, and which
//! snapshot this device could acknowledge next. Both are expensive — the
//! reclaim one walks every candidate snapshot's stability and every device's
//! acknowledgement chain — and both are asked every thirty seconds by a store
//! where nothing has happened. A settled store spent thirty-one of its
//! thirty-nine cycle seconds re-deriving "nothing to do".
//!
//! The facts those answers depend on all live in this database, because every
//! way an answer can change arrives as a commit: a package-bearing commit, an
//! acknowledgement, a membership control, a device registration. The one that
//! does not is a snapshot this device publishes itself, which is recorded here
//! directly. So a cycle whose [`CycleInputs`] match the ones an evaluation last
//! ran against cannot reach a different answer, and asking again is asking a
//! question already answered.
//!
//! Held in memory, not on disk. A restart re-asks once, which is a cost paid
//! per launch rather than per cycle, and it means no durable state to keep
//! correct — the memo can only ever be discarded, never wrong.

use std::sync::Mutex;

use coven_protocol::membership::MembershipHeadRef;
use coven_protocol::store_commit::{
    CommitFrontier, StoreDeviceProposalAck, StoreDeviceRegistrationRef, StoreSnapshotLocator,
    StoreSnapshotRef,
};

/// The local facts a provider-side evaluation depends on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CycleInputs {
    /// Every commit this device has materialized. Moves for a new package, a
    /// new acknowledgement, a membership control, a registration activation —
    /// every arrival that can change what may be reclaimed or acknowledged.
    frontier: CommitFrontier,
    /// Membership, which decides who must acknowledge before anything is
    /// deleted and who may still ask for the history behind it.
    membership_heads: Vec<MembershipHeadRef>,
    /// Which devices are activated, which is the set whose snapshot streams an
    /// evaluation reads and whose acknowledgements it counts.
    registrations: Vec<StoreDeviceRegistrationRef>,
    /// A snapshot this device published. The one input that arrives without a
    /// commit: the device wrote the object itself, and its own acknowledgement
    /// of it has not been made yet.
    published_snapshot: Option<StoreSnapshotRef>,
    /// Exclusion freezes, which an acknowledgement asserts and which are staged
    /// locally before any commit carries them.
    exclusion_freezes: Vec<StoreDeviceProposalAck>,
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
            published_snapshot: database
                .latest_local_store_snapshot()
                .await?
                .map(|snapshot| snapshot.reference),
            exclusion_freezes: database.store_device_exclusion_freezes().await?,
        })
    }
}

/// What each of a cycle's provider-side evaluations last ran against.
#[derive(Default)]
pub(crate) struct SettledCycle {
    inner: Mutex<Settled>,
}

#[derive(Default)]
struct Settled {
    reclaim: Option<CycleInputs>,
    acknowledgeable: Option<(CycleInputs, Option<StoreSnapshotLocator>)>,
}

impl SettledCycle {
    /// Whether reclaim has already been evaluated against exactly these inputs.
    pub(crate) fn reclaim_evaluated(&self, inputs: &CycleInputs) -> bool {
        self.locked().reclaim.as_ref() == Some(inputs)
    }

    /// Record that a reclaim evaluation ran to completion against `inputs`.
    ///
    /// Recorded whatever it decided, including "nothing may be deleted": the
    /// decision cannot change while the inputs do not, and every way it can
    /// change moves one of them.
    pub(crate) fn record_reclaim_evaluated(&self, inputs: CycleInputs) {
        self.locked().reclaim = Some(inputs);
    }

    /// The snapshot this device could acknowledge next, if that was already
    /// worked out against exactly these inputs. The inner `Option` is the
    /// answer — `Some(None)` means "there is none", which is as much an answer
    /// as a snapshot is.
    pub(crate) fn acknowledgeable_snapshot(
        &self,
        inputs: &CycleInputs,
    ) -> Option<Option<StoreSnapshotLocator>> {
        self.locked()
            .acknowledgeable
            .as_ref()
            .filter(|(recorded, _)| recorded == inputs)
            .map(|(_, locator)| locator.clone())
    }

    pub(crate) fn record_acknowledgeable_snapshot(
        &self,
        inputs: CycleInputs,
        locator: Option<StoreSnapshotLocator>,
    ) {
        self.locked().acknowledgeable = Some((inputs, locator));
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, Settled> {
        self.inner.lock().expect("settled cycle memo poisoned")
    }
}
