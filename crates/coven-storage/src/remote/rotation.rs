use super::*;

pub struct PendingRotation(std::sync::RwLock<Option<RotationGate>>);

#[derive(Debug, thiserror::Error)]
pub enum RotationStateError {
    #[error("rotation gate transition failed: {0}")]
    Gate(#[from] coven_protocol::objects::RotationGateError),
    #[error("rotation state lock is poisoned")]
    LockPoisoned,
    #[error("rotation candidate gate is absent during proven nonactivation")]
    MissingCandidateDuringNonactivation,
    #[error("rotation candidate gate is absent during candidate replacement")]
    MissingCandidateDuringReplacement,
}

pub trait CloudSyncRotationStateAccess: Send + Sync {
    fn mark_candidate(
        &self,
        generation: u64,
        mutation: ObjectHash,
    ) -> Result<(), RotationStateError>;
    fn mark_committed_mutation(
        &self,
        generation: u64,
        mutation: ObjectHash,
    ) -> Result<(), RotationStateError>;
    fn remove_candidate(
        &self,
        generation: u64,
        mutation: ObjectHash,
    ) -> Result<(), RotationStateError>;
    fn replace_candidate_mutation(
        &self,
        generation: u64,
        previous: ObjectHash,
        replacement: ObjectHash,
    ) -> Result<(), RotationStateError>;
    fn gate(&self) -> Option<RotationGate>;
    fn install_durable_gate(&self, gate: Option<RotationGate>);
    fn check(&self, live_generation: Option<u64>) -> Result<(), RotationPending>;
}

impl Default for PendingRotation {
    fn default() -> Self {
        Self(std::sync::RwLock::new(None))
    }
}

impl PendingRotation {
    pub fn none() -> Self {
        Self::default()
    }

    pub fn mark_candidate(
        &self,
        generation: u64,
        mutation: coven_protocol::store_commit::ObjectHash,
    ) -> Result<(), RotationStateError> {
        let mut recorded = self
            .0
            .write()
            .map_err(|_| RotationStateError::LockPoisoned)?;
        *recorded = Some(RotationGate::with_candidate(
            recorded.clone(),
            generation,
            mutation,
        )?);
        Ok(())
    }

    pub fn mark_committed_mutation(
        &self,
        generation: u64,
        mutation: coven_protocol::store_commit::ObjectHash,
    ) -> Result<(), RotationStateError> {
        let mut recorded = self
            .0
            .write()
            .map_err(|_| RotationStateError::LockPoisoned)?;
        *recorded = Some(RotationGate::commit_candidate(
            recorded.clone(),
            generation,
            mutation,
        )?);
        Ok(())
    }

    pub fn remove_candidate(
        &self,
        generation: u64,
        mutation: coven_protocol::store_commit::ObjectHash,
    ) -> Result<(), RotationStateError> {
        let mut recorded = self
            .0
            .write()
            .map_err(|_| RotationStateError::LockPoisoned)?;
        let gate = recorded
            .clone()
            .ok_or(RotationStateError::MissingCandidateDuringNonactivation)?;
        *recorded = gate.remove_candidate(generation, mutation)?;
        Ok(())
    }

    pub fn replace_candidate_mutation(
        &self,
        generation: u64,
        previous: coven_protocol::store_commit::ObjectHash,
        replacement: coven_protocol::store_commit::ObjectHash,
    ) -> Result<(), RotationStateError> {
        let mut recorded = self
            .0
            .write()
            .map_err(|_| RotationStateError::LockPoisoned)?;
        let gate = recorded
            .clone()
            .ok_or(RotationStateError::MissingCandidateDuringReplacement)?;
        *recorded = Some(gate.replace_candidate_mutation(generation, previous, replacement)?);
        Ok(())
    }

    pub fn gate(&self) -> Option<RotationGate> {
        self.0.read().unwrap().clone()
    }

    pub fn install_durable_gate(&self, gate: Option<RotationGate>) {
        *self.0.write().unwrap() = gate;
    }

    /// Check the live generation against the committed generation, if one is pending. A
    /// plaintext home never rotates a store key (sharing, and hence removal,
    /// requires an encrypted home), so it is never blocked.
    pub fn check(&self, live_generation: Option<u64>) -> Result<(), RotationPending> {
        let Some(live_generation) = live_generation else {
            return Ok(());
        };
        if let Some(gate) = self.gate() {
            return Err(RotationPending {
                state: gate.pending_state(),
                live_generation,
            });
        }
        Ok(())
    }

    /// Record that the cloud has committed `generation` and this device has not
    /// folded it into its live cipher. Forward-only: a generation not newer than
    /// one already recorded leaves the recorded value untouched, so an older
    /// rediscovery (e.g. a decoy wrap from a non-rotating owner) can never erase
    /// a genuinely newer generation already known to be pending.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn mark_committed(&self, generation: u64) -> Result<(), RotationStateError> {
        let mut recorded = self
            .0
            .write()
            .map_err(|_| RotationStateError::LockPoisoned)?;
        *recorded = Some(RotationGate::merge_peer_commit(
            recorded.clone(),
            generation,
        )?);
        Ok(())
    }

    /// The recorded committed generation, if any is pending — for status
    /// reporting independent of a specific cipher snapshot.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn pending_generation(&self) -> Option<u64> {
        self.0
            .read()
            .unwrap()
            .as_ref()
            .map(|gate| gate.generation().get())
    }
}

impl CloudSyncRotationStateAccess for PendingRotation {
    fn mark_candidate(
        &self,
        generation: u64,
        mutation: ObjectHash,
    ) -> Result<(), RotationStateError> {
        PendingRotation::mark_candidate(self, generation, mutation)
    }

    fn mark_committed_mutation(
        &self,
        generation: u64,
        mutation: ObjectHash,
    ) -> Result<(), RotationStateError> {
        PendingRotation::mark_committed_mutation(self, generation, mutation)
    }

    fn remove_candidate(
        &self,
        generation: u64,
        mutation: ObjectHash,
    ) -> Result<(), RotationStateError> {
        PendingRotation::remove_candidate(self, generation, mutation)
    }

    fn replace_candidate_mutation(
        &self,
        generation: u64,
        previous: ObjectHash,
        replacement: ObjectHash,
    ) -> Result<(), RotationStateError> {
        PendingRotation::replace_candidate_mutation(self, generation, previous, replacement)
    }

    fn gate(&self) -> Option<RotationGate> {
        PendingRotation::gate(self)
    }

    fn install_durable_gate(&self, gate: Option<RotationGate>) {
        PendingRotation::install_durable_gate(self, gate);
    }

    fn check(&self, live_generation: Option<u64>) -> Result<(), RotationPending> {
        PendingRotation::check(self, live_generation)
    }
}
