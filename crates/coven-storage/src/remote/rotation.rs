use super::*;

pub struct PendingRotation(std::sync::RwLock<Option<RotationGate>>);

pub trait CloudSyncRotationStateAccess: Send + Sync {
    fn mark_candidate(&self, generation: u64, mutation: ObjectHash) -> Result<(), String>;
    fn mark_committed_mutation(&self, generation: u64, mutation: ObjectHash) -> Result<(), String>;
    fn remove_candidate(&self, generation: u64, mutation: ObjectHash) -> Result<(), String>;
    fn replace_candidate_mutation(
        &self,
        generation: u64,
        previous: ObjectHash,
        replacement: ObjectHash,
    ) -> Result<(), String>;
    fn gate(&self) -> Option<RotationGate>;
    fn install_durable_gate(&self, gate: Option<RotationGate>);
    fn check(&self, cipher: &CloudCipher) -> Result<(), RotationPending>;
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
    ) -> Result<(), String> {
        let mut recorded = self.0.write().unwrap();
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
    ) -> Result<(), String> {
        let mut recorded = self.0.write().unwrap();
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
    ) -> Result<(), String> {
        let mut recorded = self.0.write().unwrap();
        let gate = recorded.clone().ok_or_else(|| {
            "rotation candidate gate is absent during proven nonactivation".to_string()
        })?;
        *recorded = gate.remove_candidate(generation, mutation)?;
        Ok(())
    }

    pub fn replace_candidate_mutation(
        &self,
        generation: u64,
        previous: coven_protocol::store_commit::ObjectHash,
        replacement: coven_protocol::store_commit::ObjectHash,
    ) -> Result<(), String> {
        let mut recorded = self.0.write().unwrap();
        let gate = recorded.clone().ok_or_else(|| {
            "rotation candidate gate is absent during candidate replacement".to_string()
        })?;
        *recorded = Some(gate.replace_candidate_mutation(generation, previous, replacement)?);
        Ok(())
    }

    pub fn gate(&self) -> Option<RotationGate> {
        self.0.read().unwrap().clone()
    }

    pub fn install_durable_gate(&self, gate: Option<RotationGate>) {
        *self.0.write().unwrap() = gate;
    }

    /// Check `cipher` against the committed generation, if one is pending. A
    /// plaintext home never rotates a store key (sharing, and hence removal,
    /// requires an encrypted home), so it is never blocked.
    pub fn check(&self, cipher: &CloudCipher) -> Result<(), RotationPending> {
        let live_generation = match cipher {
            CloudCipher::Encrypted(enc) => enc.current_generation(),
            CloudCipher::Plaintext => return Ok(()),
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
    pub fn mark_committed(&self, generation: u64) -> Result<(), String> {
        let mut recorded = self.0.write().unwrap();
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
    fn mark_candidate(&self, generation: u64, mutation: ObjectHash) -> Result<(), String> {
        PendingRotation::mark_candidate(self, generation, mutation)
    }

    fn mark_committed_mutation(&self, generation: u64, mutation: ObjectHash) -> Result<(), String> {
        PendingRotation::mark_committed_mutation(self, generation, mutation)
    }

    fn remove_candidate(&self, generation: u64, mutation: ObjectHash) -> Result<(), String> {
        PendingRotation::remove_candidate(self, generation, mutation)
    }

    fn replace_candidate_mutation(
        &self,
        generation: u64,
        previous: ObjectHash,
        replacement: ObjectHash,
    ) -> Result<(), String> {
        PendingRotation::replace_candidate_mutation(self, generation, previous, replacement)
    }

    fn gate(&self) -> Option<RotationGate> {
        PendingRotation::gate(self)
    }

    fn install_durable_gate(&self, gate: Option<RotationGate>) {
        PendingRotation::install_durable_gate(self, gate);
    }

    fn check(&self, cipher: &CloudCipher) -> Result<(), RotationPending> {
        PendingRotation::check(self, cipher)
    }
}
