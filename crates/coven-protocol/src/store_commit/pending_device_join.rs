use super::*;

impl StoreBatchCommit {
    /// Opening an Attempt creates an image consumer. Separate registration or
    /// abandonment ends that consumer; combined registration keeps the image
    /// until the registered device has published against accepted history.
    pub fn has_pending_device_join_bootstrap<'a>(
        &self,
        state: &ResolvedStoreDeviceState,
        frontier: &CommitFrontier,
        accepted: impl IntoIterator<Item = &'a StoreBatchCommit>,
    ) -> Result<bool, StoreProtocolError> {
        let accepted = accepted.into_iter().collect::<Vec<_>>();
        for decision in self.device_join_attempt_decisions() {
            let DeviceJoinAttemptDecisionRef::Attempt(attempt_id) = decision else {
                continue;
            };
            if let Some(activation) = self.device_registrations().iter().find(|activation| {
                matches!(&activation.authority, StoreDeviceRegistrationActivationRef::Join { attempt_id: registered } if registered == attempt_id)
            }) {
                if self.registration_has_unconsumed_bootstrap(&activation.registration, state, frontier)? {
                    return Ok(true);
                }
                continue;
            }
            let terminated = accepted.iter().any(|commit| {
                commit.device_registrations().iter().any(|activation| {
                    matches!(&activation.authority, StoreDeviceRegistrationActivationRef::Join { attempt_id: registered } if registered == attempt_id)
                }) || commit.device_join_attempt_decisions().iter().any(|decision| {
                    matches!(decision, DeviceJoinAttemptDecisionRef::Abandoned(abandonment) if &abandonment.attempt_id == attempt_id)
                })
            });
            if !terminated {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// A same-principal activation opens its attempt and registers the device
    /// together. Until that exact device publishes, its original handoff is a
    /// live consumer of the selected snapshot and following accepted interval.
    pub fn pending_bootstrap_registration(
        &self,
        state: &ResolvedStoreDeviceState,
        frontier: &CommitFrontier,
    ) -> Result<Option<&StoreDeviceRegistrationRef>, StoreProtocolError> {
        let [registration] = self.device_registrations() else {
            return Ok(None);
        };
        let StoreDeviceRegistrationActivationRef::Join { attempt_id } = &registration.authority
        else {
            return Ok(None);
        };
        if !self.device_join_attempt_decisions().iter().any(|decision| {
            matches!(decision, DeviceJoinAttemptDecisionRef::Attempt(opened) if opened == attempt_id)
        }) {
            return Ok(None);
        }
        let target = &registration.registration;
        Ok(self
            .registration_has_unconsumed_bootstrap(target, state, frontier)?
            .then_some(target))
    }

    fn registration_has_unconsumed_bootstrap(
        &self,
        target: &StoreDeviceRegistrationRef,
        state: &ResolvedStoreDeviceState,
        frontier: &CommitFrontier,
    ) -> Result<bool, StoreProtocolError> {
        let device = state
            .devices
            .get(&target.device_id)
            .filter(|device| &device.registration == target)
            .ok_or(StoreProtocolError::DeviceStateMismatch)?;
        let stream = StreamActivation::device_authorized_stream_id(
            self.store_root_hash,
            target,
            StreamAnchorDomain::StoreAnnouncements,
        );
        Ok(matches!(device.status, StoreDeviceStatus::Active)
            && !frontier.commits().contains_key(&stream))
    }
}

impl RetainedVerifiedMergeHistorySummary {
    pub fn pending_device_join_accepted_commit(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Result<Option<AcceptedStoreCommitPublication>, StoreProtocolError> {
        let mut accepted = None;
        for closure in self.pending_device_joins.values() {
            if !closure
                .commits
                .iter()
                .any(|commit| &commit.reference == reference)
            {
                continue;
            }
            let exact = closure.accepted_commit(reference)?;
            if accepted.as_ref().is_some_and(|previous| previous != &exact) {
                return Err(StoreProtocolError::Malformed(
                    "pending Join closures disagree on an accepted commit".into(),
                ));
            }
            accepted = Some(exact);
        }
        Ok(accepted)
    }

    /// Physical deletion stays unfinished while an accepted Join still consumes
    /// the original snapshot and the exact interval offered with it.
    pub fn pending_device_join_artifacts(
        &self,
    ) -> Result<BTreeSet<crate::objects::ExactObjectRef>, StoreProtocolError> {
        let mut objects = BTreeSet::new();
        for closure in self.pending_device_joins.values() {
            let snapshot = closure
                .publication
                .current
                .latest_snapshot()
                .ok_or_else(|| {
                    StoreProtocolError::Malformed("pending Join has no snapshot boundary".into())
                })?;
            let retained = self
                .reclaim
                .snapshots
                .get(&snapshot.snapshot.snapshot_hash)
                .filter(|retained| &retained.accepted == snapshot)
                .ok_or_else(|| {
                    StoreProtocolError::Malformed(
                        "pending Join snapshot has no exact retained artifact ownership".into(),
                    )
                })?;
            objects.extend(retained.objects().into_iter().cloned());
            objects.extend(
                closure
                    .publication
                    .verify()?
                    .entries()
                    .iter()
                    .map(|entry| entry.reference().object.clone()),
            );
        }
        Ok(objects)
    }

    pub fn pending_device_join_snapshot_slots(&self) -> BTreeSet<crate::objects::ObjectSlot> {
        self.pending_device_joins
            .values()
            .filter_map(|closure| {
                closure
                    .publication
                    .current
                    .latest_snapshot()
                    .map(|snapshot| snapshot.snapshot.object.slot().clone())
            })
            .collect()
    }
}
