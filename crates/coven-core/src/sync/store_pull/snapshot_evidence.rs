use super::*;

#[derive(Debug)]
pub(crate) struct VerifiedStoreSnapshotStability {
    authority: super::retained_replay::RetainedReplaySnapshotAuthority,
}

impl VerifiedStoreSnapshotStability {
    pub(crate) fn into_authority(self) -> super::retained_replay::RetainedReplaySnapshotAuthority {
        self.authority
    }
}

pub(crate) struct VerifiedActivatedStoreAck {
    pub(crate) reference: super::store_commit::StoreAckRef,
    pub(crate) value: super::store_commit::StoreAck,
    pub(crate) chain: BTreeMap<
        u64,
        (
            super::store_commit::StoreAckRef,
            super::store_commit::StoreAck,
        ),
    >,
    pub(crate) activating_commit: StoreBatchCommitRef,
    pub(crate) activating_commit_value: StoreBatchCommit,
}

pub(crate) struct VerifiedSnapshotState {
    pub(crate) device_state: ResolvedStoreDeviceState,
    pub(crate) active_registrations: BTreeMap<
        super::store_commit::StoreDeviceId,
        (StoreDeviceRegistrationRef, StoreDeviceRegistration),
    >,
}

pub(crate) async fn load_active_history_registrations(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    state: &ResolvedStoreDeviceState,
) -> Result<
    BTreeMap<
        super::store_commit::StoreDeviceId,
        (StoreDeviceRegistrationRef, StoreDeviceRegistration),
    >,
    StorePullError,
> {
    let mut active = BTreeMap::new();
    for (device_id, record) in &state.devices {
        if !matches!(record.status, StoreDeviceStatus::Active) {
            continue;
        }
        let registration = load_registration_ref(storage, root, &record.registration).await?;
        if registration.value.device_id != *device_id {
            return Err(StorePullError::Database(
                "resolved Store device state names another exact registration".to_string(),
            ));
        }
        active.insert(
            *device_id,
            (record.registration.clone(), registration.value),
        );
    }
    Ok(active)
}

pub(crate) async fn assemble_snapshot_stability(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    snapshot: &crate::database::PublishedStoreSnapshot,
    snapshot_cut: StoreHistoryCut,
    accepted_cut: StoreHistoryCut,
    snapshot_state: VerifiedSnapshotState,
    accepted_acknowledgements: Vec<VerifiedActivatedStoreAck>,
) -> Result<VerifiedStoreSnapshotStability, StorePullError> {
    let mut acknowledgements = BTreeMap::new();
    for (device_id, (registration_ref, registration)) in &snapshot_state.active_registrations {
        let matching = accepted_acknowledgements
            .iter()
            .filter(|ack| {
                ack.value.registration == *registration_ref
                    && ack.value.snapshot.as_ref().is_some_and(|acknowledged| {
                        acknowledged.author_registration == snapshot.meta.author_registration
                            && acknowledged.snapshot == snapshot.reference
                    })
                    && ack.value.device_state == snapshot.meta.state.devices
                    && ack
                        .value
                        .store_cut
                        .frontier()
                        .covers(&snapshot.meta.coverage)
            })
            .max_by_key(|ack| (ack.reference.sequence, ack.activating_commit.clone()))
            .ok_or_else(|| StorePullError::SnapshotNotStable {
                member: registration.author_pubkey.clone(),
                device_id: device_id.to_string(),
            })?;
        acknowledgements.insert(
            *device_id,
            super::store_commit::RetainedVerifiedActivatedAck {
                chain: matching.chain.clone(),
                activating_commit: matching.activating_commit.clone(),
                activating_commit_value: matching.activating_commit_value.clone(),
            },
        );
    }
    let founder = load_founder_registration(storage, root).await?;
    let authority = super::retained_replay::RetainedReplaySnapshotAuthority {
        store_root: root.clone(),
        founder_registration: StoreDeviceRegistrationRef::from_registration(
            &founder.value,
            founder.object,
        ),
        snapshot: snapshot.reference.clone(),
        metadata: snapshot.meta.clone(),
        snapshot_cut,
        accepted_cut,
        device_state: snapshot_state.device_state,
        active_registrations: snapshot_state
            .active_registrations
            .into_iter()
            .map(|(device_id, (reference, value))| {
                (
                    device_id,
                    super::store_commit::RetainedVerifiedRegistration { reference, value },
                )
            })
            .collect(),
        acknowledgements,
    };
    authority
        .validate()
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    Ok(VerifiedStoreSnapshotStability { authority })
}
