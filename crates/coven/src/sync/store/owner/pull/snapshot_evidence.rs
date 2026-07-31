use super::*;

#[derive(Debug)]
pub(crate) struct VerifiedStoreSnapshotStability {
    authority: super::retained_replay::RetainedReplaySnapshotAuthority,
}

impl VerifiedStoreSnapshotStability {
    pub(crate) fn from_authority(
        authority: super::retained_replay::RetainedReplaySnapshotAuthority,
    ) -> Result<Self, StorePullError> {
        authority
            .validate()
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        Ok(Self { authority })
    }

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
    commit_verifier: &StoreCommitVerifier<'_>,
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
        let registration = commit_verifier
            .load_registration(&record.registration)
            .await?;
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
