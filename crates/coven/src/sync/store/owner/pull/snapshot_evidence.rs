use super::*;

#[derive(Debug)]
pub(crate) struct VerifiedStoreSnapshotStability {
    authority: crate::database::RetainedReplaySnapshotAuthority,
}

impl VerifiedStoreSnapshotStability {
    pub(crate) fn from_authority(
        authority: crate::database::RetainedReplaySnapshotAuthority,
    ) -> Result<Self, StorePullError> {
        authority
            .validate()
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        Ok(Self { authority })
    }

    pub(crate) fn into_authority(self) -> crate::database::RetainedReplaySnapshotAuthority {
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
