use std::sync::Arc;

use crate::store_sync::{StoreSync, SyncError};
use coven_protocol::membership::{MemberInfo, MemberRole};

const DEVICE_EXCLUSION_CODE_PREFIX: &str = "coven:device-exclusion:";
const OWNER_PROMOTION_REQUEST_CODE_PREFIX: &str = "coven:owner-promotion-request:";
const OWNER_PROMOTION_ACCEPTANCE_CODE_PREFIX: &str = "coven:owner-promotion-acceptance:";

#[derive(Clone)]
pub(crate) struct StoreMembership {
    sync: StoreSync,
    mutations: Arc<tokio::sync::Mutex<()>>,
}

impl StoreMembership {
    pub(crate) fn new(sync: StoreSync) -> Self {
        Self {
            sync,
            mutations: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub(crate) async fn members(&self) -> Result<Vec<MemberInfo>, SyncError> {
        if !self.sync.is_command_configured() {
            return Err(SyncError::NotConfigured);
        }
        self.sync.members().await
    }

    pub(crate) async fn conflict(
        &self,
    ) -> Result<Option<crate::MembershipConflictInfo>, SyncError> {
        if !self.sync.is_command_configured() {
            return Err(SyncError::NotConfigured);
        }
        self.sync.membership_conflict().await
    }

    pub(crate) async fn admit(
        &self,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: MemberRole,
    ) -> Result<coven_replication::sync::MemberInvitation, SyncError> {
        let _mutation = self.mutations.lock().await;
        self.sync
            .invite_member(public_key_hex, invitee_email, role)
            .await
    }

    pub(crate) async fn remove(&self, public_key_hex: &str) -> Result<(), SyncError> {
        let _mutation = self.mutations.lock().await;
        self.sync.remove_store_member(public_key_hex).await
    }

    pub(crate) async fn resolve_conflict(
        &self,
        choice: &crate::MembershipConflictChoice,
    ) -> Result<(), SyncError> {
        let _mutation = self.mutations.lock().await;
        self.sync.resolve_membership_conflict(choice).await
    }

    pub(crate) async fn propose_device_exclusion(
        &self,
        device_id: crate::StoreDeviceId,
    ) -> Result<String, SyncError> {
        let _mutation = self.mutations.lock().await;
        let proposal = self.sync.propose_device_exclusion(device_id).await?;
        Ok(coven_foundation::code_envelope::encode_code(
            DEVICE_EXCLUSION_CODE_PREFIX,
            &proposal,
        ))
    }

    pub(crate) async fn cancel_device_exclusion(&self, code: &str) -> Result<(), SyncError> {
        let proposal = decode_operation_code(DEVICE_EXCLUSION_CODE_PREFIX, code)?;
        let _mutation = self.mutations.lock().await;
        self.sync.cancel_device_exclusion(&proposal).await
    }

    pub(crate) async fn finalize_device_exclusion(&self, code: &str) -> Result<(), SyncError> {
        let proposal = decode_operation_code(DEVICE_EXCLUSION_CODE_PREFIX, code)?;
        let _mutation = self.mutations.lock().await;
        self.sync.finalize_device_exclusion(&proposal).await
    }

    pub(crate) async fn begin_owner_promotion(
        &self,
        device_id: crate::StoreDeviceId,
    ) -> Result<String, SyncError> {
        let _mutation = self.mutations.lock().await;
        let request = self.sync.begin_owner_promotion(device_id).await?;
        Ok(coven_foundation::code_envelope::encode_code(
            OWNER_PROMOTION_REQUEST_CODE_PREFIX,
            &request,
        ))
    }

    pub(crate) async fn accept_owner_promotion(&self, code: &str) -> Result<String, SyncError> {
        let request = decode_operation_code(OWNER_PROMOTION_REQUEST_CODE_PREFIX, code)?;
        let _mutation = self.mutations.lock().await;
        let acceptance = self.sync.accept_owner_promotion(request).await?;
        Ok(coven_foundation::code_envelope::encode_code(
            OWNER_PROMOTION_ACCEPTANCE_CODE_PREFIX,
            &acceptance,
        ))
    }

    pub(crate) async fn finalize_owner_promotion(&self, code: &str) -> Result<(), SyncError> {
        let acceptance = decode_operation_code(OWNER_PROMOTION_ACCEPTANCE_CODE_PREFIX, code)?;
        let _mutation = self.mutations.lock().await;
        self.sync.finalize_owner_promotion(acceptance).await
    }
}

fn decode_operation_code<T: serde::de::DeserializeOwned>(
    prefix: &str,
    code: &str,
) -> Result<T, SyncError> {
    coven_foundation::code_envelope::decode_code(prefix, code)
        .map_err(SyncError::InvalidMembershipOperationCode)
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct TestOperation {
        id: u64,
    }

    #[test]
    fn operation_codes_require_their_exact_workflow_prefix() {
        let encoded = coven_foundation::code_envelope::encode_code(
            DEVICE_EXCLUSION_CODE_PREFIX,
            &TestOperation { id: 7 },
        );

        assert_eq!(
            decode_operation_code::<TestOperation>(DEVICE_EXCLUSION_CODE_PREFIX, &encoded)
                .expect("decode matching operation code"),
            TestOperation { id: 7 },
        );
        assert!(matches!(
            decode_operation_code::<TestOperation>(OWNER_PROMOTION_REQUEST_CODE_PREFIX, &encoded),
            Err(SyncError::InvalidMembershipOperationCode(_))
        ));
    }
}
