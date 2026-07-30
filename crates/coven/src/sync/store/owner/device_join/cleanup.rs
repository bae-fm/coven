use super::journal::require_distinct_slots;
use super::*;

impl Store {
    #[doc(hidden)]
    pub(crate) async fn prepare_device_join_cleanup(
        &self,
        cancellation: DeviceJoinCancellation,
        administrator_terminal: ProviderAdminJoinTerminal,
        joiner_terminal: JoinerJoinTerminal,
    ) -> Result<DeviceJoinCleanupReceipt, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        writer
            .join_operation()
            .prepare_cleanup(cancellation, administrator_terminal, joiner_terminal)
            .await
    }

    #[doc(hidden)]
    pub(crate) async fn activate_device_join_cleanup(
        &self,
        receipt: DeviceJoinCleanupReceipt,
    ) -> Result<DeviceJoinCleanupActivation, DeviceJoinError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
        writer.join_operation().activate_cleanup(receipt).await
    }
}

#[async_trait::async_trait]
pub trait DeviceJoinWriteRevocationExecutor: Send + Sync {
    /// Idempotently withdraws the exact provider authority, then verifies that
    /// the withdrawn authority cannot write any `protected_slots` before
    /// returning its provider-specific evidence.
    async fn revoke_write_authority(
        &self,
        producer: DeviceJoinProducer,
        authority: &ProviderWriteAuthorityRef,
        locator: &crate::protocol::provider::ProviderAccessLocator,
        protected_slots: &[ObjectSlot],
    ) -> Result<ProviderAccessWithdrawal, DeviceJoinError>;
}

pub(super) fn canonical_cleanup_slots(
    attempt: &DeviceJoinAttempt,
) -> Result<Vec<ObjectSlot>, DeviceJoinError> {
    let mut slots = vec![
        attempt.registration_slot.clone(),
        attempt
            .expected_registration
            .acknowledgements
            .first_slot()
            .clone(),
    ];
    match (
        &attempt.provider_approval.admission,
        &attempt.provider_response,
    ) {
        (
            DeviceProviderAdmissionChallenge::SamePrincipal,
            DeviceProviderResponseReservation::SamePrincipal,
        ) => {}
        (
            DeviceProviderAdmissionChallenge::CrossPrincipal(challenge),
            DeviceProviderResponseReservation::CrossPrincipal { response_slot },
        ) => {
            slots.push(challenge.administrator_object.slot.clone());
            slots.push(response_slot.clone());
        }
        _ => return Err(DeviceJoinError::AttemptMismatch),
    }
    slots.sort();
    require_distinct_slots(&slots)?;
    Ok(slots)
}

pub(super) fn require_cancelled_outcome(
    outcome: &DeviceJoinOutcomeRef,
) -> Result<(), DeviceJoinError> {
    if matches!(outcome, DeviceJoinOutcomeRef::Cancelled { .. }) {
        Ok(())
    } else {
        Err(DeviceJoinError::AttemptMismatch)
    }
}

pub(super) fn validate_terminals(
    cancellation: &DeviceJoinOutcomeRef,
    administrator: &ProviderAdminJoinTerminal,
    joiner: &JoinerJoinTerminal,
) -> Result<(), DeviceJoinError> {
    let administrator_cancellation = match administrator {
        ProviderAdminJoinTerminal::Completed(completion) => {
            if completion.readiness.proof.attempt != *cancellation.attempt() {
                return Err(DeviceJoinError::AttemptMismatch);
            }
            None
        }
        ProviderAdminJoinTerminal::Cancelled(closure) => Some(&closure.cancellation),
        ProviderAdminJoinTerminal::WriteRevoked(revocation) => Some(&revocation.cancellation),
    };
    let joiner_cancellation = match joiner {
        JoinerJoinTerminal::Ready(readiness) => {
            if readiness.proof.attempt != *cancellation.attempt() {
                return Err(DeviceJoinError::AttemptMismatch);
            }
            None
        }
        JoinerJoinTerminal::Cancelled(closure) => Some(&closure.cancellation),
        JoinerJoinTerminal::WriteRevoked(revocation) => Some(&revocation.cancellation),
    };
    if administrator_cancellation.is_some_and(|value| value != cancellation)
        || joiner_cancellation.is_some_and(|value| value != cancellation)
    {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    Ok(())
}
