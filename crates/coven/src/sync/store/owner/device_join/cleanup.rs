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
        locator: &coven_protocol::provider::ProviderAccessLocator,
        protected_slots: &[ObjectSlot],
    ) -> Result<ProviderAccessWithdrawal, DeviceJoinError>;
}
