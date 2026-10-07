//! Provider composition shared by bootstrap and subsequent connections.

use crate::*;
use coven_storage::{providers::ProviderConnector, RestoreStorage, Storage};
use std::sync::Arc;

pub(crate) async fn connect(
    data: &RestoreStorage,
    device: DeviceId,
    cloudkit: Option<Arc<dyn CloudKitOps>>,
    clock: ClockRef,
    ids: IdSourceRef,
) -> Result<Arc<dyn Storage>, StorageError> {
    ProviderConnector::new(clock, ids, cloudkit)
        .connect(data.location.clone(), data.credentials.clone(), device)
        .await
}
