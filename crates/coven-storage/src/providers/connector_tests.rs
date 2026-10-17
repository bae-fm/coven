use super::*;
use coven_foundation::{clock::SystemClock, id_source::UuidIds};

#[tokio::test]
async fn cloudkit_is_typed_unavailable_without_an_apple_framework() {
    let connector = ProviderConnector::new(Arc::new(SystemClock), Arc::new(UuidIds));
    let store = StoreId(uuid::Uuid::from_u128(1));
    assert_eq!(
        connector
            .prepare_cloudkit("iCloud.fm.bae.desktop", store)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::ProviderUnavailable
    );
    let result = connector
        .connect(
            StorageConfig::CloudKit {
                container: "iCloud.fm.bae.desktop".into(),
                owner: "owner".into(),
                zone: "zone".into(),
            },
            ProviderAuth::CloudKit,
            Arc::new(crate::test_utils::UnrestrictedPublications),
            Arc::new(crate::NoCooldown),
            Arc::new(crate::TransferBudget::new(crate::TransferLimits::default())),
        )
        .await;
    assert_eq!(
        result
            .err()
            .expect("unsupported provider refused")
            .failure(),
        StorageFailure::ProviderUnavailable
    );
}
