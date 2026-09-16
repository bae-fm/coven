use super::*;
use crate::provider::test_fixtures::{test_device_binding, test_exact_receipt, test_store_binding};

#[test]
fn credential_rotation_generation_exhaustion_is_not_a_successor() {
    assert!(ProviderAccessWithdrawal::S3CredentialRotation {
        retired_generation: u64::MAX,
        active_generation: u64::MAX,
        retired_credential_verified_rejected: true,
    }
    .validate()
    .is_err());
}

#[test]
fn exact_probe_verifier_rejects_two_created_contenders() {
    let mut receipt = test_exact_receipt();
    receipt
        .verify(&test_store_binding(), &test_device_binding())
        .expect("baseline exact receipt verifies");
    receipt.transcript.contenders[1].outcome = ProbeCreateOutcome::Created;

    assert!(receipt
        .verify(&test_store_binding(), &test_device_binding())
        .is_err());
}

#[test]
fn custom_s3_origin_rejects_paths_and_canonicalizes_default_port() {
    assert_eq!(
        canonical_custom_s3_origin("HTTPS://Objects.Example:443").unwrap(),
        "https://objects.example"
    );
    assert!(canonical_custom_s3_origin("https://objects.example/").is_err());
    assert!(canonical_custom_s3_origin("https://objects.example/bucket").is_err());
}

#[test]
fn private_cloudkit_owner_exposes_its_exact_administrator_locator() {
    let binding = private_cloudkit_binding();

    let locator = ProviderAccessLocator::for_current_administrator(&binding)
        .expect("private CloudKit owner exposes its exact administrator locator");

    assert_eq!(
        locator,
        ProviderAccessLocator::CloudKitPrivateZoneOwner {
            owner_name: "private-owner".to_string(),
            zone_name: "private-zone".to_string(),
            owner_record_name: "current-user".to_string(),
        }
    );
    locator
        .validate_for(&binding.store, &binding.device)
        .expect("private CloudKit owner locator matches its binding");
}

#[test]
fn shared_cloudkit_participant_is_not_treated_as_the_zone_owner() {
    let mut binding = private_cloudkit_binding();
    binding.device.principal = crate::objects::ProviderPrincipalId::CloudKitSharedZoneParticipant {
        record_name: "current-user".to_string(),
    };

    assert!(ProviderAccessLocator::for_current_administrator(&binding).is_err());
}

fn private_cloudkit_binding() -> crate::objects::ResolvedProviderBinding {
    crate::objects::ResolvedProviderBinding {
        store: crate::objects::StoreProviderBinding::CloudKit {
            container_id: "iCloud.example.coven".to_string(),
            environment: crate::objects::CloudKitEnvironment::Development,
            owner_name: "private-owner".to_string(),
            zone_name: "private-zone".to_string(),
        },
        device: crate::objects::ProviderDeviceBinding {
            principal: crate::objects::ProviderPrincipalId::CloudKitPrivateZoneOwner {
                record_name: "current-user".to_string(),
            },
        },
    }
}
