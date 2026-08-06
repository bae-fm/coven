//! Restore-code generation from a Store's security owner: what each provider
//! configuration encodes, and which configurations refuse to generate one.

use std::sync::Arc;

use super::*;
use crate::config::{CloudProvider, HomeStorage};
use crate::restore_code::decode_restore_code;
use crate::storage::cloud::CloudHomeJoinInfo;
use crate::store_dir::StoreDir;

fn membership_floor(author_pubkey: String) -> Vec<crate::protocol::membership::MembershipHeadRef> {
    let coord = crate::protocol::membership::MembershipCoord {
        author_pubkey,
        author_owner_grant: crate::protocol::membership::MembershipGrantId(
            crate::protocol::store_commit::ObjectHash::digest(b"restore test owner grant"),
        ),
        stream_id: "0000000000000000000000000000000000000000000000000000000000000001"
            .parse()
            .expect("canonical test author stream id"),
        seq: 1,
        entry_hash: crate::protocol::store_commit::ObjectHash::digest(
            b"restore test founder entry",
        ),
    };
    let stored = b"restore setup membership head";
    vec![crate::protocol::membership::MembershipHeadRef {
        coord,
        head_hash: crate::protocol::store_commit::ObjectHash::digest(
            b"restore setup membership head semantic bytes",
        ),
        object: crate::protocol::objects::ExactObjectRef::new(
            crate::protocol::objects::ObjectSlot::logical(
                "store-v1/membership/heads/restore-setup/1.json".to_string(),
            )
            .expect("valid test membership-head slot"),
            stored.len() as u64,
            crate::protocol::store_commit::ObjectHash::digest(stored),
        ),
    }]
}

fn store_root() -> crate::protocol::store_commit::StoreRootRef {
    let stored = b"restore setup Store root";
    crate::protocol::store_commit::StoreRootRef {
        store_root_id: crate::protocol::store_commit::ObjectHash::digest(
            b"restore setup Store root identity",
        ),
        store_root_hash: crate::protocol::store_commit::ObjectHash::digest(stored),
        object: crate::protocol::objects::ExactObjectRef::new(
            crate::protocol::objects::ObjectSlot::logical(
                "store-v1/protocol/root/restore-setup.json".to_string(),
            )
            .expect("valid test Store-root slot"),
            stored.len() as u64,
            crate::protocol::store_commit::ObjectHash::digest(stored),
        ),
    }
}

fn restore_authority() -> crate::protocol::recovery::RestoreAuthority {
    let owner_grant = crate::protocol::membership::MembershipGrantId(
        crate::protocol::store_commit::ObjectHash::digest(b"restore test owner grant"),
    );
    let root = store_root();
    let owner_pubkey = hex::encode([7u8; 32]);
    let anchor = crate::protocol::store_commit::GrantStreamAnchor::OwnerRecovery {
        first_slot: crate::protocol::objects::ObjectSlot::logical(
            "store-v1/recovery/restore-setup/first.json".to_string(),
        )
        .expect("valid recovery slot"),
    };
    let activation = crate::protocol::store_commit::OwnerRecoveryActivationId::derive(
        &root,
        &owner_pubkey,
        &owner_grant,
        &anchor,
    )
    .expect("valid recovery activation");
    crate::protocol::recovery::RestoreAuthority::OwnerRecovery(
        crate::protocol::recovery::OwnerRecoveryAuthority {
            owner_identity_secret: hex::encode(
                crate::keys::UserKeypair::generate().to_keypair_bytes(),
            ),
            owner_grant: owner_grant.clone(),
            recovery: crate::protocol::store_commit::OwnerRecoveryCursor {
                owner_grant,
                position: crate::protocol::store_commit::OwnerRecoveryPosition::BeforeFirst {
                    activation,
                },
            },
            published_at: "2026-07-17T00:00:00Z".to_string(),
        },
    )
}

/// A CloudKit config with `storage: Browsable` so the test exercises only
/// the CloudKit provider arm, never the opaque-home encryption-key read
/// (that path is unrelated to the restore-code provider guard under test).
fn cloudkit_config(owner_zone: Option<(&str, &str)>) -> Config {
    let mut config = Config::with_defaults(
        "store-1".to_string(),
        "device-1".to_string(),
        StoreDir::new("unused-store-dir"),
        "CloudKit Store".to_string(),
    );
    config.cloud_home.provider = Some(CloudProvider::CloudKit);
    config.cloud_home.storage = HomeStorage::Browsable;
    if let Some((owner, zone)) = owner_zone {
        config.cloud_home.cloudkit_owner_name = Some(owner.to_string());
        config.cloud_home.cloudkit_zone_name = Some(zone.to_string());
    }
    config
}

fn store_security(
    config: &Config,
    keys: StoreKeys,
    master_keys: Arc<dyn MasterKeyCustody>,
) -> StoreSecurity {
    let identity = crate::identity_custody::IdentityCustody::InMemory(UserKeypair::generate())
        .resolve(&keys, &config.store_dir);
    StoreSecurity::new(keys, master_keys, identity)
}

#[test]
fn custom_s3_exact_slot_assertion_stays_out_of_restore_wire() {
    crate::keys::test_keyring::install();
    let dir = tempfile::tempdir().expect("store directory");
    let mut config = Config::with_defaults(
        "local-assertion-wire-test".to_string(),
        "device-1".to_string(),
        StoreDir::new(dir.path()),
        "Wire Test".to_string(),
    );
    config.cloud_home.provider = Some(CloudProvider::S3);
    config.cloud_home.storage = HomeStorage::Browsable;
    config.cloud_home.s3_bucket = Some("bucket".to_string());
    config.cloud_home.s3_region = Some("region".to_string());
    config.cloud_home.s3_endpoint = Some("https://objects.example".to_string());
    config.cloud_home.s3_exact_slots = Some(crate::CustomS3ExactSlots::StandardConditionalRequests);
    let key_service = StoreKeys::bind(config.store_id.clone());
    key_service
        .set_cloud_home_credentials(&crate::keys::CloudHomeCredentials::S3 {
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
        })
        .expect("store credentials");
    let custody =
        crate::custody::KeyCustody::InMemory(crate::encryption::MasterKeyring::generate())
            .resolve(&key_service, &config.store_dir);
    let security = store_security(&config, key_service, custody);
    let encoded = security
        .generate_restore_code(
            &config,
            store_root(),
            hex::encode([7u8; 32]),
            crate::protocol::membership::MembershipFloor(membership_floor(hex::encode([7u8; 32]))),
            restore_authority(),
        )
        .expect("generate restore code");
    let decoded = decode_restore_code(&encoded).expect("decode restore code");
    let provider_wire = serde_json::to_string(&decoded.provider).expect("serialize provider");

    assert!(!provider_wire.contains("s3_exact_slots"));
    assert!(!provider_wire.contains("strong_reads"));
    assert_eq!(
        config.cloud_home.s3_exact_slots,
        Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
    );
}

/// A device that joined via a CloudKit share has `cloudkit_owner_name` /
/// `cloudkit_zone_name` set (`build_config`'s `CloudKitShare` arm).
/// Restoring a share is illegitimate — decode already rejects
/// `CloudKitShare` (`RestoreCodeError::CloudKitShareNotRestorable`) — so
/// generation must refuse a share-joined config too, rather than mapping
/// it to `CloudHomeJoinInfo::CloudKit`, which would restore against the
/// restoring device's own empty private zone.
#[test]
fn generate_restore_code_rejects_a_share_joined_cloudkit_config() {
    crate::keys::test_keyring::install();

    let config = cloudkit_config(Some(("owner-name", "zone-name")));
    let key_service = StoreKeys::bind(config.store_id.clone());
    let custody = crate::custody::KeyCustody::Keyring.resolve(&key_service, &config.store_dir);
    let security = store_security(&config, key_service, custody);
    let err = security
        .generate_restore_code(
            &config,
            store_root(),
            hex::encode([7u8; 32]),
            crate::protocol::membership::MembershipFloor(membership_floor(hex::encode([7u8; 32]))),
            restore_authority(),
        )
        .expect_err("a share-joined CloudKit config must not generate a restore code");
    let message = err.to_string();
    assert!(
        message.contains("share"),
        "error should explain the store was joined via a share: {message}"
    );
}

/// A truly private CloudKit config (no owner/zone set) still emits a `ck`
/// restore code that decodes back to `CloudHomeJoinInfo::CloudKit`.
#[test]
fn generate_restore_code_private_cloudkit_round_trips() {
    crate::keys::test_keyring::install();

    let config = cloudkit_config(None);
    let key_service = StoreKeys::bind(config.store_id.clone());
    let custody = crate::custody::KeyCustody::Keyring.resolve(&key_service, &config.store_dir);
    let security = store_security(&config, key_service, custody);
    let code = security
        .generate_restore_code(
            &config,
            store_root(),
            hex::encode([7u8; 32]),
            crate::protocol::membership::MembershipFloor(membership_floor(hex::encode([7u8; 32]))),
            restore_authority(),
        )
        .expect("a private CloudKit config generates a restore code");
    let decoded = decode_restore_code(&code).expect("generated code decodes");
    assert!(
        matches!(decoded.provider, CloudHomeJoinInfo::CloudKit),
        "expected CloudKit provider, got {:?}",
        decoded.provider
    );
}
