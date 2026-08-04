use super::*;
use crate::blob::locator::{BlobLocator, RemoteAudience, StoredBlobRef};
use crate::protocol::circle::CircleId;
use crate::protocol::circle_control::CircleControlCoord;
use crate::protocol::objects::ExactObjectRef;
use crate::protocol::objects::ObjectSlot;
use crate::protocol::store_commit::{ObjectHash, StoreDeviceRegistrationRef};
use crate::KeyFingerprint;

fn uploader() -> StoreDeviceRegistrationRef {
    let bytes = b"row blob reference uploader";
    StoreDeviceRegistrationRef {
        device_id: "11".repeat(32).parse().expect("test device id"),
        registration_hash: ObjectHash::digest(bytes),
        object: ExactObjectRef::new(
            ObjectSlot::logical("store-v1/devices/row-blob-reference.json".to_string())
                .expect("test uploader slot"),
            bytes.len() as u64,
            ObjectHash::digest(bytes),
        ),
    }
}

fn circle_control() -> CircleControlCoord {
    CircleControlCoord {
        device_id: "device-a".to_string(),
        stream_id: crate::protocol::causal_grants::AuthorStreamId::from_bytes([4; 32]),
        author_pubkey: "22".repeat(32),
        author_owner_grant: crate::protocol::causal_grants::MembershipGrantId::from_test_label(
            "row blob reference owner",
        ),
        seq: 1,
        control_hash: ObjectHash::digest(b"row blob reference control"),
    }
}

fn blob() -> BlobRef {
    BlobRef {
        namespace: "covers".to_string(),
        id: "cover-a".to_string(),
        scope: BlobScope::Derived("album-a".to_string()),
        cloud_path: Some("Album A/cover.jpg".to_string()),
        provenance: Provenance::HostProvided,
        fill: CacheFill::CacheEager,
    }
}

fn stored(locator: BlobLocator) -> StoredBlobRef {
    let semantic_key = locator.semantic_key();
    StoredBlobRef::new(
        locator,
        ExactObjectRef::new(
            ObjectSlot::logical(semantic_key).expect("test blob slot"),
            91,
            ObjectHash::digest(b"stored blob bytes"),
        ),
    )
    .expect("test stored blob")
}

fn row_ref(
    blob: BlobRef,
    plaintext_size: u64,
    plaintext_hash: ObjectHash,
    authority: RowBlobAuthority,
    stored: StoredBlobRef,
) -> Result<RowBlobRef, String> {
    RowBlobRef::new(
        "albums".to_string(),
        "album-a".to_string(),
        "0000000001000-0000-device-a".to_string(),
        "cover_blob_id".to_string(),
        blob,
        plaintext_size,
        plaintext_hash,
        authority,
        Some(stored),
    )
}

#[test]
fn row_blob_reference_rejects_locator_facts_from_another_blob() {
    let row_blob = blob();
    let plaintext = b"cover plaintext";
    let plaintext_size = plaintext.len() as u64;
    let plaintext_hash = ObjectHash::digest(plaintext);
    let fingerprint = KeyFingerprint::from_bytes([7; 32]);
    let authority =
        RowBlobAuthority::Remote(crate::protocol::audience_package::PackageAudience::Circle {
            circle_id: CircleId::from_bytes([3; 16]),
            control: circle_control(),
            key_fingerprint: fingerprint,
        });

    let valid_locator = BlobLocator::opaque(
        row_blob.namespace.clone(),
        row_blob.id.clone(),
        uploader(),
        RemoteAudience::Circle(CircleId::from_bytes([3; 16])),
        row_blob.scope.clone(),
        fingerprint,
        plaintext_size,
        plaintext_hash,
    )
    .expect("valid locator");
    assert!(row_ref(
        row_blob.clone(),
        plaintext_size,
        plaintext_hash,
        authority.clone(),
        stored(valid_locator),
    )
    .is_ok());

    let mismatches = [
        BlobLocator::opaque(
            "other-namespace",
            row_blob.id.clone(),
            uploader(),
            RemoteAudience::Circle(CircleId::from_bytes([3; 16])),
            row_blob.scope.clone(),
            fingerprint,
            plaintext_size,
            plaintext_hash,
        )
        .expect("namespace-mismatched locator"),
        BlobLocator::opaque(
            row_blob.namespace.clone(),
            "other-blob",
            uploader(),
            RemoteAudience::Circle(CircleId::from_bytes([3; 16])),
            row_blob.scope.clone(),
            fingerprint,
            plaintext_size,
            plaintext_hash,
        )
        .expect("id-mismatched locator"),
        BlobLocator::opaque(
            row_blob.namespace.clone(),
            row_blob.id.clone(),
            uploader(),
            RemoteAudience::Circle(CircleId::from_bytes([3; 16])),
            BlobScope::Master,
            fingerprint,
            plaintext_size,
            plaintext_hash,
        )
        .expect("scope-mismatched locator"),
        BlobLocator::opaque(
            row_blob.namespace.clone(),
            row_blob.id.clone(),
            uploader(),
            RemoteAudience::Circle(CircleId::from_bytes([3; 16])),
            row_blob.scope.clone(),
            fingerprint,
            plaintext_size + 1,
            plaintext_hash,
        )
        .expect("size-mismatched locator"),
        BlobLocator::opaque(
            row_blob.namespace.clone(),
            row_blob.id.clone(),
            uploader(),
            RemoteAudience::Circle(CircleId::from_bytes([3; 16])),
            row_blob.scope.clone(),
            fingerprint,
            plaintext_size,
            ObjectHash::digest(b"other plaintext"),
        )
        .expect("hash-mismatched locator"),
        BlobLocator::opaque(
            row_blob.namespace.clone(),
            row_blob.id.clone(),
            uploader(),
            RemoteAudience::Circle(CircleId::from_bytes([3; 16])),
            row_blob.scope.clone(),
            KeyFingerprint::from_bytes([8; 32]),
            plaintext_size,
            plaintext_hash,
        )
        .expect("key-mismatched locator"),
    ];
    for locator in mismatches {
        assert!(row_ref(
            row_blob.clone(),
            plaintext_size,
            plaintext_hash,
            authority.clone(),
            stored(locator),
        )
        .is_err());
    }
}

#[test]
fn browsable_row_blob_reference_requires_its_declared_cloud_path() {
    let row_blob = blob();
    let plaintext = b"cover plaintext";
    let plaintext_size = plaintext.len() as u64;
    let plaintext_hash = ObjectHash::digest(plaintext);
    let wrong_path = BlobLocator::browsable(
        row_blob.namespace.clone(),
        row_blob.id.clone(),
        uploader(),
        "Album B/cover.jpg",
        plaintext_size,
        plaintext_hash,
    )
    .expect("path-mismatched locator");

    assert!(row_ref(
        row_blob,
        plaintext_size,
        plaintext_hash,
        RowBlobAuthority::Remote(crate::protocol::audience_package::PackageAudience::Store),
        stored(wrong_path),
    )
    .is_err());
}
