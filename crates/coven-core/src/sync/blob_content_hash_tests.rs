//! A blob's bytes are pinned by the content hash on its (signed) row, not by
//! where the bytes were found. Two attacks the hash closes, driven against the
//! real [`CloudSyncStorage`] over an [`InMemoryCloudHome`] so the true
//! current generated hashed keying and per-member prefixes are
//! exercised:
//!
//! 1. A member plants a same-size blob of its choosing under its OWN uploader
//!    prefix for a victim's blob id. The uploader is resolved from the row's
//!    recorded uploader (no listing scan), so the planted prefix is never
//!    consulted; and even if a reader is pointed at the attacker's prefix, the
//!    row's author-signed hash refuses the attacker's bytes before they are
//!    cached or returned.
//! 2. A blob legitimately re-uploaded under the same id (new bytes, same size) is
//!    rolled back by the provider to a prior same-size version at the same key.
//!    Nothing distinguishes the versions by key/AAD/size — but the row's hash is
//!    the current version's, so the rolled-back bytes are refused.
//!
//! A partial (range) read cannot verify the whole-blob content hash — it only
//! holds a slice — so it relies on the per-chunk AEAD (which authenticates each
//! chunk's bytes and position under the store key), the existing guarantee. The
//! whole-blob hash is verified by the whole-file read paths before a blob lands
//! in the cache, so a later ranged read serves a cache file that was already
//! hash-verified when it was written.

use std::sync::Arc;

use crate::blob::cache::{read_blob, BlobCacheError};
use crate::blob::{content_hash, BlobRef, BlobScope, CacheFill, Provenance};
use crate::database::Database;
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::session::BlobDecl;
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::{
    open_test_db_with_blob, plant_blob_row, temp_store_dir, test_blob_location,
};

const STORE_ID: &str = "lib-content-hash-test";
const STORE_KEY: [u8; 32] = [7u8; 32];
const BLOB_ID: &str = "blobxxxx";

/// A `CloudSyncStorage` over the shared cloud home, sealing under the one store
/// key with the hashed (per-uploader-prefix) scheme, keyed under `member`'s own
/// public key — the prefix its uploads land in. Every member computes the same
/// AAD and holds the same store key, so a member can seal a valid object under
/// its own prefix for any blob id; only the row's content hash distinguishes
/// whose bytes are the ones the row's author pinned.
fn member_storage(home: &InMemoryCloudHome, member: &UserKeypair) -> CloudSyncStorage {
    CloudSyncStorage::new(
        Arc::new(home.clone()),
        CloudCipher::Encrypted(EncryptionService::from_key(STORE_KEY)),
        BlobPathScheme::Hashed,
        STORE_ID,
        member.clone(),
    )
}

/// A user-provided, cache-lazy blob in the `photos` namespace — Remote reads go
/// straight to the cloud (a user-provided blob never consults the host-provided
/// local store), so the whole-blob download-and-verify path is exercised.
fn photo_db() -> Database {
    open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ))
}

fn photo_ref() -> BlobRef {
    BlobRef {
        namespace: "photos".to_string(),
        id: BLOB_ID.to_string(),
        scope: BlobScope::Master,
        cloud_path: None,
        provenance: Provenance::UserProvided,
        fill: CacheFill::CacheLazy,
    }
}

/// Member A owns the row for blob X; member B writes a same-size blob of its
/// choosing under B's own uploader prefix for X. A device reading X must never
/// serve B's bytes: the uploader is taken from the row's recorded uploader (the
/// listing scan is gone), so B's prefix is not even consulted; and if a device is
/// pointed at B's prefix, the row's author-signed hash refuses B's bytes before
/// they are cached.
#[tokio::test]
async fn a_planted_blob_under_another_uploader_is_not_served() {
    let owner = UserKeypair::generate();
    let attacker = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let attacker_pk = hex::encode(attacker.public_key());

    let home = InMemoryCloudHome::new();
    let owner_storage = member_storage(&home, &owner);
    let attacker_storage = member_storage(&home, &attacker);
    let owner_location = test_blob_location(&owner_pk, 1002);
    let attacker_location = test_blob_location(&attacker_pk, 1001);

    // A's real bytes for X, under A's prefix.
    let real = b"THE-OWNERS-REAL-BLOB".to_vec();
    // B's planted bytes for the SAME id and the SAME length, under B's prefix.
    let planted = b"THE-ATTACKERS-FAKED!".to_vec();
    assert_eq!(real.len(), planted.len(), "the plant is the same size");
    assert_ne!(real, planted);

    owner_storage
        .put_blob(
            "photos",
            &owner_location,
            BLOB_ID,
            BlobScope::Master,
            None,
            real.clone(),
        )
        .await
        .expect("owner uploads its real blob under its own prefix");
    attacker_storage
        .put_blob(
            "photos",
            &attacker_location,
            BLOB_ID,
            BlobScope::Master,
            None,
            planted.clone(),
        )
        .await
        .expect("attacker plants a same-size blob under its own prefix");

    // The reading device's row for X carries A's real content hash (signed by A on
    // the row). Reads go through A's storage (any member's storage can decrypt,
    // since the store key is shared; the uploader prefix is passed per read).
    let db = photo_db();
    let (_tmp, ld) = temp_store_dir();
    plant_blob_row(&db, BLOB_ID, true, &real).await;

    // 1. The uploader is authoritative state, not a scanned listing: with no
    //    recorded uploader the read fails loud rather than scanning and finding
    //    the attacker's planted prefix (the old, exploited behavior).
    let unresolved = read_blob(&db, &ld, Some(&owner_storage), &photo_ref())
        .await
        .expect_err("no recorded uploader must not resolve by scanning the listing");
    assert!(
        matches!(unresolved, BlobCacheError::UploaderUnresolved { .. }),
        "with no recorded uploader the read refuses instead of scanning, got {unresolved:?}",
    );

    // 2. Even when a reader is pointed at the attacker's prefix (the worst case a
    //    poisoned resolution could produce), the row's content hash refuses the
    //    attacker's bytes: they are the same size and validly sealed, but they are
    //    not what A signed.
    db.record_blob_location("photos", BLOB_ID, &attacker_location)
        .await
        .unwrap();
    let refused = read_blob(&db, &ld, Some(&attacker_storage), &photo_ref())
        .await
        .expect_err("the attacker's same-size bytes must be refused by the hash");
    match refused {
        BlobCacheError::CloudHashMismatch {
            expected, actual, ..
        } => {
            assert_eq!(expected, content_hash(&real));
            assert_eq!(actual, content_hash(&planted));
        }
        other => panic!("expected a content-hash mismatch, got {other:?}"),
    }
    assert!(
        !ld.cache_blob_path("photos", BLOB_ID, &content_hash(&real))
            .unwrap()
            .exists(),
        "the refused attacker bytes are not cached",
    );

    // 3. Pointed at A's real prefix, the read serves A's bytes and they verify.
    db.record_blob_location("photos", BLOB_ID, &owner_location)
        .await
        .unwrap();
    let served = read_blob(&db, &ld, Some(&owner_storage), &photo_ref())
        .await
        .expect("the owner's real, hash-matching bytes are served");
    assert_eq!(served, real);
}

/// A blob re-uploaded under the same id with new bytes (same size) can be rolled
/// back by the provider to the prior same-size version at the same key — same
/// key, same AAD, valid tag, matching declared size. The row carries the current
/// version's content hash, so the rolled-back bytes are refused as tamper.
#[tokio::test]
async fn a_rolled_back_blob_version_is_refused() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());

    let home = InMemoryCloudHome::new();
    let owner_storage = member_storage(&home, &owner);
    let owner_location = test_blob_location(&owner_pk, 1000);

    // Version 2 is the current content the row commits to; version 1 is a prior
    // same-size version the provider rolls the object back to.
    let v1 = b"blob-content-VERSION1".to_vec();
    let v2 = b"blob-content-VERSION2".to_vec();
    assert_eq!(v1.len(), v2.len(), "the rollback is the same size");
    assert_ne!(v1, v2);

    // The row commits to v2's hash (the current version).
    let db = photo_db();
    let (_tmp, ld) = temp_store_dir();
    plant_blob_row(&db, BLOB_ID, true, &v2).await;
    db.record_blob_location("photos", BLOB_ID, &owner_location)
        .await
        .unwrap();

    // The provider serves the rolled-back v1 at the object's key.
    owner_storage
        .put_blob(
            "photos",
            &owner_location,
            BLOB_ID,
            BlobScope::Master,
            None,
            v1.clone(),
        )
        .await
        .expect("provider serves the prior version");

    let refused = read_blob(&db, &ld, Some(&owner_storage), &photo_ref())
        .await
        .expect_err("a rolled-back prior version must be refused");
    match refused {
        BlobCacheError::CloudHashMismatch {
            expected, actual, ..
        } => {
            assert_eq!(expected, content_hash(&v2), "the row commits to v2");
            assert_eq!(actual, content_hash(&v1), "the cloud served rolled-back v1");
        }
        other => panic!("expected a content-hash mismatch, got {other:?}"),
    }
    assert!(
        !ld.cache_blob_path("photos", BLOB_ID, &content_hash(&v2))
            .unwrap()
            .exists(),
        "the rolled-back bytes are not cached",
    );

    // Once the object is restored to v2, the read verifies and serves it — the
    // round-trip: import (hash on the row) → seal → download → hash-verify.
    owner_storage
        .put_blob(
            "photos",
            &owner_location,
            BLOB_ID,
            BlobScope::Master,
            None,
            v2.clone(),
        )
        .await
        .expect("restore the current version");
    let served = read_blob(&db, &ld, Some(&owner_storage), &photo_ref())
        .await
        .expect("the current, hash-matching version is served");
    assert_eq!(served, v2);
}
