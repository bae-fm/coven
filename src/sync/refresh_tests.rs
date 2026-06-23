//! Per-cycle authorization/decryption refresh (#85/#87).
//!
//! `run_single_sync_cycle` re-reads membership, the authorized-keys set, and the
//! rotatable library key at the top of every cycle, so a running device picks up
//! a membership change or a key rotation made by another device WITHOUT a restart.
//! Before this, those were loaded once at init/join and never again, so a removed
//! member kept acting on stale authorization (#85) and a non-rotating device kept
//! using a dead library key after a rotation it didn't perform, silently diverging
//! (#87).
//!
//! Each test proves fail-before/pass-after: the assertion that passes with the
//! refresh in place fails when the corresponding refresh step is dropped (the
//! "mutation" each test documents).

use std::sync::RwLock;

use crate::clock::SystemClock;
use crate::encryption::EncryptionService;
use crate::keys::{KeyService, UserKeypair};
use crate::library_dir::LibraryDir;
use crate::sync::cloud_storage::CloudCipher;
use crate::sync::cycle::run_single_sync_cycle;
use crate::sync::hlc::Hlc;
use crate::sync::invite::{revoke_member, signed_wrapped_key_for_test, unwrap_library_key};
use crate::sync::membership::{MemberRole, MembershipAction, MembershipChain};
use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::{
    bootstrap_chain, make_entry, open_test_db, pubkey_hex, temp_library_dir, MockSyncStorage,
    NoopBlobSource,
};

const LIB_ID: &str = "lib-refresh-test";

/// Upload `chain`'s entries to the mock storage exactly as the membership ops do
/// (one object per entry under `membership/{author}/{seq}`), so the device under
/// test reloads the same chain a real cloud holds.
async fn upload_chain(storage: &MockSyncStorage, chain: &MembershipChain) {
    use std::collections::HashMap;
    let mut next: HashMap<String, u64> = HashMap::new();
    for entry in chain.entries() {
        let seq = next.entry(entry.author_pubkey.clone()).or_insert(0);
        *seq += 1;
        storage
            .put_membership_entry(
                &entry.author_pubkey,
                *seq,
                serde_json::to_vec(entry).expect("serialize entry"),
            )
            .await
            .expect("upload membership entry");
    }
}

/// Run one real sync cycle for `device_id` over an encrypted home, with the mock
/// as both the SyncStorage and the CloudHome (so the refresh's `auth/keys` writes
/// and wrapped-key reads hit the same store).
async fn run_cycle(
    storage: &MockSyncStorage,
    db: &crate::database::Database,
    cipher: &RwLock<CloudCipher>,
    keypair: &UserKeypair,
    device_id: &str,
    ld: &LibraryDir,
) -> Result<(), String> {
    let hlc = Hlc::new(device_id.to_string());
    run_single_sync_cycle(
        storage,
        LIB_ID,
        device_id,
        &hlc,
        &SystemClock,
        db,
        cipher,
        keypair,
        ld,
        Some(storage as &dyn crate::storage::cloud::CloudHome),
        &NoopBlobSource,
        None,
    )
    .await
    .map(|_| ())
}

/// #87: a NON-rotating running device B adopts a rotated library key on its next
/// cycle, with no restart. Device A removes a member, which rotates the key and
/// re-wraps B's `keys/{B}` under the new key; B's next `run_single_sync_cycle`
/// re-reads that wrapped key, authenticates it against the pinned owner, and swaps
/// its live cipher — so it can now decrypt content sealed under the new key, and
/// its keyring holds the new key for the next restart.
///
/// Mutation proof: drop the wrapped-key re-fetch (step 3 of the refresh) and B
/// keeps its old cipher — the post-rotation key never reaches it and the final
/// fingerprint assertion fails. (Asserted here by checking B's live cipher and
/// keyring both hold the rotated key, which only the adoption can produce.)
#[tokio::test]
async fn non_rotating_device_adopts_rotated_key_without_restart() {
    crate::keys::test_keyring::install();

    let owner = UserKeypair::generate(); // device A, the founder/owner
    let device_b = UserKeypair::generate();
    let victim = UserKeypair::generate(); // the member A will remove
    let owner_pk = pubkey_hex(&owner);
    let old_key: [u8; 32] = [11u8; 32];

    let storage = MockSyncStorage::new();

    // Chain: owner founds, adds B and the victim as members.
    let mut chain = bootstrap_chain(&owner);
    chain
        .add_entry(make_entry(
            &owner,
            MembershipAction::Add,
            &device_b,
            MemberRole::Member,
            "0000000002000-0000-A",
        ))
        .unwrap();
    chain
        .add_entry(make_entry(
            &owner,
            MembershipAction::Add,
            &victim,
            MemberRole::Member,
            "0000000003000-0000-A",
        ))
        .unwrap();
    upload_chain(&storage, &chain).await;

    // The owner wraps the OLD key for B (what B adopted at join), signed by the
    // owner B pins, so B's refresh can authenticate it.
    let b_x = device_b.to_x25519_public_key();
    let wrapped_old =
        signed_wrapped_key_for_test(LIB_ID, &pubkey_hex(&device_b), &b_x, &old_key, &owner);
    storage
        .put_wrapped_key(&pubkey_hex(&device_b), wrapped_old)
        .await
        .unwrap();

    // B's local state: pinned owner + its keyring holds the OLD key + its live
    // cipher is the OLD key. This is the just-joined steady state.
    let db_b = open_test_db();
    db_b.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let ks_b = KeyService::new(LIB_ID.to_string());
    ks_b.set_encryption_key(&hex::encode(old_key)).unwrap();
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(old_key)));
    let (_tmp_b, ld_b) = temp_library_dir();

    // Sanity: before the rotation, B's refresh is a no-op — it already holds the
    // current key, so the cycle leaves the cipher unchanged.
    run_cycle(&storage, &db_b, &cipher_b, &device_b, "B", &ld_b)
        .await
        .expect("pre-rotation cycle");
    assert_eq!(
        cipher_key(&cipher_b),
        old_key,
        "before any rotation B keeps the key it joined with",
    );

    // --- Device A removes the victim: rotates the key, re-wraps B's keys/{B}. ---
    let new_key = revoke_member(
        &storage,
        &storage,
        &mut chain,
        &owner,
        &pubkey_hex(&victim),
        LIB_ID,
        "0000000004000-0000-A",
    )
    .await
    .expect("revoke rotates the key");
    assert_ne!(new_key, old_key, "removal rotates to a fresh key");

    // A seals a control object under the NEW key (a changeset) so we can prove B
    // can decrypt post-rotation content only if it adopted the new key. We assert
    // adoption directly via B's cipher + keyring below, which is what gates that.

    // --- B's NEXT cycle, no restart: it must adopt the rotated key. ---
    run_cycle(&storage, &db_b, &cipher_b, &device_b, "B", &ld_b)
        .await
        .expect("post-rotation cycle");

    // B's live cipher now holds the rotated key (it can decrypt what A seals under
    // it this cycle), and its keyring was updated so a restart reads the new key —
    // the two halves `apply_key_rotation` performs.
    assert_eq!(
        cipher_key(&cipher_b),
        new_key,
        "B adopted the rotated key into its live cipher without a restart (#87)",
    );
    assert_eq!(
        ks_b.get_encryption_key().unwrap().as_deref(),
        Some(&*hex::encode(new_key)),
        "B persisted the rotated key to its keyring, so its restart reads the current key",
    );

    // And the rotated key B now holds is exactly the one the owner re-wrapped for
    // it — i.e. B can independently unwrap keys/{B} to the same bytes.
    let reunwrapped = unwrap_library_key(&storage, &device_b, LIB_ID, &owner_pk)
        .await
        .expect("B can unwrap its re-wrapped key");
    assert_eq!(reunwrapped, new_key);
}

/// #87 (authentication invariant, #99): the wrapped-key adoption refuses a forged
/// `keys/{self}` — one not signed by the pinned owner. A bucket writer overwrites
/// B's slot with a key they chose, sealed to B's public key and signed by an
/// attacker. B's refresh authenticates against the pinned owner, so it does NOT
/// adopt the attacker's key; the cycle fails closed and B keeps its real key.
#[tokio::test]
async fn refresh_rejects_a_forged_wrapped_key() {
    crate::keys::test_keyring::install();

    let owner = UserKeypair::generate();
    let attacker = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let real_key: [u8; 32] = [22u8; 32];

    let storage = MockSyncStorage::new();

    // Chain: owner founds + adds B.
    let mut chain = bootstrap_chain(&owner);
    chain
        .add_entry(make_entry(
            &owner,
            MembershipAction::Add,
            &device_b,
            MemberRole::Member,
            "0000000002000-0000-A",
        ))
        .unwrap();
    upload_chain(&storage, &chain).await;

    // The attacker forges B's wrapped key: a key THEY chose, sealed to B's real
    // public key, signed by the attacker (not the owner), in B's slot.
    let forged_key: [u8; 32] = [0xCDu8; 32];
    let b_x = device_b.to_x25519_public_key();
    let forged =
        signed_wrapped_key_for_test(LIB_ID, &pubkey_hex(&device_b), &b_x, &forged_key, &attacker);
    storage
        .put_wrapped_key(&pubkey_hex(&device_b), forged)
        .await
        .unwrap();

    // B holds its real key (live + keyring) and pins the owner.
    let db_b = open_test_db();
    db_b.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let ks_b = KeyService::new(LIB_ID.to_string());
    ks_b.set_encryption_key(&hex::encode(real_key)).unwrap();
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        real_key,
    )));
    let (_tmp_b, ld_b) = temp_library_dir();

    // B's cycle aborts (refuses the forged key) rather than adopting it.
    let result = run_cycle(&storage, &db_b, &cipher_b, &device_b, "B", &ld_b).await;
    assert!(
        result.is_err(),
        "a forged wrapped key must abort the cycle, not be adopted: {result:?}",
    );

    // Critically, B did NOT swap its cipher to the attacker's key.
    assert_eq!(
        cipher_key(&cipher_b),
        real_key,
        "B keeps its real key; the unauthenticated forged key is never adopted (#99)",
    );
    assert_ne!(
        cipher_key(&cipher_b),
        forged_key,
        "the attacker's key was rejected",
    );
}

/// #85: a membership change made by another device is reflected in B's
/// authorized-keys set on B's next cycle, no restart. `sync_authorized_keys` —
/// which materializes the proxy's `auth/keys/{pubkey}` files from the current
/// member set — used to run only in `init_sync`, so a long-running device never
/// pruned a removed member's key or wrote a newly-added member's. The per-cycle
/// refresh re-runs it: after device A removes the victim and adds a newcomer, B's
/// next cycle drops `auth/keys/{victim}` and writes `auth/keys/{newcomer}`.
///
/// Mutation proof: drop the auth-keys refresh (step 2) and `auth/keys/{victim}`
/// survives B's cycle and `auth/keys/{newcomer}` is never written — exactly the
/// init-only staleness this fixes. The assertions below fail under that mutation.
#[tokio::test]
async fn authorized_keys_reconcile_each_cycle_without_restart() {
    use crate::storage::cloud::CloudHome;

    crate::keys::test_keyring::install();

    let owner = UserKeypair::generate(); // device A
    let device_b = UserKeypair::generate();
    let victim = UserKeypair::generate();
    let newcomer = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let key: [u8; 32] = [33u8; 32];

    let storage = MockSyncStorage::new();

    // Chain: owner founds, adds B and the victim.
    let mut chain = bootstrap_chain(&owner);
    chain
        .add_entry(make_entry(
            &owner,
            MembershipAction::Add,
            &device_b,
            MemberRole::Member,
            "0000000002000-0000-A",
        ))
        .unwrap();
    chain
        .add_entry(make_entry(
            &owner,
            MembershipAction::Add,
            &victim,
            MemberRole::Member,
            "0000000003000-0000-A",
        ))
        .unwrap();
    upload_chain(&storage, &chain).await;

    // The owner wraps B's key, signed by the owner, so B's per-cycle wrapped-key
    // read authenticates and is a no-op (B already holds this key).
    let b_x = device_b.to_x25519_public_key();
    let wrapped = signed_wrapped_key_for_test(LIB_ID, &pubkey_hex(&device_b), &b_x, &key, &owner);
    storage
        .put_wrapped_key(&pubkey_hex(&device_b), wrapped)
        .await
        .unwrap();

    // Seed the auth/keys set as it stood at B's join: owner, B, victim. (This is
    // what `init_sync`'s one-time bootstrap would have written.)
    crate::sync::membership_ops::sync_authorized_keys(&storage, &chain)
        .await
        .unwrap();
    assert!(
        storage
            .exists(&format!("auth/keys/{}", pubkey_hex(&victim)))
            .await
            .unwrap(),
        "precondition: the victim's auth key exists before removal",
    );

    // B's local state.
    let db_b = open_test_db();
    db_b.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let ks_b = KeyService::new(LIB_ID.to_string());
    ks_b.set_encryption_key(&hex::encode(key)).unwrap();
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(key)));
    let (_tmp_b, ld_b) = temp_library_dir();

    // --- Device A changes membership: remove the victim, add a newcomer. Upload
    //     only the two new entries (the chain in storage is what B reloads). ---
    chain
        .add_entry(make_entry(
            &owner,
            MembershipAction::Remove,
            &victim,
            MemberRole::Member,
            "0000000004000-0000-A",
        ))
        .unwrap();
    storage
        .put_membership_entry(
            &owner_pk,
            4,
            serde_json::to_vec(chain.entries().last().unwrap()).unwrap(),
        )
        .await
        .unwrap();
    chain
        .add_entry(make_entry(
            &owner,
            MembershipAction::Add,
            &newcomer,
            MemberRole::Member,
            "0000000005000-0000-A",
        ))
        .unwrap();
    storage
        .put_membership_entry(
            &owner_pk,
            5,
            serde_json::to_vec(chain.entries().last().unwrap()).unwrap(),
        )
        .await
        .unwrap();

    // --- B's next cycle, no restart: reconcile auth/keys from the reloaded chain.
    run_cycle(&storage, &db_b, &cipher_b, &device_b, "B", &ld_b)
        .await
        .expect("B's cycle");

    assert!(
        !storage
            .exists(&format!("auth/keys/{}", pubkey_hex(&victim)))
            .await
            .unwrap(),
        "the removed member's auth key is pruned this cycle, no restart (#85)",
    );
    assert!(
        storage
            .exists(&format!("auth/keys/{}", pubkey_hex(&newcomer)))
            .await
            .unwrap(),
        "the newly-added member's auth key is written this cycle, no restart (#85)",
    );
    // B's own and the owner's keys remain.
    assert!(storage
        .exists(&format!("auth/keys/{}", pubkey_hex(&device_b)))
        .await
        .unwrap());
    assert!(storage
        .exists(&format!("auth/keys/{owner_pk}"))
        .await
        .unwrap());
}

/// #88 invariant carried into the refresh: for an owner-pinned library a chain the
/// refresh can't load must ABORT the cycle, never fall open to "no chain, act
/// anyway". A membership LIST that fails (the wiped/flaky case) aborts B's cycle
/// rather than letting it push or judge under no authorization.
#[tokio::test]
async fn refresh_fails_closed_when_the_chain_cannot_be_loaded() {
    crate::keys::test_keyring::install();

    let owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let key: [u8; 32] = [44u8; 32];

    let storage = MockSyncStorage::new();

    // A real owner-anchored chain exists, but listing it is made to fail this cycle.
    let mut chain = bootstrap_chain(&owner);
    chain
        .add_entry(make_entry(
            &owner,
            MembershipAction::Add,
            &device_b,
            MemberRole::Member,
            "0000000002000-0000-A",
        ))
        .unwrap();
    upload_chain(&storage, &chain).await;
    storage.fail_membership_listing();

    let db_b = open_test_db();
    db_b.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let ks_b = KeyService::new(LIB_ID.to_string());
    ks_b.set_encryption_key(&hex::encode(key)).unwrap();
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(key)));
    let (_tmp_b, ld_b) = temp_library_dir();

    let result = run_cycle(&storage, &db_b, &cipher_b, &device_b, "B", &ld_b).await;
    assert!(
        result.is_err(),
        "a membership-list failure under a pinned owner must abort the cycle, not \
         fall open (#88): {result:?}",
    );
}

/// The raw 32-byte key inside an `Encrypted` cipher (panics on a plaintext cipher —
/// these tests only ever build encrypted ones).
fn cipher_key(cipher: &RwLock<CloudCipher>) -> [u8; 32] {
    match &*cipher.read().unwrap() {
        CloudCipher::Encrypted(enc) => enc.key_bytes(),
        CloudCipher::Plaintext => panic!("expected an encrypted cipher"),
    }
}
