//! Per-cycle authorization/decryption refresh.
//!
//! `run_single_sync_cycle` re-reads membership and the rotatable library key at
//! the top of every cycle, so a running device picks up a membership change or a
//! key rotation made by another device without a restart. Loaded only once at
//! init/join, a removed member would keep acting on stale authorization and
//! a non-rotating device would keep using a dead library key after a rotation it
//! didn't perform, silently diverging.
//!
//! Each test proves fail-before/pass-after: the assertion that passes with the
//! refresh in place fails when the corresponding refresh step is dropped (the
//! "mutation" each test documents).

use std::sync::{Mutex, RwLock};

use crate::clock::SystemClock;
use crate::encryption::EncryptionService;
use crate::keys::{KeyError, KeyPersistence, UserKeypair};
use crate::library_dir::LibraryDir;
use crate::sync::cloud_storage::CloudCipher;
use crate::sync::cycle::run_single_sync_cycle;
use crate::sync::hlc::Hlc;
use crate::sync::invite::{
    revoke_member, signed_wrapped_key_for_test, signed_wrapped_keyring_for_test,
    unwrap_library_keyring_for_owners_with_activation,
};
use crate::sync::membership::{MemberRole, MembershipAction, MembershipChain, MembershipCoord};
use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::{
    bootstrap_chain, make_entry, open_test_db, pubkey_hex, temp_library_dir, MockSyncStorage,
};

const LIB_ID: &str = "lib-refresh-test";

#[derive(Default)]
struct TestKeyPersistence {
    value: Mutex<Option<String>>,
}

impl TestKeyPersistence {
    fn set_initial_key(&self, key: [u8; 32]) {
        self.set_encryption_key(&hex::encode(key)).unwrap();
    }

    fn stored_key(&self) -> Option<String> {
        self.value.lock().unwrap().clone()
    }
}

impl KeyPersistence for TestKeyPersistence {
    fn set_encryption_key(&self, value: &str) -> Result<(), KeyError> {
        *self.value.lock().unwrap() = Some(value.to_string());
        Ok(())
    }
}

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
/// as both the SyncStorage and the CloudHome (so the refresh's wrapped-key reads
/// hit the same store).
async fn run_cycle(
    storage: &MockSyncStorage,
    db: &crate::database::Database,
    cipher: &RwLock<CloudCipher>,
    keypair: &UserKeypair,
    device_id: &str,
    ld: &LibraryDir,
    key_persistence: Option<&dyn KeyPersistence>,
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
        key_persistence,
        ld,
        Some(storage as &dyn crate::storage::cloud::CloudHome),
        None,
    )
    .await
    .map(|_| ())
}

/// A non-rotating running device B adopts a rotated library key on its next cycle,
/// with no restart. Device A removes a member, which rotates the key and re-wraps
/// B's `keys/{B}` under the new key; B's next `run_single_sync_cycle` re-reads
/// that wrapped key, authenticates it against the current Owner set, and swaps
/// its live cipher — so it can now decrypt content sealed under the new key, and
/// its keyring holds the new key for the next restart.
///
/// Mutation proof: drop the wrapped-key re-fetch (step 3 of the refresh) and B
/// keeps its old cipher — the post-rotation key never reaches it and the final
/// fingerprint assertion fails. (Asserted here by checking B's live cipher and
/// keyring both hold the rotated key, which only the adoption can produce.)
#[tokio::test]
async fn non_rotating_device_adopts_rotated_key_without_restart() {
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
    let ks_b = TestKeyPersistence::default();
    ks_b.set_initial_key(old_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(old_key)));
    let (_tmp_b, ld_b) = temp_library_dir();

    // Sanity: before the rotation, B's refresh is a no-op — it already holds the
    // current key, so the cycle leaves the cipher unchanged.
    run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
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
        storage.list_membership_entries().await.unwrap(),
        &owner,
        &pubkey_hex(&victim),
        LIB_ID,
        "0000000004000-0000-A",
        &EncryptionService::from_key(old_key),
    )
    .await
    .expect("revoke rotates the key");
    assert_ne!(
        new_key.key_bytes(),
        old_key,
        "removal rotates to a fresh key"
    );

    // A seals a control object under the NEW key (a changeset) so we can prove B
    // can decrypt post-rotation content only if it adopted the new key. We assert
    // adoption directly via B's cipher + keyring below, which is what gates that.

    // --- B's NEXT cycle, no restart: it must adopt the rotated key. ---
    run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await
    .expect("post-rotation cycle");

    // B's live cipher now holds the rotated key (it can decrypt what A seals under
    // it this cycle), and its keyring was updated so a restart reads the new key —
    // the two halves `apply_key_rotation` performs.
    assert_eq!(
        cipher_key(&cipher_b),
        new_key.key_bytes(),
        "B adopted the rotated key into its live cipher without a restart",
    );
    assert_eq!(
        ks_b.stored_key().as_deref(),
        Some(new_key.to_keyring_string().unwrap().as_str()),
        "B persisted the rotated key to its keyring, so its restart reads the current key",
    );

    // And the rotated key B now holds is exactly the one the owner re-wrapped for
    // it — i.e. B can independently unwrap keys/{B} to the same bytes.
    let visible_entries = membership_coords(&storage.list_membership_entries().await.unwrap());
    let reunwrapped = unwrap_library_keyring_for_owners_with_activation(
        &storage,
        &device_b,
        LIB_ID,
        std::iter::once(owner_pk.as_str()),
        Some(&visible_entries),
    )
    .await
    .expect("B can unwrap its re-wrapped key")
    .key_bytes();
    assert_eq!(reunwrapped, new_key.key_bytes());
}

#[tokio::test]
async fn inactive_removal_key_aborts_refresh_until_remove_is_visible() {
    let owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let victim = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let old_key: [u8; 32] = [31u8; 32];
    let rotated_key: [u8; 32] = [32u8; 32];

    let storage = MockSyncStorage::new();
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

    let activation = MembershipCoord {
        author_pubkey: owner_pk.clone(),
        seq: 4,
    };
    let pending_keyring = EncryptionService::from_key(old_key)
        .with_appended_generation(2, rotated_key)
        .unwrap();
    let b_x = device_b.to_x25519_public_key();
    let pending_wrapped = signed_wrapped_keyring_for_test(
        LIB_ID,
        &pubkey_hex(&device_b),
        &b_x,
        &pending_keyring,
        &owner,
        Some(activation),
    );
    storage
        .put_wrapped_key(&pubkey_hex(&device_b), pending_wrapped)
        .await
        .unwrap();

    let db_b = open_test_db();
    db_b.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let ks_b = TestKeyPersistence::default();
    ks_b.set_initial_key(old_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(old_key)));
    let (_tmp_b, ld_b) = temp_library_dir();

    let result = run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await;

    assert!(
        result.is_err(),
        "a removal key whose Remove entry is absent must abort the cycle",
    );
    assert_eq!(
        cipher_key(&cipher_b),
        old_key,
        "the inactive key must not replace the live cipher",
    );
}

#[tokio::test]
async fn replayed_pre_rotation_wrapped_key_is_not_adopted() {
    let owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let victim = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let old_key: [u8; 32] = [12u8; 32];

    let storage = MockSyncStorage::new();
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

    let b_x = device_b.to_x25519_public_key();
    let wrapped_old =
        signed_wrapped_key_for_test(LIB_ID, &pubkey_hex(&device_b), &b_x, &old_key, &owner);
    storage
        .put_wrapped_key(&pubkey_hex(&device_b), wrapped_old.clone())
        .await
        .unwrap();

    let db_b = open_test_db();
    db_b.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let ks_b = TestKeyPersistence::default();
    ks_b.set_initial_key(old_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(old_key)));
    let (_tmp_b, ld_b) = temp_library_dir();

    let new_key = revoke_member(
        &storage,
        &storage,
        &mut chain,
        storage.list_membership_entries().await.unwrap(),
        &owner,
        &pubkey_hex(&victim),
        LIB_ID,
        "0000000004000-0000-A",
        &EncryptionService::from_key(old_key),
    )
    .await
    .expect("revoke rotates key");

    run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await
    .expect("adopt generation 2");
    assert_eq!(cipher_generation(&cipher_b), new_key.current_generation());

    storage
        .put_wrapped_key(&pubkey_hex(&device_b), wrapped_old)
        .await
        .unwrap();

    run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await
    .expect("replayed old wrapped key is ignored");

    assert_eq!(
        cipher_key(&cipher_b),
        new_key.key_bytes(),
        "a replayed older wrapped key must not roll the device back",
    );
    assert_eq!(
        cipher_generation(&cipher_b),
        new_key.current_generation(),
        "the accepted generation floor remains at the rotated generation",
    );
}

#[tokio::test]
async fn same_generation_wrapped_key_is_not_adopted() {
    let owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let current_key: [u8; 32] = [13u8; 32];
    let replacement_key: [u8; 32] = [14u8; 32];

    let storage = MockSyncStorage::new();
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

    let b_x = device_b.to_x25519_public_key();
    let same_generation_replacement = signed_wrapped_key_for_test(
        LIB_ID,
        &pubkey_hex(&device_b),
        &b_x,
        &replacement_key,
        &owner,
    );
    storage
        .put_wrapped_key(&pubkey_hex(&device_b), same_generation_replacement)
        .await
        .unwrap();

    let db_b = open_test_db();
    db_b.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let ks_b = TestKeyPersistence::default();
    ks_b.set_initial_key(current_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        current_key,
    )));
    let (_tmp_b, ld_b) = temp_library_dir();

    run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await
    .expect("same-generation wrapped key is ignored");

    assert_eq!(
        cipher_key(&cipher_b),
        current_key,
        "same-generation wrapped keys are not adopted even when signed by an owner",
    );
}

#[tokio::test]
async fn second_owner_rotation_is_adoptable_by_existing_members() {
    let founder = UserKeypair::generate();
    let second_owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let victim = UserKeypair::generate();
    let founder_pk = pubkey_hex(&founder);
    let old_key: [u8; 32] = [15u8; 32];

    let storage = MockSyncStorage::new();
    let mut chain = bootstrap_chain(&founder);
    chain
        .add_entry(make_entry(
            &founder,
            MembershipAction::Add,
            &second_owner,
            MemberRole::Owner,
            "0000000002000-0000-A",
        ))
        .unwrap();
    chain
        .add_entry(make_entry(
            &founder,
            MembershipAction::Add,
            &device_b,
            MemberRole::Member,
            "0000000003000-0000-A",
        ))
        .unwrap();
    chain
        .add_entry(make_entry(
            &founder,
            MembershipAction::Add,
            &victim,
            MemberRole::Member,
            "0000000004000-0000-A",
        ))
        .unwrap();
    upload_chain(&storage, &chain).await;

    let db_b = open_test_db();
    db_b.set_sync_state(OWNER_PUBKEY_STATE_KEY, &founder_pk)
        .await
        .unwrap();
    let ks_b = TestKeyPersistence::default();
    ks_b.set_initial_key(old_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(old_key)));
    let (_tmp_b, ld_b) = temp_library_dir();

    let new_key = revoke_member(
        &storage,
        &storage,
        &mut chain,
        storage.list_membership_entries().await.unwrap(),
        &second_owner,
        &pubkey_hex(&victim),
        LIB_ID,
        "0000000005000-0000-B",
        &EncryptionService::from_key(old_key),
    )
    .await
    .expect("second owner can revoke");

    run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await
    .expect("existing member adopts a current owner's rotation");

    assert_eq!(cipher_key(&cipher_b), new_key.key_bytes());
}

#[tokio::test]
async fn removed_owner_key_is_not_adopted() {
    let founder = UserKeypair::generate();
    let second_owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let founder_pk = pubkey_hex(&founder);
    let current_key: [u8; 32] = [16u8; 32];
    let removed_owner_key: [u8; 32] = [17u8; 32];

    let storage = MockSyncStorage::new();
    let mut chain = bootstrap_chain(&founder);
    chain
        .add_entry(make_entry(
            &founder,
            MembershipAction::Add,
            &second_owner,
            MemberRole::Owner,
            "0000000002000-0000-A",
        ))
        .unwrap();
    chain
        .add_entry(make_entry(
            &founder,
            MembershipAction::Add,
            &device_b,
            MemberRole::Member,
            "0000000003000-0000-A",
        ))
        .unwrap();
    chain
        .add_entry(make_entry(
            &second_owner,
            MembershipAction::Remove,
            &founder,
            MemberRole::Owner,
            "0000000004000-0000-B",
        ))
        .unwrap();
    upload_chain(&storage, &chain).await;

    let b_x = device_b.to_x25519_public_key();
    let removed_owner_wrapped = signed_wrapped_key_for_test(
        LIB_ID,
        &pubkey_hex(&device_b),
        &b_x,
        &removed_owner_key,
        &founder,
    );
    storage
        .put_wrapped_key(&pubkey_hex(&device_b), removed_owner_wrapped)
        .await
        .unwrap();

    let db_b = open_test_db();
    db_b.set_sync_state(OWNER_PUBKEY_STATE_KEY, &founder_pk)
        .await
        .unwrap();
    let ks_b = TestKeyPersistence::default();
    ks_b.set_initial_key(current_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        current_key,
    )));
    let (_tmp_b, ld_b) = temp_library_dir();

    let result = run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await;

    assert!(
        result.is_err(),
        "a key signed by a removed owner must fail closed, not be adopted",
    );
    assert_eq!(
        cipher_key(&cipher_b),
        current_key,
        "the removed owner's key must not replace the current key",
    );
}

/// Wrapped-key adoption refuses a forged `keys/{self}` — one not signed by a
/// current Owner. A bucket writer overwrites B's slot with a key they chose,
/// sealed to B's public key and signed by an attacker. B's refresh authenticates
/// against current Owners, so it does NOT adopt the attacker's key; the cycle
/// fails closed and B keeps its real key.
#[tokio::test]
async fn refresh_rejects_a_forged_wrapped_key() {
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
    let ks_b = TestKeyPersistence::default();
    ks_b.set_initial_key(real_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        real_key,
    )));
    let (_tmp_b, ld_b) = temp_library_dir();

    // B's cycle aborts (refuses the forged key) rather than adopting it.
    let result = run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await;
    assert!(
        result.is_err(),
        "a forged wrapped key must abort the cycle, not be adopted: {result:?}",
    );

    // Critically, B did NOT swap its cipher to the attacker's key.
    assert_eq!(
        cipher_key(&cipher_b),
        real_key,
        "B keeps its real key; the unauthenticated forged key is never adopted",
    );
    assert_ne!(
        cipher_key(&cipher_b),
        forged_key,
        "the attacker's key was rejected",
    );
}

/// For an owner-pinned library, a chain the refresh can't load must abort the
/// cycle, never fall open to "no chain, act anyway". A membership list that fails
/// aborts B's cycle rather than letting it push or judge under no authorization.
#[tokio::test]
async fn refresh_fails_closed_when_the_chain_cannot_be_loaded() {
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
    let ks_b = TestKeyPersistence::default();
    ks_b.set_initial_key(key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(key)));
    let (_tmp_b, ld_b) = temp_library_dir();

    let result = run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await;
    assert!(
        result.is_err(),
        "a membership-list failure under a pinned owner must abort the cycle, not fall open: {result:?}",
    );
}

/// The raw 32-byte key inside an `Encrypted` cipher (panics on a plaintext cipher —
/// these tests only ever build encrypted ones).
fn membership_coords(entry_keys: &[(String, u64)]) -> Vec<MembershipCoord> {
    entry_keys
        .iter()
        .map(|(author_pubkey, seq)| MembershipCoord {
            author_pubkey: author_pubkey.clone(),
            seq: *seq,
        })
        .collect()
}

fn cipher_key(cipher: &RwLock<CloudCipher>) -> [u8; 32] {
    match &*cipher.read().unwrap() {
        CloudCipher::Encrypted(enc) => enc.key_bytes(),
        CloudCipher::Plaintext => panic!("expected an encrypted cipher"),
    }
}

fn cipher_generation(cipher: &RwLock<CloudCipher>) -> u64 {
    match &*cipher.read().unwrap() {
        CloudCipher::Encrypted(enc) => enc.current_generation(),
        CloudCipher::Plaintext => panic!("expected an encrypted cipher"),
    }
}
