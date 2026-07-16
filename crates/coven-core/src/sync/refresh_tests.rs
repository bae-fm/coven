//! Per-cycle authorization/decryption refresh.
//!
//! `run_single_sync_cycle` re-reads membership and the rotatable store key at
//! the top of every cycle, so a running device picks up a membership change or a
//! key rotation made by another device without a restart. Loaded only once at
//! init/join, a removed member would keep acting on stale authorization and
//! a non-rotating device would keep using a dead store key after a rotation it
//! didn't perform, silently diverging.
//!
//! Each test proves fail-before/pass-after: the assertion that passes with the
//! refresh in place fails when the corresponding refresh step is dropped (the
//! "mutation" each test documents).

use std::sync::RwLock;

use crate::clock::SystemClock;
use crate::encryption::EncryptionService;
use crate::keys::{MasterKeyCustody, UserKeypair};
use crate::store_dir::StoreDir;
use crate::sync::cloud_storage::{CloudCipher, PendingRotation, PENDING_ROTATION_STATE_KEY};
use crate::sync::cycle::run_single_sync_cycle;
use crate::sync::hlc::Hlc;
use crate::sync::invite::{
    revoke_member, revoke_member_durable, signed_wrapped_key_for_test,
    signed_wrapped_keyring_for_test, unwrap_store_keyring_for_owners_with_activation,
};
use crate::sync::membership::{MemberRole, MembershipChain, MembershipCoord};
use crate::sync::membership_ops::{
    load_anchored_chain, remove_member, MembershipOpsError, OWNER_PUBKEY_STATE_KEY,
};
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::{
    bind_mock_store_protocol, bootstrap_chain, capture_bytes, open_test_db, pubkey_hex,
    temp_store_dir, MockSyncStorage, TestCustody,
};
use crate::sync::wrapped_store_key::WrappedKeyActivation;

const LIB_ID: &str = "lib-refresh-test";

/// Upload `chain`'s entries to the mock storage exactly as the membership ops do
/// (one object per entry under `membership/{author}/{seq}`), so the device under
/// test reloads the same chain a real cloud holds.
async fn upload_chain(storage: &MockSyncStorage, chain: &MembershipChain, signer: &UserKeypair) {
    use std::collections::HashMap;
    let mut next: HashMap<String, u64> = HashMap::new();
    for entry in chain.entries() {
        let seq = next.entry(entry.author_pubkey.clone()).or_insert(0);
        *seq += 1;
        storage
            .append_membership_entry_bytes(
                &entry.author_pubkey,
                *seq,
                serde_json::to_vec(entry).expect("serialize entry"),
            )
            .await
            .expect("upload membership entry");
    }
    crate::sync::membership_ops::publish_membership_head(
        storage,
        storage.store_root_hash(),
        chain,
        signer,
    )
    .await
    .expect("publish membership head");
}

/// Run one real sync cycle for `device_id` over an encrypted home, with the mock
/// as both the SyncStorage and the CloudHome (so the refresh's wrapped-key reads
/// hit the same store).
async fn run_cycle(
    storage: &MockSyncStorage,
    db: &crate::database::Database,
    cipher: &RwLock<CloudCipher>,
    pending_rotation: &PendingRotation,
    keypair: &UserKeypair,
    device_id: &str,
    ld: &StoreDir,
    custody: Option<&dyn MasterKeyCustody>,
) -> Result<super::cycle::SyncCycleResult, String> {
    bind_mock_store_protocol(db, storage, device_id).await;
    let hlc = Hlc::new(device_id.to_string());
    run_single_sync_cycle(
        storage,
        LIB_ID,
        device_id,
        &hlc,
        &SystemClock,
        db,
        cipher,
        pending_rotation,
        keypair,
        custody,
        ld,
        Some(storage as &dyn crate::storage::cloud::CloudHome),
        None,
    )
    .await
    .map_err(|error| error.to_string())
}

/// A non-rotating running device B adopts a rotated store key on its next cycle,
/// with no restart. Device A removes a member, which rotates the key and re-wraps
/// B's `keys/{A}/{B}` under the new key; B's next `run_single_sync_cycle` re-reads
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

    let storage = MockSyncStorage::with_keypair(owner.clone());

    // Chain: owner founds, adds B and the victim as members.
    let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());
    {
        let entry = chain
            .signed_set_member(
                &owner,
                pubkey_hex(&device_b),
                None,
                MemberRole::Member,
                "0000000002000-0000-A".to_string(),
            )
            .expect("active Owner signs membership grant");
        chain.add_entry(entry).expect("valid membership grant");
    }
    {
        let entry = chain
            .signed_set_member(
                &owner,
                pubkey_hex(&victim),
                None,
                MemberRole::Member,
                "0000000003000-0000-A".to_string(),
            )
            .expect("active Owner signs membership grant");
        chain.add_entry(entry).expect("valid membership grant");
    }
    upload_chain(&storage, &chain, &owner).await;
    let db = open_test_db();
    db.set_protocol_state(
        crate::database::STORE_ROOT_HASH_STATE_KEY,
        &storage.store_root_hash().to_string(),
    )
    .await
    .expect("bind refresh fixture to its Store protocol root");
    let listed = storage.discover_membership_entries().await;
    crate::sync::membership_ops::load_and_persist_owner_anchor(
        &storage,
        storage.store_root_hash(),
        &listed,
        &owner_pk,
        &db,
    )
    .await
    .unwrap()
    .unwrap();

    // The owner wraps the OLD key for B (what B adopted at join), signed by the
    // owner B pins, so B's refresh can authenticate it.
    let b_x = device_b.to_x25519_public_key();
    let wrapped_old =
        signed_wrapped_key_for_test(LIB_ID, &pubkey_hex(&device_b), &b_x, &old_key, &owner);
    storage
        .put_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&device_b), wrapped_old)
        .await
        .unwrap();

    // B's local state: pinned owner + its keyring holds the OLD key + its live
    // cipher is the OLD key. This is the just-joined steady state.
    let db_b = open_test_db();
    db_b.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(old_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(old_key)));
    let (_tmp_b, ld_b) = temp_store_dir();

    // Sanity: before the rotation, B's refresh is a no-op — it already holds the
    // current key, so the cycle leaves the cipher unchanged.
    run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
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

    // --- Device A removes the victim: rotates the key, re-wraps B's keys/{A}/{B}. ---
    let new_key = revoke_member(
        &storage,
        &storage,
        storage.store_root_hash(),
        &mut chain,
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
        &PendingRotation::none(),
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
    // it — i.e. B can independently unwrap keys/{A}/{B} to the same bytes.
    let visible_entries =
        membership_coords(&storage, &storage.discover_membership_entries().await).await;
    let visible_activations: Vec<_> = visible_entries
        .into_iter()
        .map(WrappedKeyActivation::MergeConcurrent)
        .collect();
    let reunwrapped = unwrap_store_keyring_for_owners_with_activation(
        &storage,
        &device_b,
        LIB_ID,
        std::iter::once(owner_pk.as_str()),
        Some(&visible_activations),
    )
    .await
    .expect("B can unwrap its re-wrapped key")
    .key_bytes();
    assert_eq!(reunwrapped, new_key.key_bytes());
}

/// The mid-removal crash state: an owner overwrote this device's wrap with a
/// rotated keyring whose activation (the Remove entry) is not yet visible, then
/// crashed before uploading that entry. The device cannot adopt the rotation —
/// but it holds a working old keyring and must not be wedged. The cycle
/// COMPLETES: the refresh marks the rotation pending (at the wrap's committed
/// generation, read from the signed envelope without opening the sealed box),
/// the pull still applies a peer's changeset, the live cipher is untouched, and
/// the pause is persisted. Nothing new is sealed — the seal paths are gated on
/// `rotation_pending`.
#[tokio::test]
async fn inactive_removal_key_pauses_sealing_but_completes_the_cycle() {
    let owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let victim = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let old_key: [u8; 32] = [31u8; 32];
    let rotated_key: [u8; 32] = [32u8; 32];

    // The cloud is the owner's, so a changeset it serves is authored by a member
    // the pull will authorize against the chain.
    let storage = MockSyncStorage::with_keypair(owner.clone());
    let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());
    {
        let entry = chain
            .signed_set_member(
                &owner,
                pubkey_hex(&device_b),
                None,
                MemberRole::Member,
                "0000000002000-0000-A".to_string(),
            )
            .expect("active Owner signs membership grant");
        chain.add_entry(entry).expect("valid membership grant");
    }
    {
        let entry = chain
            .signed_set_member(
                &owner,
                pubkey_hex(&victim),
                None,
                MemberRole::Member,
                "0000000003000-0000-A".to_string(),
            )
            .expect("active Owner signs membership grant");
        chain.add_entry(entry).expect("valid membership grant");
    }
    upload_chain(&storage, &chain, &owner).await;

    let activation = MembershipCoord {
        author_pubkey: owner_pk.clone(),
        author_owner_grant: chain
            .active_owner_grant(&owner_pk)
            .expect("founder Owner grant"),
        stream_id: chain
            .preferred_author_stream(
                &owner_pk,
                &chain
                    .active_owner_grant(&owner_pk)
                    .expect("founder Owner grant"),
            )
            .expect("founder author stream"),
        seq: 4,
        entry_hash: crate::sync::store_commit::ObjectHash::digest(b"pending removal"),
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
        .put_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&device_b), pending_wrapped)
        .await
        .unwrap();

    let db_b = open_test_db();
    db_b.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    // A peer changeset waiting to be pulled, so the cycle proving it "completes"
    // also proves the pull ran and applied it while sealing was paused.
    let peer_src = open_test_db();
    let peer_cs = capture_bytes(
        &peer_src,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('peer1', 'FromOwner', NULL, 1, '0000000005000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_with_grant(
        "owner-device",
        1,
        &peer_cs,
        db_b.schema_version(),
        chain.write_grant_coord(&owner_pk),
    );

    let ks_b = TestCustody::default();
    ks_b.set_initial_key(old_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(old_key)));
    let (_tmp_b, ld_b) = temp_store_dir();

    let result = run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await
    .expect("the cycle completes; an inactive removal key pauses sealing, it does not abort");

    assert_eq!(
        result.changesets_applied, 1,
        "the pull still applies a peer's changeset while sealing is paused",
    );
    let pending = result
        .rotation_pending
        .expect("the inactive removal key marks the rotation pending");
    assert_eq!(pending.committed_generation, 2);
    assert_eq!(pending.live_generation, 1);
    assert_eq!(
        cipher_key(&cipher_b),
        old_key,
        "the inactive key must not replace the live cipher",
    );
    assert_eq!(
        db_b.get_protocol_state(PENDING_ROTATION_STATE_KEY)
            .await
            .unwrap(),
        Some("2".to_string()),
        "the pause is persisted so a restart does not forget it and seal under the old generation",
    );
}

#[tokio::test]
async fn replayed_pre_rotation_wrapped_key_is_not_adopted() {
    let owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let victim = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let old_key: [u8; 32] = [12u8; 32];

    let storage = MockSyncStorage::with_keypair(owner.clone());
    let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());
    {
        let entry = chain
            .signed_set_member(
                &owner,
                pubkey_hex(&device_b),
                None,
                MemberRole::Member,
                "0000000002000-0000-A".to_string(),
            )
            .expect("active Owner signs membership grant");
        chain.add_entry(entry).expect("valid membership grant");
    }
    {
        let entry = chain
            .signed_set_member(
                &owner,
                pubkey_hex(&victim),
                None,
                MemberRole::Member,
                "0000000003000-0000-A".to_string(),
            )
            .expect("active Owner signs membership grant");
        chain.add_entry(entry).expect("valid membership grant");
    }
    upload_chain(&storage, &chain, &owner).await;

    let b_x = device_b.to_x25519_public_key();
    let wrapped_old =
        signed_wrapped_key_for_test(LIB_ID, &pubkey_hex(&device_b), &b_x, &old_key, &owner);
    storage
        .put_wrapped_key(
            &pubkey_hex(&owner),
            &pubkey_hex(&device_b),
            wrapped_old.clone(),
        )
        .await
        .unwrap();

    let db_b = open_test_db();
    db_b.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(old_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(old_key)));
    let (_tmp_b, ld_b) = temp_store_dir();

    let new_key = revoke_member(
        &storage,
        &storage,
        storage.store_root_hash(),
        &mut chain,
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
        &PendingRotation::none(),
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await
    .expect("adopt generation 2");
    assert_eq!(cipher_generation(&cipher_b), new_key.current_generation());

    storage
        .put_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&device_b), wrapped_old)
        .await
        .unwrap();

    run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
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

    let storage = MockSyncStorage::with_keypair(owner.clone());
    let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());
    {
        let entry = chain
            .signed_set_member(
                &owner,
                pubkey_hex(&device_b),
                None,
                MemberRole::Member,
                "0000000002000-0000-A".to_string(),
            )
            .expect("active Owner signs membership grant");
        chain.add_entry(entry).expect("valid membership grant");
    }
    upload_chain(&storage, &chain, &owner).await;

    let b_x = device_b.to_x25519_public_key();
    let same_generation_replacement = signed_wrapped_key_for_test(
        LIB_ID,
        &pubkey_hex(&device_b),
        &b_x,
        &replacement_key,
        &owner,
    );
    storage
        .put_wrapped_key(
            &pubkey_hex(&owner),
            &pubkey_hex(&device_b),
            same_generation_replacement,
        )
        .await
        .unwrap();

    let db_b = open_test_db();
    db_b.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(current_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        current_key,
    )));
    let (_tmp_b, ld_b) = temp_store_dir();

    run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
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

    let storage = MockSyncStorage::with_store_and_keypair(LIB_ID, founder.clone());
    let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());
    {
        let entry = chain
            .signed_set_member(
                &founder,
                pubkey_hex(&second_owner),
                None,
                MemberRole::Owner,
                "0000000002000-0000-A".to_string(),
            )
            .expect("active Owner signs membership grant");
        chain.add_entry(entry).expect("valid membership grant");
    }
    {
        let entry = chain
            .signed_set_member(
                &founder,
                pubkey_hex(&device_b),
                None,
                MemberRole::Member,
                "0000000003000-0000-A".to_string(),
            )
            .expect("active Owner signs membership grant");
        chain.add_entry(entry).expect("valid membership grant");
    }
    {
        let entry = chain
            .signed_set_member(
                &founder,
                pubkey_hex(&victim),
                None,
                MemberRole::Member,
                "0000000004000-0000-A".to_string(),
            )
            .expect("active Owner signs membership grant");
        chain.add_entry(entry).expect("valid membership grant");
    }
    upload_chain(&storage, &chain, &founder).await;

    let db_b = open_test_db();
    db_b.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &founder_pk)
        .await
        .unwrap();
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(old_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(old_key)));
    let (_tmp_b, ld_b) = temp_store_dir();

    let new_key = revoke_member_durable(
        &storage,
        &storage,
        storage.store_root_hash(),
        &mut chain,
        &second_owner,
        &pubkey_hex(&victim),
        LIB_ID,
        "0000000005000-0000-B",
        &EncryptionService::from_key(old_key),
        &db_b,
    )
    .await
    .expect("second owner can revoke");

    run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
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

    let storage = MockSyncStorage::with_keypair(founder.clone());
    let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());
    {
        let entry = chain
            .signed_set_member(
                &founder,
                pubkey_hex(&second_owner),
                None,
                MemberRole::Owner,
                "0000000002000-0000-A".to_string(),
            )
            .expect("active Owner signs membership grant");
        chain.add_entry(entry).expect("valid membership grant");
    }
    {
        let entry = chain
            .signed_set_member(
                &founder,
                pubkey_hex(&device_b),
                None,
                MemberRole::Member,
                "0000000003000-0000-A".to_string(),
            )
            .expect("active Owner signs membership grant");
        chain.add_entry(entry).expect("valid membership grant");
    }
    {
        let entry = chain
            .signed_remove_member(
                &second_owner,
                pubkey_hex(&founder),
                "0000000004000-0000-B".to_string(),
            )
            .expect("active Owner removes membership grant");
        chain.add_entry(entry).expect("valid membership removal");
    }
    upload_chain(&storage, &chain, &second_owner).await;

    let b_x = device_b.to_x25519_public_key();
    let removed_owner_wrapped = signed_wrapped_key_for_test(
        LIB_ID,
        &pubkey_hex(&device_b),
        &b_x,
        &removed_owner_key,
        &founder,
    );
    // The removed founder, using residual bucket write, drops its wrap into the
    // current owner's prefix — where B's scan will read it. B authenticates the
    // wrap against that prefix's owner (second_owner), so the founder's signature
    // fails and the key is refused.
    storage
        .put_wrapped_key(
            &pubkey_hex(&second_owner),
            &pubkey_hex(&device_b),
            removed_owner_wrapped,
        )
        .await
        .unwrap();

    let db_b = open_test_db();
    db_b.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &founder_pk)
        .await
        .unwrap();
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(current_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        current_key,
    )));
    let (_tmp_b, ld_b) = temp_store_dir();

    let result = run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
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

/// Wrapped-key adoption refuses a forged wrap — one not signed by the owner whose
/// prefix it sits under. A bucket writer drops a key they chose, sealed to B's
/// public key and signed by an attacker, into the current owner's `keys/{owner}/`
/// prefix. B's refresh scans the current owners and authenticates each wrap
/// against its prefix owner, so it does NOT adopt the attacker's key; the cycle
/// fails closed and B keeps its real key.
#[tokio::test]
async fn refresh_rejects_a_forged_wrapped_key() {
    let owner = UserKeypair::generate();
    let attacker = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let real_key: [u8; 32] = [22u8; 32];

    let storage = MockSyncStorage::with_keypair(owner.clone());

    // Chain: owner founds + adds B.
    let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());
    {
        let entry = chain
            .signed_set_member(
                &owner,
                pubkey_hex(&device_b),
                None,
                MemberRole::Member,
                "0000000002000-0000-A".to_string(),
            )
            .expect("active Owner signs membership grant");
        chain.add_entry(entry).expect("valid membership grant");
    }
    upload_chain(&storage, &chain, &owner).await;

    // The attacker forges B's wrapped key: a key THEY chose, sealed to B's real
    // public key, signed by the attacker (not the owner), dropped into the current
    // owner's prefix where B's scan will read it.
    let forged_key: [u8; 32] = [0xCDu8; 32];
    let b_x = device_b.to_x25519_public_key();
    let forged =
        signed_wrapped_key_for_test(LIB_ID, &pubkey_hex(&device_b), &b_x, &forged_key, &attacker);
    storage
        .put_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&device_b), forged)
        .await
        .unwrap();

    // B holds its real key (live + keyring) and pins the owner.
    let db_b = open_test_db();
    db_b.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(real_key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        real_key,
    )));
    let (_tmp_b, ld_b) = temp_store_dir();

    // B's cycle aborts (refuses the forged key) rather than adopting it.
    let result = run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
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

/// For an owner-pinned store, a chain the refresh can't load must abort the
/// cycle, never fall open to "no chain, act anyway". A membership list that fails
/// aborts B's cycle rather than letting it push or judge under no authorization.
#[tokio::test]
async fn refresh_fails_closed_when_the_chain_cannot_be_loaded() {
    let owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let key: [u8; 32] = [44u8; 32];

    let storage = MockSyncStorage::with_keypair(owner.clone());

    // A real owner-anchored chain exists, but listing it is made to fail this cycle.
    let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());
    {
        let entry = chain
            .signed_set_member(
                &owner,
                pubkey_hex(&device_b),
                None,
                MemberRole::Member,
                "0000000002000-0000-A".to_string(),
            )
            .expect("active Owner signs membership grant");
        chain.add_entry(entry).expect("valid membership grant");
    }
    upload_chain(&storage, &chain, &owner).await;
    storage.fail_membership_listing();

    let db_b = open_test_db();
    db_b.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(key)));
    let (_tmp_b, ld_b) = temp_store_dir();

    let result = run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
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
async fn membership_coords(
    _storage: &MockSyncStorage,
    entry_keys: &[MembershipCoord],
) -> Vec<MembershipCoord> {
    entry_keys.to_vec()
}

/// One sync cycle lists the membership chain exactly once. The chain is loaded and
/// anchored a single time at the top of the cycle and threaded to every
/// authorization site — the refresh, the pull, the outgoing write-grant, the
/// snapshot-author check, and the tombstone GC — so the whole cycle judges one
/// chain state and pays a single listing round-trip. `MockSyncStorage` counts the
/// `list_membership_entries` calls.
///
/// Mutation proof: route any of those sites through its own membership load and
/// the delta rises above one.
#[tokio::test]
async fn one_cycle_lists_membership_once() {
    let owner = UserKeypair::generate();
    let device_b = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let key: [u8; 32] = [7u8; 32];

    let storage = MockSyncStorage::with_keypair(owner.clone());

    // Chain: owner founds and adds B as a Member.
    let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());
    {
        let entry = chain
            .signed_set_member(
                &owner,
                pubkey_hex(&device_b),
                None,
                MemberRole::Member,
                "0000000002000-0000-A".to_string(),
            )
            .expect("active Owner signs membership grant");
        chain.add_entry(entry).expect("valid membership grant");
    }
    upload_chain(&storage, &chain, &owner).await;

    // B's steady state: owner pinned + an encrypted cipher on the shared key, so the
    // cycle's refresh and pull both run against the anchored chain rather than
    // short-circuiting as a plaintext no-op. B is a Member, so it authors no
    // snapshot — whose reclaim would be a separate, out-of-scope membership read.
    let db_b = open_test_db();
    db_b.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(key);
    let cipher_b = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(key)));
    let (_tmp_b, ld_b) = temp_store_dir();

    let before = storage.membership_list_count();
    run_cycle(
        &storage,
        &db_b,
        &cipher_b,
        &PendingRotation::none(),
        &device_b,
        "B",
        &ld_b,
        Some(&ks_b),
    )
    .await
    .expect("B's cycle");
    assert_eq!(
        storage.membership_list_count() - before,
        1,
        "one cycle loads and anchors the membership chain exactly once",
    );
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

/// Removing a member commits the cloud key rotation before this device adopts the
/// rotated key locally. When the local keyring write fails, the removal is durable
/// — the member is out and the store is rotated for everyone — but this device's
/// live cipher stays on the superseded generation. That half-applied state is its
/// own typed error, marks this device's rotation-pending gate (so it seals
/// nothing new for the cloud in the meantime — see `rotation_pending_tests`), and
/// both remedies it names converge without losing the rotation: the device's next
/// sync cycle adopts the key from its own `keys/{self}` wrap, and retrying the
/// removal re-derives and re-adopts it.
#[tokio::test]
async fn removal_rotation_commits_even_when_local_adoption_fails_then_both_remedies_converge() {
    let owner = UserKeypair::generate(); // this device — performs the removal
    let member = UserKeypair::generate(); // the member being removed
    let owner_pk = pubkey_hex(&owner);
    let old_key: [u8; 32] = [11u8; 32];

    let storage = MockSyncStorage::with_store_and_keypair(LIB_ID, owner.clone());

    // Chain: owner founds and adds the member.
    let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());
    {
        let entry = chain
            .signed_set_member(
                &owner,
                pubkey_hex(&member),
                None,
                MemberRole::Member,
                "0000000002000-0000-A".to_string(),
            )
            .expect("active Owner signs membership grant");
        chain.add_entry(entry).expect("valid membership grant");
    }
    upload_chain(&storage, &chain, &owner).await;

    let db = open_test_db();
    db.set_protocol_state(
        crate::database::STORE_ROOT_HASH_STATE_KEY,
        &storage.store_root_hash().to_string(),
    )
    .await
    .expect("bind removal fixture to its Store protocol root");
    let listed = storage.discover_membership_entries().await;
    crate::sync::membership_ops::load_and_persist_owner_anchor(
        &storage,
        storage.store_root_hash(),
        &listed,
        &owner_pk,
        &db,
    )
    .await
    .unwrap()
    .unwrap();

    // This device's steady state: keyring and live cipher hold the pre-rotation key.
    let ks = TestCustody::default();
    ks.set_initial_key(old_key);
    let cipher = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(old_key)));
    let pending_rotation = PendingRotation::none();
    let hlc = Hlc::new("A".to_string());

    // The keyring is momentarily unwritable, so local adoption fails after the
    // cloud rotation commits.
    ks.fail_writes();
    let err = remove_member(
        &storage,
        &storage,
        &owner,
        &hlc,
        &pubkey_hex(&member),
        LIB_ID,
        &EncryptionService::from_key(old_key),
        &ks,
        &cipher,
        &pending_rotation,
        &db,
    )
    .await
    .expect_err("local adoption fails while the keyring is unwritable");
    assert!(
        matches!(
            err,
            MembershipOpsError::RotationCommittedAdoptionFailed { .. }
        ),
        "the failure is the rotation-committed/adoption-failed variant, got {err:?}",
    );

    // The cloud rotation committed: the member is durably removed from the
    // committed chain even though this device could not adopt the new key.
    let entries = storage.discover_membership_entries().await;
    let committed = load_anchored_chain(
        &storage,
        storage.store_root_hash(),
        &entries,
        Some(&owner_pk),
        None,
    )
    .await
    .expect("committed chain loads");
    assert!(
        !committed
            .current_members()
            .iter()
            .any(|(pk, _)| pk == &pubkey_hex(&member)),
        "the member is durably removed despite the failed local adoption",
    );

    // The failed adoption left this device's live cipher and keyring on the
    // pre-rotation generation — no half-applied swap.
    assert_eq!(
        cipher_generation(&cipher),
        1,
        "the failed adoption did not swap the live cipher",
    );
    assert_eq!(
        ks.stored_key(),
        Some(
            crate::encryption::MasterKeyring::from(EncryptionService::from_key(old_key))
                .to_serialized(),
        ),
        "the failed adoption did not persist a new key",
    );

    // The failed adoption marks this device's own rotation-pending gate — the
    // structural half of the fix: every seal for the cloud through this same
    // storage now refuses until one of the two remedies below clears it (see
    // `rotation_pending_tests` for the end-to-end proof that nothing seals in
    // the meantime).
    assert_eq!(
        pending_rotation.pending_generation(),
        Some(2),
        "the failed adoption marks the committed generation as pending",
    );

    // Remedy 1 — the next sync cycle: a still-stale device (generation 1) adopts
    // the rotated key from its own `keys/{owner}/{owner}` wrap, no retry needed.
    {
        let db = open_test_db();
        db.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
            .await
            .unwrap();
        let ks_refresh = TestCustody::default();
        ks_refresh.set_initial_key(old_key);
        let cipher_refresh =
            RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(old_key)));
        let pending_rotation_refresh = PendingRotation::none();
        let (_tmp, ld) = temp_store_dir();
        run_cycle(
            &storage,
            &db,
            &cipher_refresh,
            &pending_rotation_refresh,
            &owner,
            "A",
            &ld,
            Some(&ks_refresh),
        )
        .await
        .expect("refresh cycle");
        assert_eq!(
            cipher_generation(&cipher_refresh),
            2,
            "the next sync cycle adopts the rotated key",
        );
    }

    // Remedy 2 — retry the removal: the member is already removed, so the retry
    // re-derives the rotated keyring from the current owner set and adopts it now
    // that the keyring is writable again.
    ks.allow_writes();
    let fingerprint = remove_member(
        &storage,
        &storage,
        &owner,
        &hlc,
        &pubkey_hex(&member),
        LIB_ID,
        &EncryptionService::from_key(old_key),
        &ks,
        &cipher,
        &pending_rotation,
        &db,
    )
    .await
    .expect("retrying the removal converges");
    assert!(
        !fingerprint.is_empty(),
        "the retry returns the key fingerprint"
    );
    assert_eq!(
        cipher_generation(&cipher),
        2,
        "retrying the removal adopts the rotated key",
    );
    assert_eq!(
        pending_rotation.pending_generation(),
        None,
        "the retried removal clears this device's own rotation-pending gate",
    );
}
