use super::*;

#[tokio::test]
async fn non_rotating_device_adopts_rotated_key_without_restart() {
    let owner = UserKeypair::generate(); // device A, the founder/owner
    let device_b = UserKeypair::generate();
    let victim = UserKeypair::generate(); // the member A will remove
    let old_key: [u8; 32] = [11u8; 32];

    let encryption = EncryptionService::from_key(old_key);
    let ExactStoreFixture {
        store: storage,
        home: _,
        cloud_storage: _,
        db: owner_db,
        db_store_dir: owner_db_store_dir,
    } = exact_store(&owner, &encryption).await;
    storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &device_b,
            MemberRole::Member,
            &encryption,
        )
        .await;
    storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &victim,
            MemberRole::Member,
            &encryption,
        )
        .await;

    // B's local state: pinned owner + its keyring holds the OLD key + its live
    // cipher is the OLD key. This is the just-joined steady state.
    let db_b_store_dir = crate::sync::test_helpers::test_store_dir();
    let db_b = crate::sync::test_helpers::open_test_db(db_b_store_dir.clone());
    let running_b = storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &db_b,
            db_b_store_dir.clone(),
            &device_b,
            "0000000001000-0000-refresh",
        )
        .await
        .expect("activate exact joined test device");
    let ks_b = TestCustody::default();
    ks_b.set_initial_key(old_key);

    // Sanity: before the rotation, B's refresh is a no-op — it already holds the
    // current key, so the cycle leaves the cipher unchanged.
    running_b
        .run_cycle_with(&SystemClock, Some(Arc::new(ks_b.clone())), None)
        .await
        .expect("pre-rotation cycle");
    assert_eq!(
        running_b
            .current_keyring_for_test()
            .expect("B has encrypted storage")
            .seal_key(),
        old_key,
        "before any rotation B keeps the key it joined with",
    );

    // Device A removes the victim, rotates the key, and activates B's new exact wrap.
    let rotated_membership = storage
        .revoke_member_durable(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &pubkey_hex(&victim),
            "0000000004000-0000-A",
            &encryption,
            &PendingRotation::none(),
        )
        .await
        .expect("revoke rotates the key");
    let new_key = crate::sync::store::open_store_keyring(&owner, &rotated_membership)
        .expect("open the exact accepted rotation keys");
    assert_ne!(
        new_key.key_bytes(),
        old_key,
        "removal rotates to a fresh key"
    );

    // --- B's NEXT cycle, no restart: it must adopt the rotated key. ---
    running_b
        .run_cycle_with(&SystemClock, Some(Arc::new(ks_b.clone())), None)
        .await
        .expect("post-rotation cycle");

    // B's live cipher now holds the rotated key (it can decrypt what A seals under
    // it this cycle), and its keyring was updated so a restart reads the new key —
    // the two halves `apply_key_rotation` performs.
    assert_eq!(
        running_b
            .current_keyring_for_test()
            .expect("B retains encrypted storage")
            .seal_key(),
        new_key.key_bytes(),
        "B adopted the rotated key into its live cipher without a restart",
    );
    assert_eq!(
        ks_b.stored_key().as_deref(),
        Some(new_key.to_keyring_string().unwrap().as_str()),
        "B persisted the rotated key to its keyring, so its restart reads the current key",
    );

    // The chain carries the sealed keys; no path search chooses the key.
    let (reopened, _) = storage
        .bind_device_in(&db_b, db_b_store_dir.clone(), &device_b)
        .await
        .expect("bind refreshed member Store")
        .membership_keyring_facts()
        .await
        .expect("B opens the key the rotation sealed to it");
    assert_eq!(reopened, new_key.key_bytes());
}

#[tokio::test]
async fn admission_after_rotation_uses_the_membership_selected_keyring() {
    let owner = UserKeypair::generate();
    let removed_member = UserKeypair::generate();
    let admitted_member = UserKeypair::generate();
    let initial = EncryptionService::from_key([52u8; 32]);
    let ExactStoreFixture {
        store: storage,
        home: _,
        cloud_storage,
        db: owner_db,
        db_store_dir: owner_db_store_dir,
    } = exact_store(&owner, &initial).await;
    storage
        .admit_exact_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &removed_member,
            MemberRole::Member,
            &initial,
        )
        .await;
    let custody = TestCustody::default();
    custody.set_initial_key(initial.key_bytes());
    let cipher = RwLock::new(CloudCipher::Encrypted(initial.clone()));
    let pending_rotation = PendingRotation::none();
    let rotated_membership = storage
        .revoke_member_durable(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &pubkey_hex(&removed_member),
            "0000000004000-0000-owner",
            &initial,
            &pending_rotation,
        )
        .await
        .expect("remove member and rotate the Store key");
    let rotated = crate::sync::store::open_store_keyring(&owner, &rotated_membership)
        .expect("open the exact accepted rotation keys");
    cipher
        .adopt_key_rotation(&rotated, &custody)
        .expect("owner adopts the activated rotation");
    storage
        .bind_device_in(&owner_db, owner_db_store_dir.clone(), &owner)
        .await
        .expect("bind rotation owner")
        .complete_revoke_rotation_adoption_for_test(&pending_rotation, rotated.current_generation())
        .await
        .expect("owner completes the activated removal journal");

    let admission = storage
        .admit_member(
            &owner_db,
            owner_db_store_dir.clone(),
            &owner,
            &pubkey_hex(&admitted_member),
            None,
            MemberRole::Member,
            &initial,
            "Refresh Test Store",
        )
        .await
        .expect("publish post-rotation admission");
    let history = crate::sync::store::HistoryConstructionAuthority::admission()
        .open_pinned(&*cloud_storage, &admission.store_root)
        .await
        .expect("open admission history");
    let chain = history
        .load_accepted_anchored_membership(
            &admission.membership_floor.0,
            Some(&admission.owner_pubkey),
        )
        .await
        .expect("load admission membership");
    let admitted_keyring = crate::sync::store::open_granted_store_keyring(
        &admitted_member,
        &chain,
        &admission.grant_id,
    )
    .expect("admitted member opens the key its accepted grant activates");
    let sealed = rotated.seal_app_data(b"current Store data", b"post-rotation admission");
    assert_eq!(
        admitted_keyring
            .open_app_data(&sealed, b"post-rotation admission")
            .expect("admit retains the current Store key"),
        b"current Store data",
    );
}
