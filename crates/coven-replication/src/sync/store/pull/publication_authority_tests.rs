use crate::sync::test_helpers::{
    open_test_db, pubkey_hex, test_cloud_home, test_store_dir, TestCustody, TestStore,
};
use coven_database::StoreDatabase;
use coven_keys::encryption::EncryptionService;
use coven_keys::keys::UserKeypair;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{
    store_current_publication_semantic_prefix, store_publication_entry_semantic_prefix,
    StoreCurrentPublicationRecord, StorePublicationEntry, StorePublicationRef,
};
use coven_storage::CloudSyncObjectStorage;

#[tokio::test]
async fn a_missing_snapshot_cannot_admit_new_publication_authority() {
    let directory = test_store_dir();
    let database = open_test_db(directory.clone());
    let identity = UserKeypair::generate();
    let (store, storage) = TestStore::create_with_connection(
        &database,
        directory.clone(),
        "missing-snapshot-authority",
        identity.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let owner = store
        .bind_device_in(&database, directory, &identity)
        .await
        .expect("bind owner");
    owner.publish_fixture_position("accepted-prefix").await;
    let prefix = owner
        .latest_local_store_position()
        .await
        .expect("read accepted prefix")
        .expect("the prefix has a committed row");
    let mut history = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
        .open_pinned(storage.as_ref(), &store.root())
        .await
        .expect("open snapshot verification owner");
    history
        .load_initial_store_publication_interval()
        .await
        .expect("verify the accepted prefix");

    owner.publish_fixture_position("unobserved-successor").await;
    let successor = owner
        .latest_local_store_position()
        .await
        .expect("read published successor")
        .expect("the successor has a committed row");
    assert_ne!(successor, prefix);
    let rejection = history
        .load_current_accepted_snapshot()
        .await
        .expect_err("a Store without a snapshot cannot provide one");
    assert!(
        rejection.to_string().contains("no accepted snapshot"),
        "{rejection}"
    );
    let rejection = history
        .verify_refs([successor.clone()])
        .await
        .expect_err("a failed snapshot load cannot admit its observed successor");
    assert!(
        rejection
            .to_string()
            .contains("absent from accepted publication history"),
        "{rejection}"
    );
    history
        .verify_refs([prefix])
        .await
        .expect("the failed snapshot load preserves accepted prefix authority");
    history
        .load_initial_store_publication_interval()
        .await
        .expect("the publication owner can accept the observed successor");
    history
        .verify_refs([successor])
        .await
        .expect("successful publication verification admits the successor");
}

#[tokio::test]
async fn a_new_publication_cannot_use_a_grant_revoked_after_the_commit_was_prepared() {
    Box::pin(assert_publication_requires_current_authority(
        AuthorityLoss::MemberRemoval,
        false,
    ))
    .await;
}

#[tokio::test]
async fn a_new_publication_cannot_use_a_device_excluded_after_the_commit_was_prepared() {
    Box::pin(assert_publication_requires_current_authority(
        AuthorityLoss::DeviceExclusion,
        false,
    ))
    .await;
}

#[tokio::test]
async fn a_rejected_snapshot_tail_cannot_seed_accepted_history() {
    Box::pin(assert_publication_requires_current_authority(
        AuthorityLoss::MemberRemoval,
        true,
    ))
    .await;
}

enum AuthorityLoss {
    MemberRemoval,
    DeviceExclusion,
}

async fn assert_publication_requires_current_authority(
    authority_loss: AuthorityLoss,
    snapshot: bool,
) {
    let owner_dir = test_store_dir();
    let owner_db = open_test_db(owner_dir.clone());
    let owner_identity = UserKeypair::generate();
    let (store, storage) = TestStore::create_with_connection(
        &owner_db,
        owner_dir.clone(),
        "publication-authority-after-revocation",
        owner_identity.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let member_dir = test_store_dir();
    let member_db = open_test_db(member_dir.clone());
    let member_identity = UserKeypair::generate();
    let member = store
        .admit_and_activate_peer(
            &owner_db,
            owner_dir.clone(),
            &member_db,
            member_dir,
            &member_identity,
        )
        .await
        .expect("admit and activate member");
    let owner = store
        .bind_device_in(&owner_db, owner_dir.clone(), &owner_identity)
        .await
        .expect("bind owner");
    owner.pull_store().await.expect("install member activation");

    let snapshot_frontier = if snapshot {
        let database = StoreDatabase::new(&owner_db);
        let directory = tempfile::tempdir().expect("create snapshot directory");
        let image = database
            .capture_snapshot_image_for_test(store.root(), directory.path().to_path_buf(), None)
            .await
            .expect("capture accepted history before candidate preparation");
        let frontier = database
            .materialized_frontier()
            .await
            .expect("read snapshot coverage");
        let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(frontier.clone())
            .expect("shape snapshot coverage");
        owner
            .publish_snapshot(image, coverage)
            .await
            .expect("publish the accepted snapshot");
        member
            .pull_store()
            .await
            .expect("prepare the candidate against the accepted snapshot base");
        Some(frontier)
    } else {
        None
    };

    member_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('late-member-row', 'Prepared before removal', 1, \
             '0000000002000-0000-member', '2026-01-01')",
        )
        .await;
    let member_database = StoreDatabase::new(&member_db);
    let mut writer = member.authorize_writer().await.expect("authorize member");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare member row"));
    let pending = member_database
        .oldest_prepared_store_write()
        .await
        .expect("load member candidate")
        .expect("member row is prepared");
    let (uploaded, _resume) = member_db.arm_test_pause(
        coven_database::DatabaseTestPoint::StoreWriteCommitUploaded {
            write_id: pending.commit.value.write_id.clone(),
        },
    );
    {
        let publication = writer.drain_store_writes();
        tokio::pin!(publication);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                _ = uploaded.notified() => {},
                result = &mut publication => panic!("publication returned before commit upload: {result:?}"),
            }
        })
        .await
        .expect("upload the actual package and commit before interrupting publication");
    }
    drop(writer);

    match authority_loss {
        AuthorityLoss::MemberRemoval => {
            let encryption = EncryptionService::from_key([42; 32]);
            let custody = TestCustody::default();
            custody.set_initial_key([42; 32]);
            store
                .remove_member(
                    &owner_db,
                    owner_dir.clone(),
                    &owner_identity,
                    &pubkey_hex(&member_identity),
                    &encryption,
                    &custody,
                )
                .await
                .expect("remove the candidate's exact member grant");
        }
        AuthorityLoss::DeviceExclusion => {
            Box::pin(owner.finalize_peer_exclusion(&pending.commit.value.author_registration))
                .await;
        }
    }
    owner_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('removal-witness', 'Accepted after removal', 1, \
             '0000000003000-0000-owner', '2026-01-01')",
        )
        .await;
    let mut writer = owner.authorize_writer().await.expect("authorize owner");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare the accepted removal witness"));
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("publish a commit carrying the removal"),
        1,
    );
    drop(writer);
    let current_membership = owner
        .membership_for_test()
        .await
        .expect("load accepted membership");
    let commit = &pending.commit.value;
    assert_eq!(
        current_membership.authorizes_write_authority(
            commit
                .membership_authority
                .as_ref()
                .expect("candidate carries its captured grant"),
            &pubkey_hex(&member_identity),
        ),
        matches!(authority_loss, AuthorityLoss::DeviceExclusion),
        "device exclusion preserves membership; member removal revokes the exact grant",
    );
    let owner_database = StoreDatabase::new(&owner_db);
    let (before, retained_before) = owner_database
        .retained_store_publication()
        .await
        .expect("capture the accepted publication boundary");
    let retained_before = retained_before
        .into_iter()
        .map(|entry| (entry.prepared.reference().clone(), entry.bytes))
        .collect::<Vec<_>>();
    let frontier_before = owner_database
        .materialized_frontier()
        .await
        .expect("capture the fully materialized frontier");
    let cut = coven_protocol::store_commit::CommitFrontier::from_refs(frontier_before.clone())
        .expect("shape the accepted device-state cut");
    let (_, device_state) = owner_database
        .store_device_state_for_history_cut(&coven_protocol::store_commit::StoreHistoryCut(cut.0))
        .await
        .expect("read the accepted device state");
    let member_state = device_state
        .devices
        .get(&commit.author_registration.device_id)
        .expect("the member's exact device remains recorded");
    assert_eq!(member_state.registration, commit.author_registration);
    assert_eq!(
        matches!(
            member_state.status,
            coven_protocol::store_commit::StoreDeviceStatus::Active,
        ),
        matches!(authority_loss, AuthorityLoss::MemberRemoval),
        "member removal preserves device registration; exclusion makes it inactive",
    );

    let receiving_store = store
        .open_store_with_identity(&owner_db, owner_dir, &owner_identity)
        .await
        .expect("open the receiving Store");
    let mut reused_history = receiving_store
        .authorize_history_for_test()
        .await
        .expect("open the receiving history owner before the invalid publication");

    // A device can still sign bytes after its authority ends. Bypass the
    // local publisher to test whether the receiver checks acceptance authority.
    let device_signer = commit
        .author()
        .device_signer(&member_identity)
        .expect("derive the member's unchanged device key");
    let entry = StorePublicationEntry::signed_commit(before.record(), commit, &device_signer)
        .expect("sign a valid envelope preserving the captured commit");
    let prefix = store_publication_entry_semantic_prefix(&entry);
    let context = ProtocolObjectContext::signed_plaintext(
        store.root().store_root_hash,
        ProtocolObjectDomain::StorePublicationEntry,
    );
    let object = store
        .create_exact_protocol_object(&context, &prefix, ".json", &entry.to_bytes())
        .await
        .expect("upload the new publication envelope");
    let reference = StorePublicationRef::from_entry(&entry, object)
        .expect("reference the new exact publication");
    let replacement = StoreCurrentPublicationRecord::advance_commit(
        before.record(),
        &entry,
        reference,
        commit,
        &device_signer,
    )
    .expect("sign a successor current record with the old device key");
    let context = ProtocolObjectContext::signed_plaintext(
        store.root().store_root_hash,
        ProtocolObjectDomain::StoreCurrentPublication,
    );
    let replaced = storage
        .replace_protocol_record_if_version(
            &context,
            &owner
                .protocol_root_for_test()
                .descriptor
                .current_publication_slot,
            store_current_publication_semantic_prefix(),
            before
                .require_observed()
                .expect("accepted provider observation")
                .version(),
            replacement.to_bytes(),
        )
        .await
        .expect("the provider accepts the matching revision");
    assert!(matches!(
        replaced,
        coven_storage::cloud::ConditionalWriteOutcome::Replaced(_),
    ));

    if let Some(snapshot_frontier) = snapshot_frontier {
        let mut history = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
            .open_pinned(storage.as_ref(), &store.root())
            .await
            .expect("open a fresh snapshot verification owner");
        let rejection = history
            .load_current_accepted_snapshot()
            .await
            .expect_err("snapshot verification rejects its unauthorized tail");
        assert!(
            rejection.to_string().contains("authority")
                || rejection.to_string().contains("inactive"),
            "{rejection}",
        );
        let seeded_authority = history
            .verify_refs(snapshot_frontier.values().cloned())
            .await;
        assert!(
            seeded_authority.is_err(),
            "a rejected snapshot tail cannot seed accepted history in the same verifier",
        );
    }

    let rejection = reused_history
        .prepare_store_publication_history_for_test()
        .await
        .expect_err("the reused history owner rejects the publication authority");
    assert!(
        rejection.to_string().contains("authority") || rejection.to_string().contains("inactive"),
        "{rejection}",
    );
    let reused_authority = reused_history
        .verified_merge_membership_prefix_for_test(
            [commit.reference().clone()],
            [commit.reference().clone()],
        )
        .await;
    assert!(
        reused_authority.is_err(),
        "a rejected publication cannot supply authority to the same history owner",
    );

    reused_history
        .verified_merge_membership_prefix_for_test(
            frontier_before.values().cloned(),
            frontier_before.values().cloned(),
        )
        .await
        .expect("rejection preserves the previously accepted authority");

    let error = owner
        .pull_store()
        .await
        .expect_err("receiver must reject authority lost before this publication");
    let message = error.to_string();
    assert!(
        message.contains("authority") || message.contains("inactive"),
        "{error}",
    );
    let (after, retained_after) = owner_database
        .retained_store_publication()
        .await
        .expect("read the preserved publication boundary");
    assert_eq!(after, before);
    assert_eq!(
        retained_after
            .into_iter()
            .map(|entry| (entry.prepared.reference().clone(), entry.bytes))
            .collect::<Vec<_>>(),
        retained_before,
    );
    assert_eq!(
        owner_database
            .materialized_frontier()
            .await
            .expect("read the preserved materialized frontier"),
        frontier_before,
    );
    assert_eq!(
        owner_db
            .query_test_text(
                "SELECT CAST(COUNT(*) AS TEXT) FROM notes WHERE id = 'late-member-row'"
            )
            .await,
        "0",
    );
    assert_eq!(
        owner_db
            .query_test_text("SELECT title FROM notes WHERE id = 'removal-witness'")
            .await,
        "Accepted after removal",
    );
}
