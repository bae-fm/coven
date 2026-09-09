use crate::sync::test_helpers::{open_test_db, test_cloud_home, test_store_dir, TestStore};
use coven_database::StoreDatabase;
use coven_keys::keys::UserKeypair;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::prepared_commit::PreparedStorePublication;
use coven_protocol::store_commit::{
    store_publication_entry_semantic_prefix, StoreCurrentPublicationRecord, StorePublicationEntry,
    StorePublicationRef,
};
use coven_storage::CloudSyncObjectStorage;

#[tokio::test]
async fn a_same_base_publication_conflict_retries_the_original_mixed_write() {
    assert_mixed_write_publication_retry(false).await;
}

#[tokio::test]
async fn a_conditional_replacement_conflict_retries_the_original_mixed_write() {
    assert_mixed_write_publication_retry(true).await;
}

async fn assert_mixed_write_publication_retry(conflict_after_read: bool) {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let (store, storage) = TestStore::create_with_connection(
        &source,
        source_dir.clone(),
        "retry-concurrent-publication",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let target_dir = test_store_dir();
    let target = open_test_db(target_dir.clone());
    let peer = store
        .admit_and_activate_peer(
            &source,
            source_dir.clone(),
            &target,
            target_dir,
            &UserKeypair::generate(),
        )
        .await
        .expect("activate peer");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind owner");
    owner.pull_store().await.expect("observe peer activation");
    source
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
             ('owner-shared', 'Owner shared effect', 1, '0000000002000-0000-owner', '2026-01-01'), \
             ('owner-private', 'Owner private effect', 0, '0000000002000-0000-owner', '2026-01-01')",
        )
        .await;
    target
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
             ('peer-shared', 'Independent peer effect', 1, '0000000002000-0000-peer', '2026-01-01')",
        )
        .await;
    let mut owner_writer = owner.authorize_writer().await.expect("authorize owner");
    assert!(owner_writer
        .prepare_pending_store_write()
        .await
        .expect("prepare mixed owner write"));
    let mut peer_writer = peer.authorize_writer().await.expect("authorize peer");
    assert!(peer_writer
        .prepare_pending_store_write()
        .await
        .expect("prepare independent peer write"));
    let database = StoreDatabase::new(&source);
    let peer_database = StoreDatabase::new(&target);
    let pending = database
        .oldest_prepared_store_write()
        .await
        .expect("read owner write")
        .expect("owner write is prepared");
    let peer_pending = peer_database
        .oldest_prepared_store_write()
        .await
        .expect("read peer write")
        .expect("peer write is prepared");
    assert_eq!(
        pending.publication.previous,
        peer_pending.publication.previous
    );
    assert_eq!(
        pending.publication.previous_version, peer_pending.publication.previous_version,
        "both attempts must contend for the same provider revision",
    );
    let reservation = database
        .active_store_publication()
        .await
        .expect("read owner reservation")
        .expect("owner write holds its reservation");
    if !conflict_after_read {
        assert_eq!(
            peer_writer.drain_store_writes().await.expect("peer wins"),
            1
        );
    }
    // This write creates its Store package, signed commit, then publication
    // entry. Pause on the entry to let the peer change the provider revision
    // after the publisher has already read it.
    let provider_pause = conflict_after_read.then(|| store.pause_after_exact_create_call(3));
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("owner still has its preparation boundary")
            .record(),
        &pending.publication.previous,
    );

    let (accepted, resume) = source.arm_test_pause(
        coven_database::DatabaseTestPoint::StoreWritePublicationAccepted {
            write_id: pending.commit.value.write_id.clone(),
        },
    );
    {
        let publication = owner_writer.drain_store_writes();
        tokio::pin!(publication);
        if let Some((reached, release)) = provider_pause {
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                tokio::select! {
                    _ = reached.notified() => {},
                    result = &mut publication => panic!("publication returned before its provider pause: {result:?}"),
                }
            }).await.expect("reach the immutable entry before conditional replacement");
            assert_eq!(
                store.exact_creates().last(),
                Some(pending.publication.entry_object.slot()),
                "the pause must be after the current-record read and before replacement",
            );
            assert_eq!(
                peer_writer
                    .drain_store_writes()
                    .await
                    .expect("peer wins the read revision"),
                1
            );
            release.notify_one();
        }
        let winner = peer_database
            .store_current_publication()
            .await
            .expect("accepted peer boundary");

        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                _ = accepted.notified() => {},
                result = &mut publication => panic!("the owner must install the winner and retry its envelope: {result:?}"),
            }
        })
        .await
        .expect("the same drain must reach acceptance after the conflict");
        let retried = database
            .oldest_prepared_store_write()
            .await
            .expect("read accepted pending write")
            .expect("original write still awaits completion");
        assert_eq!(retried.commit.bytes, pending.commit.bytes);
        assert_eq!(retried.commit.value.write_id, pending.commit.value.write_id);
        assert_eq!(
            retried.commit.value.reference(),
            pending.commit.value.reference()
        );
        assert_eq!(retried.publication.previous, *winner.record());
        assert_ne!(
            retried.publication.entry_object,
            pending.publication.entry_object
        );
        let active = database
            .active_store_publication()
            .await
            .expect("read retried reservation")
            .expect("original write retains its reservation until completion");
        assert!(active.same_commit_reservation(&reservation));
        assert!(active.superseded_entry().is_none());
        assert_eq!(
            source
                .query_test_text("SELECT title FROM notes WHERE id = 'peer-shared'")
                .await,
            "Independent peer effect",
            "the winner must be installed before the replacement can be accepted",
        );
        resume.notify_one();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(10), &mut publication)
                .await
                .expect("complete the accepted original write")
                .expect("conflict retry completes within the original drain"),
            1,
        );
    }
    assert_eq!(
        owner_writer
            .drain_store_writes()
            .await
            .expect("drain again"),
        0
    );
    drop(owner_writer);
    assert!(database
        .oldest_prepared_store_write()
        .await
        .expect("completed write journal")
        .is_none());
    assert!(database
        .active_store_publication()
        .await
        .expect("released publication reservation")
        .is_none());
    let (_, entries) = database
        .retained_store_publication()
        .await
        .expect("read accepted history");
    for payload in [
        &pending.publication.entry.payload,
        &peer_pending.publication.entry.payload,
    ] {
        assert_eq!(
            entries
                .iter()
                .filter(|entry| &entry.value.payload == payload)
                .count(),
            1
        );
    }
    let commit = &pending.commit.value;
    let context = ProtocolObjectContext::signed_plaintext(
        store.root().store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let prefix = coven_protocol::store_commit::commit_semantic_prefix(
        commit.candidate_family(),
        &commit.reference().coord.stream_id.to_string(),
        commit.seq(),
        commit.commit_hash(),
    );
    assert_eq!(
        storage
            .read_protocol_object(&context, &commit.reference().object, &prefix)
            .await
            .expect("read the original accepted signed commit"),
        pending.commit.bytes,
        "retry must preserve the signed commit and all captured dependencies",
    );
    for (id, title) in [
        ("owner-shared", "Owner shared effect"),
        ("owner-private", "Owner private effect"),
        ("peer-shared", "Independent peer effect"),
    ] {
        assert_eq!(
            source
                .query_test_text(&format!("SELECT title FROM notes WHERE id = '{id}'"))
                .await,
            title,
        );
    }
    peer.pull_store()
        .await
        .expect("peer observes the retried write");
    assert_eq!(
        target
            .query_test_text("SELECT title FROM notes WHERE id = 'owner-shared'")
            .await,
        "Owner shared effect",
    );
    assert_eq!(
        target
            .query_test_text("SELECT title FROM notes WHERE id = 'peer-shared'")
            .await,
        "Independent peer effect",
    );
    assert_eq!(
        target
            .query_test_text("SELECT CAST(COUNT(*) AS TEXT) FROM notes WHERE id = 'owner-private'")
            .await,
        "0",
    );
}

#[tokio::test]
async fn retry_completes_a_write_installed_by_pull_without_publishing_or_replaying_it_again() {
    assert_installed_write_completion(0).await;
}

#[tokio::test]
async fn installed_write_completion_survives_concurrent_snapshot_retirement() {
    assert_installed_write_completion(1).await;
}

#[tokio::test]
async fn installed_write_completion_survives_a_new_covering_snapshot() {
    assert_installed_write_completion(2).await;
}

async fn assert_installed_write_completion(snapshot_generations: usize) {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let (store, _) = TestStore::create_with_connection(
        &source,
        source_dir.clone(),
        "complete-pulled-local-publication",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let target_dir = test_store_dir();
    let target = open_test_db(target_dir.clone());
    let peer = store
        .activate_joined_device(
            &source,
            source_dir.clone(),
            &target,
            target_dir,
            &signer,
            "2026-01-01T00:00:00Z",
        )
        .await
        .expect("activate snapshot publisher");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind owner");
    owner.pull_store().await.expect("observe peer activation");
    source
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
             ('shared-row', 'Shared effect', 1, '0000000002000-0000-writer', '2026-01-01'), \
             ('private-row', 'Private effect', 0, '0000000002000-0000-writer', '2026-01-01')",
        )
        .await;
    let database = StoreDatabase::new(&source);
    let mut writer = owner.authorize_writer().await.expect("authorize writer");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare mixed write"));
    let pending = database
        .oldest_prepared_store_write()
        .await
        .expect("load pending write")
        .expect("mixed write is prepared");
    let (accepted, _resume) = source.arm_test_pause(
        coven_database::DatabaseTestPoint::StoreWritePublicationAccepted {
            write_id: pending.commit.value.write_id.clone(),
        },
    );
    {
        let publication = writer.drain_store_writes();
        tokio::pin!(publication);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                _ = accepted.notified() => {},
                result = &mut publication => panic!("publication returned before its acceptance pause: {result:?}"),
            }
        })
        .await
        .expect("reach remote acceptance before interrupting local completion");
    }
    drop(writer);
    let (_, pulled) = owner
        .pull_store()
        .await
        .expect("install accepted own write through pull");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    let mut before = database
        .retained_store_publication()
        .await
        .expect("installed boundary");
    assert!(database
        .oldest_prepared_store_write()
        .await
        .expect("unfinished owner completion")
        .is_some());
    for _ in 1..snapshot_generations {
        peer.pull_store().await.expect("peer installs the write");
        peer.publish_snapshot_generation_for_test()
            .await
            .expect("publish initial covering snapshot");
        owner
            .pull_store()
            .await
            .expect("install initial covering snapshot");
    }
    let mut writer = owner
        .authorize_writer()
        .await
        .expect("retry owner completion");
    if snapshot_generations > 0 {
        peer.pull_store().await.expect("peer installs the write");
        let receipt = database
            .installed_store_commit_evidence(pending.commit.value.clone())
            .await
            .expect("read exact receipt")
            .expect("installed write");
        assert_eq!(
            receipt.exact_publication().is_some(),
            snapshot_generations == 1
        );
        let (reached, release) = source.arm_test_pause(
            coven_database::DatabaseTestPoint::StoreWritePublicationAccepted {
                write_id: pending.commit.value.write_id.clone(),
            },
        );
        let completion = writer.drain_store_writes();
        tokio::pin!(completion);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                _ = reached.notified() => {},
                result = &mut completion => panic!("completion returned before its receipt pause: {result:?}"),
            }
        }).await.expect("publisher has read its installed receipt");
        if snapshot_generations > 1 {
            target.execute_test_host_write(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
                 ('peer-successor', 'Later shared effect', 1, '0000000003000-0000-peer', '2026-01-01')"
            ).await;
            let mut peer_writer = peer
                .authorize_writer()
                .await
                .expect("authorize peer successor");
            assert!(peer_writer
                .prepare_pending_store_write()
                .await
                .expect("prepare successor"));
            assert_eq!(
                peer_writer
                    .drain_store_writes()
                    .await
                    .expect("publish successor"),
                1
            );
        }
        peer.publish_snapshot_generation_for_test()
            .await
            .expect("publish covering snapshot");
        owner
            .pull_store()
            .await
            .expect("install snapshot while completion waits");
        let compacted = database
            .installed_store_commit_evidence(pending.commit.value.clone())
            .await
            .expect("read snapshot receipt")
            .expect("snapshot covers write");
        assert!(
            compacted.exact_publication().is_none(),
            "snapshot must retire the exact receipt"
        );
        assert_eq!(receipt.commit_ref(), compacted.commit_ref());
        assert_ne!(
            receipt, compacted,
            "the installed proof must change during completion"
        );
        before = database
            .retained_store_publication()
            .await
            .expect("snapshot boundary");
        release.notify_one();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(10), &mut completion)
                .await
                .expect("completion resumes")
                .expect("complete against current snapshot authority"),
            1
        );
    } else {
        assert_eq!(
            writer
                .drain_store_writes()
                .await
                .expect("complete already-installed write"),
            1
        );
    }
    drop(writer);
    let after = database
        .retained_store_publication()
        .await
        .expect("boundary after completion");
    assert_eq!(after.0, before.0);
    assert_eq!(after.1.len(), before.1.len());
    assert!(database
        .active_store_publication()
        .await
        .expect("released reservation")
        .is_none());
    assert!(database
        .oldest_prepared_store_write()
        .await
        .expect("completed journal")
        .is_none());
    assert_eq!(
        source
            .query_test_text("SELECT title FROM notes WHERE id = 'shared-row'")
            .await,
        "Shared effect"
    );
    assert_eq!(
        source
            .query_test_text("SELECT title FROM notes WHERE id = 'private-row'")
            .await,
        "Private effect"
    );
}

#[tokio::test]
async fn a_live_publisher_completes_after_pull_installs_its_accepted_write() {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let (store, _) = TestStore::create_with_connection(
        &source,
        source_dir.clone(),
        "complete-concurrently-pulled-publication",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind owner");
    source
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
             ('shared-row', 'Shared effect', 1, '0000000002000-0000-writer', '2026-01-01'), \
             ('private-row', 'Private effect', 0, '0000000002000-0000-writer', '2026-01-01')",
        )
        .await;
    let database = StoreDatabase::new(&source);
    let mut writer = owner.authorize_writer().await.expect("authorize writer");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare mixed write"));
    let pending = database
        .oldest_prepared_store_write()
        .await
        .expect("load pending write")
        .expect("mixed write is prepared");
    let (accepted, resume) = source.arm_test_pause(
        coven_database::DatabaseTestPoint::StoreWritePublicationAccepted {
            write_id: pending.commit.value.write_id.clone(),
        },
    );
    let before;
    {
        let publication = writer.drain_store_writes();
        tokio::pin!(publication);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                _ = accepted.notified() => {},
                result = &mut publication => panic!("publication returned before its acceptance pause: {result:?}"),
            }
        })
        .await
        .expect("reach cloud acceptance before local completion");
        let (_, pulled) = owner
            .pull_store()
            .await
            .expect("pull installs the accepted write");
        assert!(pulled.held_positions.is_empty(), "{pulled:?}");
        before = database
            .retained_store_publication()
            .await
            .expect("pulled boundary");
        resume.notify_one();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(10), &mut publication)
                .await
                .expect("publisher finishes after concurrent pull")
                .expect("the same publisher recognizes its installed write"),
            1,
        );
    }
    drop(writer);
    let after = database
        .retained_store_publication()
        .await
        .expect("completed boundary");
    assert_eq!(after.0, before.0);
    assert_eq!(after.1.len(), before.1.len());
    assert!(database
        .active_store_publication()
        .await
        .expect("released reservation")
        .is_none());
    assert!(database
        .oldest_prepared_store_write()
        .await
        .expect("completed journal")
        .is_none());
    assert_eq!(
        source
            .query_test_text("SELECT title FROM notes WHERE id = 'shared-row'")
            .await,
        "Shared effect"
    );
    assert_eq!(
        source
            .query_test_text("SELECT title FROM notes WHERE id = 'private-row'")
            .await,
        "Private effect"
    );
}

#[tokio::test]
async fn replacing_a_publication_requires_the_installed_boundary_and_preserves_the_write() {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let (store, storage) = TestStore::create_with_connection(
        &source,
        source_dir.clone(),
        "replace-publication-attempt",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let target_dir = test_store_dir();
    let target = open_test_db(target_dir.clone());
    let peer = store
        .admit_and_activate_peer(
            &source,
            source_dir.clone(),
            &target,
            target_dir,
            &UserKeypair::generate(),
        )
        .await
        .expect("activate peer");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind owner");
    owner.pull_store().await.expect("observe peer activation");
    for (database, device, id) in [
        (&source, &owner, "pending-row"),
        (&target, &peer, "accepted-peer-row"),
    ] {
        database
            .execute_test_host_write(&format!(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('{id}', '{id}', 1, '0000000002000-0000-writer', '2026-01-01')"
            ))
            .await;
        let mut writer = device.authorize_writer().await.expect("authorize writer");
        assert!(writer
            .prepare_pending_store_write()
            .await
            .expect("prepare row"));
    }
    let database = StoreDatabase::new(&source);
    let pending = database
        .oldest_prepared_store_write()
        .await
        .expect("load prepared row")
        .expect("row has a publication attempt");
    let reservation = database
        .active_store_publication()
        .await
        .expect("read reservation")
        .expect("row owns the publication reservation");
    let context = ProtocolObjectContext::signed_plaintext(
        store.root().store_root_hash,
        ProtocolObjectDomain::StorePublicationEntry,
    );
    let superseded = pending
        .publication
        .reference()
        .expect("original entry reference");
    let superseded_prefix = store_publication_entry_semantic_prefix(&pending.publication.entry);
    storage
        .create_verified_protocol_object(
            &context,
            &pending
                .publication
                .prepared_entry()
                .expect("original prepared entry"),
            &superseded_prefix,
            &pending.publication.entry.to_bytes(),
        )
        .await
        .expect("upload the original owned entry before losing its publication position");
    let mut writer = peer.authorize_writer().await.expect("authorize peer");
    assert_eq!(
        writer.drain_store_writes().await.expect("publish peer row"),
        1
    );
    drop(writer);
    let winner = StoreDatabase::new(&target)
        .store_current_publication()
        .await
        .expect("read the peer's accepted boundary");
    let commit = &pending.commit.value;
    let device_signer = commit
        .author()
        .device_signer(&signer)
        .expect("device signer");
    let entry = StorePublicationEntry::signed_commit(winner.record(), commit, &device_signer)
        .expect("sign replacement envelope without changing the captured commit");
    let prefix = store_publication_entry_semantic_prefix(&entry);
    let slot = storage
        .allocate_protocol_slot(&context, &prefix, ".json")
        .await
        .expect("allocate replacement object");
    let prepared = storage
        .prepare_protocol_object(&context, slot, &prefix, entry.to_bytes())
        .expect("prepare replacement object before upload");
    let reference = StorePublicationRef::from_entry(&entry, prepared.reference().clone())
        .expect("reference replacement envelope");
    let replacement = StoreCurrentPublicationRecord::advance_commit(
        winner.record(),
        &entry,
        reference,
        commit,
        &device_signer,
    )
    .expect("sign replacement current record");
    let attempt = PreparedStorePublication {
        previous: winner.record().clone(),
        previous_version: winner
            .require_observed()
            .expect("accepted provider observation")
            .version()
            .clone(),
        entry,
        entry_object: prepared.reference().clone(),
        replacement,
    };
    let replacement = reservation
        .replace_attempt(attempt.clone())
        .expect("preserve reservation");
    let error = database
        .replace_active_store_commit_publication(
            commit.clone(),
            reservation.clone(),
            replacement.clone(),
        )
        .await
        .expect_err("a peer's observation cannot substitute for installing its accepted work");
    assert!(error.to_string().contains("installed"), "{error}");
    assert_eq!(
        database
            .active_store_publication()
            .await
            .expect("preserved reservation"),
        Some(reservation.clone())
    );

    owner
        .pull_store()
        .await
        .expect("install the peer's accepted work");
    let mut invalid_transition = attempt.clone();
    invalid_transition.replacement = winner.record().clone();
    let invalid_replacement = reservation
        .replace_attempt(invalid_transition)
        .expect("same reservation with an invalid signed transition");
    database
        .replace_active_store_commit_publication(
            commit.clone(),
            reservation.clone(),
            invalid_replacement,
        )
        .await
        .expect_err("invalid signed transition cannot replace the durable attempt");
    assert_eq!(
        database
            .active_store_publication()
            .await
            .expect("read after rejected transition"),
        Some(reservation.clone())
    );
    database
        .replace_active_store_commit_publication(
            commit.clone(),
            reservation.clone(),
            replacement.clone(),
        )
        .await
        .expect("replace envelope against installed work");
    let reopened = database
        .oldest_prepared_store_write()
        .await
        .expect("reopen pending row")
        .expect("row remains prepared");
    assert_eq!(reopened.commit.bytes, pending.commit.bytes);
    assert_eq!(reopened.publication, attempt);
    let retained = database
        .active_store_publication()
        .await
        .expect("read replacement")
        .expect("replacement still owns the original write");
    assert_eq!(retained.attempt().expect("prepared publication"), &attempt);
    assert_eq!(
        retained
            .attempt()
            .expect("prepared publication")
            .prepared_entry()
            .expect("durable upload bytes"),
        prepared
    );
    assert!(retained.same_commit_reservation(&reservation));
    assert_eq!(retained.superseded_entry(), Some(&superseded));
    assert!(database
        .replace_active_store_commit_publication(commit.clone(), retained.clone(), replacement)
        .await
        .expect_err("cleanup must finish before replacing another attempt")
        .to_string()
        .contains("cleanup"));
    store.fail_nth_exact_delete_of(&[superseded.object.slot()], 1);
    let mut writer = owner.authorize_writer().await.expect("reopen publisher");
    let error = writer
        .drain_store_writes()
        .await
        .expect_err("failed cleanup must fail this attempt");
    assert!(
        error.to_string().contains("forced exact delete failure"),
        "{error}"
    );
    drop(writer);
    assert_eq!(
        database
            .active_store_publication()
            .await
            .expect("cleanup ownership survives failure"),
        Some(retained)
    );
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("failed cleanup cannot accept replacement"),
        winner
    );
    assert_eq!(
        storage
            .read_protocol_object(&context, &superseded.object, &superseded_prefix)
            .await
            .expect("losing entry remains owned until deletion succeeds"),
        pending.publication.entry.to_bytes()
    );
    let mut writer = owner
        .authorize_writer()
        .await
        .expect("retry interrupted cleanup");
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("publish the replacement attempt"),
        1
    );
    drop(writer);
    assert!(database
        .active_store_publication()
        .await
        .expect("completed publication")
        .is_none());
    let (_, accepted_entries) = database
        .retained_store_publication()
        .await
        .expect("accepted history");
    assert_eq!(
        accepted_entries
            .iter()
            .filter(|entry| entry.value.payload == attempt.entry.payload)
            .count(),
        1
    );
    let removed = storage
        .read_protocol_object(&context, &superseded.object, &superseded_prefix)
        .await
        .expect_err("the settled losing entry must be deleted");
    assert!(
        matches!(removed, coven_protocol::objects::StorageError::NotFound(_)),
        "{removed}"
    );
    assert_eq!(
        storage
            .read_protocol_object(&context, &attempt.entry_object, &prefix)
            .await
            .expect("accepted replacement entry stays available"),
        attempt.entry.to_bytes()
    );
    assert_eq!(
        source
            .query_test_text("SELECT title FROM notes WHERE id = 'pending-row'")
            .await,
        "pending-row"
    );
    assert_eq!(
        source
            .query_test_text("SELECT title FROM notes WHERE id = 'accepted-peer-row'")
            .await,
        "accepted-peer-row"
    );
}
