use super::*;

#[tokio::test]
async fn reader_refuses_a_head_that_regresses_below_its_cursor() {
    let fixture = MergeFixture::new("cursor-regression").await;
    let member = UserKeypair::generate();
    fixture.admit_member(&member, MemberRole::Member).await;
    fixture.remove_member(&member).await;
    let chain = fixture.load().await;
    let latest = chain.head_refs().last().expect("latest head").clone();
    let latest_head = fixture
        .device
        .load_membership_head_for_test(&latest)
        .await
        .expect("load latest head");
    let predecessor = latest_head
        .body
        .predecessor_head()
        .cloned()
        .expect("remove predecessor");
    fixture
        .storage
        .delete_protocol_object(&latest.object)
        .await
        .expect("remove latest exact head");

    let error = fixture
        .load_result()
        .await
        .expect_err("the accepted cursor cannot regress to its predecessor");
    assert_eq!(
        fixture
            .database
            .membership_head_cursors()
            .await
            .expect("read preserved exact cursors")
            .head_refs,
        chain.head_refs(),
        "a missing accepted head cannot move the durable cursor",
    );
    let crate::sync::store::StoreError::SyncCycle(failure) = &error else {
        panic!("unexpected cursor refusal: {error:?}");
    };
    let cause = std::error::Error::source(failure.as_ref())
        .and_then(|cause| cause.downcast_ref::<crate::sync::cycle::SyncCycleCause>());
    assert!(
        matches!(cause,
            Some(crate::sync::cycle::SyncCycleCause::Pull(
                crate::sync::store::StorePullError::Object(
                    coven_protocol::objects::StoreObjectError::Storage(
                        coven_protocol::objects::StorageError::NotFound(key)
                    )
                )
            )) if key == latest.object.slot().logical_key()
        ),
        "{error:?}"
    );
    assert!(predecessor.coord.seq < latest.coord.seq);
}

#[tokio::test]
async fn membership_projection_handles_a_deep_valid_predecessor_path_iteratively() {
    let fixture = MergeFixture::new("deep-membership-projection").await;
    let chain = fixture.load().await;
    fixture
        .device
        .assert_deep_membership_projection_for_test(chain.head_refs())
        .await
        .expect("project deep membership path");
}

/// Signed entries, heads and commits are rebuilt from their canonical values.
/// Only the sealed wrapped keys need their exact stored payload carried beside
/// the reference; serializing them again would produce different ciphertext.
#[tokio::test]
async fn the_membership_mutation_journal_carries_no_object_it_already_names() {
    let fixture = MergeFixture::new("mutation-journal-names-its-objects").await;
    let member = UserKeypair::generate();
    fixture.admit_member(&member, MemberRole::Member).await;
    let member_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let member_db = crate::sync::test_helpers::open_test_db(member_db_store_dir.clone());
    fixture
        .store
        .activate_joined_device(
            &fixture.db,
            fixture.store_dir.clone(),
            &member_db,
            member_db_store_dir.clone(),
            &member,
            "2026-07-21T00:00:00Z",
        )
        .await
        .expect("activate the member's device");

    // Stop the removal before it publishes, leaving its plan durable to read.
    fixture.home.fail_exact_create_before_call(1);
    Box::pin(fixture.try_remove_member(&member))
        .await
        .expect_err("the interrupted removal cannot publish its membership authority");
    let staged = fixture
        .database
        .outbound_membership_mutation()
        .await
        .expect("read the staged removal")
        .expect("the interrupted removal stays durable");
    let plan = String::from_utf8(staged.plan_bytes).expect("the plan is JSON");
    let envelope: serde_json::Value = serde_json::from_str(&plan).unwrap();
    let candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit =
        serde_json::from_value(envelope["plan"]["publication"]["candidate"].clone()).unwrap();
    candidate.validate_closed_shape().unwrap();
    candidate
        .prepared_commit()
        .expect("rebuild the exact commit");
    candidate
        .publication
        .prepared_entry()
        .expect("the Store publication entry_object is a reference, not carried bytes");
    let membership = candidate.prepared_membership_publication().unwrap();
    membership
        .prepared_entry()
        .expect("rebuild the membership entry");
    membership
        .prepared_head()
        .expect("rebuild the membership head");

    for carried in ["head_object", "resolution_object", "prepared_head"] {
        assert!(
            !plan.contains(carried),
            "the journal carries {carried}, whose bytes its own reference already names"
        );
    }
    // One replacement wrapped key, for the one member who remains, and its
    // sealed keyring is the only value in the plan without a sibling field the
    // upload could rebuild it from.
    assert_eq!(
        plan.matches("stored_bytes").count(),
        1,
        "the journal's only carried payload is the replacement wrapped key"
    );
}

#[tokio::test]
async fn host_writes_and_member_removal_keep_their_reserved_publication_positions() {
    let fixture = MergeFixture::new("membership-and-host-reservations").await;
    let encryption = EncryptionService::from_key([42; 32]);
    let member = UserKeypair::generate();
    fixture.admit_member(&member, MemberRole::Member).await;
    let member_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let member_db = crate::sync::test_helpers::open_test_db(member_db_store_dir.clone());
    fixture
        .store
        .activate_joined_device(
            &fixture.db,
            fixture.store_dir.clone(),
            &member_db,
            member_db_store_dir.clone(),
            &member,
            "2026-07-21T00:00:00Z",
        )
        .await
        .expect("activate the member's device");
    Box::pin(fixture.store.promote_active_member_fixture(
        &fixture.db,
        fixture.store_dir.clone(),
        &member_db,
        member_db_store_dir.clone(),
        &fixture.owner,
        &member,
        &encryption,
    ))
    .await
    .expect("promote the member to Owner");

    // Preparing the host write reserves its author position through publication.
    fixture
        .db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('contended-note', 'contended', NULL, 1, \
                 '0000000001000-0000-owner', '2026-07-21')",
        )
        .await;
    let loaded_store = fixture
        .store
        .bind_device(&fixture.db, fixture.store_dir.clone(), &fixture.owner)
        .await
        .expect("load owner Store");
    let mut writer = loaded_store
        .authorize_writer()
        .await
        .expect("authorize owner writer");
    assert!(Box::pin(writer.prepare_pending_store_write())
        .await
        .expect("queue a host write at the contended position"));

    let host_reservation = fixture
        .database
        .active_store_publication()
        .await
        .unwrap()
        .unwrap();
    let access_before = fixture.home.access_requests();
    let blocked = Box::pin(fixture.try_remove_member(&member))
        .await
        .expect_err("a removal cannot take the prepared host write's reservation");
    assert!(
        blocked
            .to_string()
            .contains("another local Store operation owns publication"),
        "expected reservation refusal, got {blocked}"
    );
    assert!(fixture
        .database
        .outbound_membership_mutation()
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        fixture
            .database
            .active_store_publication()
            .await
            .unwrap()
            .as_ref(),
        Some(&host_reservation)
    );
    assert_eq!(fixture.home.access_requests(), access_before);
    assert_eq!(Box::pin(writer.drain_store_writes()).await.unwrap(), 1);
    assert!(fixture
        .database
        .active_store_publication()
        .await
        .unwrap()
        .is_none());

    // An interrupted removal has the same protection against a later host write.
    fixture.home.fail_exact_create_before_call(1);
    let interrupted = Box::pin(fixture.try_remove_member(&member))
        .await
        .expect_err("interrupt the staged removal's authority upload");
    assert!(
        interrupted
            .to_string()
            .contains("forced failure before exact create call"),
        "{interrupted}"
    );
    let staged = fixture
        .database
        .outbound_membership_mutation()
        .await
        .unwrap()
        .unwrap();
    let removal_reservation = fixture
        .database
        .active_store_publication()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        removal_reservation.owner(),
        &coven_database::ActiveStorePublicationOwner::MembershipMutation
    );
    fixture
        .db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('later-note', 'later', NULL, 1, '0000000002000-0000-owner', '2026-07-21')",
        )
        .await;
    assert!(!Box::pin(writer.prepare_pending_store_write())
        .await
        .unwrap());
    assert_eq!(
        fixture
            .database
            .active_store_publication()
            .await
            .unwrap()
            .as_ref(),
        Some(&removal_reservation)
    );
    let retained = fixture
        .database
        .outbound_membership_mutation()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.intent_hash, staged.intent_hash);
    assert_eq!(retained.plan_bytes, staged.plan_bytes);
    assert_eq!(retained.progress_bytes, staged.progress_bytes);

    Box::pin(fixture.try_remove_member(&member))
        .await
        .expect("resume the removal at its reserved position");
    assert!(!fixture.load().await.can_write_now(&pubkey_hex(&member)));
    assert!(fixture
        .database
        .outbound_membership_mutation()
        .await
        .unwrap()
        .is_none());
    assert!(fixture
        .database
        .active_store_publication()
        .await
        .unwrap()
        .is_none());
    let entries = fixture.database.store_publication_entries().await.unwrap();
    for reservation in [&host_reservation, &removal_reservation] {
        let payload = &reservation.attempt().unwrap().entry.payload;
        assert_eq!(
            entries
                .iter()
                .filter(|entry| &entry.value.payload == payload)
                .count(),
            1,
            "each reserved candidate is accepted exactly once"
        );
    }
    let mut writer = loaded_store.authorize_writer().await.unwrap();
    assert!(Box::pin(writer.prepare_pending_store_write())
        .await
        .unwrap());
    assert_eq!(Box::pin(writer.drain_store_writes()).await.unwrap(), 1);
    assert!(fixture
        .database
        .active_store_publication()
        .await
        .unwrap()
        .is_none());
}

/// A membership stream is a hash-linked list, so its heads have to be verified
/// in order — but they do not have to be *fetched* in order. Every head's slot
/// is named by its coordinate, so the whole stream shares one provider prefix,
/// and a reader that lists it fetches the stream at once instead of spending a
/// round trip per head purely to learn where the next one lives.
#[tokio::test]
async fn a_membership_stream_is_listed_once_and_fetched_together() {
    let fixture = MergeFixture::new("membership-list-then-fetch").await;
    let mut admission = fixture
        .admit_member(&UserKeypair::generate(), MemberRole::Member)
        .await;
    for _ in 0..3 {
        admission = fixture
            .admit_member(&UserKeypair::generate(), MemberRole::Member)
            .await;
    }
    let expected = fixture.load().await;

    fixture.home.clear_exact_reads();
    fixture.home.clear_exact_listings();
    fixture
        .home
        .delay_exact_full_reads(std::time::Duration::from_millis(20));
    let history = crate::sync::store::HistoryConstructionAuthority::admission()
        .open_pinned(&*fixture.storage, &admission.store_root)
        .await
        .expect("open admission history");
    let walked = history
        .load_accepted_anchored_membership(
            &admission.membership_floor.0,
            Some(&admission.owner_pubkey),
        )
        .await
        .expect("walk membership from the cloud");

    assert_eq!(walked.head_refs(), expected.head_refs());
    let founder = expected
        .head_refs()
        .first()
        .expect("founder membership head")
        .clone();
    assert_eq!(
        fixture.home.exact_listed_prefixes(),
        vec![coven_protocol::store_commit::membership_head_stream_prefix(
            &founder.coord.author_pubkey,
            &founder.coord.author_owner_grant,
            founder.coord.stream_id,
        )],
        "one listing per membership stream, naming only that stream's prefix"
    );
    let head_reads = fixture
        .home
        .exact_reads()
        .into_iter()
        .filter(|slot| slot.logical_key().starts_with("store-v1/membership/heads/"))
        .collect::<Vec<_>>();
    let distinct = head_reads
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let stream_length = founder.coord.seq as usize;
    assert!(
        distinct > stream_length,
        "every head in the stream is read, plus the absent slot that ends it; \
         got {distinct} distinct slots for a stream of {stream_length}"
    );
    // One anchored-chain load walks the founder's stream several times. The
    // heads it fetched the first time serve every later walk, so what repeats
    // is the read of the one absent slot each walk ends on.
    assert!(
        head_reads.len() < 2 * distinct,
        "a repeated walk re-reads no head it already fetched; \
         got {} reads over {distinct} slots",
        head_reads.len()
    );
    assert!(
        fixture.home.exact_full_read_max_inflight() > 1,
        "membership heads are fetched together, not each one gated on the last"
    );
}

/// A published membership rollup carries the chain, so a reader takes it in one
/// read — and reaches exactly the chain the full walk reaches.
///
/// This is the property the whole rollup rests on: it is a carrier, not an
/// authority. The chain it produces is compared against one walked entirely off
/// the provider over the same Store, and the Store is built so the comparison
/// has something to say — a member admitted and then removed, which rotates the
/// wrapped keys and retires that member's grant, plus a membership change
/// published *after* the snapshot so the rollup is deliberately stale and the
/// reader has a tail to walk.
#[tokio::test]
async fn a_membership_rollup_reaches_the_chain_the_full_walk_reaches() {
    let fixture = MergeFixture::new("membership-rollup-equivalence").await;
    let removed = UserKeypair::generate();
    let kept = UserKeypair::generate();
    fixture.admit_member(&removed, MemberRole::Member).await;
    fixture.admit_member(&kept, MemberRole::Member).await;
    fixture.remove_member(&removed).await;
    fixture
        .device
        .ensure_device_join_snapshot_for_test()
        .await
        .expect("publish the snapshot the rollup rides");
    // Published after the snapshot: the rollup cannot cover this, so the reader
    // that adopts it still has to walk the tail to find it.
    let after_snapshot = UserKeypair::generate();
    let admission = fixture
        .admit_member(&after_snapshot, MemberRole::Member)
        .await;

    let walked = walk_admission_membership(&fixture, &admission, false).await;
    let rolled = walk_admission_membership(&fixture, &admission, true).await;

    assert_eq!(
        walked.head_refs(),
        rolled.head_refs(),
        "the rollup reader ends on another membership frontier"
    );
    assert_eq!(
        walked.resolution_refs(),
        rolled.resolution_refs(),
        "the rollup reader ends on another resolution cut"
    );
    assert_eq!(
        walked.status(),
        rolled.status(),
        "the rollup reader resolves another member set"
    );
    for (label, pubkey) in [
        ("the owner", fixture.owner_pubkey.clone()),
        ("the removed member", pubkey_hex(&removed)),
        ("the kept member", pubkey_hex(&kept)),
        (
            "the member admitted after the snapshot",
            pubkey_hex(&after_snapshot),
        ),
    ] {
        assert_eq!(
            walked.can_write_now(&pubkey),
            rolled.can_write_now(&pubkey),
            "the rollup reader disagrees about whether {label} can write"
        );
        assert_eq!(
            walked.is_owner_now(&pubkey),
            rolled.is_owner_now(&pubkey),
            "the rollup reader disagrees about whether {label} is an owner"
        );
    }
    assert!(
        !walked.can_write_now(&pubkey_hex(&removed)),
        "the fixture never removed anyone, so this proves nothing about revocation"
    );
    assert!(
        walked.can_write_now(&pubkey_hex(&after_snapshot)),
        "the member admitted after the snapshot is absent from both chains, \
         so the tail walk is untested"
    );
}

/// The reads a joining device spends on membership do not grow with the Store's
/// membership history.
///
/// Every membership change used to cost a joining device two provider round
/// trips — the head, then the entry it selects — back to the founding entry, on
/// a chain nothing had touched in months. The rollup makes that one read
/// whatever the history is; what is left is the probe that finds each stream's
/// end, which is about the tail and not about the past.
#[tokio::test]
async fn membership_reads_on_a_fresh_reader_do_not_grow_with_the_chain() {
    let shallow = membership_reads_after_admissions("rollup-reads-shallow", 1).await;
    let deep = membership_reads_after_admissions("rollup-reads-deep", 9).await;

    assert_eq!(
        shallow, deep,
        "a reader of a nine-change chain spent {deep} membership operations \
         against {shallow} for a one-change chain, so the walk still follows history",
    );
    // Listing the snapshot prefix, the newest snapshot's metadata, the rollup,
    // the newest covered head the rollup deliberately does not hold, and the
    // absent slot that ends the stream. Written as a number rather than a bound
    // because a budget nobody wrote down is one nobody notices doubling.
    assert_eq!(
        deep, 5,
        "a fresh reader spends {deep} membership operations, not the read and a \
         probe per stream the rollup exists to make it",
    );
}

/// Provider operations one fresh reader spends reaching a Store's membership,
/// over a chain of `admissions` changes with a snapshot published at the end.
///
/// Counted are the reads and listings under the membership, rollup, and
/// snapshot-metadata prefixes: everything the reader spends deciding what the
/// membership is. The snapshot *image* is not among them — a joining device
/// downloads one of those on purpose, and it is the one large transfer the
/// round-trip budget allows.
async fn membership_reads_after_admissions(store_id: &str, admissions: usize) -> usize {
    let fixture = MergeFixture::new(store_id).await;
    let mut admission = fixture
        .admit_member(&UserKeypair::generate(), MemberRole::Member)
        .await;
    for _ in 1..admissions {
        admission = fixture
            .admit_member(&UserKeypair::generate(), MemberRole::Member)
            .await;
    }
    fixture
        .device
        .ensure_device_join_snapshot_for_test()
        .await
        .expect("publish the snapshot the rollup rides");

    // Read the owner's own frontier before counting: resolving it walks the
    // chain on a verifier of its own, and those reads are not the joiner's.
    let expected = fixture.load().await;
    fixture.home.clear_exact_reads();
    fixture.home.clear_exact_listings();
    let membership = walk_admission_membership(&fixture, &admission, true).await;
    assert_eq!(
        membership.head_refs(),
        expected.head_refs(),
        "the counted read ended on another frontier than the owner's own"
    );
    // The rollup holds every covered head but the newest, so the newest is read
    // from its create-once slot. That read is what pins the whole covered
    // prefix: it names its predecessor, which names its own, down to the
    // sequence-one slot the signed Store root names. Without it a rollup could
    // hand a reader one branch of a forked author stream while the provider
    // holds the other.
    let tip = expected
        .head_refs()
        .last()
        .expect("the chain has a newest head")
        .object
        .slot()
        .logical_key()
        .to_string();
    assert!(
        fixture
            .home
            .exact_reads()
            .iter()
            .any(|slot| slot.logical_key() == tip),
        "the newest covered membership head was not read from its own slot: {tip}"
    );

    let counted = |key: &str| {
        key.starts_with("store-v1/membership/")
            || key.starts_with("store-v1/membership-rollups/")
            || key.starts_with("store-v1/snapshots/")
    };
    fixture
        .home
        .exact_reads()
        .iter()
        .filter(|slot| counted(slot.logical_key()))
        .count()
        + fixture
            .home
            .exact_listed_prefixes()
            .iter()
            .filter(|prefix| counted(prefix))
            .count()
}
