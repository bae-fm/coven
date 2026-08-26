use super::*;

/// Publishing a write's prepared objects overlaps them, up to the transfer
/// limit, and still finishes all of them before the commit that names them.
///
/// Live, a twenty-two file Move-to-Cloud spent 8977 ms of an 11027 ms
/// publication here, one provider round trip after another. Each package is
/// independent — its own bytes, its own create, its own durable mark — so the
/// only thing that has to hold is the barrier at the end.
#[tokio::test]
async fn publication_overlaps_prepared_packages_but_not_the_commit() {
    let fixture = UploadFixture::scoped(4).await;
    fixture.write_two_audiences("overlap").await;
    // Holding each create open is what makes the overlap observable. The chunk
    // is larger than any package here, so each create sleeps exactly once
    // instead of once per chunk — a chunk of one byte holds a package open for
    // minutes.
    fixture
        .home
        .slow_creates(1 << 20, std::time::Duration::from_millis(20));
    fixture.home.reset_observations();
    fixture.home.inner.clear_exact_creates();

    assert!(
        fixture
            .device
            .prepare_pending_store_write()
            .await
            .expect("prepare the scoped Store write"),
        "the two-audience write is ready to publish",
    );
    assert_eq!(
        fixture
            .device
            .drain_store_writes()
            .await
            .expect("publish the scoped Store write"),
        1,
    );

    let created = fixture
        .home
        .inner
        .exact_creates()
        .into_iter()
        .map(|slot| slot.logical_key().to_string())
        .collect::<Vec<_>>();
    let packages = created
        .iter()
        .filter(|key| key.contains("/packages/"))
        .count();
    assert!(
        packages > 1,
        "the write did not publish more than one package: {created:?}",
    );
    assert_eq!(
        fixture.home.max_inflight(),
        2,
        "publication issued its package creates one at a time",
    );
    let last_package = created
        .iter()
        .rposition(|key| key.contains("/packages/"))
        .expect("the write published its packages");
    let commit = created
        .iter()
        .position(|key| key.contains("/commits/"))
        .expect("the write published its commit");
    assert!(
        last_package < commit,
        "the commit was created before a package it names: {created:?}",
    );
}

/// The limit is the ceiling, not a target: one at a time stays one at a time.
#[tokio::test]
async fn publication_respects_a_transfer_limit_of_one() {
    let fixture = UploadFixture::scoped(1).await;
    fixture.write_two_audiences("serial").await;
    fixture
        .home
        .slow_creates(1 << 20, std::time::Duration::from_millis(20));
    fixture.home.reset_observations();

    fixture
        .device
        .prepare_pending_store_write()
        .await
        .expect("prepare the scoped Store write");
    fixture
        .device
        .drain_store_writes()
        .await
        .expect("publish the scoped Store write");

    assert_eq!(
        fixture.home.max_inflight(),
        1,
        "a limit of one still publishes one object at a time",
    );
}

/// A blob whose ownership flips to `RetirementPending` — its last pending
/// candidate lost — is a blob nothing will ever upload, and publication has to
/// refuse the write loudly rather than skip it.
///
/// This state is reachable on a write still being drained. Publication reads
/// each record live through `reopen_remote_object_on`, not from a snapshot
/// taken when the write was prepared, and the nonactivation machinery retires
/// ownership the moment a candidate loses a merge race, is abandoned, or has
/// its author excluded — here driven through the same
/// `begin_remote_candidate_nonactivation_on` those paths call. Skipping the
/// blob would publish a commit naming bytes nobody put at the provider; going
/// to the provider to check would be the round trip this path exists to avoid.
#[tokio::test]
async fn publication_refuses_a_blob_whose_candidate_ownership_was_retired() {
    let fixture = UploadFixture::new(4).await;
    fixture.seed_uploads(1).await;
    fixture.drain(&fixed_clock(T0), None).await.unwrap();
    assert!(
        fixture
            .device
            .prepare_pending_store_write()
            .await
            .expect("prepare the Store write"),
        "the seeded blob produces a Store write to publish",
    );

    let prepared = fixture
        .database
        .oldest_prepared_store_write()
        .await
        .expect("load the prepared write")
        .expect("the prepared write exists");
    let write_id = prepared.commit.value.value().write_id.clone();
    let candidate = prepared.commit.value.reference().clone();
    let candidate_bytes = prepared.commit.bytes.clone();
    let blob = fixture
        .database
        .prepared_remote_objects(&write_id)
        .await
        .expect("load the prepared remote objects")
        .into_iter()
        .find(|prepared| {
            matches!(
                prepared.closed.payloads(),
                coven_protocol::remote_object::RemoteObjectPayloads::RowBlob { .. }
            )
        })
        .expect("the write names a prepared blob");
    assert!(
        blob.closed.record().records_verified_upload(),
        "the write's blob starts out as one this device uploaded and verified",
    );
    let blob_object = blob.closed.object().clone();
    let blob_key = blob_object.slot().logical_key().to_string();

    fixture
        .database
        .begin_remote_candidate_nonactivation_for_test(
            coven_protocol::remote_object::remote_object_id(&blob_object),
            losing_candidate_nonactivation(&candidate, candidate_bytes),
        )
        .await
        .expect("the losing candidate retires the blob's ownership");
    fixture.home.inner.clear_exact_reads();
    fixture.home.reset_observations();

    let error = fixture
        .device
        .drain_store_writes()
        .await
        .expect_err("publication refuses the retired blob");
    assert!(
        error
            .to_string()
            .contains("no durable record of its upload"),
        "publication failed for another reason: {error}",
    );

    let created = fixture.home.keys();
    let touched = fixture
        .home
        .exact_reads()
        .into_iter()
        .map(|slot| slot.logical_key().to_string())
        .chain(created.iter().cloned())
        .collect::<Vec<_>>();
    assert!(
        !touched.contains(&blob_key),
        "publication went to the provider for the retired blob: {touched:?}",
    );
    // The refusal lands before the barrier, so the commit that would have named
    // the retired blob never reaches the provider. Without the guard the write
    // gets that far and only trips over the blob at activation, with the commit
    // already published.
    assert!(
        !created.iter().any(|key| key.contains("/commits/")),
        "publication created the commit naming the retired blob: {created:?}",
    );
}

/// The receipt a candidate's loss carries: another head won the position this
/// candidate wanted.
fn losing_candidate_nonactivation(
    candidate: &coven_protocol::store_commit::StoreBatchCommitRef,
    candidate_bytes: Vec<u8>,
) -> coven_protocol::remote_object::CandidateNonactivation {
    let winner_bytes = b"the head that won this position";
    let winner_object = coven_protocol::objects::ExactObjectRef::new(
        coven_protocol::objects::ObjectSlot::logical(
            "store-v1/heads/retired-blob-winner.json".to_string(),
        )
        .expect("construct the winning head slot"),
        winner_bytes.len() as u64,
        coven_protocol::store_commit::ObjectHash::digest(winner_bytes),
    );
    coven_protocol::remote_object::CandidateNonactivation::unverified_for_test(
        coven_protocol::store_commit::StoreBatchCommitDeletionTarget {
            coord: candidate.coord.clone(),
            object: candidate.object.clone(),
            canonical_signed_bytes: candidate_bytes,
        },
        coven_protocol::remote_object::CandidateNonactivationProof::MergeWinner {
            winner_head: coven_protocol::store_commit::StoreDeviceHeadRef {
                head_hash: coven_protocol::store_commit::ObjectHash::digest(winner_bytes),
                object: winner_object,
            },
        },
    )
}

/// A write publishes its package before the commit that names it, and never
/// reads back the blobs the upload queue already put at the provider.
///
/// The upload hashed each file locally and the provider settled the create, so
/// reading those bytes home again proves nothing about them. Live, a write that
/// created one 74 KB package spent 14990 ms in this stage re-downloading the
/// thirteen blobs it referenced — hundreds of megabytes, every time.
#[tokio::test]
async fn publication_creates_the_package_before_the_commit_and_reads_no_blob() {
    let fixture = UploadFixture::new(4).await;
    fixture.seed_uploads(6).await;
    fixture.drain(&fixed_clock(T0), None).await.unwrap();

    let uploaded = fixture.home.keys();
    assert_eq!(
        uploaded.len(),
        6,
        "the seeded blobs were uploaded: {uploaded:?}"
    );
    fixture.home.inner.clear_exact_creates();
    fixture.home.inner.clear_exact_reads();

    assert!(
        fixture
            .device
            .prepare_pending_store_write()
            .await
            .expect("prepare the Store write"),
        "the seeded blobs produce a Store write to publish",
    );
    assert_eq!(
        fixture
            .device
            .drain_store_writes()
            .await
            .expect("publish the Store write"),
        1,
    );

    let read = fixture
        .home
        .exact_reads()
        .into_iter()
        .map(|slot| slot.logical_key().to_string())
        .collect::<Vec<_>>();
    let reread = uploaded
        .iter()
        .filter(|key| read.contains(key))
        .collect::<Vec<_>>();
    assert!(
        reread.is_empty(),
        "publication read back blobs this device uploaded: {reread:?}",
    );

    let created = fixture
        .home
        .inner
        .exact_creates()
        .into_iter()
        .map(|slot| slot.logical_key().to_string())
        .collect::<Vec<_>>();
    let package = created
        .iter()
        .position(|key| key.contains("/packages/"))
        .expect("the write published its Store package");
    let commit = created
        .iter()
        .position(|key| key.contains("/commits/"))
        .expect("the write published its commit");
    assert!(
        package < commit,
        "the commit was created before the package it names: {created:?}",
    );
}
