//! Resolving a Circle control conflict by choosing one branch.

use std::collections::BTreeSet;

use super::conflict_fixture::{routing, ConflictFixture};
use super::*;
use coven_database::Database;
use coven_protocol::circle::{CircleInfo, CircleRole};

#[tokio::test]
async fn concurrent_successors_retain_and_surface_as_a_conflict() {
    let fixture = ConflictFixture::build("resolve-surface").await;
    let _ = fixture.fork().await;

    assert_eq!(
        fixture.circles_device1().await,
        vec![CircleInfo::Conflicted {
            id: fixture.circle_id(),
            branches: fixture.conflict_branches_device1().await,
        }]
    );

    // Authoring refuses on a conflicted Circle: there is no single resolved
    // control to succeed.
    let rename = fixture
        .store1()
        .await
        .circles()
        .rename_circle("0000000001300-0000-device1", fixture.circle_id(), "Gamma")
        .await;
    assert!(rename.is_err(), "conflicted Circle refuses authoring");

    // Package publication refuses too: no active publication key.
    let package = fixture
        .publication_context_device1(fixture.conflict_branches_device1().await[0].clone())
        .await;
    assert!(
        package.is_err(),
        "conflicted Circle refuses package publication"
    );
}

#[tokio::test]
async fn resolution_collapses_the_conflict_on_every_device() {
    let fixture = ConflictFixture::build("resolve-collapse").await;
    let (chosen, _losing) = fixture.fork().await;

    fixture
        .store1()
        .await
        .circles()
        .resolve_circle_control(fixture.circle_id(), chosen.clone())
        .await
        .expect("resolve the control conflict");

    let device1 = fixture.circles_device1().await;
    assert!(
        matches!(device1.as_slice(), [CircleInfo::Active { id, .. }] if *id == fixture.circle_id()),
        "resolution collapses the conflict on the resolving device: {device1:?}"
    );

    // Authoring resumes under the resolution control.
    fixture
        .store1()
        .await
        .circles()
        .rename_circle("0000000001400-0000-device1", fixture.circle_id(), "Resumed")
        .await
        .expect("authoring resumes after resolution");

    // Device 2, which retained the other branch, collapses to the same
    // resolution after pulling it — the opposite arrival order.
    fixture.pull_device2().await;
    let device2 = fixture.circles_device2().await;
    assert!(
        matches!(device2.as_slice(), [CircleInfo::Active { id, .. }] if *id == fixture.circle_id()),
        "resolution collapses the conflict on the other device: {device2:?}"
    );
}

#[tokio::test]
async fn resolving_to_another_devices_branch_merges_head_frontiers() {
    let fixture = ConflictFixture::build("resolve-frontier-merge").await;
    // `fork` renames on each device; device 2's stamp sorts after device 1's, so
    // device 2's metadata ("Beta") is the deterministic canonical selection.
    let (device1_branch, device2_branch) = fixture.fork().await;

    // Device 1 resolves the conflict to the branch device 2 authored. The
    // resolution inherits the chosen branch's state — its name stays "Beta" — but
    // must cover every branch's metadata head, not only the chosen branch's.
    fixture
        .store1()
        .await
        .circles()
        .resolve_circle_control(fixture.circle_id(), device2_branch.clone())
        .await
        .expect("resolve the control conflict to device 2's branch");
    assert_eq!(
        fixture.circles_device1().await,
        vec![CircleInfo::Active {
            id: fixture.circle_id(),
            name: "Beta".to_string(),
            role: CircleRole::Owner,
            rotation_required: false,
        }],
        "the resolution inherits the chosen branch's name verbatim"
    );

    // The losing branch (device 1's own) advanced device 1's metadata stream.
    // Authoring again on device 1 must continue that stream from the head the
    // losing branch left — the resolution covers it — rather than re-deriving a
    // sequence whose head slot the losing branch already created.
    let _ = &device1_branch;
    fixture
        .store1()
        .await
        .circles()
        .rename_circle("0000000001500-0000-device1", fixture.circle_id(), "Gamma")
        .await
        .expect("device 1 authoring resumes without a metadata head-slot collision");
    assert_eq!(
        fixture.circles_device1().await,
        vec![CircleInfo::Active {
            id: fixture.circle_id(),
            name: "Gamma".to_string(),
            role: CircleRole::Owner,
            rotation_required: false,
        }],
        "the resumed rename takes effect over the merged metadata frontier"
    );
}

#[tokio::test]
async fn stale_resolution_is_refused_and_a_late_branch_resurfaces_the_conflict() {
    let fixture = ConflictFixture::build("resolve-stale").await;
    let (chosen, _losing) = fixture.fork_with_pending_successor(true).await;
    let store = fixture.bind_device1().await;
    let mut authority = store
        .authorize_writer()
        .await
        .expect("authorize Circle writer");
    let mut circles = authority.circles();
    let stale_request = circles
        .resolution_request_for_test(fixture.circle_id(), &chosen, vec![chosen.clone()])
        .await
        .expect("build stale resolution request");

    // A resolution whose captured conflicting set omits a currently retained
    // branch no longer equals the retained set inside the journal transaction,
    // so preparation fails loud rather than silently dropping the omitted
    // branch. Here the captured set names only the chosen branch.
    let stale = circles
        .preparer()
        .prepare_request(stale_request)
        .await
        .expect_err("a stale conflicting set is refused");
    assert!(
        matches!(&stale, CircleOperationError::InvalidState(reason)
            if reason.contains("conflict changed since the operation was requested")),
        "{stale}"
    );

    // Naming the complete current set (both branches) still equals the retained
    // set, so the resolution prepares and collapses the conflict.
    fixture
        .store1()
        .await
        .circles()
        .resolve_circle_control(fixture.circle_id(), chosen.clone())
        .await
        .expect("resolving the complete current set succeeds");
    assert!(
        matches!(fixture.circles_device1().await.as_slice(),
            [CircleInfo::Active { id, .. }] if *id == fixture.circle_id()),
        "resolving the complete current set collapses the conflict"
    );

    // The durable successor captured on device 2 before it observed the
    // competing branch is published after resolution. The resolution cannot claim it
    // covered this later control, so the conflict resurfaces.
    fixture
        .bind_device2()
        .await
        .resume_circle_operations()
        .await
        .expect("publish the already captured late successor");
    fixture.pull_device1().await;
    let resurfaced = fixture.conflict_branches_device1().await;
    assert_eq!(
        resurfaced.len(),
        2,
        "the resolution and the late branch conflict anew"
    );
    assert_eq!(
        fixture.circles_device1().await,
        vec![CircleInfo::Conflicted {
            id: fixture.circle_id(),
            branches: resurfaced,
        }]
    );
}

/// A Store + Circle carrying one member, on the founder's first device, with the
/// owner's production sync components (to add the member and close over its
/// removal) and a registered second founder device that can author a concurrent
/// successor.
fn open_routing_db(store_dir: coven_foundation::store_dir::StoreDir) -> Database {
    crate::sync::test_helpers::open_test_db_schema(
        store_dir,
        vec![coven_protocol::synced_schema::SyncedTable::new(
            "documents",
            coven_protocol::synced_schema::RowIdentity::IndependentUuid,
        )
        .scoped_by("audience")],
        vec![coven_database::Migration::sql(
            1,
            "Circle routing schema",
            "CREATE TABLE documents (
                 id TEXT PRIMARY KEY,
                 audience TEXT,
                 _updated_at TEXT NOT NULL
             ) STRICT;",
        )],
    )
}

#[tokio::test]
async fn concurrent_closes_can_cancel_one_branch_then_resolve_the_other() {
    use crate::sync::cycle::{PreparedSyncComponents, StoreInitialization};
    use coven_protocol::membership::MemberRole;

    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = open_routing_db(db1_store_dir.clone());
    let (store_fixture, _home, founder, journal) =
        persist_merge_operation_fixture(&db1, db1_store_dir.clone(), "resolve-closing").await;
    let (store, cloud_storage) = store_fixture;
    let circle_id = journal.circle_id();
    store
        .bind_device_in(&db1, db1_store_dir.clone(), &founder)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("activate founder transition");

    // Admission and add a Circle member so a removal has something to close over.
    let member = UserKeypair::generate();
    let member_pubkey = keys::public_key_hex(&member);
    store
        .admit_member(
            &db1,
            db1_store_dir.clone(),
            &founder,
            &member_pubkey,
            None,
            MemberRole::Member,
            &routing(),
            "Resolve closing Store",
        )
        .await
        .expect("admit Store member");
    let member_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let member_db = open_routing_db(member_db_store_dir.clone());
    store
        .activate_joined_device(
            &db1,
            db1_store_dir.clone(),
            &member_db,
            member_db_store_dir.clone(),
            &member,
            "0000000001100-0000-member",
        )
        .await
        .expect("activate Store member device");

    let store_dir = db1_store_dir.clone();
    let owner_storage = coven_storage::CloudSyncConnection::new(
        _home.clone(),
        coven_storage::CloudCipher::Encrypted(routing()),
        coven_storage::BlobPathScheme::Hashed,
        "resolve-closing",
        founder.clone(),
    );
    let components = PreparedSyncComponents::prepare(
        StoreDatabase::new(&db1),
        store_dir.clone(),
        owner_storage,
        founder.clone(),
        StoreInitialization::OpenStore {
            expected_store_root: store.root().clone(),
        },
        Some(routing()),
        circle_test_custody(),
    )
    .await
    .expect("prepare Circle owner sync")
    .initialize(None)
    .await
    .expect("initialize Circle owner sync");
    components
        .add_circle_member(circle_id, member_pubkey.clone(), CircleRole::Member)
        .await
        .expect("add Circle member");

    // The founder's second device pulls the with-member Circle so it can author a
    // successor concurrent with the removal.
    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = open_routing_db(db2_store_dir.clone());
    store
        .activate_joined_device(
            &db1,
            db1_store_dir.clone(),
            &db2,
            db2_store_dir.clone(),
            &founder,
            "0000000001100-0000-device2",
        )
        .await
        .expect("register the founder's second device");
    let dir2 = db2_store_dir.clone();
    Store::load(
        StoreDatabase::new(&db2),
        cloud_storage.clone(),
        dir2.clone(),
        founder.clone(),
        Some(routing()),
    )
    .await
    .expect("load device 2 Store")
    .authorize_writer()
    .await
    .expect("authorize device 2 pull")
    .pull(Some(&routing()))
    .await
    .expect("device 2 pulls the with-member Circle");

    // Each founder device removes the same member from the shared predecessor
    // without seeing the other's close, producing two concurrent close controls.
    Store::load(
        StoreDatabase::new(&db1),
        cloud_storage.clone(),
        store_dir.clone(),
        founder.clone(),
        Some(routing()),
    )
    .await
    .expect("load device 1 Store")
    .circles()
    .remove_circle_member(circle_id, member_pubkey.clone())
    .await
    .expect("device 1 authors an epoch close");
    Store::load(
        StoreDatabase::new(&db2),
        cloud_storage.clone(),
        dir2,
        founder.clone(),
        Some(routing()),
    )
    .await
    .expect("load device 2 Store")
    .circles()
    .remove_circle_member(circle_id, member_pubkey)
    .await
    .expect("device 2 authors a concurrent epoch close");

    Store::load(
        StoreDatabase::new(&db1),
        cloud_storage.clone(),
        store_dir.clone(),
        founder.clone(),
        Some(routing()),
    )
    .await
    .expect("load device 1 Store")
    .authorize_writer()
    .await
    .expect("authorize device 1 pull")
    .pull(Some(&routing()))
    .await
    .expect("device 1 pulls the concurrent close");
    let branches = StoreDatabase::new(&db1)
        .circle_control_conflict_branches(circle_id)
        .await
        .expect("read conflict branches")
        .expect("the two closes conflict");
    assert_eq!(branches.len(), 2, "the two closes conflict");

    let mut closing = Vec::new();
    for branch in &branches {
        let activation = store
            .bind_device_in(&db1, db1_store_dir.clone(), &founder)
            .await
            .expect("bind branch Store")
            .verified_circle_activation_for_test(circle_id, branch.clone())
            .await
            .expect("read branch activation")
            .expect("branch is retained");
        if matches!(
            activation.control.value.state(),
            coven_protocol::circle::CircleControlState::EpochClose(_)
        ) {
            closing.push(branch.clone());
        }
    }
    assert_eq!(closing.len(), 2, "both conflict branches are epoch closes");

    // Resolving to the closing branch is refused with the typed reason: a
    // resolution successor under a new control coordinate would strand the close's
    // participant responses, which bind to the closing control at create-once
    // slots.
    let error = Store::load(
        StoreDatabase::new(&db1),
        cloud_storage.clone(),
        store_dir.clone(),
        founder.clone(),
        Some(routing()),
    )
    .await
    .expect("load device 1 Store")
    .circles()
    .resolve_circle_control(circle_id, closing[0].clone())
    .await
    .expect_err("resolving to the closing branch is refused");
    assert!(
        matches!(&error, CircleOperationError::ResolveToClosingBranch { circle_id: id }
            if *id == circle_id),
        "{error}"
    );

    // Device 1 can still cancel its exact close while the Circle is conflicted.
    // The cancellation reopens that branch without covering the other close, so
    // the Circle remains conflicted until the Owner explicitly selects the
    // reopened branch.
    Store::load(
        StoreDatabase::new(&db1),
        cloud_storage.clone(),
        store_dir.clone(),
        founder.clone(),
        Some(routing()),
    )
    .await
    .expect("load device 1 Store")
    .circles()
    .cancel_circle_epoch_close(circle_id)
    .await
    .expect("cancel device 1's close while conflicted");
    let after_cancel = StoreDatabase::new(&db1)
        .circle_control_conflict_branches(circle_id)
        .await
        .expect("read conflict after cancellation")
        .expect("cancelling one close retains the other branch");
    let mut reopened = None;
    let mut closing_count = 0;
    for branch in &after_cancel {
        let activation = store
            .bind_device_in(&db1, db1_store_dir.clone(), &founder)
            .await
            .expect("bind post-cancellation branch Store")
            .verified_circle_activation_for_test(circle_id, branch.clone())
            .await
            .expect("read post-cancellation branch activation")
            .expect("post-cancellation branch is retained");
        match activation.control.value.state() {
            coven_protocol::circle::CircleControlState::ActiveEpoch(_) => {
                assert!(
                    reopened.replace(branch.clone()).is_none(),
                    "cancellation produces one active successor"
                );
            }
            coven_protocol::circle::CircleControlState::EpochClose(_) => closing_count += 1,
            coven_protocol::circle::CircleControlState::Deleted(_) => {
                panic!("cancellation cannot introduce a deleted branch")
            }
        }
    }
    let reopened = reopened.expect("one branch is the cancelled close's active successor");
    assert_eq!(closing_count, 1, "the concurrent close remains retained");
    Store::load(
        StoreDatabase::new(&db1),
        cloud_storage,
        store_dir,
        founder.clone(),
        Some(routing()),
    )
    .await
    .expect("load device 1 Store")
    .circles()
    .resolve_circle_control(circle_id, reopened)
    .await
    .expect("resolve to the reopened branch");
    let circles = StoreDatabase::new(&db1)
        .get_circles(
            &keys::public_key_hex(&founder),
            BTreeSet::from([keys::public_key_hex(&founder)]),
        )
        .await
        .expect("read resolved Circle");
    assert!(
        matches!(
            circles.as_slice(),
            [CircleInfo::Active { id, .. }] if *id == circle_id
        ),
        "cancel then resolve restores an active Circle"
    );
}

#[tokio::test]
async fn resolving_a_nonconflicted_circle_is_refused() {
    let fixture = ConflictFixture::build("resolve-nonconflicted").await;
    let (chosen, _commit) = fixture.authoring_context_device1().await;
    let error = fixture
        .store1()
        .await
        .circles()
        .resolve_circle_control(fixture.circle_id(), chosen.control.coord.clone())
        .await
        .expect_err("resolving an unconflicted Circle is refused");
    assert!(
        matches!(error, CircleOperationError::NotConflicted { circle_id } if circle_id == fixture.circle_id()),
        "{error}"
    );
}

#[tokio::test]
async fn non_owner_resolution_is_refused() {
    let fixture = ConflictFixture::build("resolve-non-owner").await;

    // A Store member who is not the Circle Owner, registered before the fork so
    // its device bootstrap does not race the concurrent conflict commits. They
    // observe the public conflict but hold no Circle access, so they cannot
    // author a resolution.
    let outsider = UserKeypair::generate();
    let outsider_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let outsider_db = crate::sync::test_helpers::open_test_db(outsider_db_store_dir.clone());
    fixture
        .admit_outsider(
            &outsider,
            &outsider_db,
            outsider_db_store_dir.clone(),
            "0000000001600-0000-outsider",
        )
        .await;

    let (chosen, _losing) = fixture.fork().await;

    fixture
        .load_outsider_store(&outsider_db, outsider_db_store_dir.clone(), &outsider)
        .await
        .authorize_writer()
        .await
        .expect("authorize non-owner pull")
        .pull(Some(&routing()))
        .await
        .expect("non-owner pulls the public conflict");
    assert!(
        StoreDatabase::new(&outsider_db)
            .circle_control_conflict_branches(fixture.circle_id())
            .await
            .expect("read non-owner conflict view")
            .is_some(),
        "the non-owner observes the public conflict"
    );

    let error = fixture
        .load_outsider_store(&outsider_db, outsider_db_store_dir, &outsider)
        .await
        .circles()
        .resolve_circle_control(fixture.circle_id(), chosen)
        .await
        .expect_err("a non-owner cannot resolve the conflict");
    assert!(
        matches!(error, CircleOperationError::InvalidState(_)),
        "{error}"
    );
}

#[tokio::test]
async fn resolution_resumes_idempotently_after_a_restart() {
    // A crash between journaling the command and publishing it: the durable
    // operation resumes and completes exactly once.
    let before_publication = ConflictFixture::build("resolve-restart-before").await;
    let (chosen, _losing) = before_publication.fork().await;
    let journal = before_publication.journal_resolution(&chosen).await;
    before_publication
        .bind_device1()
        .await
        .resume_circle_operations()
        .await
        .expect("resume completes the resolution");
    // A second resume is idempotent — the operation has already cleared.
    before_publication
        .bind_device1()
        .await
        .resume_circle_operations()
        .await
        .expect("second resume is idempotent");
    before_publication
        .assert_resolution_activated(&journal)
        .await;

    // A crash between publication and activation: the resolution control commit
    // reaches durable storage, but the operation is interrupted before it claims
    // its device-stream head and records the activation. Resume finds the commit
    // already published and completes idempotently. The resolution publishes four
    // exact objects (the control, then the commit and the publication head);
    // failing before the final head create leaves the commit published and
    // activation not yet recorded.
    let after_publication = ConflictFixture::build("resolve-restart-after").await;
    let (chosen, _losing) = after_publication.fork().await;
    let journal = after_publication.journal_resolution(&chosen).await;
    // The founder control and both conflicting branches are already activated;
    // the resolution must not add its activation while it is interrupted.
    let activations_before = after_publication.activation_count_device1().await;
    let head_create_call = 3;
    after_publication.fail_exact_create_before_call(head_create_call);

    let interrupted = after_publication
        .bind_device1()
        .await
        .resume_circle_operations()
        .await
        .expect_err("the head create fails after the commit is published");
    assert!(
        matches!(&interrupted, CircleOperationError::StoreOutbound(_))
            && crate::sync::error::error_chain_contains_transport(&interrupted),
        "{interrupted}"
    );
    assert!(
        after_publication.published_exact_object(&journal.operation().commit_ref().object),
        "the exact resolution commit is visible before its publication entry succeeds"
    );
    assert_eq!(
        after_publication.activation_count_device1().await,
        activations_before,
        "the interrupted resolution has not activated"
    );
    let persisted = after_publication
        .circle_operation_device1(&journal.operation_id)
        .await
        .expect("the interrupted resolution remains durable");
    assert_eq!(persisted.state(), CircleOperationState::Pending);

    after_publication
        .bind_device1()
        .await
        .resume_circle_operations()
        .await
        .expect("resume completes the published-but-unactivated resolution");
    after_publication
        .assert_resolution_activated(&journal)
        .await;
}
