use std::collections::BTreeSet;

use super::*;
use crate::protocol::circle::{CircleControlCoord, CircleInfo, CircleRole};
use crate::sync::store::Store;

const ROUTING_KEY: [u8; 32] = [42; 32];

fn routing() -> EncryptionService {
    EncryptionService::from_key(ROUTING_KEY)
}

/// A founder Circle activated on the founder's first device, plus a second
/// device of the same founder identity that has pulled the founder control and
/// can author a concurrent control successor. Both devices publish to one shared
/// cloud home, so a control successor authored on each and not seen by the other
/// forms a genuine `ControlConflict` once either device pulls both.
struct ConflictFixture {
    db1: Database,
    device1: String,
    db2: Database,
    store: TestStore,
    founder: UserKeypair,
    founder_pubkey: String,
    circle_id: CircleId,
    _dir1: tempfile::TempDir,
    dir1: crate::store_dir::StoreDir,
    _dir2: tempfile::TempDir,
    dir2: crate::store_dir::StoreDir,
}

impl ConflictFixture {
    async fn store1(&self) -> Store {
        Store::load(
            StoreDatabase::new(&self.db1),
            self.store.storage.clone(),
            self.founder.clone(),
        )
        .await
        .expect("load founder Store on device 1")
    }

    async fn store2(&self) -> Store {
        Store::load(
            StoreDatabase::new(&self.db2),
            self.store.storage.clone(),
            self.founder.clone(),
        )
        .await
        .expect("load founder Store on device 2")
    }

    async fn pull_device1(&self) {
        self.store1()
            .await
            .authorize_writer()
            .await
            .expect("authorize device 1 pull")
            .pull(&self.dir1, Some(&routing()))
            .await
            .expect("device 1 pull");
    }

    async fn pull_device2(&self) {
        self.store2()
            .await
            .authorize_writer()
            .await
            .expect("authorize device 2 pull")
            .pull(&self.dir2, Some(&routing()))
            .await
            .expect("device 2 pull");
    }

    async fn conflict_branches_device1(&self) -> Vec<CircleControlCoord> {
        StoreDatabase::new(&self.db1)
            .circle_control_conflict_branches(self.circle_id)
            .await
            .expect("read device 1 conflict branches")
            .expect("device 1 Circle is conflicted")
    }

    async fn circles_device1(&self) -> Vec<CircleInfo> {
        StoreDatabase::new(&self.db1)
            .get_circles(
                &self.founder_pubkey,
                BTreeSet::from([self.founder_pubkey.clone()]),
            )
            .await
            .expect("list device 1 Circles")
    }

    async fn circles_device2(&self) -> Vec<CircleInfo> {
        StoreDatabase::new(&self.db2)
            .get_circles(
                &self.founder_pubkey,
                BTreeSet::from([self.founder_pubkey.clone()]),
            )
            .await
            .expect("list device 2 Circles")
    }

    /// Author a control successor on each device from the shared founder
    /// control without either device seeing the other's, then pull both onto
    /// device 1 so its current state retains the conflict.
    async fn fork(&self) -> (CircleControlCoord, CircleControlCoord) {
        self.store1()
            .await
            .circles()
            .rename_circle("0000000001200-0000-device1", self.circle_id, "Alpha")
            .await
            .expect("device 1 authors a control successor");
        self.store2()
            .await
            .circles()
            .rename_circle("0000000001200-0000-device2", self.circle_id, "Beta")
            .await
            .expect("device 2 authors a concurrent control successor");
        self.pull_device1().await;
        let branches = self.conflict_branches_device1().await;
        assert_eq!(branches.len(), 2, "two concurrent successors are retained");
        let chosen = branches
            .iter()
            .find(|branch| branch.device_id == self.device1)
            .expect("device 1 authored one branch")
            .clone();
        let losing = branches
            .into_iter()
            .find(|branch| *branch != chosen)
            .expect("the other device authored the losing branch");
        (chosen, losing)
    }

    async fn assert_resolution_activated(&self, journal: &CircleOperationJournal) {
        assert!(
            StoreDatabase::new(&self.db1)
                .circle_operation(&journal.operation_id)
                .await
                .expect("read resolution journal")
                .is_none(),
            "the durable resolution clears on completion"
        );
        let circles = self.circles_device1().await;
        assert!(
            matches!(circles.as_slice(), [CircleInfo::Active { id, .. }] if *id == self.circle_id),
            "the resolution collapses the conflict: {circles:?}"
        );
    }
}

async fn conflict_fixture(label: &str) -> ConflictFixture {
    let db1 = open_test_db();
    let (store, founder, journal) = persist_merge_operation(&db1, label).await;
    let circle_id = journal.circle_id();
    let device1 = db1
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device id")
        .expect("local Store device is active");
    store
        .bind_device(&db1, &founder)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("activate founder transition");

    let db2 = open_test_db();
    store
        .activate_joined_device(&db1, &db2, &founder, "0000000001100-0000-device2")
        .await
        .expect("register the founder's second device");
    let (_dir1, dir1) = temp_store_dir();
    let (_dir2, dir2) = temp_store_dir();
    let founder_pubkey = keys::public_key_hex(&founder);

    let fixture = ConflictFixture {
        db1,
        device1,
        db2,
        store,
        founder,
        founder_pubkey,
        circle_id,
        _dir1,
        dir1,
        _dir2,
        dir2,
    };
    // Device 2 must materialize the founder control before it can author a
    // concurrent successor of it.
    fixture.pull_device2().await;
    fixture
}

#[tokio::test]
async fn concurrent_successors_retain_and_surface_as_a_conflict() {
    let fixture = conflict_fixture("resolve-surface").await;
    let _ = fixture.fork().await;

    assert_eq!(
        fixture.circles_device1().await,
        vec![CircleInfo::Conflicted {
            id: fixture.circle_id,
            branches: fixture.conflict_branches_device1().await,
        }]
    );

    // Authoring refuses on a conflicted Circle: there is no single resolved
    // control to succeed.
    let rename = fixture
        .store1()
        .await
        .circles()
        .rename_circle("0000000001300-0000-device1", fixture.circle_id, "Gamma")
        .await;
    assert!(rename.is_err(), "conflicted Circle refuses authoring");

    // Package publication refuses too: no active publication key.
    let package = StoreDatabase::new(&fixture.db1)
        .circle_publication_context(
            fixture.circle_id,
            fixture.conflict_branches_device1().await[0].clone(),
        )
        .await;
    assert!(
        package.is_err(),
        "conflicted Circle refuses package publication"
    );
}

#[tokio::test]
async fn resolution_collapses_the_conflict_on_every_device() {
    let fixture = conflict_fixture("resolve-collapse").await;
    let (chosen, _losing) = fixture.fork().await;

    fixture
        .store1()
        .await
        .circles()
        .resolve_circle_control(fixture.circle_id, chosen.clone())
        .await
        .expect("resolve the control conflict");

    let device1 = fixture.circles_device1().await;
    assert!(
        matches!(device1.as_slice(), [CircleInfo::Active { id, .. }] if *id == fixture.circle_id),
        "resolution collapses the conflict on the resolving device: {device1:?}"
    );

    // Authoring resumes under the resolution control.
    fixture
        .store1()
        .await
        .circles()
        .rename_circle("0000000001400-0000-device1", fixture.circle_id, "Resumed")
        .await
        .expect("authoring resumes after resolution");

    // Device 2, which retained the other branch, collapses to the same
    // resolution after pulling it — the opposite arrival order.
    fixture.pull_device2().await;
    let device2 = fixture.circles_device2().await;
    assert!(
        matches!(device2.as_slice(), [CircleInfo::Active { id, .. }] if *id == fixture.circle_id),
        "resolution collapses the conflict on the other device: {device2:?}"
    );
}

#[tokio::test]
async fn resolving_to_another_devices_branch_merges_head_frontiers() {
    let fixture = conflict_fixture("resolve-frontier-merge").await;
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
        .resolve_circle_control(fixture.circle_id, device2_branch.clone())
        .await
        .expect("resolve the control conflict to device 2's branch");
    assert_eq!(
        fixture.circles_device1().await,
        vec![CircleInfo::Active {
            id: fixture.circle_id,
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
        .rename_circle("0000000001500-0000-device1", fixture.circle_id, "Gamma")
        .await
        .expect("device 1 authoring resumes without a metadata head-slot collision");
    assert_eq!(
        fixture.circles_device1().await,
        vec![CircleInfo::Active {
            id: fixture.circle_id,
            name: "Gamma".to_string(),
            role: CircleRole::Owner,
            rotation_required: false,
        }],
        "the resumed rename takes effect over the merged metadata frontier"
    );
}

#[tokio::test]
async fn deleting_a_conflicted_circle_is_refused_until_resolved() {
    let fixture = conflict_fixture("delete-conflicted").await;
    let (chosen, _losing) = fixture.fork().await;

    // A conflicted Circle refuses deletion: the conflicting set may carry
    // membership intent the deletion would otherwise bury.
    let refused = fixture
        .store1()
        .await
        .circles()
        .delete_circle(fixture.circle_id)
        .await
        .expect_err("deleting a conflicted Circle is refused");
    assert!(
        matches!(&refused, CircleOperationError::Conflicted { circle_id }
            if *circle_id == fixture.circle_id),
        "{refused:?}"
    );

    fixture
        .store1()
        .await
        .circles()
        .resolve_circle_control(fixture.circle_id, chosen.clone())
        .await
        .expect("resolve the control conflict");

    // Once resolved, deletion proceeds and the Circle surfaces as deleted.
    fixture
        .store1()
        .await
        .circles()
        .delete_circle(fixture.circle_id)
        .await
        .expect("delete the resolved Circle");
    let device1 = fixture.circles_device1().await;
    assert!(
        matches!(device1.as_slice(), [CircleInfo::Deleted { id }] if *id == fixture.circle_id),
        "the resolving device reports the Circle as deleted: {device1:?}"
    );

    // A second deletion is refused: the Circle is already terminal.
    let already = fixture
        .store1()
        .await
        .circles()
        .delete_circle(fixture.circle_id)
        .await
        .expect_err("deleting an already-deleted Circle is refused");
    assert!(
        matches!(&already, CircleOperationError::Deleted { circle_id }
            if *circle_id == fixture.circle_id),
        "{already:?}"
    );
}

#[tokio::test]
async fn stale_resolution_is_refused_and_a_late_branch_resurfaces_the_conflict() {
    let fixture = conflict_fixture("resolve-stale").await;
    let (chosen, _losing) = fixture.fork().await;
    let store = fixture
        .store
        .bind_device(&fixture.db1, &fixture.founder)
        .await
        .expect("authorize Circle resolution");
    let mut authority = store
        .authorize_writer()
        .await
        .expect("authorize Circle writer");
    let mut circles = authority.circles();
    let stale_request = circles
        .resolution_request_for_test(fixture.circle_id, &chosen, vec![chosen.clone()])
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
            if reason.contains("conflict changed since the resolution was requested")),
        "{stale}"
    );

    // Naming the complete current set (both branches) still equals the retained
    // set, so the resolution prepares and collapses the conflict.
    fixture
        .store1()
        .await
        .circles()
        .resolve_circle_control(fixture.circle_id, chosen.clone())
        .await
        .expect("resolving the complete current set succeeds");
    assert!(
        matches!(fixture.circles_device1().await.as_slice(),
            [CircleInfo::Active { id, .. }] if *id == fixture.circle_id),
        "resolving the complete current set collapses the conflict"
    );

    // After the resolution activates, a branch authored concurrently on device 2
    // (which never saw the resolution) is discovered on device 1. The reduction
    // retains it against the resolution and resurfaces ControlConflict — nothing
    // pretends the resolution covered a set it did not.
    fixture
        .store2()
        .await
        .circles()
        .rename_circle("0000000001700-0000-device2", fixture.circle_id, "Delta")
        .await
        .expect("device 2 authors a late concurrent successor");
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
            id: fixture.circle_id,
            branches: resurfaced,
        }]
    );
}

/// A Store + Circle carrying one member, on the founder's first device, with the
/// owner's production sync components (to add the member and close over its
/// removal) and a registered second founder device that can author a concurrent
/// successor.
fn open_routing_db() -> Database {
    crate::sync::test_helpers::open_test_db_schema(
        vec![crate::sync::session::SyncedTable::new(
            "documents",
            crate::sync::session::RowIdentity::IndependentUuid,
        )
        .scoped_by("audience")],
        vec![crate::Migration::sql(
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
    use crate::protocol::membership::MemberRole;
    use crate::sync::cycle::{PreparedSyncComponents, StoreInitialization};

    let db1 = open_routing_db();
    let (store, founder, journal) = persist_merge_operation(&db1, "resolve-closing").await;
    let circle_id = journal.circle_id();
    store
        .bind_device(&db1, &founder)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("activate founder transition");

    // Invite and add a Circle member so a removal has something to close over.
    let member = UserKeypair::generate();
    let member_pubkey = keys::public_key_hex(&member);
    store
        .invite_member(
            &db1,
            &founder,
            &crate::sync::hlc::Hlc::new(
                "resolve-closing-owner".to_string(),
                std::sync::Arc::new(crate::clock::SystemClock),
            ),
            &member_pubkey,
            None,
            MemberRole::Member,
            &routing(),
            "Resolve closing Store",
        )
        .await
        .expect("invite Store member");
    let member_db = open_routing_db();
    store
        .activate_joined_device(&db1, &member_db, &member, "0000000001100-0000-member")
        .await
        .expect("activate Store member device");

    let (_store_temp, store_dir) = temp_store_dir();
    let owner_storage = crate::storage::CloudSyncStorage::new(
        store.home.clone(),
        crate::storage::CloudCipher::Encrypted(routing()),
        crate::storage::BlobPathScheme::Hashed,
        store.storage.store_id(),
        founder.clone(),
    )
    .expect("open Circle owner storage");
    let components = PreparedSyncComponents::prepare(
        StoreDatabase::new(&db1),
        crate::sync::test_owner_graph::local_blob_access(
            StoreDatabase::new(&db1),
            store_dir.clone(),
        ),
        owner_storage,
        StoreInitialization::OpenStore {
            expected_store_root: store.root.clone(),
        },
        Some(routing()),
    )
    .await
    .expect("prepare Circle owner sync")
    .initialize()
    .await
    .expect("initialize Circle owner sync");
    components
        .add_circle_member(
            &store_dir,
            circle_id,
            member_pubkey.clone(),
            CircleRole::Member,
        )
        .await
        .expect("add Circle member");

    // The founder's second device pulls the with-member Circle so it can author a
    // successor concurrent with the removal.
    let db2 = open_routing_db();
    store
        .activate_joined_device(&db1, &db2, &founder, "0000000001100-0000-device2")
        .await
        .expect("register the founder's second device");
    let (_dir2, dir2) = temp_store_dir();
    Store::load(
        StoreDatabase::new(&db2),
        store.storage.clone(),
        founder.clone(),
    )
    .await
    .expect("load device 2 Store")
    .authorize_writer()
    .await
    .expect("authorize device 2 pull")
    .pull(&dir2, Some(&routing()))
    .await
    .expect("device 2 pulls the with-member Circle");

    // Each founder device removes the same member from the shared predecessor
    // without seeing the other's close, producing two concurrent close controls.
    Store::load(
        StoreDatabase::new(&db1),
        store.storage.clone(),
        founder.clone(),
    )
    .await
    .expect("load device 1 Store")
    .circles()
    .remove_circle_member(circle_id, member_pubkey.clone())
    .await
    .expect("device 1 authors an epoch close");
    Store::load(
        StoreDatabase::new(&db2),
        store.storage.clone(),
        founder.clone(),
    )
    .await
    .expect("load device 2 Store")
    .circles()
    .remove_circle_member(circle_id, member_pubkey)
    .await
    .expect("device 2 authors a concurrent epoch close");

    let (_dir1, dir1) = temp_store_dir();
    Store::load(
        StoreDatabase::new(&db1),
        store.storage.clone(),
        founder.clone(),
    )
    .await
    .expect("load device 1 Store")
    .authorize_writer()
    .await
    .expect("authorize device 1 pull")
    .pull(&dir1, Some(&routing()))
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
            .bind_device(&db1, &founder)
            .await
            .expect("bind branch Store")
            .verified_circle_activation_for_test(circle_id, branch.clone())
            .await
            .expect("read branch activation")
            .expect("branch is retained");
        if matches!(
            activation.control.value.state(),
            crate::protocol::circle::CircleControlState::EpochClose(_)
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
        store.storage.clone(),
        founder.clone(),
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
        store.storage.clone(),
        founder.clone(),
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
            .bind_device(&db1, &founder)
            .await
            .expect("bind post-cancellation branch Store")
            .verified_circle_activation_for_test(circle_id, branch.clone())
            .await
            .expect("read post-cancellation branch activation")
            .expect("post-cancellation branch is retained");
        match activation.control.value.state() {
            crate::protocol::circle::CircleControlState::ActiveEpoch(_) => {
                assert!(
                    reopened.replace(branch.clone()).is_none(),
                    "cancellation produces one active successor"
                );
            }
            crate::protocol::circle::CircleControlState::EpochClose(_) => closing_count += 1,
            crate::protocol::circle::CircleControlState::Deleted(_) => {
                panic!("cancellation cannot introduce a deleted branch")
            }
        }
    }
    let reopened = reopened.expect("one branch is the cancelled close's active successor");
    assert_eq!(closing_count, 1, "the concurrent close remains retained");
    Store::load(
        StoreDatabase::new(&db1),
        store.storage.clone(),
        founder.clone(),
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
    let fixture = conflict_fixture("resolve-nonconflicted").await;
    let (chosen, _commit) = StoreDatabase::new(&fixture.db1)
        .circle_authoring_context(fixture.circle_id, &fixture.founder_pubkey)
        .await
        .expect("read the active founder control");
    let error = fixture
        .store1()
        .await
        .circles()
        .resolve_circle_control(fixture.circle_id, chosen.control.coord.clone())
        .await
        .expect_err("resolving an unconflicted Circle is refused");
    assert!(
        matches!(error, CircleOperationError::NotConflicted { circle_id } if circle_id == fixture.circle_id),
        "{error}"
    );
}

#[tokio::test]
async fn non_owner_resolution_is_refused() {
    let fixture = conflict_fixture("resolve-non-owner").await;

    // A Store member who is not the Circle Owner, registered before the fork so
    // its device bootstrap does not race the concurrent conflict commits. They
    // observe the public conflict but hold no Circle access, so they cannot
    // author a resolution.
    let outsider = UserKeypair::generate();
    let outsider_pubkey = keys::public_key_hex(&outsider);
    fixture
        .store
        .invite_member(
            &fixture.db1,
            &fixture.founder,
            &crate::sync::hlc::Hlc::new(
                "resolve-non-owner".to_string(),
                std::sync::Arc::new(crate::clock::SystemClock),
            ),
            &outsider_pubkey,
            None,
            MemberRole::Member,
            &routing(),
            "Resolution test Store",
        )
        .await
        .expect("invite a non-owner Store member");
    let outsider_db = open_test_db();
    fixture
        .store
        .activate_joined_device(
            &fixture.db1,
            &outsider_db,
            &outsider,
            "0000000001600-0000-outsider",
        )
        .await
        .expect("register the non-owner device");

    let (chosen, _losing) = fixture.fork().await;

    let (_outsider_dir_temp, outsider_dir) = temp_store_dir();
    Store::load(
        StoreDatabase::new(&outsider_db),
        fixture.store.storage.clone(),
        outsider.clone(),
    )
    .await
    .expect("load non-owner Store")
    .authorize_writer()
    .await
    .expect("authorize non-owner pull")
    .pull(&outsider_dir, Some(&routing()))
    .await
    .expect("non-owner pulls the public conflict");
    assert!(
        StoreDatabase::new(&outsider_db)
            .circle_control_conflict_branches(fixture.circle_id)
            .await
            .expect("read non-owner conflict view")
            .is_some(),
        "the non-owner observes the public conflict"
    );

    let error = Store::load(
        StoreDatabase::new(&outsider_db),
        fixture.store.storage.clone(),
        outsider.clone(),
    )
    .await
    .expect("load non-owner Store")
    .circles()
    .resolve_circle_control(fixture.circle_id, chosen)
    .await
    .expect_err("a non-owner cannot resolve the conflict");
    assert!(
        matches!(error, CircleOperationError::InvalidState(_)),
        "{error}"
    );
}

/// Prepare and durably journal a resolution of the chosen branch without
/// publishing it — the state left by a crash between the command and its
/// publication.
async fn journal_resolution(
    fixture: &ConflictFixture,
    chosen: &CircleControlCoord,
) -> CircleOperationJournal {
    let store = fixture
        .store
        .bind_device(&fixture.db1, &fixture.founder)
        .await
        .expect("authorize Circle resolution");
    let mut authority = store
        .authorize_writer()
        .await
        .expect("authorize Circle writer");
    let mut circles = authority.circles();
    let request = circles
        .resolution_request_for_test(
            fixture.circle_id,
            chosen,
            fixture.conflict_branches_device1().await,
        )
        .await
        .expect("build resolution request");
    let journal = circles
        .preparer()
        .prepare_request(request)
        .await
        .expect("prepare resolution operation");
    StoreDatabase::new(&fixture.db1)
        .insert_circle_operation(journal.clone())
        .await
        .expect("journal the resolution before publication");
    journal
}

#[tokio::test]
async fn resolution_resumes_idempotently_after_a_restart() {
    // A crash between journaling the command and publishing it: the durable
    // operation resumes and completes exactly once.
    let before_publication = conflict_fixture("resolve-restart-before").await;
    let (chosen, _losing) = before_publication.fork().await;
    let journal = journal_resolution(&before_publication, &chosen).await;
    before_publication
        .store
        .bind_device(&before_publication.db1, &before_publication.founder)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("resume completes the resolution");
    // A second resume is idempotent — the operation has already cleared.
    before_publication
        .store
        .bind_device(&before_publication.db1, &before_publication.founder)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("second resume is idempotent");
    before_publication
        .assert_resolution_activated(&journal)
        .await;

    // A crash between publication and activation: the resolution control commit
    // reaches durable storage, but the operation is interrupted before it claims
    // its device-stream head and records the activation. Resume finds the commit
    // already published and completes idempotently. The resolution publishes
    // 2*access + 4 exact objects (access leaves, control, control head, access
    // envelopes, then the commit and the head); failing before the final head
    // create leaves the commit published and activation not yet recorded.
    let after_publication = conflict_fixture("resolve-restart-after").await;
    let (chosen, _losing) = after_publication.fork().await;
    let journal = journal_resolution(&after_publication, &chosen).await;
    // The founder control and both conflicting branches are already activated;
    // the resolution must not add its activation while it is interrupted.
    let activations_before = StoreDatabase::new(&after_publication.db1)
        .circle_control_activation_count_for_test(after_publication.circle_id)
        .await
        .expect("count circle activations");
    let head_create_call = 2 * journal.operation().creation.access.len() + 4;
    after_publication
        .store
        .home
        .fail_exact_create_before_call(head_create_call);

    let interrupted = after_publication
        .store
        .bind_device(&after_publication.db1, &after_publication.founder)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect_err("the head create fails after the commit is published");
    assert!(
        matches!(interrupted, CircleOperationError::Object(_)),
        "{interrupted}"
    );
    assert_eq!(
        StoreDatabase::new(&after_publication.db1)
            .circle_control_activation_count_for_test(after_publication.circle_id)
            .await
            .expect("count circle activations"),
        activations_before,
        "the interrupted resolution has not activated"
    );
    let persisted = StoreDatabase::new(&after_publication.db1)
        .circle_operation(&journal.operation_id)
        .await
        .expect("read interrupted resolution")
        .expect("the interrupted resolution remains durable");
    assert_eq!(persisted.state(), CircleOperationState::Pending);

    after_publication
        .store
        .bind_device(&after_publication.db1, &after_publication.founder)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("resume completes the published-but-unactivated resolution");
    after_publication
        .assert_resolution_activated(&journal)
        .await;
}
