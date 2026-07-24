use std::collections::BTreeSet;

use super::super::commands::{CircleOperationRequest, CircleResolveControlRequest};
use super::*;
use crate::sync::circle::{CircleControlCoord, CircleInfo};
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
    device2: String,
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
        Store::load(StoreDatabase::new(&self.db1), self.store.storage.clone())
            .await
            .expect("load founder Store on device 1")
    }

    async fn store2(&self) -> Store {
        Store::load(StoreDatabase::new(&self.db2), self.store.storage.clone())
            .await
            .expect("load founder Store on device 2")
    }

    async fn pull_device1(&self) {
        Store::authorize_borrowed(&*self.store.storage, &self.db1)
            .await
            .expect("authorize device 1 pull")
            .pull(&self.dir1, &self.founder, Some(&routing()))
            .await
            .expect("device 1 pull");
    }

    async fn pull_device2(&self) {
        Store::authorize_borrowed(&*self.store.storage, &self.db2)
            .await
            .expect("authorize device 2 pull")
            .pull(&self.dir2, &self.founder, Some(&routing()))
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
}

async fn conflict_fixture(label: &str) -> ConflictFixture {
    let db1 = open_test_db();
    let (store, founder, journal) = persist_merge_operation(&db1, label).await;
    let circle_id = journal.circle_id();
    let device1 = local_device_id(&db1).await;
    resume_circle_operations(&db1, &store.storage, &founder)
        .await
        .expect("activate founder transition");

    let db2 = open_test_db();
    install_active_device_fixture(&store, &db1, &db2, &founder, "0000000001100-0000-device2")
        .await
        .expect("register the founder's second device");
    let device2 = local_device_id(&db2).await;

    let (_dir1, dir1) = temp_store_dir();
    let (_dir2, dir2) = temp_store_dir();
    let founder_pubkey = keys::public_key_hex(&founder);

    let fixture = ConflictFixture {
        db1,
        device1,
        db2,
        device2,
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

/// Author a control successor on each device from the shared founder control
/// without either device seeing the other's, then pull both onto device 1 so
/// its current state retains the conflict. Returns the canonical branch set.
async fn fork(fixture: &ConflictFixture) -> (CircleControlCoord, CircleControlCoord) {
    fixture
        .store1()
        .await
        .rename_circle(
            &fixture.device1,
            "0000000001200-0000-device1",
            fixture.circle_id,
            "Alpha",
            &fixture.founder,
        )
        .await
        .expect("device 1 authors a control successor");
    fixture
        .store2()
        .await
        .rename_circle(
            &fixture.device2,
            "0000000001200-0000-device2",
            fixture.circle_id,
            "Beta",
            &fixture.founder,
        )
        .await
        .expect("device 2 authors a concurrent control successor");
    fixture.pull_device1().await;
    let branches = fixture.conflict_branches_device1().await;
    assert_eq!(branches.len(), 2, "two concurrent successors are retained");
    // The resolver chooses its own device's branch; `chosen` is device 1's.
    let chosen = branches
        .iter()
        .find(|branch| branch.device_id == fixture.device1)
        .expect("device 1 authored one branch")
        .clone();
    let losing = branches
        .into_iter()
        .find(|branch| *branch != chosen)
        .expect("the other device authored the losing branch");
    (chosen, losing)
}

#[tokio::test]
async fn concurrent_successors_retain_and_surface_as_a_conflict() {
    let fixture = conflict_fixture("resolve-surface").await;
    let _ = fork(&fixture).await;

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
        .rename_circle(
            &fixture.device1,
            "0000000001300-0000-device1",
            fixture.circle_id,
            "Gamma",
            &fixture.founder,
        )
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
    let (chosen, _losing) = fork(&fixture).await;

    fixture
        .store1()
        .await
        .resolve_circle_control(
            &fixture.device1,
            fixture.circle_id,
            chosen.clone(),
            &fixture.founder,
        )
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
        .rename_circle(
            &fixture.device1,
            "0000000001400-0000-device1",
            fixture.circle_id,
            "Resumed",
            &fixture.founder,
        )
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
async fn deleting_a_conflicted_circle_is_refused_until_resolved() {
    let fixture = conflict_fixture("delete-conflicted").await;
    let (chosen, _losing) = fork(&fixture).await;

    // A conflicted Circle refuses deletion: the conflicting set may carry
    // membership intent the deletion would otherwise bury.
    let refused = fixture
        .store1()
        .await
        .delete_circle(&fixture.device1, fixture.circle_id, &fixture.founder)
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
        .resolve_circle_control(
            &fixture.device1,
            fixture.circle_id,
            chosen.clone(),
            &fixture.founder,
        )
        .await
        .expect("resolve the control conflict");

    // Once resolved, deletion proceeds and the Circle surfaces as deleted.
    fixture
        .store1()
        .await
        .delete_circle(&fixture.device1, fixture.circle_id, &fixture.founder)
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
        .delete_circle(&fixture.device1, fixture.circle_id, &fixture.founder)
        .await
        .expect_err("deleting an already-deleted Circle is refused");
    assert!(
        matches!(&already, CircleOperationError::Deleted { circle_id }
            if *circle_id == fixture.circle_id),
        "{already:?}"
    );
}

/// Build a `ResolveControl` request for the chosen branch that names an
/// explicit conflicting set — used to drive preparation with a set that no
/// longer matches the retained branches.
async fn resolve_request(
    fixture: &ConflictFixture,
    chosen: &CircleControlCoord,
    conflicting_branches: Vec<CircleControlCoord>,
) -> CircleOperationRequest {
    let database = StoreDatabase::new(&fixture.db1);
    let chosen_activation = database
        .verified_circle_activation(fixture.circle_id, chosen.clone())
        .await
        .expect("read chosen branch activation")
        .expect("chosen branch is retained");
    let mut losing_heads = Vec::new();
    for branch in fixture.conflict_branches_device1().await {
        if branch == *chosen {
            continue;
        }
        let activation = database
            .verified_circle_activation(fixture.circle_id, branch)
            .await
            .expect("read losing branch activation")
            .expect("losing branch is retained");
        losing_heads.push(crate::sync::circle::MergeCircleControlHeadRef {
            coord: activation.reference.control.clone(),
            head_hash: activation.reference.head_hash,
            object: activation.reference.head_object.clone(),
        });
    }
    let access = chosen_activation
        .local_access
        .as_ref()
        .expect("chosen branch access");
    let active = access.active.as_ref().expect("chosen branch active");
    CircleOperationRequest::ResolveControl(Box::new(CircleResolveControlRequest {
        circle_id: fixture.circle_id,
        chosen: CircleAuthoringState {
            candidate_family: access.leaf.value.candidate_family,
            control: chosen_activation.control.clone(),
            access: access.leaf.value.clone(),
            roster: active.roster.clone(),
            metadata: active.metadata.clone(),
        },
        previous_control: chosen_activation.reference.clone(),
        losing_heads,
        conflicting_branches,
    }))
}

#[tokio::test]
async fn stale_resolution_is_refused_and_a_late_branch_resurfaces_the_conflict() {
    let fixture = conflict_fixture("resolve-stale").await;
    let (chosen, _losing) = fork(&fixture).await;

    // A resolution whose captured conflicting set omits a currently retained
    // branch no longer equals the retained set inside the journal transaction,
    // so preparation fails loud rather than silently dropping the omitted
    // branch. Here the captured set names only the chosen branch.
    let stale = prepare_circle_operation_request(
        &StoreDatabase::new(&fixture.db1),
        &*fixture.store.storage,
        &fixture.device1,
        resolve_request(&fixture, &chosen, vec![chosen.clone()]).await,
        &fixture.founder,
    )
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
        .resolve_circle_control(
            &fixture.device1,
            fixture.circle_id,
            chosen.clone(),
            &fixture.founder,
        )
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
        .rename_circle(
            &fixture.device2,
            "0000000001700-0000-device2",
            fixture.circle_id,
            "Delta",
            &fixture.founder,
        )
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
        .resolve_circle_control(
            &fixture.device1,
            fixture.circle_id,
            chosen.control.coord.clone(),
            &fixture.founder,
        )
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
    crate::sync::store::membership::invite_member(
        &fixture.store.storage,
        fixture.store.home.as_ref(),
        &fixture.founder,
        &crate::sync::hlc::Hlc::new("resolve-non-owner".to_string()),
        &outsider_pubkey,
        None,
        MemberRole::Member,
        &routing(),
        fixture.store.storage.store_id(),
        "Resolution test Store",
        &StoreDatabase::new(&fixture.db1),
    )
    .await
    .expect("invite a non-owner Store member");
    let outsider_db = open_test_db();
    install_active_device_fixture(
        &fixture.store,
        &fixture.db1,
        &outsider_db,
        &outsider,
        "0000000001600-0000-outsider",
    )
    .await
    .expect("register the non-owner device");
    let outsider_device = local_device_id(&outsider_db).await;

    let (chosen, _losing) = fork(&fixture).await;

    let (_outsider_dir_temp, outsider_dir) = temp_store_dir();
    Store::authorize_borrowed(&*fixture.store.storage, &outsider_db)
        .await
        .expect("authorize non-owner pull")
        .pull(&outsider_dir, &outsider, Some(&routing()))
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
    )
    .await
    .expect("load non-owner Store")
    .resolve_circle_control(&outsider_device, fixture.circle_id, chosen, &outsider)
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
    let request = resolve_request(fixture, chosen, fixture.conflict_branches_device1().await).await;
    let journal = prepare_circle_operation_request(
        &StoreDatabase::new(&fixture.db1),
        &*fixture.store.storage,
        &fixture.device1,
        request,
        &fixture.founder,
    )
    .await
    .expect("prepare resolution operation");
    StoreDatabase::new(&fixture.db1)
        .insert_circle_operation(journal.clone())
        .await
        .expect("journal the resolution before publication");
    journal
}

async fn assert_resolution_activated(fixture: &ConflictFixture, journal: &CircleOperationJournal) {
    assert!(
        StoreDatabase::new(&fixture.db1)
            .circle_operation(&journal.operation_id)
            .await
            .expect("read resolution journal")
            .is_none(),
        "the durable resolution clears on completion"
    );
    let circles = fixture.circles_device1().await;
    assert!(
        matches!(circles.as_slice(), [CircleInfo::Active { id, .. }] if *id == fixture.circle_id),
        "the resolution collapses the conflict: {circles:?}"
    );
}

#[tokio::test]
async fn resolution_resumes_idempotently_after_a_restart() {
    // A crash between journaling the command and publishing it: the durable
    // operation resumes and completes exactly once.
    let before_publication = conflict_fixture("resolve-restart-before").await;
    let (chosen, _losing) = fork(&before_publication).await;
    let journal = journal_resolution(&before_publication, &chosen).await;
    resume_circle_operations(
        &before_publication.db1,
        &before_publication.store.storage,
        &before_publication.founder,
    )
    .await
    .expect("resume completes the resolution");
    // A second resume is idempotent — the operation has already cleared.
    resume_circle_operations(
        &before_publication.db1,
        &before_publication.store.storage,
        &before_publication.founder,
    )
    .await
    .expect("second resume is idempotent");
    assert_resolution_activated(&before_publication, &journal).await;

    // A crash between publication and activation: the resolution control commit
    // reaches durable storage, but the operation is interrupted before it claims
    // its device-stream head and records the activation. Resume finds the commit
    // already published and completes idempotently. The resolution publishes
    // 2*access + 4 exact objects (access leaves, control, control head, access
    // envelopes, then the commit and the head); failing before the final head
    // create leaves the commit published and activation not yet recorded.
    let after_publication = conflict_fixture("resolve-restart-after").await;
    let (chosen, _losing) = fork(&after_publication).await;
    let journal = journal_resolution(&after_publication, &chosen).await;
    // The founder control and both conflicting branches are already activated;
    // the resolution must not add its activation while it is interrupted.
    let activations_before =
        activation_count(&after_publication.db1, after_publication.circle_id).await;
    let head_create_call = 2 * journal.operation().creation.access.len() + 4;
    after_publication
        .store
        .home
        .fail_exact_create_before_call(head_create_call);

    let interrupted = resume_circle_operations(
        &after_publication.db1,
        &after_publication.store.storage,
        &after_publication.founder,
    )
    .await
    .expect_err("the head create fails after the commit is published");
    assert!(
        matches!(interrupted, CircleOperationError::Object(_)),
        "{interrupted}"
    );
    assert_eq!(
        activation_count(&after_publication.db1, after_publication.circle_id).await,
        activations_before,
        "the interrupted resolution has not activated"
    );
    let persisted = StoreDatabase::new(&after_publication.db1)
        .circle_operation(&journal.operation_id)
        .await
        .expect("read interrupted resolution")
        .expect("the interrupted resolution remains durable");
    assert_eq!(persisted.state(), CircleOperationState::Pending);

    resume_circle_operations(
        &after_publication.db1,
        &after_publication.store.storage,
        &after_publication.founder,
    )
    .await
    .expect("resume completes the published-but-unactivated resolution");
    assert_resolution_activated(&after_publication, &journal).await;
}
