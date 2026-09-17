//! The two-device control conflict every resolution and deletion test starts
//! from. Both devices publish to one shared cloud home, so a control successor
//! authored on each and not seen by the other forms a genuine `ControlConflict`
//! once either device pulls both.

use std::collections::BTreeSet;

use super::*;
use crate::sync::store::Store;
use coven_database::Database;
use coven_protocol::circle::{CircleControlCoord, CircleInfo};

pub(super) const ROUTING_KEY: [u8; 32] = [42; 32];

pub(super) fn routing() -> EncryptionService {
    EncryptionService::from_key(ROUTING_KEY)
}

/// A founder Circle activated on the founder's first device, plus a second
/// device of the same founder identity that has pulled the founder control and
/// can author a concurrent control successor. Both devices publish to one shared
/// cloud home, so a control successor authored on each and not seen by the other
/// forms a genuine `ControlConflict` once either device pulls both.
/// What a one-position fork attempt leaves behind: the refusal its own device
/// raised, and the prepared operation whose commit a peer can still be handed.
pub(super) struct OnePositionFork {
    pub(super) refusal: CircleOperationError,
    pub(super) journal: CircleOperationJournal,
}

pub(super) struct ConflictFixture {
    db1: Database,
    device1: String,
    db2: Database,
    store: std::sync::Arc<TestStore>,
    cloud_storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
    home: std::sync::Arc<coven_storage::InMemoryCloudHome>,
    founder: UserKeypair,
    founder_pubkey: String,
    circle_id: CircleId,
    dir1: coven_foundation::store_dir::StoreDir,
    dir2: coven_foundation::store_dir::StoreDir,
}

impl ConflictFixture {
    pub(super) async fn build(label: &str) -> Self {
        let db1_store_dir = crate::sync::test_helpers::test_store_dir();
        let db1 = crate::sync::test_helpers::open_test_db(db1_store_dir.clone());
        let (store_fixture, _home, founder, journal) =
            persist_merge_operation_fixture(&db1, db1_store_dir.clone(), label).await;
        let (store, cloud_storage) = store_fixture;
        let circle_id = journal.circle_id();
        let device1 = db1
            .get_protocol_state(coven_database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("read local Store device id")
            .expect("local Store device is active");
        store
            .bind_device_in(&db1, db1_store_dir.clone(), &founder)
            .await
            .expect("bind Circle test Store")
            .resume_circle_operations()
            .await
            .expect("activate founder transition");

        let db2_store_dir = crate::sync::test_helpers::test_store_dir();
        let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
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
        let founder_pubkey = keys::public_key_hex(&founder);

        let fixture = Self {
            db1,
            device1,
            db2,
            store,
            cloud_storage,
            home: _home,
            founder,
            founder_pubkey,
            circle_id,
            dir1: db1_store_dir,
            dir2: db2_store_dir,
        };
        // Device 2 must materialize the founder control before it can author a
        // concurrent successor of it.
        fixture.pull_device2().await;
        fixture
    }

    pub(super) async fn store1(&self) -> Store {
        Store::load(
            StoreDatabase::new(&self.db1),
            self.cloud_storage.clone(),
            self.dir1.clone(),
            self.founder.clone(),
            Some(routing()),
        )
        .await
        .expect("load founder Store on device 1")
    }

    pub(super) async fn store2(&self) -> Store {
        Store::load(
            StoreDatabase::new(&self.db2),
            self.cloud_storage.clone(),
            self.dir2.clone(),
            self.founder.clone(),
            Some(routing()),
        )
        .await
        .expect("load founder Store on device 2")
    }

    pub(super) async fn pull_device1(&self) {
        self.store1()
            .await
            .authorize_writer()
            .await
            .expect("authorize device 1 pull")
            .pull(Some(&routing()))
            .await
            .expect("device 1 pull");
    }

    pub(super) async fn pull_device2(&self) {
        self.store2()
            .await
            .authorize_writer()
            .await
            .expect("authorize device 2 pull")
            .pull(Some(&routing()))
            .await
            .expect("device 2 pull");
    }

    pub(super) fn circle_id(&self) -> CircleId {
        self.circle_id
    }

    /// Bind the founder's own devices for the commands that need a writer
    /// rather than the loaded Store handle.
    pub(super) async fn bind_device1(&self) -> crate::sync::test_helpers::TestDevice {
        self.store
            .bind_device(&self.db1, self.dir1.clone(), &self.founder)
            .await
            .expect("bind device 1")
    }

    pub(super) async fn bind_device2(&self) -> crate::sync::test_helpers::TestDevice {
        self.store
            .bind_device(&self.db2, self.dir2.clone(), &self.founder)
            .await
            .expect("bind device 2")
    }

    /// How many Circle activations device 1 has installed for this Circle.
    pub(super) async fn activation_count_device1(&self) -> i64 {
        StoreDatabase::new(&self.db1)
            .circle_control_activation_count_for_test(self.circle_id)
            .await
            .expect("count circle activations")
    }

    pub(super) async fn circle_operation_device1(
        &self,
        operation_id: &CircleOperationId,
    ) -> Option<CircleOperationJournal> {
        StoreDatabase::new(&self.db1)
            .circle_operation(operation_id)
            .await
            .expect("read the durable Circle operation")
    }

    /// Fail the provider's next exact create at `call`, so a test can interrupt
    /// a publication at a named step.
    pub(super) fn fail_exact_create_before_call(&self, call: usize) {
        self.home.fail_exact_create_before_call(call);
    }

    pub(super) fn published_exact_object(
        &self,
        object: &coven_protocol::objects::ExactObjectRef,
    ) -> bool {
        self.home.contains_exact_object(object)
    }

    pub(super) async fn authoring_context_device1(
        &self,
    ) -> (
        coven_protocol::circle_activation::CircleAuthoringState,
        coven_protocol::store_commit::StoreBatchCommitRef,
    ) {
        StoreDatabase::new(&self.db1)
            .circle_authoring_context(self.circle_id, &self.founder_pubkey)
            .await
            .expect("read device 1 authoring context")
    }

    pub(super) async fn publication_context_device1(
        &self,
        control: CircleControlCoord,
    ) -> Result<coven_protocol::circle_activation::CircleEpochAccess, coven_database::DbError> {
        StoreDatabase::new(&self.db1)
            .circle_publication_context(self.circle_id, control)
            .await
    }

    /// Admit a Store member who is not on the Circle roster and activate a
    /// device for it, so it observes the public conflict and nothing more.
    pub(super) async fn admit_outsider(
        &self,
        outsider: &UserKeypair,
        outsider_db: &Database,
        outsider_dir: coven_foundation::store_dir::StoreDir,
        device_stamp: &str,
    ) {
        self.store
            .admit_member(
                &self.db1,
                self.dir1.clone(),
                &self.founder,
                &keys::public_key_hex(outsider),
                None,
                MemberRole::Member,
                &routing(),
                "Resolution test Store",
            )
            .await
            .expect("admit a non-owner Store member");
        self.store
            .activate_joined_device(
                &self.db1,
                self.dir1.clone(),
                outsider_db,
                outsider_dir,
                outsider,
                device_stamp,
            )
            .await
            .expect("register the non-owner device");
    }

    /// The same shared cloud home, opened as the outsider's own Store.
    pub(super) async fn load_outsider_store(
        &self,
        outsider_db: &Database,
        outsider_dir: coven_foundation::store_dir::StoreDir,
        outsider: &UserKeypair,
    ) -> Store {
        Store::load(
            StoreDatabase::new(outsider_db),
            self.cloud_storage.clone(),
            outsider_dir,
            outsider.clone(),
            Some(routing()),
        )
        .await
        .expect("load non-owner Store")
    }

    /// The conflict this device currently retains, or `None` once some control
    /// covers every branch.
    pub(super) async fn retained_conflict_device1(&self) -> Option<Vec<CircleControlCoord>> {
        StoreDatabase::new(&self.db1)
            .circle_control_conflict_branches(self.circle_id)
            .await
            .expect("read device 1 conflict branches")
    }

    pub(super) async fn conflict_branches_device1(&self) -> Vec<CircleControlCoord> {
        StoreDatabase::new(&self.db1)
            .circle_control_conflict_branches(self.circle_id)
            .await
            .expect("read device 1 conflict branches")
            .expect("device 1 Circle is conflicted")
    }

    pub(super) async fn circles_device1(&self) -> Vec<CircleInfo> {
        StoreDatabase::new(&self.db1)
            .get_circles(
                &self.founder_pubkey,
                BTreeSet::from([self.founder_pubkey.clone()]),
            )
            .await
            .expect("list device 1 Circles")
    }

    pub(super) async fn circles_device2(&self) -> Vec<CircleInfo> {
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
    pub(super) async fn fork(&self) -> (CircleControlCoord, CircleControlCoord) {
        self.fork_with_pending_successor(false).await
    }

    pub(super) async fn fork_with_pending_successor(
        &self,
        prepare_late_successor: bool,
    ) -> (CircleControlCoord, CircleControlCoord) {
        if prepare_late_successor {
            self.home.fail_exact_create_before_call(1);
        }
        let first = self
            .store1()
            .await
            .circles()
            .rename_circle("0000000001200-0000-device1", self.circle_id, "Alpha")
            .await;
        if prepare_late_successor {
            let error =
                first.expect_err("keep the first branch unpublished while capturing its peer");
            assert!(
                crate::sync::error::error_chain_contains_transport(&error),
                "{error}"
            );
        } else {
            first.expect("device 1 authors a control successor");
        }
        self.store2()
            .await
            .circles()
            .rename_circle("0000000001200-0000-device2", self.circle_id, "Beta")
            .await
            .expect("device 2 authors a concurrent control successor");
        if prepare_late_successor {
            self.home.fail_exact_create_before_call(1);
            let error = self
                .store2()
                .await
                .circles()
                .rename_circle("0000000001700-0000-device2", self.circle_id, "Delta")
                .await
                .expect_err("capture the late successor before its device observes a conflict");
            assert!(
                crate::sync::error::error_chain_contains_transport(&error),
                "{error}"
            );
            self.store
                .bind_device(&self.db1, self.dir1.clone(), &self.founder)
                .await
                .expect("reopen first branch publisher")
                .resume_circle_operations()
                .await
                .expect("publish the first captured branch");
        }
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

    /// Author a second Circle entry at one author-stream position on device 1:
    /// publish a rename, settle it, then prepare another from the authoring
    /// context captured before it. Only a device that ignores its own durable
    /// operation journal authors that way, and this plays it exactly.
    ///
    /// Returns the refusal device 1's own verifier raises and the prepared
    /// operation behind it, so a test can also put the commit in front of a
    /// peer that never ran the local check.
    pub(super) async fn attempt_one_position_fork(&self) -> OnePositionFork {
        let store = self
            .store
            .bind_device(&self.db1, self.dir1.clone(), &self.founder)
            .await
            .expect("authorize device 1 authoring");
        let mut authority = store
            .authorize_writer()
            .await
            .expect("authorize Circle writer");
        let mut circles = authority.circles();
        let (stale_state, stale_activation) = circles
            .authoring_context_for_test(self.circle_id)
            .await
            .expect("capture the founder authoring context");
        drop(circles);
        drop(authority);
        drop(store);

        self.store1()
            .await
            .circles()
            .rename_circle("0000000001200-0000-device1", self.circle_id, "Alpha")
            .await
            .expect("device 1 authors the first successor");
        self.pull_device1().await;

        let store = self
            .store
            .bind_device(&self.db1, self.dir1.clone(), &self.founder)
            .await
            .expect("authorize device 1 re-authoring");
        let mut authority = store
            .authorize_writer()
            .await
            .expect("authorize Circle writer");
        let mut circles = authority.circles();
        let prepared = circles
            .preparer()
            .prepare_request(CircleOperationRequest::Rename(Box::new(
                super::commands::CircleRenameRequest {
                    circle_id: self.circle_id,
                    name: "Beta".to_string(),
                    metadata_stamp: "0000000001300-0000-device1".to_string(),
                    current: stale_state,
                    previous_control: stale_activation,
                },
            )))
            .await
            .expect("prepare a second successor from the stale context");
        StoreDatabase::new(&self.db1)
            .insert_circle_operation(prepared.journal.clone(), prepared.prepared_objects)
            .await
            .expect("journal the second successor");
        let refusal = circles
            .publish_prepared_operation_for_test(&prepared.journal.operation_id, None)
            .await
            .expect_err("a device's own verifier refuses its second entry at one position");
        drop(circles);
        drop(authority);
        drop(store);

        OnePositionFork {
            refusal,
            journal: prepared.journal,
        }
    }

    /// Discard a refused operation on device 1, as its initiator would.
    pub(super) async fn discard_device1(
        &self,
        operation_id: &CircleOperationId,
    ) -> Result<(), CircleOperationError> {
        self.store1()
            .await
            .circles()
            .discard_circle_operation(operation_id)
            .await
    }

    /// Put the refused commit in front of device 2, which never ran device 1's
    /// local check: publish every object the operation owns, then verify the
    /// commit as device 2 does when it pulls.
    pub(super) async fn peer_verifies(
        &self,
        journal: &CircleOperationJournal,
    ) -> Result<coven_protocol::circle_activation::VerifiedCircleActivations, CircleOperationError>
    {
        super::publish_prepared_objects(&self.store, &self.db1, journal).await;
        let commit = journal.commit().expect("parse the refused commit");
        let commit_ref = journal.operation().commit_ref().clone();
        let author = StoreDatabase::new(&self.db1)
            .activated_store_device_registration(commit.author_registration.clone())
            .await
            .expect("load the refused commit's author registration");
        self.store
            .bind_device(&self.db2, self.dir2.clone(), &self.founder)
            .await
            .expect("bind device 2")
            .load_circle_activations(&commit_ref, &commit, author.value())
            .await
    }

    pub(super) async fn assert_resolution_activated(&self, journal: &CircleOperationJournal) {
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

    /// Prepare and durably journal a resolution without publishing it, matching
    /// the state left by a crash between the command and publication.
    pub(super) async fn journal_resolution(
        &self,
        chosen: &CircleControlCoord,
    ) -> CircleOperationJournal {
        let store = self
            .store
            .bind_device(&self.db1, self.dir1.clone(), &self.founder)
            .await
            .expect("authorize Circle resolution");
        let mut authority = store
            .authorize_writer()
            .await
            .expect("authorize Circle writer");
        let mut circles = authority.circles();
        let request = circles
            .resolution_request_for_test(
                self.circle_id,
                chosen,
                self.conflict_branches_device1().await,
            )
            .await
            .expect("build resolution request");
        let prepared = circles
            .preparer()
            .prepare_request(request)
            .await
            .expect("prepare resolution operation");
        StoreDatabase::new(&self.db1)
            .insert_circle_operation(prepared.journal.clone(), prepared.prepared_objects)
            .await
            .expect("journal the resolution before publication");
        prepared.journal
    }
}
