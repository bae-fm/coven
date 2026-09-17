//! Where a Circle roster or metadata entry entered accepted Store history.
//!
//! An entry is published exactly once, by the commit whose signing device
//! authored it. Every later control that still carries the entry names that
//! exact earlier accepted activation. These tests exercise both halves: the
//! introduction proof a publishing commit owes, and the inheritance proof every
//! successor owes.

use super::*;
use coven_protocol::store_commit::{
    CircleEntryOrigin, CircleRosterEntryRef, StoreCommitCoord as Coord,
};

/// A founder Circle prepared but not yet published, with everything a test
/// needs to re-sign its activating commit around a tampered object graph.
struct PreparedFounder {
    db: Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
    store: Arc<TestStore>,
    cloud_storage: Arc<dyn CloudSyncObjectStorage>,
    founder: UserKeypair,
    journal: CircleOperationJournal,
    old_commit: StoreBatchCommit,
    author: coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
}

impl PreparedFounder {
    async fn build(label: &str) -> Self {
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let founder = UserKeypair::generate();
        let (store, cloud_storage) = TestStore::create_with_connection(
            &db,
            db_store_dir.clone(),
            label,
            founder.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create exact Circle test Store");
        let journal = store
            .bind_device_in(&db, db_store_dir.clone(), &founder)
            .await
            .expect("bind Circle preparation Store")
            .prepare_circle_operation("0000000001000-0000-founder", "Household")
            .await
            .expect("prepare founder Circle")
            .journal;
        let old_commit = journal.commit().expect("parse prepared Store commit");
        let author = StoreDatabase::new(&db)
            .activated_store_device_registration(old_commit.author_registration.clone())
            .await
            .expect("load exact Circle commit author");
        Self {
            db,
            db_store_dir,
            store,
            cloud_storage,
            founder,
            journal,
            old_commit,
            author,
        }
    }

    /// Re-prepare the founder's objects from `draft`, apply `tamper` to the
    /// signed object graph, publish every object, and activate the result.
    async fn activation_error(
        &self,
        draft: CircleTransitionDraft,
        tamper: impl FnOnce(&mut coven_protocol::store_commit::CircleActivationObjects),
    ) -> CircleOperationError {
        let device = self
            .store
            .bind_device_in(&self.db, self.db_store_dir.clone(), &self.founder)
            .await
            .expect("bind Circle object Store");
        let (creation, mut objects, prepared) = device
            .prepare_circle_activation_objects(draft, &self.journal.operation().history)
            .await
            .expect("prepare exact Circle objects");
        for object in prepared.values() {
            self.cloud_storage
                .create_protocol_object(object)
                .await
                .expect("publish exact Circle object");
            install_substituted_object(&self.db, object).await;
        }
        tamper(&mut objects);
        let commit_coord = self.journal.operation().commit_ref().coord.clone();
        let circle_reference = creation.control_ref(objects);
        let commit = device
            .sign_circle_commit(&self.old_commit, commit_coord.clone(), circle_reference)
            .await
            .expect("sign tampered Circle commit");
        let Coord { stream_id, .. } = commit_coord.clone();
        let commit_prepared = device
            .prepare_circle_object(
                &ProtocolObjectContext::signed_plaintext(
                    commit.store_root_hash,
                    ProtocolObjectDomain::StoreCommit,
                ),
                &commit_semantic_prefix(
                    commit.candidate_family(),
                    &stream_id.to_string(),
                    commit.seq(),
                    commit.commit_hash(),
                ),
                ".json",
                commit.to_bytes(),
            )
            .await
            .expect("prepare tampered Circle Store commit");
        self.cloud_storage
            .create_protocol_object(&commit_prepared)
            .await
            .expect("publish tampered Circle Store commit");
        let commit_ref = StoreBatchCommitRef::from_commit(
            &commit,
            commit_coord,
            commit_prepared.reference().clone(),
        )
        .expect("bind tampered Circle Store commit");
        self.store
            .bind_device_in(&self.db, self.db_store_dir.clone(), &self.founder)
            .await
            .expect("bind tampered Circle activation Store")
            .load_circle_activations(&commit_ref, &commit, self.author.value())
            .await
            .expect_err("a tampered Circle provenance graph must be rejected")
    }

    fn draft(&self) -> CircleTransitionDraft {
        draft_from_transition(&self.journal.operation().creation)
    }

    /// A well-formed commit reference this Store never accepted.
    fn foreign_commit_ref(&self) -> StoreBatchCommitRef {
        let bytes = b"a Circle activation this Store never accepted";
        StoreBatchCommitRef {
            coord: Coord {
                stream_id: coven_protocol::membership::AuthorStreamId::from_digest(
                    ObjectHash::digest(bytes),
                ),
                sequence: 9,
            },
            commit_hash: ObjectHash::digest(bytes),
            object: ExactObjectRef::new(
                coven_protocol::objects::ObjectSlot::logical(
                    "store-v1/test/foreign-activation.json".to_string(),
                )
                .expect("valid foreign commit slot"),
                bytes.len() as u64,
                ObjectHash::digest(bytes),
            ),
        }
    }
}

#[tokio::test]
async fn activation_rejects_an_introduced_entry_authored_by_another_device() {
    let fixture = PreparedFounder::build("circle-introduced-foreign-device").await;
    let mut draft = fixture.draft();
    // The device that signs the activating Store commit is the entry's
    // device-authorship proof, so an entry naming another device has none.
    let CircleRosterDraftPolicy::Founder { entry } = &draft.policy.roster else {
        panic!("a founder draft carries a founder roster entry")
    };
    let relabelled = coven_protocol::circle::CircleRosterEntry::founder(
        entry.store_root_hash,
        entry.circle_id,
        "another-device",
        entry.author_owner_grant.clone(),
        &fixture.founder,
    );
    draft.policy.roster = CircleRosterDraftPolicy::Founder { entry: relabelled };

    let error = fixture.activation_error(draft, |_| {}).await;

    assert!(
        error.to_string().contains(
            "introduced Circle roster entry was not authored by the device that signed its \
             activating commit"
        ),
        "{error}"
    );
}

#[tokio::test]
async fn activation_rejects_an_inherited_entry_outside_the_accepted_predecessor_history() {
    let fixture = PreparedFounder::build("circle-inherited-foreign-activation").await;
    let foreign = fixture.foreign_commit_ref();
    let draft = fixture.draft();

    let error = fixture
        .activation_error(draft, |objects| {
            for entry in objects.roster_entries.values_mut() {
                entry.origin = CircleEntryOrigin::Inherited {
                    activating_commit: foreign.clone(),
                };
            }
        })
        .await;

    assert!(
        error
            .to_string()
            .contains("outside its accepted predecessor history"),
        "{error}"
    );
}

#[tokio::test]
async fn activation_rejects_a_roster_inventory_its_frontier_never_reaches() {
    let fixture = PreparedFounder::build("circle-unreachable-roster-entry").await;
    let draft = fixture.draft();

    let error = fixture
        .activation_error(draft, |objects| {
            let (coord, reference) = objects
                .roster_entries
                .iter()
                .next()
                .map(|(coord, reference)| (coord.clone(), reference.clone()))
                .expect("founder graph carries a roster entry");
            let mut unreachable = coord;
            unreachable.seq = 9;
            objects.roster_entries.insert(
                unreachable,
                CircleRosterEntryRef {
                    object: reference.object,
                    origin: CircleEntryOrigin::Introduced,
                },
            );
        })
        .await;

    assert!(
        error
            .to_string()
            .contains("Circle roster inventory differs from the history its frontier reaches"),
        "{error}"
    );
}

#[tokio::test]
async fn activation_rejects_a_metadata_inventory_its_frontier_never_reaches() {
    let fixture = PreparedFounder::build("circle-unreachable-metadata-entry").await;
    let draft = fixture.draft();

    let error = fixture
        .activation_error(draft, |objects| {
            let (coord, reference) = objects
                .metadata_entries
                .iter()
                .next()
                .map(|(coord, reference)| (coord.clone(), reference.clone()))
                .expect("founder graph carries a metadata entry");
            let mut unreachable = coord;
            unreachable.seq = 9;
            objects.metadata_entries.insert(unreachable, reference);
        })
        .await;

    assert!(
        error
            .to_string()
            .contains("Circle metadata inventory differs from the history its frontier reaches"),
        "{error}"
    );
}

/// A published Circle renamed once, so its current control inherits every entry
/// the founder introduced.
struct RenamedCircle {
    db: Database,
    store: Arc<TestStore>,
    circle_id: CircleId,
    founder_pubkey: String,
}

impl RenamedCircle {
    async fn build(label: &str) -> Self {
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let founder = UserKeypair::generate();
        let (store, _cloud_storage) = TestStore::create_with_connection(
            &db,
            db_store_dir.clone(),
            label,
            founder.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create exact Circle test Store");
        let device = store
            .bind_device_in(&db, db_store_dir.clone(), &founder)
            .await
            .expect("bind Circle authoring Store");
        let circle_id = device
            .create_circle("0000000001000-0000-founder", "Household")
            .await
            .expect("create founder Circle");
        device
            .rename_circle("0000000002000-0000-founder", circle_id, "Cottage")
            .await
            .expect("rename the Circle");
        device
            .rename_circle("0000000003000-0000-founder", circle_id, "Lodge")
            .await
            .expect("rename the Circle again");
        Self {
            db,
            store,
            circle_id,
            founder_pubkey: keys::public_key_hex(&founder),
        }
    }
}

/// Across three controls — founder, rename, rename — every entry the newest
/// control carries still names the exact activation that introduced it, not the
/// one it was carried through. The founder's roster entry names the founder's
/// commit two controls later, and the first rename's metadata entry names the
/// first rename's commit.
#[tokio::test]
async fn an_inherited_entry_resolves_to_the_activation_that_introduced_it() {
    let fixture = RenamedCircle::build("circle-inherited-entry-provenance").await;
    let database = StoreDatabase::new(&fixture.db);
    let (current, _) = database
        .circle_authoring_context(fixture.circle_id, &fixture.founder_pubkey)
        .await
        .expect("read the renamed Circle's current control");
    let (renamed, _) = database
        .verified_circle_activation_context(
            fixture.store.root().clone(),
            fixture.circle_id,
            current.control.coord.clone(),
        )
        .await
        .expect("read the renamed activation")
        .expect("the renamed control is retained");
    let objects = renamed.reference.objects().clone();

    // Each rename authored its own metadata and inherited everything else, so
    // the newest control carries one roster entry and three metadata entries.
    let roster = objects.roster_entries.clone();
    assert_eq!(roster.len(), 1, "a renamed Circle keeps one roster entry");
    assert_eq!(
        objects.metadata_entries.len(),
        3,
        "each control introduced one metadata entry"
    );
    let (roster_coord, inherited) = roster.iter().next().expect("one inherited roster entry");
    let CircleEntryOrigin::Inherited { activating_commit } = &inherited.origin else {
        panic!("a rename inherits the founder's roster entry")
    };

    // The named activation is retained, and its own graph introduced that exact
    // entry at that exact object.
    let founder = database
        .retained_circle_activation(
            fixture.store.root().clone(),
            fixture.circle_id,
            activating_commit.clone(),
        )
        .await
        .expect("resolve the named activation")
        .expect("the introducing activation is retained");
    let founder_objects = founder.reference.objects().clone();
    let introduced = founder_objects
        .roster_entries
        .get(roster_coord)
        .expect("the named activation introduced this entry");
    assert_eq!(introduced.object, inherited.object);
    assert_eq!(introduced.origin, CircleEntryOrigin::Introduced);
    // The founder control is two controls back, so the roster entry survived a
    // carry through the first rename without its origin moving to it.
    assert_eq!(
        founder_objects.metadata_entries.len(),
        1,
        "the resolved activation is the founder's, not the first rename's"
    );

    // Every metadata entry resolves the same way, each to its own introduction:
    // the two inherited ones to the two earlier activations, the newest to this
    // control itself.
    let mut introductions = Vec::new();
    for (coord, entry) in &objects.metadata_entries {
        let CircleEntryOrigin::Inherited { activating_commit } = &entry.origin else {
            continue;
        };
        let accepted = database
            .retained_circle_activation(
                fixture.store.root().clone(),
                fixture.circle_id,
                activating_commit.clone(),
            )
            .await
            .expect("resolve the named activation")
            .expect("the introducing activation is retained");
        let introduced = accepted
            .reference
            .objects()
            .metadata_entries
            .get(coord)
            .expect("the named activation introduced this metadata entry");
        assert_eq!(introduced.object, entry.object);
        assert_eq!(introduced.origin, CircleEntryOrigin::Introduced);
        introductions.push(activating_commit.clone());
    }
    assert_eq!(
        introductions.len(),
        2,
        "two of the three metadata entries are inherited"
    );
    introductions.dedup();
    assert_eq!(
        introductions.len(),
        2,
        "each inherited metadata entry names its own introduction, not one shared commit"
    );
}

/// A published founder Circle plus a prepared, unpublished rename of it. The
/// rename covers the founder control, so it owes the inheritance proof for
/// every entry the founder published.
pub(super) struct PreparedSuccessor {
    db: Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
    store: Arc<TestStore>,
    cloud_storage: Arc<dyn CloudSyncObjectStorage>,
    founder: UserKeypair,
    /// The activating commit of a second Circle: an accepted commit that
    /// carries a Circle control, but not one for this Circle.
    another_circles_activation: StoreBatchCommitRef,
    /// A Store member admitted outside the Circle: it holds no Circle key, so
    /// it verifies exactly the public arm of a Circle activation.
    peer: UserKeypair,
    journal: CircleOperationJournal,
    prepared_objects: coven_database::PreparedCircleObjects,
    old_commit: StoreBatchCommit,
    author: coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
}

impl PreparedSuccessor {
    pub(super) async fn build(label: &str) -> Self {
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let founder = UserKeypair::generate();
        let (store, cloud_storage) = TestStore::create_with_connection(
            &db,
            db_store_dir.clone(),
            label,
            founder.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create exact Circle test Store");
        let peer = UserKeypair::generate();
        store
            .admit_member(
                &db,
                db_store_dir.clone(),
                &founder,
                &keys::public_key_hex(&peer),
                None,
                coven_protocol::membership::MemberRole::Member,
                &EncryptionService::from_key([43; 32]),
                "Circle provenance nonrecipient",
            )
            .await
            .expect("admit a Store member outside the Circle");
        let device = store
            .bind_device_in(&db, db_store_dir.clone(), &founder)
            .await
            .expect("bind Circle authoring Store");
        let circle_id = device
            .create_circle("0000000001000-0000-founder", "Household")
            .await
            .expect("create founder Circle");
        let another_circle = device
            .create_circle("0000000001500-0000-founder", "Allotment")
            .await
            .expect("create a second Circle in the same Store");

        // Prepare a rename without publishing it, so its object graph can be
        // tampered with before its activating commit is signed.
        let identity_pubkey = keys::public_key_hex(&founder);
        let (_, another_circles_activation) = StoreDatabase::new(&db)
            .circle_authoring_context(another_circle, &identity_pubkey)
            .await
            .expect("read the second Circle's authoring context");
        let (current, activation_commit_ref) = StoreDatabase::new(&db)
            .circle_authoring_context(circle_id, &identity_pubkey)
            .await
            .expect("read the founder authoring context");
        let activation_commit = device
            .load_commit_for_test(&activation_commit_ref)
            .await
            .expect("load the founder activating commit");
        let previous_control = coven_protocol::circle_journal::CircleControlActivation {
            reference: activation_commit
                .value()
                .circle_controls()
                .iter()
                .find(|reference| {
                    reference.circle_id() == circle_id
                        && reference.control() == &current.control.coord
                })
                .expect("founder control is present in its activating commit")
                .clone(),
            activating_commit: activation_commit_ref,
        };
        let mut authority = device
            .authorize_writer()
            .await
            .expect("authorize Circle successor writer");
        let journal = authority
            .circles()
            .preparer()
            .prepare_request(CircleOperationRequest::Rename(Box::new(
                super::commands::CircleRenameRequest {
                    circle_id,
                    name: "Cottage".to_string(),
                    metadata_stamp: "0000000002000-0000-founder".to_string(),
                    current,
                    previous_control,
                },
            )))
            .await
            .expect("prepare Circle rename");
        drop(authority);
        let prepared_objects = journal.prepared_objects;
        let journal = journal.journal;
        let old_commit = journal.commit().expect("parse prepared successor commit");
        let author = StoreDatabase::new(&db)
            .activated_store_device_registration(old_commit.author_registration.clone())
            .await
            .expect("load exact successor commit author");
        Self {
            db,
            db_store_dir,
            store,
            cloud_storage,
            founder,
            another_circles_activation,
            journal,
            prepared_objects,
            old_commit,
            author,
            peer,
        }
    }

    fn objects(&self) -> coven_protocol::store_commit::CircleActivationObjects {
        let [reference] = self.old_commit.circle_controls() else {
            panic!("a Circle successor commit carries one control reference")
        };
        reference.objects().clone()
    }

    /// Publish the rename's prepared objects, apply `tamper` to the signed
    /// object graph, re-sign the activating commit around it, and activate as
    /// the Circle's own Owner.
    async fn activation_error(
        &self,
        tamper: impl FnOnce(&mut coven_protocol::store_commit::CircleActivationObjects),
    ) -> CircleOperationError {
        let founder = self.founder.clone();
        self.activate_as(&founder, tamper)
            .await
            .expect_err("a tampered Circle successor must be rejected")
    }

    /// The same, activated by the Store member who holds no Circle key.
    pub(super) async fn nonrecipient_activation(
        &self,
        tamper: impl FnOnce(&mut coven_protocol::store_commit::CircleActivationObjects),
    ) -> Result<coven_protocol::circle_activation::VerifiedCircleActivations, CircleOperationError>
    {
        let peer = self.peer.clone();
        self.activate_as(&peer, tamper).await
    }

    async fn activate_as(
        &self,
        identity: &UserKeypair,
        tamper: impl FnOnce(&mut coven_protocol::store_commit::CircleActivationObjects),
    ) -> Result<coven_protocol::circle_activation::VerifiedCircleActivations, CircleOperationError>
    {
        let device = self
            .store
            .bind_device_in(&self.db, self.db_store_dir.clone(), &self.founder)
            .await
            .expect("bind Circle object Store");
        for object in self.prepared_objects.values() {
            self.cloud_storage
                .create_protocol_object(object)
                .await
                .expect("publish the rename's exact object");
            install_substituted_object(&self.db, object).await;
        }
        let mut objects = self.objects();
        tamper(&mut objects);
        let commit_coord = self.journal.operation().commit_ref().coord.clone();
        let circle_reference = self.journal.operation().creation.control_ref(objects);
        let commit = device
            .sign_circle_commit(&self.old_commit, commit_coord.clone(), circle_reference)
            .await
            .expect("sign tampered Circle successor commit");
        let Coord { stream_id, .. } = commit_coord.clone();
        let commit_prepared = device
            .prepare_circle_object(
                &ProtocolObjectContext::signed_plaintext(
                    commit.store_root_hash,
                    ProtocolObjectDomain::StoreCommit,
                ),
                &commit_semantic_prefix(
                    commit.candidate_family(),
                    &stream_id.to_string(),
                    commit.seq(),
                    commit.commit_hash(),
                ),
                ".json",
                commit.to_bytes(),
            )
            .await
            .expect("prepare tampered successor Store commit");
        self.cloud_storage
            .create_protocol_object(&commit_prepared)
            .await
            .expect("publish tampered successor Store commit");
        let commit_ref = StoreBatchCommitRef::from_commit(
            &commit,
            commit_coord,
            commit_prepared.reference().clone(),
        )
        .expect("bind tampered successor Store commit");
        self.store
            .bind_device_in(&self.db, self.db_store_dir.clone(), identity)
            .await
            .expect("bind Circle activation Store")
            .load_circle_activations(&commit_ref, &commit, self.author.value())
            .await
    }

    /// The founder's roster entry, which the rename inherits unchanged.
    pub(super) fn inherited_roster_entry(&self) -> coven_protocol::circle::CircleRosterCoord {
        self.objects()
            .roster_entries
            .keys()
            .next()
            .cloned()
            .expect("the rename inherits the founder roster entry")
    }

    /// The metadata entry the rename itself introduces.
    fn introduced_metadata_entry(&self) -> coven_protocol::circle::CircleMetadataCoord {
        self.objects()
            .metadata_entries
            .iter()
            .find(|(_, reference)| reference.origin == CircleEntryOrigin::Introduced)
            .map(|(coord, _)| coord.clone())
            .expect("the rename introduces its own metadata entry")
    }
}

#[tokio::test]
async fn activation_rejects_a_successor_that_drops_an_entry_its_predecessor_published() {
    let fixture = PreparedSuccessor::build("circle-successor-drops-entry").await;
    let inherited = fixture.inherited_roster_entry();

    let error = fixture
        .activation_error(|objects| {
            objects.roster_entries.remove(&inherited);
        })
        .await;

    assert!(
        error
            .to_string()
            .contains("Circle control drops or replaces an entry a covered predecessor published"),
        "{error}"
    );
}

#[tokio::test]
async fn activation_rejects_a_successor_that_replaces_an_entry_its_predecessor_published() {
    let fixture = PreparedSuccessor::build("circle-successor-replaces-entry").await;
    let inherited = fixture.inherited_roster_entry();
    let substitute = fixture
        .objects()
        .metadata_entries
        .values()
        .next()
        .expect("the rename carries a metadata entry")
        .object
        .clone();

    let error = fixture
        .activation_error(|objects| {
            objects
                .roster_entries
                .get_mut(&inherited)
                .expect("the rename inherits the founder roster entry")
                .object = substitute;
        })
        .await;

    assert!(
        error
            .to_string()
            .contains("Circle control drops or replaces an entry a covered predecessor published"),
        "{error}"
    );
}

#[tokio::test]
async fn activation_rejects_a_successor_that_renames_the_activation_an_entry_came_from() {
    let fixture = PreparedSuccessor::build("circle-successor-renames-introduction").await;
    let inherited = fixture.inherited_roster_entry();
    let foreign = fixture.another_circles_activation.clone();

    let error = fixture
        .activation_error(|objects| {
            objects
                .roster_entries
                .get_mut(&inherited)
                .expect("the rename inherits the founder roster entry")
                .origin = CircleEntryOrigin::Inherited {
                activating_commit: foreign,
            };
        })
        .await;

    assert!(
        error
            .to_string()
            .contains("Circle control drops or replaces an entry a covered predecessor published"),
        "{error}"
    );
}

/// Re-introducing an entry an earlier accepted control already published is the
/// equivocation the removed create-once successor slot used to make impossible.
/// Monotone inheritance is what refuses it now: the successor authored the
/// founder entry itself, so the device check would pass.
#[tokio::test]
async fn activation_rejects_a_successor_that_re_introduces_an_inherited_entry() {
    let fixture = PreparedSuccessor::build("circle-successor-reintroduces-entry").await;
    let inherited = fixture.inherited_roster_entry();

    let error = fixture
        .activation_error(|objects| {
            objects
                .roster_entries
                .get_mut(&inherited)
                .expect("the rename inherits the founder roster entry")
                .origin = CircleEntryOrigin::Introduced;
        })
        .await;

    assert!(
        error
            .to_string()
            .contains("Circle control drops or replaces an entry a covered predecessor published"),
        "{error}"
    );
}

/// An entry no covered predecessor carries is proved by its own origin: the
/// named activation's graph must introduce that exact coordinate.
#[tokio::test]
async fn activation_rejects_an_inherited_entry_its_named_activation_never_introduced() {
    let fixture = PreparedSuccessor::build("circle-inherited-entry-absent-there").await;
    let introduced = fixture.introduced_metadata_entry();
    let founder_activation = {
        let objects = fixture.objects();
        objects
            .roster_entries
            .values()
            .find_map(|entry| entry.origin.inherited_from().cloned())
            .expect("the rename inherits the founder roster entry")
    };

    let error = fixture
        .activation_error(|objects| {
            objects
                .metadata_entries
                .get_mut(&introduced)
                .expect("the rename introduces its own metadata entry")
                .origin = CircleEntryOrigin::Inherited {
                activating_commit: founder_activation,
            };
        })
        .await;

    assert!(
        error.to_string().contains(
            "inherited Circle metadata entry was not introduced by the accepted activation it \
             names"
        ),
        "{error}"
    );
}

/// An `Inherited` origin must name a commit that activated a control for *this*
/// Circle. Another Circle's activation is an accepted commit and sits in the
/// predecessor history, and is still refused.
#[tokio::test]
async fn activation_rejects_an_inherited_entry_naming_a_commit_without_a_control_for_the_circle() {
    let fixture = PreparedSuccessor::build("circle-inherited-entry-foreign-circle").await;
    let introduced = fixture.introduced_metadata_entry();
    let another = fixture.another_circles_activation.clone();

    let error = fixture
        .activation_error(|objects| {
            objects
                .metadata_entries
                .get_mut(&introduced)
                .expect("the rename introduces its own metadata entry")
                .origin = CircleEntryOrigin::Inherited {
                activating_commit: another,
            };
        })
        .await;

    assert!(
        error
            .to_string()
            .contains("which activates no control for it"),
        "{error}"
    );
}
