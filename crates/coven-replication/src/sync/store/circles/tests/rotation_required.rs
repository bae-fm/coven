use super::*;
use coven_database::Database;
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::sync::cycle::SyncComponents;
use crate::sync::test_helpers::TestDevice;
use coven_keys::keys::MasterKeyCustody;

fn circle_routing_migrations() -> Vec<coven_database::Migration> {
    vec![coven_database::Migration::sql(
        1,
        "Circle routing schema",
        "CREATE TABLE documents (
                 id TEXT PRIMARY KEY,
                 audience TEXT,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE document_files (
                 id TEXT PRIMARY KEY,
                 document_id TEXT NOT NULL REFERENCES documents(id),
                 size INTEGER NOT NULL,
                 hash TEXT NOT NULL,
                 _updated_at TEXT NOT NULL
             ) STRICT;",
    )]
}

/// `documents` is scoped by its audience column; `document_files` carries a blob
/// and inherits its document's audience, so moving a document between audiences
/// republishes its file's ciphertext under the destination audience's locator.
fn circle_routing_tables() -> Vec<coven_protocol::synced_schema::SyncedTable> {
    vec![
        coven_protocol::synced_schema::SyncedTable::new(
            "documents",
            coven_protocol::synced_schema::RowIdentity::IndependentUuid,
        )
        .scoped_by("audience"),
        coven_protocol::synced_schema::SyncedTable::new(
            "document_files",
            coven_protocol::synced_schema::RowIdentity::IndependentUuid,
        )
        .inherits_audience_through("document_id")
        .carries_blob(coven_protocol::synced_schema::BlobDecl::new(
            "files",
            coven_protocol::blob::Provenance::HostProvided,
            coven_protocol::blob::CacheFill::CacheEager,
        )),
    ]
}

fn open_circle_routing_test_db(store_dir: coven_foundation::store_dir::StoreDir) -> Database {
    crate::sync::test_helpers::open_test_db_schema(
        store_dir,
        circle_routing_tables(),
        circle_routing_migrations(),
    )
}

/// A two-member Store with an activated Circle whose roster names both the owner
/// and one member. The owner drives every operation through the production sync
/// components; the member exists only as a Store identity and Circle roster
/// entry whose removal makes the Circle rotation-required.
struct RotationFixture {
    db: Database,
    store: std::sync::Arc<TestStore>,
    cloud_storage: Arc<coven_storage::CloudSyncConnection>,
    home: Arc<coven_storage::InMemoryCloudHome>,
    owner_device: TestDevice,
    signer: UserKeypair,
    components: SyncComponents,
    circle_id: CircleId,
    member: UserKeypair,
    member_pubkey: String,
    member_db: Database,
    member_device: RotationMemberDevice,
    store_dir: coven_foundation::store_dir::StoreDir,
    custody: crate::sync::test_helpers::TestCustody,
}

struct RotationMemberDevice {
    device: TestDevice,
    store_dir: coven_foundation::store_dir::StoreDir,
}

impl RotationMemberDevice {
    async fn pull(&self) {
        self.device
            .pull_store()
            .await
            .expect("member installs the Circle bootstrap");
    }

    async fn publish_acknowledgements(&self, stamp: &str) {
        let frontier = coven_protocol::store_commit::CommitFrontier::from_refs(
            self.device
                .materialized_frontier()
                .await
                .expect("read member frontier"),
        )
        .expect("shape member frontier");
        self.device
            .stage_acknowledgement(frontier.clone(), stamp.to_string())
            .await
            .expect("stage member Store acknowledgement");
        self.device
            .stage_circle_acknowledgements(&frontier, stamp)
            .await
            .expect("stage member Circle acknowledgement");
        self.device
            .drain_acknowledgements()
            .await
            .expect("publish member acknowledgements");
    }
}

impl RotationFixture {
    /// Reclaim the way the sync cycle does, with the Store's row-routing
    /// encryption in hand.
    ///
    /// This Store routes its rows, so reclaim needs the key: advancing the
    /// replay baseline rebuilds the image from a scoped replay, which cannot be
    /// projected without it. Passing `None` here would fail for a reason that
    /// has nothing to do with what any of these tests are about.
    async fn reclaim_packages(
        &self,
    ) -> Result<crate::sync::store::StoreReclaimResult, crate::sync::store::StoreReclaimError> {
        let result = self.owner_device.reclaim_packages().await?;
        assert_eq!(
            result.stuck,
            0,
            "reclaim must not leave failed operations: {:?}",
            StoreDatabase::new(&self.db)
                .stuck_reclaim_operations()
                .await?,
        );
        Ok(result)
    }

    async fn build(label: &str) -> Self {
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = open_circle_routing_test_db(db_store_dir.clone());
        let (store_fixture, _home, signer, founder) =
            persist_merge_operation_fixture(&db, db_store_dir.clone(), label).await;
        let (store, cloud_storage) = store_fixture;
        let circle_id = founder.circle_id();
        let owner_device = store
            .bind_device_in(&db, db_store_dir.clone(), &signer)
            .await
            .expect("bind Circle test Store");
        owner_device
            .resume_circle_operations()
            .await
            .expect("activate founder transition");

        let member = UserKeypair::generate();
        let member_pubkey = keys::public_key_hex(&member);
        store
            .admit_member(
                &db,
                db_store_dir.clone(),
                &signer,
                &member_pubkey,
                None,
                MemberRole::Member,
                &EncryptionService::from_key([42; 32]),
                "Rotation Store",
            )
            .await
            .expect("admit Store member");
        let member_db_store_dir = crate::sync::test_helpers::test_store_dir();
        let member_db = open_circle_routing_test_db(member_db_store_dir.clone());
        let member_device = store
            .activate_joined_device(
                &db,
                db_store_dir.clone(),
                &member_db,
                member_db_store_dir.clone(),
                &member,
                "2026-07-23T00:00:00Z",
            )
            .await
            .expect("activate Store member device");
        let member_device = RotationMemberDevice {
            device: member_device,
            store_dir: member_db_store_dir,
        };

        let custody = crate::sync::test_helpers::TestCustody::default();
        custody.set_initial_key([42; 32]);
        let store_dir = db_store_dir;
        let components = prepare_owner_sync_components(
            &db,
            &store,
            &_home,
            &store_dir,
            &signer,
            label,
            Arc::new(custody.clone()),
        )
        .await;
        components
            .add_circle_member(circle_id, member_pubkey.clone(), CircleRole::Member)
            .await
            .expect("add Circle member");

        Self {
            db,
            store,
            cloud_storage,
            home: _home,
            owner_device,
            signer,
            components,
            circle_id,
            member,
            member_pubkey,
            member_db,
            member_device,
            store_dir,
            custody,
        }
    }

    async fn remove_store_member(&self) {
        self.components
            .remove_member(&self.member_pubkey)
            .await
            .expect("remove Store member");
    }

    /// Removes the member from the Circle roster — which opens the epoch close —
    /// then publishes the owner's close response and activates the successor.
    async fn close_epoch_by_removing_the_circle_member(&self) {
        self.components
            .remove_circle_member(self.circle_id, self.member_pubkey.clone())
            .await
            .expect("close the epoch by removing the roster member");
        finalize_circle_epoch_close(
            &self.store,
            &self.db,
            self.store_dir.clone(),
            &self.signer,
            &self.components,
        )
        .await;
    }

    /// Authors one standalone Circle snapshot into a throwaway directory.
    async fn author_standalone_circle_snapshot(&self, stamp: &str) {
        let snapshot_temp = tempfile::tempdir().expect("snapshot temp dir");
        self.store
            .push_circle_snapshots(
                &self.db,
                self.store_dir.clone(),
                snapshot_temp.path().to_path_buf(),
                self.db.schema_version(),
                stamp,
                &EncryptionService::from_key([42; 32]),
            )
            .await
            .expect("author the standalone Circle snapshot");
    }

    async fn capture_document(
        &self,
        row_id: &str,
        audience: Option<CircleId>,
        stamp: &str,
    ) -> coven_protocol::write::WriteId {
        self.db
            .capture_document_for_test(row_id, audience, stamp)
            .await
            .expect("capture document row")
    }

    async fn active_store_members(&self) -> BTreeSet<String> {
        self.store
            .bind_device(&self.db, self.store_dir.clone(), &self.signer)
            .await
            .expect("load cycle Store")
            .membership_for_test()
            .await
            .expect("load cycle membership")
            .current_members()
            .into_iter()
            .map(|(pubkey, _)| pubkey)
            .collect()
    }

    async fn circles(&self) -> Vec<coven_protocol::circle::CircleInfo> {
        let members = self.active_store_members().await;
        StoreDatabase::new(&self.db)
            .get_circles(&keys::public_key_hex(&self.signer), members)
            .await
            .expect("list Circles")
    }

    async fn latest_circle_snapshot(
        &self,
        circle_id: CircleId,
    ) -> Option<coven_database::PublishedCircleSnapshot> {
        StoreDatabase::new(&self.db)
            .latest_local_circle_snapshot(circle_id)
            .await
            .expect("read latest local Circle snapshot")
    }

    async fn pending_circle_snapshot(
        &self,
        circle_id: CircleId,
    ) -> Option<coven_database::DurableCircleSnapshotPublication> {
        StoreDatabase::new(&self.db)
            .outbound_circle_snapshot_publication(circle_id)
            .await
            .expect("read pending Circle snapshot publication")
    }

    async fn drive_circle_snapshots(
        &self,
        stamp: &str,
    ) -> Result<(), crate::sync::store::SnapshotError> {
        self.owner_device
            .authorize_writer()
            .await
            .map_err(crate::sync::store::SnapshotError::from)?
            .circles()
            .snapshots()
            .push_circle_snapshots(
                self.db.schema_version(),
                stamp,
                Some(&EncryptionService::from_key([42; 32])),
            )
            .await
    }

    async fn release_retained_replay_ownership(&self) {
        self.db
            .release_retained_replay_ownership_for_test()
            .await
            .expect("release retained replay ownership");
    }

    async fn capture_document_with_file(
        &self,
        document_id: &str,
        file_id: &str,
        audience: Option<CircleId>,
        bytes: &[u8],
        stamp: &str,
    ) -> coven_protocol::write::WriteId {
        let write_id = self
            .db
            .capture_document_with_file_for_test(document_id, file_id, audience, bytes, stamp)
            .await
            .expect("capture document and its file row");
        coven_foundation::store_dir::StoreDir::store_local_blob(
            &self.store_dir,
            "files",
            file_id,
            bytes,
        )
        .await
        .expect("stage the document file bytes");
        write_id
    }

    async fn move_document_audience(
        &self,
        document_id: &str,
        audience: Option<CircleId>,
        stamp: &str,
    ) {
        let audience = audience.map(|circle_id| circle_id.to_string());
        let document_id = document_id.to_string();
        let stamp = stamp.to_string();
        let staging = self
            .components
            .host_write_blob_staging(tokio::runtime::Handle::current());
        StoreDatabase::new(&self.db)
            .run_host_store_write_for_test(
                Some(EncryptionService::from_key([42; 32])),
                Some(Box::new(staging) as Box<dyn coven_database::AudienceBlobMoveStaging>),
                move |transaction| {
                    transaction
                        .execute(
                            "UPDATE documents SET audience = ?2, _updated_at = ?3 WHERE id = ?1",
                            rusqlite::params![document_id, audience, stamp],
                        )
                        .map(|_| ())
                        .map_err(DbError::from)
                },
            )
            .await
            .expect("move the document to another audience");
    }

    async fn stored_blobs(&self) -> Vec<coven_protocol::blob::locator::StoredBlobRef> {
        StoreDatabase::new(&self.db)
            .stored_blob_reclaim_candidates_for_test()
            .await
            .expect("read stored blob candidates")
            .into_iter()
            .map(|(stored, _)| stored)
            .collect()
    }

    async fn document_file_stamp(&self, file_id: &str) -> String {
        self.db
            .document_file_stamp_for_test(file_id)
            .await
            .expect("read the document file row stamp")
    }

    async fn owner_circle_snapshot_stream(
        &self,
    ) -> Vec<(
        coven_protocol::store_commit::CircleSnapshotRef,
        coven_protocol::store_commit::CircleSnapshotMeta,
    )> {
        let control = StoreDatabase::new(&self.db)
            .current_circle_control(self.circle_id)
            .await
            .expect("read the current Circle control")
            .expect("the Circle has an active control");
        let device = self
            .store
            .bind_device(&self.db, self.store_dir.clone(), &self.signer)
            .await
            .expect("bind Circle snapshot access Store");
        let access = device
            .circle_epoch_access(self.circle_id, control)
            .await
            .expect("resolve Circle snapshot access")
            .expect("the Circle access is retained");
        device
            .load_circle_snapshot_refs(self.circle_id, &access)
            .await
            .expect("walk the owner's Circle snapshot stream")
    }

    async fn bootstrap_image_present(
        &self,
        image: &coven_protocol::objects::ExactObjectRef,
    ) -> bool {
        self.db
            .remote_object_exists_for_test(image.clone())
            .await
            .expect("read bootstrap image ownership presence")
    }

    async fn member_seed_image(
        &self,
        circle_id: CircleId,
    ) -> coven_protocol::objects::ExactObjectRef {
        StoreDatabase::new(&self.member_db)
            .circle_bootstrap_coverage_ref(circle_id)
            .await
            .expect("read member Circle bootstrap coverage")
            .expect("the member's projection seeded from a real bootstrap coverage row")
            .bootstrap
            .image
            .object
    }

    async fn bootstrap_image_owner_count(
        &self,
        image: &coven_protocol::objects::ExactObjectRef,
    ) -> usize {
        let record = self
            .db
            .remote_object_for_test(image.clone())
            .await
            .expect("load bootstrap image ownership");
        let coven_protocol::remote_object::RemoteObjectRecord::SharedLiveSet(shared) = record
        else {
            panic!("a live bootstrap image is a shared object");
        };
        let coven_protocol::remote_object::OwnedObjectState::UploadedVerified { ownership } =
            shared.state
        else {
            panic!("a live bootstrap image is verified");
        };
        ownership
            .activated
            .iter()
            .filter(|owner| {
                matches!(
                    owner,
                    coven_protocol::remote_object::SharedObjectOwner::StoreCommit(_)
                )
            })
            .count()
    }

    async fn live_bootstrap_images(&self) -> Vec<(coven_protocol::objects::ExactObjectRef, usize)> {
        self.db
            .remote_objects_for_test()
            .await
            .expect("read remote object records")
            .into_iter()
            .filter_map(|record| {
                let coven_protocol::remote_object::RemoteObjectRecord::SharedLiveSet(shared) =
                    record
                else {
                    return None;
                };
                if !matches!(
                    shared.identity.domain,
                    coven_protocol::remote_object::SharedLiveSetObjectDomain::CircleBootstrapImage { .. }
                ) {
                    return None;
                }
                let coven_protocol::remote_object::OwnedObjectState::UploadedVerified {
                    ownership,
                } = shared.state
                else {
                    return None;
                };
                let owners = ownership
                    .activated
                    .iter()
                    .filter(|owner| {
                        matches!(
                            owner,
                            coven_protocol::remote_object::SharedObjectOwner::StoreCommit(_)
                        )
                    })
                    .count();
                Some((shared.identity.object.clone(), owners))
            })
            .collect()
    }

    async fn publish_covered_circle_package(
        &self,
        member: &RotationMemberDevice,
        row_id: &str,
    ) -> (
        coven_protocol::store_commit::CirclePackageRef,
        coven_protocol::store_commit::StoreBatchCommitRef,
    ) {
        let write_id = self
            .capture_document(row_id, Some(self.circle_id), "2026-07-23T00:10:00Z")
            .await;
        self.components
            .run_cycle(
                &coven_foundation::clock::SystemClock,
                None,
                coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
            )
            .await
            .expect("publish the Circle package");
        let published = match coven_database::StoreDatabase::new(&self.db)
            .write_status(&write_id)
            .await
            .expect("read Circle write status")
        {
            coven_protocol::write::WriteStatus::Published(position) => position
                .exact_commit()
                .expect("published Circle write has an exact commit")
                .clone(),
            status => panic!("the Circle write must publish: {status:?}"),
        };
        let owner = self
            .store
            .bind_device(&self.db, self.store_dir.clone(), &self.signer)
            .await
            .expect("bind the Store owner");
        let package_commit = owner
            .load_commit_for_test(&published)
            .await
            .expect("load the Circle package commit");
        let [circle_package] = package_commit.value().circle_packages() else {
            panic!("the Circle write carries exactly one Circle package");
        };
        let circle_package = circle_package.clone();

        member.pull().await;
        self.author_standalone_circle_snapshot("2026-07-23T00:15:00Z")
            .await;

        self.components
            .run_cycle(
                &coven_foundation::clock::SystemClock,
                None,
                coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
            )
            .await
            .expect("owner acknowledges the snapshot cut");
        member.pull().await;
        member
            .publish_acknowledgements("2026-07-23T00:20:00Z")
            .await;
        self.components
            .run_cycle(
                &coven_foundation::clock::SystemClock,
                None,
                coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
            )
            .await
            .expect("owner activates the member acknowledgement");
        let snapshot = self.latest_circle_snapshot(self.circle_id).await.unwrap();
        let stable = self
            .store
            .circle_snapshot_is_stable(
                &self.db,
                self.store_dir.clone(),
                self.circle_id,
                &snapshot.cut,
            )
            .await
            .expect("verify the claimed stable Circle coverage");
        assert!(
            stable,
            "the covering Circle snapshot must be acknowledged before reclaim"
        );
        (circle_package, published)
    }
}

#[tokio::test]
async fn store_member_removal_blocks_affected_circle_and_leaves_others_running() {
    let fixture = RotationFixture::build("rotation-blocks-affected").await;
    let unaffected = fixture
        .components
        .create_circle("Unaffected")
        .await
        .expect("create unaffected Circle");

    fixture.remove_store_member().await;

    let circles = fixture.circles().await;
    let affected = circles
        .iter()
        .find(|circle| circle.id() == fixture.circle_id)
        .expect("affected Circle is listed");
    assert!(
        affected.rotation_required(),
        "removing a roster member makes the Circle rotation-required"
    );
    let other = circles
        .iter()
        .find(|circle| circle.id() == unaffected)
        .expect("unaffected Circle is listed");
    assert!(
        !other.rotation_required(),
        "a Circle without the removed member is not rotation-required"
    );

    // A Store-audience write and a write to an unaffected Circle both publish.
    let store_write = fixture
        .capture_document(
            "00000000-0000-4000-8000-000000000010",
            None,
            "0000000003000-0000-owner",
        )
        .await;
    let unaffected_write = fixture
        .capture_document(
            "00000000-0000-4000-8000-000000000011",
            Some(unaffected),
            "0000000003100-0000-owner",
        )
        .await;
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("publish Store-audience and unaffected-Circle writes");
    assert!(matches!(
        coven_database::StoreDatabase::new(&fixture.db)
            .write_status(&store_write)
            .await
            .expect("read Store write status"),
        coven_protocol::write::WriteStatus::Published(_)
    ));
    assert!(matches!(
        coven_database::StoreDatabase::new(&fixture.db)
            .write_status(&unaffected_write)
            .await
            .expect("read unaffected Circle write status"),
        coven_protocol::write::WriteStatus::Published(_)
    ));

    // A host write destined to the affected Circle stays durable blocked with the
    // typed rotation-required reason.
    let blocked_write = fixture
        .capture_document(
            "00000000-0000-4000-8000-000000000012",
            Some(fixture.circle_id),
            "0000000003200-0000-owner",
        )
        .await;
    let _ = fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await;
    match coven_database::StoreDatabase::new(&fixture.db)
        .write_status(&blocked_write)
        .await
        .expect("read affected Circle write status")
    {
        coven_protocol::write::WriteStatus::Blocked(
            coven_protocol::write::WriteBlock::RotationRequired {
                circle_id,
                removed_members,
            },
        ) => {
            assert_eq!(circle_id, fixture.circle_id);
            assert_eq!(removed_members, vec![fixture.member_pubkey.clone()]);
        }
        status => panic!("affected Circle write must be rotation-blocked: {status:?}"),
    }
}

#[tokio::test]
async fn rotation_required_refuses_rename_and_add_member_but_allows_removal() {
    let fixture = RotationFixture::build("rotation-gates-lifecycle").await;
    fixture.remove_store_member().await;

    let rename = fixture
        .components
        .rename_circle(fixture.circle_id, "Renamed")
        .await
        .expect_err("rename is refused while rotation is required");
    assert!(
        matches!(
            rename,
            crate::sync::store::CircleOperationError::RotationRequired { .. }
        ),
        "{rename}"
    );

    let newcomer = keys::public_key_hex(&UserKeypair::generate());
    let add = fixture
        .components
        .add_circle_member(fixture.circle_id, newcomer, CircleRole::Member)
        .await
        .expect_err("adding a member is refused while rotation is required");
    // Returned typed (not wrapped), so the public API surfaces it with its ids.
    assert!(
        matches!(
            &add,
            crate::sync::store::CircleOperationError::RotationRequired { circle_id, removed_members }
                if *circle_id == fixture.circle_id
                    && removed_members == &vec![fixture.member_pubkey.clone()]
        ),
        "add-member is refused with the typed rotation error: {add:?}"
    );

    fixture
        .components
        .remove_circle_member(fixture.circle_id, fixture.member_pubkey.clone())
        .await
        .expect("removing a member is the path out of rotation-required");
}

#[tokio::test]
async fn re_adding_the_store_member_clears_rotation_required() {
    let fixture = RotationFixture::build("rotation-readd-clears").await;
    fixture.remove_store_member().await;
    assert!(fixture
        .circles()
        .await
        .iter()
        .find(|circle| circle.id() == fixture.circle_id)
        .expect("affected Circle listed after removal")
        .rotation_required());

    fixture
        .store
        .admit_member(
            &fixture.db,
            fixture.store_dir.clone(),
            &fixture.signer,
            &fixture.member_pubkey,
            None,
            MemberRole::Member,
            &coven_keys::encryption::EncryptionService::from(
                fixture
                    .custody
                    .unlock()
                    .expect("load rotated Store keyring")
                    .expect("scoped Store has an established keyring"),
            ),
            "Rotation Store",
        )
        .await
        .expect("re-add the removed Store member");

    assert!(
        !fixture
            .circles()
            .await
            .iter()
            .find(|circle| circle.id() == fixture.circle_id)
            .expect("affected Circle listed after re-add")
            .rotation_required(),
        "a re-added Store member's roster entry is active again, clearing rotation"
    );
}

#[tokio::test]
async fn closing_the_epoch_clears_rotation_and_resumes_publication() {
    let fixture = RotationFixture::build("rotation-close-clears").await;
    fixture.remove_store_member().await;
    assert!(fixture
        .circles()
        .await
        .iter()
        .find(|circle| circle.id() == fixture.circle_id)
        .expect("affected Circle listed after removal")
        .rotation_required());

    // Removing the roster member closes the old epoch and activates a successor
    // roster without the removed identity.
    fixture.close_epoch_by_removing_the_circle_member().await;

    let (successor, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(fixture.circle_id, &keys::public_key_hex(&fixture.signer))
        .await
        .expect("load successor Circle authoring state");
    assert!(!successor
        .roster
        .members()
        .contains_key(&fixture.member_pubkey));
    assert!(
        !fixture
            .circles()
            .await
            .iter()
            .find(|circle| circle.id() == fixture.circle_id)
            .expect("Circle listed after close")
            .rotation_required(),
        "the successor roster omits the removed identity, clearing rotation"
    );
    // Publication context succeeds under the successor control.
    StoreDatabase::new(&fixture.db)
        .circle_publication_context(fixture.circle_id, successor.control.coord.clone())
        .await
        .expect("publication context resolves under the successor control");

    // New Circle content publishes again under the successor key.
    let resumed = fixture
        .capture_document(
            "00000000-0000-4000-8000-000000000031",
            Some(fixture.circle_id),
            "0000000005000-0000-owner",
        )
        .await;
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("publish Circle content after the close");
    assert!(matches!(
        coven_database::StoreDatabase::new(&fixture.db)
            .write_status(&resumed)
            .await
            .expect("read resumed write status"),
        coven_protocol::write::WriteStatus::Published(_)
    ));
}

#[tokio::test]
async fn epoch_close_finalizes_with_a_rotation_blocked_write_present() {
    let fixture = RotationFixture::build("rotation-close-with-blocked-write").await;
    fixture.remove_store_member().await;

    // A Circle write captured after the removal stays durable blocked; its rows
    // are materialized in the live database but its write never publishes.
    let blocked = fixture
        .capture_document(
            "00000000-0000-4000-8000-000000000040",
            Some(fixture.circle_id),
            "0000000003000-0000-owner",
        )
        .await;
    let _ = fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await;
    assert!(matches!(
        coven_database::StoreDatabase::new(&fixture.db)
            .write_status(&blocked)
            .await
            .expect("read blocked write status"),
        coven_protocol::write::WriteStatus::Blocked(
            coven_protocol::write::WriteBlock::RotationRequired { .. }
        )
    ));

    // The close finalizes even though a rotation-blocked write is unpublished:
    // the successor bootstrap derives from accepted history at the exact cutoff,
    // so the blocked write's live-only rows never enter the image and the cut no
    // longer demands a write-free device.
    fixture.close_epoch_by_removing_the_circle_member().await;

    let (successor, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(fixture.circle_id, &keys::public_key_hex(&fixture.signer))
        .await
        .expect("load successor Circle authoring state");
    assert!(!successor
        .roster
        .members()
        .contains_key(&fixture.member_pubkey));
    assert!(!fixture
        .circles()
        .await
        .iter()
        .find(|circle| circle.id() == fixture.circle_id)
        .expect("Circle listed after close")
        .rotation_required());
    // The blocked write survives the close as a durable write; the rows it holds
    // were never surrendered.
    assert!(matches!(
        coven_database::StoreDatabase::new(&fixture.db)
            .write_status(&blocked)
            .await
            .expect("read blocked write status after the close"),
        coven_protocol::write::WriteStatus::Blocked(_)
    ));

    // Returning the same durable write to publication (no discard, no recreate)
    // publishes it under the successor epoch: the write captured under the closed
    // epoch's control now resolves the current control.
    StoreDatabase::new(&fixture.db)
        .retry_blocked_write(&blocked)
        .await
        .expect("return the durable write to publication after the close");
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("publish the formerly blocked write under the successor epoch");
    let published = match coven_database::StoreDatabase::new(&fixture.db)
        .write_status(&blocked)
        .await
        .expect("read republished write status")
    {
        coven_protocol::write::WriteStatus::Published(position) => position
            .exact_commit()
            .expect("published Circle write has an exact commit")
            .clone(),
        status => panic!("formerly blocked write must publish under the successor: {status:?}"),
    };
    let owner = fixture
        .store
        .bind_device(&fixture.db, fixture.store_dir.clone(), &fixture.signer)
        .await
        .expect("bind the Store owner");
    let published_commit = owner
        .load_commit_for_test(&published)
        .await
        .expect("load the successor-epoch commit");
    let [circle_package] = published_commit.value().circle_packages() else {
        panic!("the successor-epoch write carries exactly one Circle package");
    };
    assert_eq!(circle_package.control, successor.control.coord);
    assert_eq!(
        circle_package.key_fingerprint,
        successor.control.value.key_fingerprint()
    );

    // Safety: the removed member's device never receives the write's content.
    // A Store-removed identity cannot decrypt the rotated-epoch objects, so its
    // pull cannot advance into the successor epoch that carries the write; the
    // write is published in the cloud yet absent from the removed member's
    // projection.
    let removed_member = fixture
        .store
        .bind_device(
            &fixture.member_db,
            fixture.member_device.store_dir.clone(),
            &fixture.member,
        )
        .await
        .expect("open the Store as the removed member");
    let mut removed_member = removed_member
        .authorize_writer()
        .await
        .expect("authorize the removed member's local Store device");
    let routing = EncryptionService::from_key([42; 32]);
    let member_pull = removed_member
        .pull(Some(&routing))
        .await
        .expect("pull the close outcome as the removed member");
    assert!(
        !member_pull
            .frontier
            .values()
            .any(|reference| reference == &published),
        "the removed member cannot advance into the successor-epoch commit"
    );
    let received = StoreDatabase::new(&fixture.member_db)
        .read(|sql| {
            sql.query_row(
                "SELECT EXISTS(
                        SELECT 1 FROM documents
                        WHERE id = '00000000-0000-4000-8000-000000000040'
                     )",
                [],
                |row| row.get::<_, bool>(0),
            )
            .map_err(DbError::from)
        })
        .await
        .expect("access the removed member's documents projection")
        .expect("read the removed member's documents projection");
    assert!(
        !received,
        "the removed member's device never receives the blocked write's content"
    );
}

#[tokio::test]
async fn close_cut_excludes_unpublished_rows_and_keeps_accepted_ones() {
    let fixture = RotationFixture::build("close-cut-projection").await;

    // An accepted Circle row: captured and published under the active control.
    let published_id = "00000000-0000-4000-8000-000000000050";
    fixture
        .capture_document(
            published_id,
            Some(fixture.circle_id),
            "0000000003000-0000-owner",
        )
        .await;
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("publish the accepted Circle row");

    // An unpublished Circle row: captured into the live database, never published.
    let unpublished_id = "00000000-0000-4000-8000-000000000051";
    fixture
        .capture_document(
            unpublished_id,
            Some(fixture.circle_id),
            "0000000004000-0000-owner",
        )
        .await;

    // Cut the successor bootstrap at the accepted frontier while the unpublished
    // write is present. The cut no longer refuses, and the image is the accepted
    // projection: the accepted row is present, the unpublished row is absent.
    let cutoff = coven_protocol::store_commit::CommitFrontier::from_refs(
        StoreDatabase::new(&fixture.db)
            .materialized_frontier()
            .await
            .expect("read the accepted materialized frontier"),
    )
    .expect("shape the accepted materialized frontier");
    let loaded_store = fixture
        .store
        .bind_device(&fixture.db, fixture.store_dir.clone(), &fixture.signer)
        .await
        .expect("load the successor bootstrap Store");
    let mut authorized = loaded_store
        .authorize_writer()
        .await
        .expect("authorize the successor bootstrap cut");
    let cut = authorized
        .circles()
        .snapshots()
        .capture_circle_snapshot_at_cutoff(
            &EncryptionService::from_key([42; 32]),
            fixture.circle_id,
            cutoff,
        )
        .await
        .expect("cut the successor bootstrap from accepted history");
    let image = coven_database::DatabaseImageTest::open(cut.image_path_for_test())
        .expect("open the bootstrap image");
    let installed_ids = image
        .query("SELECT id FROM documents ORDER BY id", [], |row| {
            row.get::<_, String>(0)
        })
        .expect("query image rows");
    assert!(
        installed_ids.iter().any(|id| id == published_id),
        "the accepted Circle row is present in the projection image: {installed_ids:?}"
    );
    assert!(
        !installed_ids.iter().any(|id| id == unpublished_id),
        "the unpublished Circle row is absent from the projection image: {installed_ids:?}"
    );
}

#[tokio::test]
async fn ordinary_store_snapshot_excludes_unpublished_rows_and_preserves_the_write() {
    let fixture = RotationFixture::build("store-cut-unpublished").await;
    let unpublished_id = "00000000-0000-4000-8000-000000000060";
    let write_id = fixture
        .capture_document(unpublished_id, None, "0000000003000-0000-owner")
        .await;
    let database = StoreDatabase::new(&fixture.db);
    let journal = database.store_write_journal_for_test().await.unwrap();
    let status = database.write_status(&write_id).await.unwrap();
    let mut authorized = fixture.owner_device.authorize_writer().await.unwrap();
    let mut snapshots = authorized.snapshots();
    let cut = snapshots
        .capture_snapshot_cut(Some(&EncryptionService::from_key([42; 32])))
        .await
        .expect("capture accepted Store rows with an unpublished write present");
    let published = snapshots
        .push_snapshot_cut(cut, "2026-07-23T00:10:00Z".to_string())
        .await
        .expect("publish accepted Store coverage without publishing the pending row");
    let image_bytes = coven_storage::CloudSyncObjectStorage::read_protocol_object(
        fixture.cloud_storage.as_ref(),
        &coven_protocol::objects::ProtocolObjectContext::store_encrypted(
            fixture.store.root().store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreSnapshotImage,
        ),
        &published.image.object,
        &coven_protocol::store_commit::semantic_prefix_from_exact_object(
            &published.image.object,
            ".db",
        )
        .unwrap(),
    )
    .await
    .expect("read the exact published image");
    let image = coven_database::DatabaseImageTest::from_bytes(&image_bytes).unwrap();
    assert!(
        image
            .query("SELECT id FROM documents", [], |row| row
                .get::<_, String>(0))
            .unwrap()
            .is_empty(),
        "the unpublished row is absent from the accepted image"
    );
    assert_eq!(
        fixture.db.query_test_text("SELECT id FROM documents").await,
        unpublished_id,
        "the unpublished row remains in the live database",
    );
    assert_eq!(database.write_status(&write_id).await.unwrap(), status);
    assert_eq!(
        database.store_write_journal_for_test().await.unwrap(),
        journal
    );
}

#[tokio::test]
async fn removing_a_store_member_outside_every_roster_blocks_nothing() {
    let fixture = RotationFixture::build("rotation-unaffected-removal").await;
    let outsider = UserKeypair::generate();
    let outsider_pubkey = keys::public_key_hex(&outsider);
    fixture
        .store
        .admit_member(
            &fixture.db,
            fixture.store_dir.clone(),
            &fixture.signer,
            &outsider_pubkey,
            None,
            MemberRole::Member,
            &EncryptionService::from_key([42; 32]),
            "Rotation Store",
        )
        .await
        .expect("admit a Store member who joins no Circle");

    fixture
        .components
        .remove_member(&outsider_pubkey)
        .await
        .expect("remove the non-Circle Store member");

    assert!(
        !fixture
            .circles()
            .await
            .iter()
            .find(|circle| circle.id() == fixture.circle_id)
            .expect("Circle listed after unrelated removal")
            .rotation_required(),
        "removing a Store member in no roster leaves every Circle running"
    );

    let write = fixture
        .capture_document(
            "00000000-0000-4000-8000-000000000020",
            Some(fixture.circle_id),
            "0000000003000-0000-owner",
        )
        .await;
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("publish Circle content after an unrelated Store removal");
    assert!(matches!(
        coven_database::StoreDatabase::new(&fixture.db)
            .write_status(&write)
            .await
            .expect("read Circle write status"),
        coven_protocol::write::WriteStatus::Published(_)
    ));
}

#[tokio::test]
async fn device_join_succeeds_after_a_circle_epoch_close() {
    let fixture = RotationFixture::build("device-join-after-close").await;

    // Drive a Circle member-removal epoch close through to successor activation.
    fixture.close_epoch_by_removing_the_circle_member().await;

    // Confirm the close activated its successor.
    let (successor, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(fixture.circle_id, &keys::public_key_hex(&fixture.signer))
        .await
        .expect("load successor Circle authoring state");
    assert!(!successor
        .roster
        .members()
        .contains_key(&fixture.member_pubkey));

    // A remaining member (the owner) installs a new device through the ordinary
    // genesis-replaying join, which reconstructs the full retained history.
    let joined_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let joined_db = open_circle_routing_test_db(joined_db_store_dir.clone());
    fixture
        .store
        .activate_joined_device(
            &fixture.db,
            fixture.store_dir.clone(),
            &joined_db,
            joined_db_store_dir.clone(),
            &fixture.signer,
            "2026-07-24T00:00:00Z",
        )
        .await
        .expect("device join succeeds after a Circle epoch close");

    // The newly joined device pulls the Circle's post-close state, including the
    // successor bootstrap, which triggers a retained-replay projection.
    let joined_store = crate::sync::store::Store::load(
        StoreDatabase::new(&joined_db),
        fixture.cloud_storage.clone(),
        joined_db_store_dir,
        fixture.signer.clone(),
        Some(coven_keys::encryption::EncryptionService::from_key(
            [42; 32],
        )),
    )
    .await
    .expect("load the joined device Store");
    let pull = joined_store
        .authorize_writer()
        .await
        .expect("authorize the joined device pull")
        .pull(Some(&EncryptionService::from_key([42; 32])))
        .await
        .expect("the joined device pulls the close successor without a foreign-key violation");
    assert!(
        pull.held_positions.is_empty(),
        "the joined device holds no positions after the close: {:?}",
        pull.held_positions
    );
}

mod blobs;
mod reclaim;
mod restoration;
mod snapshots;
