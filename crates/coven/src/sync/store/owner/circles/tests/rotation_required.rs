use super::*;
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::sync::cycle::{init_sync_over_storage, StoreInitialization, SyncComponents};
use crate::sync::test_helpers::TestDevice;

fn circle_routing_migrations() -> Vec<crate::migration::Migration> {
    vec![crate::migration::Migration::sql(
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
fn circle_routing_tables() -> Vec<crate::sync::session::SyncedTable> {
    vec![
        crate::sync::session::SyncedTable::new(
            "documents",
            crate::sync::session::RowIdentity::IndependentUuid,
        )
        .scoped_by("audience"),
        crate::sync::session::SyncedTable::new(
            "document_files",
            crate::sync::session::RowIdentity::IndependentUuid,
        )
        .inherits_audience_through("document_id")
        .carries_blob(crate::sync::session::BlobDecl::new(
            "files",
            crate::blob::Provenance::HostProvided,
            crate::blob::CacheFill::CacheEager,
        )),
    ]
}

fn open_circle_routing_test_db() -> Database {
    crate::sync::test_helpers::open_test_db_schema(
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
    store: TestStore,
    signer: UserKeypair,
    components: SyncComponents,
    circle_id: CircleId,
    member: UserKeypair,
    member_pubkey: String,
    member_db: Database,
    store_dir: crate::store_dir::StoreDir,
    _store_temp: tempfile::TempDir,
    security: crate::store_security::StoreSecurity,
}

async fn rotation_fixture(label: &str) -> RotationFixture {
    let db = open_circle_routing_test_db();
    let (store, signer, founder) = persist_merge_operation(&db, label).await;
    let circle_id = founder.circle_id();
    resume_circle_operations(&db, &store.storage, &signer)
        .await
        .expect("activate founder transition");

    let member = UserKeypair::generate();
    let member_pubkey = keys::public_key_hex(&member);
    store
        .invite_member(
            &db,
            &signer,
            &crate::sync::hlc::Hlc::new(
                format!("{label}-owner"),
                std::sync::Arc::new(crate::clock::SystemClock),
            ),
            &member_pubkey,
            None,
            MemberRole::Member,
            &EncryptionService::from_key([42; 32]),
            "Rotation Store",
        )
        .await
        .expect("invite Store member");
    let member_db = open_circle_routing_test_db();
    install_active_device_fixture(&store, &db, &member_db, &member, "2026-07-23T00:00:00Z")
        .await
        .expect("activate Store member device");

    let (store_temp, store_dir) = temp_store_dir();
    let owner_storage = crate::storage::CloudSyncStorage::new(
        store.home.clone(),
        crate::storage::CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
        crate::storage::BlobPathScheme::Hashed,
        store.storage.store_id(),
        signer.clone(),
    )
    .expect("open Circle owner storage");
    let components = init_sync_over_storage(
        &crate::database::StoreDatabase::new(&db),
        owner_storage,
        StoreInitialization::OpenStore {
            expected_store_root: store.root.clone(),
        },
        Some(EncryptionService::from_key([42; 32])),
    )
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

    let custody = crate::sync::test_helpers::TestCustody::default();
    custody.set_initial_key([42; 32]);
    let security =
        crate::sync::test_helpers::test_store_security("circle-rotation-test", Arc::new(custody));

    RotationFixture {
        db,
        store,
        signer,
        components,
        circle_id,
        member,
        member_pubkey,
        member_db,
        store_dir,
        _store_temp: store_temp,
        security,
    }
}

async fn remove_store_member(fixture: &RotationFixture) {
    fixture
        .components
        .remove_member(&fixture.member_pubkey, &fixture.security)
        .await
        .expect("remove Store member");
}

/// Cloud storage for the fixture's second member device, over the shared home.
fn member_storage(fixture: &RotationFixture) -> Arc<crate::storage::CloudSyncStorage> {
    Arc::new(
        crate::storage::CloudSyncStorage::new(
            fixture.store.home.clone(),
            crate::storage::CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
            crate::storage::BlobPathScheme::Hashed,
            fixture.store.storage.store_id(),
            fixture.member.clone(),
        )
        .expect("open Circle member storage"),
    )
}

/// The member device installs the Circle bootstrap and returns to an active
/// projection holding the current epoch key.
async fn member_pull(
    fixture: &RotationFixture,
    storage: &Arc<crate::storage::CloudSyncStorage>,
    store_dir: &crate::store_dir::StoreDir,
) {
    crate::sync::store::Store::load(
        StoreDatabase::new(&fixture.member_db),
        storage.clone(),
        fixture.member.clone(),
    )
    .await
    .expect("load Circle member Store")
    .authorize_writer()
    .await
    .expect("authorize Circle member Store")
    .pull(store_dir, Some(&EncryptionService::from_key([42; 32])))
    .await
    .expect("member installs the Circle bootstrap");
}

/// Stage and publish the member device's Store and Circle acknowledgements at its
/// current accepted frontier, riding one Store commit.
async fn member_publish_acknowledgements(
    fixture: &RotationFixture,
    storage: &Arc<crate::storage::CloudSyncStorage>,
    stamp: &str,
) {
    let device = TestDevice::load(&fixture.member_db, storage.clone(), fixture.member.clone())
        .await
        .expect("load member Store");
    let frontier = crate::protocol::store_commit::CommitFrontier::from_refs(
        StoreDatabase::new(&fixture.member_db)
            .materialized_frontier()
            .await
            .expect("read member frontier"),
    )
    .expect("shape member frontier");
    device
        .stage_acknowledgement(frontier.clone(), stamp.to_string())
        .await
        .expect("stage member Store acknowledgement");
    device
        .stage_circle_acknowledgements(&frontier, stamp)
        .await
        .expect("stage member Circle acknowledgement");
    device
        .drain_acknowledgements()
        .await
        .expect("publish member acknowledgements");
}

async fn local_device_id(db: &Database) -> crate::protocol::store_commit::StoreDeviceId {
    db.get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local device id")
        .expect("local device id is installed")
        .parse()
        .expect("parse local device id")
}

/// Capture one host row into `documents` under `audience` (a Circle id or `NULL`
/// for the Store audience) and return its durable write identity.
async fn capture_document(
    fixture: &RotationFixture,
    row_id: &str,
    audience: Option<CircleId>,
    stamp: &str,
) -> crate::WriteId {
    let write_id = fixture.db.new_write_id();
    let captured = write_id.clone();
    let tables = fixture.db.synced_tables().to_vec();
    let routing = EncryptionService::from_key([42; 32]);
    let audience_value = audience.map(|circle_id| circle_id.to_string());
    let row_id = row_id.to_string();
    let stamp = stamp.to_string();
    fixture
        .db
        .call(move |connection| {
            StoreDatabase::run_internal_store_write_transaction_on(
                connection,
                &tables,
                Some(&routing),
                captured,
                |transaction| {
                    transaction
                        .execute(
                            "INSERT INTO documents (id, audience, _updated_at)
                             VALUES (?1, ?2, ?3)",
                            rusqlite::params![row_id, audience_value, stamp],
                        )
                        .map(|_| ())
                        .map_err(DbError::from)
                },
            )
        })
        .await
        .expect("capture document row");
    write_id
}

async fn active_store_members(fixture: &RotationFixture) -> BTreeSet<String> {
    let membership = fixture
        .store
        .bind_device(&fixture.db, &fixture.signer)
        .await
        .expect("load cycle Store")
        .membership_for_test()
        .await
        .expect("load cycle membership");
    membership
        .current_members()
        .into_iter()
        .map(|(pubkey, _)| pubkey)
        .collect()
}

async fn list_circles(fixture: &RotationFixture) -> Vec<crate::protocol::circle::CircleInfo> {
    let members = active_store_members(fixture).await;
    StoreDatabase::new(&fixture.db)
        .get_circles(&keys::public_key_hex(&fixture.signer), members)
        .await
        .expect("list Circles")
}

#[tokio::test]
async fn store_member_removal_blocks_affected_circle_and_leaves_others_running() {
    let fixture = rotation_fixture("rotation-blocks-affected").await;
    let unaffected = fixture
        .components
        .create_circle("Unaffected")
        .await
        .expect("create unaffected Circle");

    remove_store_member(&fixture).await;

    let circles = list_circles(&fixture).await;
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
    let store_write = capture_document(
        &fixture,
        "00000000-0000-4000-8000-000000000010",
        None,
        "0000000003000-0000-owner",
    )
    .await;
    let unaffected_write = capture_document(
        &fixture,
        "00000000-0000-4000-8000-000000000011",
        Some(unaffected),
        "0000000003100-0000-owner",
    )
    .await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("publish Store-audience and unaffected-Circle writes");
    assert!(matches!(
        crate::database::StoreDatabase::new(&fixture.db)
            .write_status(&store_write)
            .await
            .expect("read Store write status"),
        crate::WriteStatus::Published(_)
    ));
    assert!(matches!(
        crate::database::StoreDatabase::new(&fixture.db)
            .write_status(&unaffected_write)
            .await
            .expect("read unaffected Circle write status"),
        crate::WriteStatus::Published(_)
    ));

    // A host write destined to the affected Circle stays durable blocked with the
    // typed rotation-required reason.
    let blocked_write = capture_document(
        &fixture,
        "00000000-0000-4000-8000-000000000012",
        Some(fixture.circle_id),
        "0000000003200-0000-owner",
    )
    .await;
    let _ = fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await;
    match crate::database::StoreDatabase::new(&fixture.db)
        .write_status(&blocked_write)
        .await
        .expect("read affected Circle write status")
    {
        crate::WriteStatus::Blocked(crate::WriteBlock::RotationRequired {
            circle_id,
            removed_members,
        }) => {
            assert_eq!(circle_id, fixture.circle_id);
            assert_eq!(removed_members, vec![fixture.member_pubkey.clone()]);
        }
        status => panic!("affected Circle write must be rotation-blocked: {status:?}"),
    }
}

#[tokio::test]
async fn rotation_required_refuses_rename_and_add_member_but_allows_removal() {
    let fixture = rotation_fixture("rotation-gates-lifecycle").await;
    remove_store_member(&fixture).await;

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
        .add_circle_member(
            &fixture.store_dir,
            fixture.circle_id,
            newcomer,
            CircleRole::Member,
        )
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
    let fixture = rotation_fixture("rotation-readd-clears").await;
    remove_store_member(&fixture).await;
    assert!(list_circles(&fixture)
        .await
        .iter()
        .find(|circle| circle.id() == fixture.circle_id)
        .expect("affected Circle listed after removal")
        .rotation_required());

    fixture
        .store
        .invite_member(
            &fixture.db,
            &fixture.signer,
            &crate::sync::hlc::Hlc::new(
                "rotation-readd".to_string(),
                std::sync::Arc::new(crate::clock::SystemClock),
            ),
            &fixture.member_pubkey,
            None,
            MemberRole::Member,
            &fixture
                .store
                .storage
                .cipher_state()
                .encryption()
                .expect("live Store keyring"),
            "Rotation Store",
        )
        .await
        .expect("re-add the removed Store member");

    assert!(
        !list_circles(&fixture)
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
    let fixture = rotation_fixture("rotation-close-clears").await;
    remove_store_member(&fixture).await;
    assert!(list_circles(&fixture)
        .await
        .iter()
        .find(|circle| circle.id() == fixture.circle_id)
        .expect("affected Circle listed after removal")
        .rotation_required());

    // Removing the roster member closes the old epoch and activates a successor
    // roster without the removed identity.
    fixture
        .components
        .remove_circle_member(fixture.circle_id, fixture.member_pubkey.clone())
        .await
        .expect("close the epoch by removing the roster member");
    publish_circle_epoch_close_response(&fixture.store.storage, &fixture.db, &fixture.signer)
        .await
        .expect("publish local Circle epoch-close response");
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("activate the Circle epoch-close outcome");

    let (successor, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(fixture.circle_id, &keys::public_key_hex(&fixture.signer))
        .await
        .expect("load successor Circle authoring state");
    assert!(!successor
        .roster
        .members()
        .contains_key(&fixture.member_pubkey));
    assert!(
        !list_circles(&fixture)
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
    let resumed = capture_document(
        &fixture,
        "00000000-0000-4000-8000-000000000031",
        Some(fixture.circle_id),
        "0000000005000-0000-owner",
    )
    .await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("publish Circle content after the close");
    assert!(matches!(
        crate::database::StoreDatabase::new(&fixture.db)
            .write_status(&resumed)
            .await
            .expect("read resumed write status"),
        crate::WriteStatus::Published(_)
    ));
}

#[tokio::test]
async fn epoch_close_finalizes_with_a_rotation_blocked_write_present() {
    let fixture = rotation_fixture("rotation-close-with-blocked-write").await;
    remove_store_member(&fixture).await;

    // A Circle write captured after the removal stays durable blocked; its rows
    // are materialized in the live database but its write never publishes.
    let blocked = capture_document(
        &fixture,
        "00000000-0000-4000-8000-000000000040",
        Some(fixture.circle_id),
        "0000000003000-0000-owner",
    )
    .await;
    let _ = fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await;
    assert!(matches!(
        crate::database::StoreDatabase::new(&fixture.db)
            .write_status(&blocked)
            .await
            .expect("read blocked write status"),
        crate::WriteStatus::Blocked(crate::WriteBlock::RotationRequired { .. })
    ));

    // The close finalizes even though a rotation-blocked write is unpublished:
    // the successor bootstrap derives from accepted history at the exact cutoff,
    // so the blocked write's live-only rows never enter the image and the cut no
    // longer demands a write-free device.
    fixture
        .components
        .remove_circle_member(fixture.circle_id, fixture.member_pubkey.clone())
        .await
        .expect("close the epoch by removing the roster member");
    publish_circle_epoch_close_response(&fixture.store.storage, &fixture.db, &fixture.signer)
        .await
        .expect("publish local Circle epoch-close response");
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("finalize the close while a rotation-blocked write is unpublished");

    let (successor, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(fixture.circle_id, &keys::public_key_hex(&fixture.signer))
        .await
        .expect("load successor Circle authoring state");
    assert!(!successor
        .roster
        .members()
        .contains_key(&fixture.member_pubkey));
    assert!(!list_circles(&fixture)
        .await
        .iter()
        .find(|circle| circle.id() == fixture.circle_id)
        .expect("Circle listed after close")
        .rotation_required());
    // The blocked write survives the close as a durable write; the rows it holds
    // were never surrendered.
    assert!(matches!(
        crate::database::StoreDatabase::new(&fixture.db)
            .write_status(&blocked)
            .await
            .expect("read blocked write status after the close"),
        crate::WriteStatus::Blocked(_)
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
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("publish the formerly blocked write under the successor epoch");
    let published = match crate::database::StoreDatabase::new(&fixture.db)
        .write_status(&blocked)
        .await
        .expect("read republished write status")
    {
        crate::WriteStatus::Published(position) => position.commit,
        status => panic!("formerly blocked write must publish under the successor: {status:?}"),
    };
    let owner = fixture
        .store
        .bind_device(&fixture.db, &fixture.signer)
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
        .bind_device(&fixture.member_db, &fixture.member)
        .await
        .expect("open the Store as the removed member");
    let mut removed_member = removed_member
        .authorize_writer()
        .await
        .expect("authorize the removed member's local Store device");
    let (_member_temp, member_store_dir) = temp_store_dir();
    let routing = EncryptionService::from_key([42; 32]);
    let member_pull = removed_member
        .pull(&member_store_dir, Some(&routing))
        .await
        .expect("pull the close outcome as the removed member");
    assert!(
        !member_pull
            .frontier
            .values()
            .any(|reference| reference == &published),
        "the removed member cannot advance into the successor-epoch commit"
    );
    let received = fixture
        .member_db
        .call(|connection| {
            connection
                .query_row(
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
        .expect("read the removed member's documents projection");
    assert!(
        !received,
        "the removed member's device never receives the blocked write's content"
    );
}

#[tokio::test]
async fn close_cut_excludes_unpublished_rows_and_keeps_accepted_ones() {
    let fixture = rotation_fixture("close-cut-projection").await;

    // An accepted Circle row: captured and published under the active control.
    let published_id = "00000000-0000-4000-8000-000000000050";
    capture_document(
        &fixture,
        published_id,
        Some(fixture.circle_id),
        "0000000003000-0000-owner",
    )
    .await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("publish the accepted Circle row");

    // An unpublished Circle row: captured into the live database, never published.
    let unpublished_id = "00000000-0000-4000-8000-000000000051";
    capture_document(
        &fixture,
        unpublished_id,
        Some(fixture.circle_id),
        "0000000004000-0000-owner",
    )
    .await;

    // Cut the successor bootstrap at the accepted frontier while the unpublished
    // write is present. The cut no longer refuses, and the image is the accepted
    // projection: the accepted row is present, the unpublished row is absent.
    let cutoff = fixture
        .db
        .call(|conn| {
            let refs = crate::database::StoreDatabase::materialized_frontier_on(conn, None)?;
            crate::protocol::store_commit::CommitFrontier::from_refs(refs)
                .map_err(|error| DbError::Message(error.to_string()))
        })
        .await
        .expect("read the accepted materialized frontier");
    let loaded_store = fixture
        .store
        .bind_device(&fixture.db, &fixture.signer)
        .await
        .expect("load the successor bootstrap Store");
    let mut authorized = loaded_store
        .authorize_writer()
        .await
        .expect("authorize the successor bootstrap cut");
    let (image_temp, image_dir) = temp_store_dir();
    let cut = authorized
        .circles()
        .snapshots()
        .capture_circle_snapshot_at_cutoff(
            image_dir.as_ref().to_path_buf(),
            &EncryptionService::from_key([42; 32]),
            fixture.circle_id,
            cutoff,
        )
        .await
        .expect("cut the successor bootstrap from accepted history");
    let image_path = image_temp.path().join("close-cut-image.sqlite3");
    std::fs::write(&image_path, &cut.snapshot.db_image).expect("write the bootstrap image");
    let image = rusqlite::Connection::open(&image_path).expect("open the bootstrap image");
    let installed_ids = {
        let mut statement = image
            .prepare("SELECT id FROM documents ORDER BY id")
            .expect("prepare image row query");
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query image rows")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect image rows");
        rows
    };
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
async fn ordinary_store_snapshot_cut_still_refuses_unpublished_writes() {
    let fixture = rotation_fixture("store-cut-gate").await;
    capture_document(
        &fixture,
        "00000000-0000-4000-8000-000000000060",
        None,
        "0000000003000-0000-owner",
    )
    .await;
    let loaded_store = fixture
        .store
        .bind_device(&fixture.db, &fixture.signer)
        .await
        .expect("load the ordinary snapshot Store");
    let authorized = loaded_store
        .authorize_writer()
        .await
        .expect("authorize the ordinary Store snapshot cut");
    let (_temp, cut_dir) = temp_store_dir();
    let error = match authorized
        .capture_snapshot_cut(
            cut_dir.as_ref().to_path_buf(),
            fixture.db.synced_tables().to_vec(),
            Some(&EncryptionService::from_key([42; 32])),
        )
        .await
    {
        Ok(_) => panic!("the ordinary Store snapshot cut still refuses unpublished writes"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("snapshot cut refused while unpublished Store writes exist"),
        "{error}"
    );
}

#[tokio::test]
async fn removing_a_store_member_outside_every_roster_blocks_nothing() {
    let fixture = rotation_fixture("rotation-unaffected-removal").await;
    let outsider = UserKeypair::generate();
    let outsider_pubkey = keys::public_key_hex(&outsider);
    fixture
        .store
        .invite_member(
            &fixture.db,
            &fixture.signer,
            &crate::sync::hlc::Hlc::new(
                "rotation-outsider".to_string(),
                std::sync::Arc::new(crate::clock::SystemClock),
            ),
            &outsider_pubkey,
            None,
            MemberRole::Member,
            &EncryptionService::from_key([42; 32]),
            "Rotation Store",
        )
        .await
        .expect("invite a Store member who joins no Circle");

    fixture
        .components
        .remove_member(&outsider_pubkey, &fixture.security)
        .await
        .expect("remove the non-Circle Store member");

    assert!(
        !list_circles(&fixture)
            .await
            .iter()
            .find(|circle| circle.id() == fixture.circle_id)
            .expect("Circle listed after unrelated removal")
            .rotation_required(),
        "removing a Store member in no roster leaves every Circle running"
    );

    let write = capture_document(
        &fixture,
        "00000000-0000-4000-8000-000000000020",
        Some(fixture.circle_id),
        "0000000003000-0000-owner",
    )
    .await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("publish Circle content after an unrelated Store removal");
    assert!(matches!(
        crate::database::StoreDatabase::new(&fixture.db)
            .write_status(&write)
            .await
            .expect("read Circle write status"),
        crate::WriteStatus::Published(_)
    ));
}

#[tokio::test]
async fn device_join_succeeds_after_a_circle_epoch_close() {
    let fixture = rotation_fixture("device-join-after-close").await;

    // Drive a Circle member-removal epoch close through to successor activation.
    fixture
        .components
        .remove_circle_member(fixture.circle_id, fixture.member_pubkey.clone())
        .await
        .expect("close the epoch by removing the roster member");
    publish_circle_epoch_close_response(&fixture.store.storage, &fixture.db, &fixture.signer)
        .await
        .expect("publish local Circle epoch-close response");
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("activate the Circle epoch-close outcome");

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
    let joined_db = open_circle_routing_test_db();
    install_active_device_fixture(
        &fixture.store,
        &fixture.db,
        &joined_db,
        &fixture.signer,
        "2026-07-24T00:00:00Z",
    )
    .await
    .expect("device join succeeds after a Circle epoch close");

    // The newly joined device pulls the Circle's post-close state, including the
    // successor bootstrap, which triggers a retained-replay projection.
    let (_joined_temp, joined_dir) = temp_store_dir();
    let joined_store = crate::sync::store::Store::load(
        StoreDatabase::new(&joined_db),
        fixture.store.storage.clone(),
        fixture.signer.clone(),
    )
    .await
    .expect("load the joined device Store");
    let pull = joined_store
        .authorize_writer()
        .await
        .expect("authorize the joined device pull")
        .pull(&joined_dir, Some(&EncryptionService::from_key([42; 32])))
        .await
        .expect("the joined device pulls the close successor without a foreign-key violation");
    assert!(
        pull.held_positions.is_empty(),
        "the joined device holds no positions after the close: {:?}",
        pull.held_positions
    );
}

/// A stable, acknowledged Store snapshot of a Circle store with one active
/// member, cut without an epoch close (so Circle history stays live and no
/// coverage is reclaimed). The shared base for the restore-selection cases that
/// differ only in who restores and what the storage provider serves.
struct ActiveMemberCircleSnapshot {
    db: Database,
    store: TestStore,
    signer: UserKeypair,
    member: UserKeypair,
    routing: EncryptionService,
    circle_id: crate::protocol::circle::CircleId,
    membership: crate::protocol::membership::MembershipChain,
}

/// How much Circle history the fixture builds before the Store snapshot cut.
enum CircleFixtureMode {
    /// Live Circle content, no epoch close — no image is reclaimed.
    Live,
    /// The epoch is closed by removing the member; the successor bootstrap covers
    /// the pre-close content and the remaining owner's leaf names it.
    Closed,
}

async fn write_circle_document(
    db: &Database,
    routing: &EncryptionService,
    circle_id: crate::protocol::circle::CircleId,
    id: &str,
    updated_at: &str,
) {
    let write_id = db.new_write_id();
    let tables = db.synced_tables().to_vec();
    let routing = routing.clone();
    let audience = Some(circle_id.to_string());
    let id = id.to_string();
    let updated_at = updated_at.to_string();
    db.call(move |connection| {
        StoreDatabase::run_internal_store_write_transaction_on(
            connection,
            &tables,
            Some(&routing),
            write_id,
            |transaction| {
                transaction
                    .execute(
                        "INSERT INTO documents (id, audience, _updated_at)
                         VALUES (?1, ?2, ?3)",
                        rusqlite::params![id, audience, updated_at],
                    )
                    .map(|_| ())
                    .map_err(DbError::from)
            },
        )
    })
    .await
    .expect("capture Circle content");
}

async fn publish_store_snapshot_cut(
    authorized: &mut crate::sync::store::AuthorizedWriterOperation<'_>,
    cut: crate::sync::store::snapshot::SnapshotCut,
    created_at: &str,
) {
    authorized
        .push_snapshot_cut(cut, created_at.to_string())
        .await
        .expect("publish the Store snapshot");
}

async fn setup_active_member_circle_snapshot(
    name: &str,
    mode: CircleFixtureMode,
) -> ActiveMemberCircleSnapshot {
    let routing = EncryptionService::from_key([42; 32]);
    let db = open_circle_routing_test_db();
    let (store, signer, founder) = persist_merge_operation(&db, name).await;
    let circle_id = founder.circle_id();
    resume_circle_operations(&db, &store.storage, &signer)
        .await
        .expect("activate founder transition");

    let member = UserKeypair::generate();
    let member_pubkey = keys::public_key_hex(&member);
    store
        .invite_member(
            &db,
            &signer,
            &crate::sync::hlc::Hlc::new(
                format!("{name}-owner"),
                std::sync::Arc::new(crate::clock::SystemClock),
            ),
            &member_pubkey,
            None,
            MemberRole::Member,
            &routing,
            "Restore Store",
        )
        .await
        .expect("invite Store member");
    let (_store_temp, store_dir) = temp_store_dir();
    let owner_storage = crate::storage::CloudSyncStorage::new(
        store.home.clone(),
        crate::storage::CloudCipher::Encrypted(routing.clone()),
        crate::storage::BlobPathScheme::Hashed,
        store.storage.store_id(),
        signer.clone(),
    )
    .expect("open Circle owner storage");
    let components = init_sync_over_storage(
        &crate::database::StoreDatabase::new(&db),
        owner_storage,
        StoreInitialization::OpenStore {
            expected_store_root: store.root.clone(),
        },
        Some(routing.clone()),
    )
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

    // Pre-close Circle content the member holds access to, under the live epoch.
    write_circle_document(
        &db,
        &routing,
        circle_id,
        "00000000-0000-4000-8000-000000000090",
        "0000000002000-0000-owner",
    )
    .await;
    components
        .run_cycle(&crate::clock::SystemClock, None, &store_dir, None)
        .await
        .expect("publish pre-close Circle content");

    // Close the epoch by removing the member. The successor bootstrap's activating
    // commit is retained, so the restored device resolves the head control and the
    // remaining owner's own leaf names that successor bootstrap image (in storage)
    // as its Circle image.
    if matches!(mode, CircleFixtureMode::Closed) {
        components
            .remove_circle_member(circle_id, member_pubkey.clone())
            .await
            .expect("close the epoch by removing the roster member");
        publish_circle_epoch_close_response(&store.storage, &db, &signer)
            .await
            .expect("publish local Circle epoch-close response");
        components
            .run_cycle(&crate::clock::SystemClock, None, &store_dir, None)
            .await
            .expect("activate the Circle epoch-close outcome");
    }

    let loaded_store = store
        .bind_device(&db, &signer)
        .await
        .expect("load the Store snapshot");
    let mut authorized = loaded_store
        .authorize_writer()
        .await
        .expect("authorize the Store snapshot");
    let (_snapshot_temp, snapshot_dir) = temp_store_dir();
    let cut = authorized
        .capture_snapshot_cut(
            snapshot_dir.as_ref().to_path_buf(),
            db.synced_tables().to_vec(),
            Some(&routing),
        )
        .await
        .expect("capture the Store snapshot cut");
    let coverage = cut.coverage.clone();
    publish_store_snapshot_cut(&mut authorized, cut, "2026-07-24T01:00:00Z").await;
    loaded_store
        .stage_acknowledgement(coverage.clone(), "2026-07-24T01:00:01Z".to_string())
        .await
        .expect("stage snapshot stability acknowledgement");
    loaded_store
        .drain_acknowledgements()
        .await
        .expect("activate snapshot stability acknowledgement");

    let membership = store
        .bind_device(&db, &signer)
        .await
        .expect("load snapshot Store")
        .membership_for_test()
        .await
        .expect("load membership for snapshot restore");

    ActiveMemberCircleSnapshot {
        db,
        store,
        signer,
        member,
        routing,
        circle_id,
        membership,
    }
}

#[tokio::test]
async fn restore_reports_a_circle_with_no_coverage_image() {
    let base =
        setup_active_member_circle_snapshot("snapshot-restore-no-image", CircleFixtureMode::Live)
            .await;
    let ActiveMemberCircleSnapshot {
        db,
        store,
        signer,
        routing,
        circle_id,
        membership,
        ..
    } = base;

    // The owner restores a Circle it holds access to but which no bootstrap or
    // snapshot ever covered — the no-coverage report path. Selection must not error
    // on the missing image, and must not fabricate a coverage row; the Store image
    // still restores the Circle's control indexes.
    let destination = tempfile::tempdir().expect("no-image restore destination");
    let database_path = destination.path().join("store.db");
    let bootstrap = crate::sync::store::bootstrap_from_snapshot(
        &store.storage,
        store.storage.store_id(),
        store.root.clone(),
        &crate::joining::MembershipFloor(membership.head_refs().to_vec()),
        db.schema_version(),
        &database_path,
        &signer,
    )
    .await
    .expect("restore the Store snapshot");
    let restored = bootstrap
        .open_database(
            store.storage.store_id(),
            &database_path,
            db.synced_tables().to_vec(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            "no-image-device".to_string(),
            std::sync::Arc::new(crate::clock::SystemClock),
            &circle_routing_migrations(),
            Some(&routing),
        )
        .await
        .expect("a Circle with active access but no image restores without error");

    let coverage = restored
        .circle_bootstrap_coverage_for_test(circle_id)
        .await
        .expect("read restored Circle coverage");
    assert!(
        coverage.is_none(),
        "selection stages no coverage row for a Circle it holds no image for"
    );

    let control_count: i64 = restored
        .circle_control_activation_count_for_test(circle_id)
        .await
        .expect("count restored Circle control indexes");
    assert!(
        control_count > 0,
        "the Store image still restores the Circle control indexes"
    );
}

#[tokio::test]
async fn restore_rejects_a_sabotaged_circle_image_and_exposes_no_database() {
    let base =
        setup_active_member_circle_snapshot("snapshot-restore-sabotage", CircleFixtureMode::Closed)
            .await;
    let ActiveMemberCircleSnapshot {
        db,
        store,
        signer,
        routing,
        membership,
        ..
    } = base;

    // A hostile storage provider serves the wrong bytes for the Circle bootstrap
    // image the owner's own access leaf names. The bytes are input to the verifier,
    // not trusted for being served: their digest no longer matches the image hash
    // the signed access leaf pins, so `verify_circle_bootstrap_image` rejects them
    // and the whole restore fails with no database left behind. (An image with a
    // wrong schema/routing contract or a row outside the audience closure changes
    // the bytes too, so it fails this same digest check first — those reach the
    // verifier only from a malicious author, whose defense is the verifier's own
    // tests, not a storage provider.)
    let sabotaged_keys: Vec<String> = store
        .home
        .keys()
        .into_iter()
        .filter(|key| key.contains("/bootstraps/"))
        .collect();
    assert!(
        !sabotaged_keys.is_empty(),
        "the Circle bootstrap image was uploaded to storage"
    );
    for key in &sabotaged_keys {
        store
            .home
            .insert_exact_object(key, b"sabotaged Circle bootstrap image".to_vec());
    }

    let destination = tempfile::tempdir().expect("sabotage restore destination");
    let database_path = destination.path().join("store.db");
    let bootstrap = crate::sync::store::bootstrap_from_snapshot(
        &store.storage,
        store.storage.store_id(),
        store.root.clone(),
        &crate::joining::MembershipFloor(membership.head_refs().to_vec()),
        db.schema_version(),
        &database_path,
        &signer,
    )
    .await
    .expect("the Store snapshot itself verifies; only the Circle image is sabotaged");
    let outcome = bootstrap
        .open_database(
            store.storage.store_id(),
            &database_path,
            db.synced_tables().to_vec(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            "sabotaged-restore-device".to_string(),
            std::sync::Arc::new(crate::clock::SystemClock),
            &circle_routing_migrations(),
            Some(&routing),
        )
        .await;
    let error = match outcome {
        Ok(_) => panic!("a sabotaged Circle image must fail the whole restore"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("image")
            || error.to_string().contains("hash")
            || error.to_string().contains("digest"),
        "the restore fails on image verification: {error}"
    );
    assert!(
        !database_path.exists(),
        "a failed restore exposes no database at the target path"
    );
}

#[tokio::test]
async fn restore_rolls_back_the_store_image_when_circle_install_fails() {
    let base =
        setup_active_member_circle_snapshot("snapshot-restore-crash", CircleFixtureMode::Live)
            .await;
    let ActiveMemberCircleSnapshot {
        db,
        store,
        member,
        routing,
        membership,
        ..
    } = base;

    // The member restore selects its own leaf bootstrap as a Circle image to
    // install. A failure injected into the Circle-decision step — the stand-in for
    // a crash after the Store image is installed but before the Circle image is —
    // must roll the whole install transaction back, leaving no database at all: not
    // even the Store image on its own.
    let destination = tempfile::tempdir().expect("crash restore destination");
    let database_path = destination.path().join("store.db");
    let bootstrap = crate::sync::store::bootstrap_from_snapshot(
        &store.storage,
        store.storage.store_id(),
        store.root.clone(),
        &crate::joining::MembershipFloor(membership.head_refs().to_vec()),
        db.schema_version(),
        &database_path,
        &member,
    )
    .await
    .expect("restore the Store snapshot")
    .fail_circle_install_for_test();
    let outcome = bootstrap
        .open_database(
            store.storage.store_id(),
            &database_path,
            db.synced_tables().to_vec(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            "crash-restore-device".to_string(),
            std::sync::Arc::new(crate::clock::SystemClock),
            &circle_routing_migrations(),
            Some(&routing),
        )
        .await;
    match outcome {
        Ok(_) => panic!("an injected Circle-install failure must fail the whole restore"),
        Err(error) => assert!(
            error
                .to_string()
                .contains("injected Circle install failure"),
            "the restore fails at the Circle-install step: {error}"
        ),
    }
    assert!(
        !database_path.exists(),
        "the rolled-back restore exposes no database — the Store image did not commit \
         separately from the Circle install"
    );
}

#[tokio::test]
async fn post_close_circle_store_snapshot_restores_and_converges() {
    let routing = EncryptionService::from_key([42; 32]);
    let db = open_circle_routing_test_db();
    let (store, signer, founder) =
        persist_merge_operation(&db, "snapshot-restore-after-close").await;
    let circle_id = founder.circle_id();
    resume_circle_operations(&db, &store.storage, &signer)
        .await
        .expect("activate founder transition");

    // A Circle member who is a Store member without an active device: the
    // snapshot's stability quorum stays the single owner device.
    let member = UserKeypair::generate();
    let member_pubkey = keys::public_key_hex(&member);
    store
        .invite_member(
            &db,
            &signer,
            &crate::sync::hlc::Hlc::new(
                "snapshot-restore-owner".to_string(),
                std::sync::Arc::new(crate::clock::SystemClock),
            ),
            &member_pubkey,
            None,
            MemberRole::Member,
            &routing,
            "Restore Store",
        )
        .await
        .expect("invite Store member");
    let (_store_temp, store_dir) = temp_store_dir();
    let owner_storage = crate::storage::CloudSyncStorage::new(
        store.home.clone(),
        crate::storage::CloudCipher::Encrypted(routing.clone()),
        crate::storage::BlobPathScheme::Hashed,
        store.storage.store_id(),
        signer.clone(),
    )
    .expect("open Circle owner storage");
    let components = init_sync_over_storage(
        &crate::database::StoreDatabase::new(&db),
        owner_storage,
        StoreInitialization::OpenStore {
            expected_store_root: store.root.clone(),
        },
        Some(routing.clone()),
    )
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

    // Old-epoch Circle content, published under the initial epoch.
    {
        let write_id = db.new_write_id();
        let tables = db.synced_tables().to_vec();
        let routing = routing.clone();
        let audience = Some(circle_id.to_string());
        db.call(move |connection| {
            StoreDatabase::run_internal_store_write_transaction_on(
                connection,
                &tables,
                Some(&routing),
                write_id,
                |transaction| {
                    transaction
                        .execute(
                            "INSERT INTO documents (id, audience, _updated_at)
                             VALUES (?1, ?2, ?3)",
                            rusqlite::params![
                                "00000000-0000-4000-8000-000000000090",
                                audience,
                                "0000000002000-0000-owner"
                            ],
                        )
                        .map(|_| ())
                        .map_err(DbError::from)
                },
            )
        })
        .await
        .expect("capture old-epoch Circle content");
    }
    components
        .run_cycle(&crate::clock::SystemClock, None, &store_dir, None)
        .await
        .expect("publish old-epoch Circle content");

    // Drive the member-removal epoch close through to successor activation. Its
    // successor bootstrap covers the old-epoch content up to the accepted cutoff.
    components
        .remove_circle_member(circle_id, member_pubkey.clone())
        .await
        .expect("close the epoch by removing the roster member");
    publish_circle_epoch_close_response(&store.storage, &db, &signer)
        .await
        .expect("publish local Circle epoch-close response");
    components
        .run_cycle(&crate::clock::SystemClock, None, &store_dir, None)
        .await
        .expect("activate the Circle epoch-close outcome");

    // Publish a Store snapshot covering the post-close frontier; the single owner
    // device acknowledges it stable. Its image prunes the old-epoch retained rows
    // now covered by the successor bootstrap.
    let loaded_store = store
        .bind_device(&db, &signer)
        .await
        .expect("load the post-close Store snapshot");
    let mut authorized = loaded_store
        .authorize_writer()
        .await
        .expect("authorize the post-close Store snapshot");
    let (_snapshot_temp, snapshot_dir) = temp_store_dir();
    let cut = authorized
        .capture_snapshot_cut(
            snapshot_dir.as_ref().to_path_buf(),
            db.synced_tables().to_vec(),
            Some(&routing),
        )
        .await
        .expect("capture the post-close Store snapshot cut");
    let coverage = cut.coverage.clone();
    publish_store_snapshot_cut(&mut authorized, cut, "2026-07-24T01:00:00Z").await;
    loaded_store
        .stage_acknowledgement(coverage.clone(), "2026-07-24T01:00:01Z".to_string())
        .await
        .expect("stage post-close snapshot stability acknowledgement");
    loaded_store
        .drain_acknowledgements()
        .await
        .expect("activate post-close snapshot stability acknowledgement");

    // A device is restored from the snapshot. Installation validates the image's
    // retained inputs against the retention rule; the successor bootstrap's
    // coverage keeps retained rows a Store snapshot of a Circle store legitimately
    // carries, which the validator must accept.
    let membership = store
        .bind_device(&db, &signer)
        .await
        .expect("load snapshot Store")
        .membership_for_test()
        .await
        .expect("load membership for snapshot restore");
    let destination = tempfile::tempdir().expect("snapshot restore destination");
    let database_path = destination.path().join("store.db");
    let bootstrap = crate::sync::store::bootstrap_from_snapshot(
        &store.storage,
        store.storage.store_id(),
        store.root.clone(),
        &crate::joining::MembershipFloor(membership.head_refs().to_vec()),
        db.schema_version(),
        &database_path,
        &signer,
    )
    .await
    .expect("restore the post-close Store snapshot");
    let mut restored = bootstrap
        .open_database(
            store.storage.store_id(),
            &database_path,
            db.synced_tables().to_vec(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            "restored-device".to_string(),
            std::sync::Arc::new(crate::clock::SystemClock),
            &circle_routing_migrations(),
            Some(&routing),
        )
        .await
        .expect("install the restored snapshot database");

    // The restored device pulls and converges to the owner's accepted Store
    // frontier: the installed snapshot represents the closed epoch exactly, so
    // nothing is held and the projections agree.
    let (_restored_temp, restored_dir) = temp_store_dir();
    let pull = restored
        .pull(&restored_dir, Some(&routing))
        .await
        .expect("the restored device pulls the close without a foreign-key violation");
    assert!(
        pull.held_positions.is_empty(),
        "the restored device holds no positions after the close: {:?}",
        pull.held_positions
    );
    let owner_frontier = crate::database::StoreDatabase::new(&db)
        .materialized_frontier()
        .await
        .expect("read owner Store frontier");
    let restored_frontier = restored
        .materialized_frontier_for_test()
        .await
        .expect("read restored Store frontier");
    assert_eq!(
        restored_frontier, owner_frontier,
        "the restored device converges to the owner's accepted Store frontier"
    );

    // The same snapshot, restored by the REMOVED member, must not resurrect the
    // Circle. The Store image carries the owner's preserved coverage row, but the
    // removed member cannot decrypt the Circle, so selection clears it — and a
    // forced full replay then materializes none of the Circle's content. If the
    // clear were skipped, the preserved row would reconstruct the image and hand
    // the removed member the very rows the epoch close took away.
    let removed_destination = tempfile::tempdir().expect("removed-member restore destination");
    let removed_path = removed_destination.path().join("store.db");
    let removed_bootstrap = crate::sync::store::bootstrap_from_snapshot(
        &*store.storage,
        store.storage.store_id(),
        store.root.clone(),
        &crate::joining::MembershipFloor(membership.head_refs().to_vec()),
        db.schema_version(),
        &removed_path,
        &member,
    )
    .await
    .expect("restore the post-close Store snapshot as the removed member");
    let removed_db = removed_bootstrap
        .open_database(
            store.storage.store_id(),
            &removed_path,
            db.synced_tables().to_vec(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            "removed-member-device".to_string(),
            std::sync::Arc::new(crate::clock::SystemClock),
            &circle_routing_migrations(),
            Some(&routing),
        )
        .await
        .expect("install the removed-member restore database");

    let removed_coverage = removed_db
        .circle_bootstrap_coverage_for_test(circle_id)
        .await
        .expect("read removed-member Circle coverage");
    assert!(
        removed_coverage.is_none(),
        "the removed member retains no Circle coverage row"
    );

    let control_count: i64 = removed_db
        .circle_control_activation_count_for_test(circle_id)
        .await
        .expect("count restored Circle control indexes");
    assert!(
        control_count > 0,
        "the removed member still restores the Circle control indexes"
    );

    // The exact input a full replay reconstructs Circle images from carries no
    // entry for this Circle: with the coverage row cleared there is nothing to
    // rebuild, so no replay can hand the removed member the content the epoch close
    // took away. Were the clear skipped, the preserved row would reappear here and
    // re-arm the replay — this assertion is what makes the clear load-bearing.
    let replay_inputs = removed_db
        .circle_bootstrap_replay_inputs_for_test()
        .await
        .expect("read removed-member Circle replay inputs");
    assert!(
        replay_inputs.is_empty(),
        "the removed member has no Circle image to replay"
    );
}

/// End-to-end receipt that restore selection installs from a standalone Circle
/// snapshot when it dominates the other coverage candidates. The fixture authors
/// content under the successor epoch after the close and cuts a standalone snapshot
/// over it, so its coverage strictly dominates both the leaf-named successor
/// bootstrap and the preserved author-coverage row (both fixed at the close cutoff).
/// A fresh device restores, and the staged Install decision must name the standalone
/// snapshot's image.
///
/// The standalone snapshot is authored under the SUCCESSOR control, which the head
/// control is — so it is a retained activation and restore selection does not skip
/// it as reclaimed (the reclaimed-control skip only drops snapshots authored under a
/// control a later close reclaimed). Selection reads the standalone metadata and
/// image with the Circle epoch key the restorer's active leaf carries. That
/// threading is load-bearing: decrypting the standalone stream with any other key
/// fails the read outright (a wrong key is an error, not absence), so the restore
/// cannot install the snapshot and this assertion does not hold.
#[tokio::test]
async fn restore_installs_a_dominating_standalone_circle_snapshot() {
    let routing = EncryptionService::from_key([42; 32]);
    let db = open_circle_routing_test_db();
    let (store, signer, founder) =
        persist_merge_operation(&db, "standalone-restore-dominates").await;
    let circle_id = founder.circle_id();
    resume_circle_operations(&db, &store.storage, &signer)
        .await
        .expect("activate founder transition");

    let member = UserKeypair::generate();
    let member_pubkey = keys::public_key_hex(&member);
    store
        .invite_member(
            &db,
            &signer,
            &crate::sync::hlc::Hlc::new(
                "standalone-dominates-owner".to_string(),
                std::sync::Arc::new(crate::clock::SystemClock),
            ),
            &member_pubkey,
            None,
            MemberRole::Member,
            &routing,
            "Restore Store",
        )
        .await
        .expect("invite Store member");
    let (_store_temp, store_dir) = temp_store_dir();
    let owner_storage = crate::storage::CloudSyncStorage::new(
        store.home.clone(),
        crate::storage::CloudCipher::Encrypted(routing.clone()),
        crate::storage::BlobPathScheme::Hashed,
        store.storage.store_id(),
        signer.clone(),
    )
    .expect("open Circle owner storage");
    let components = init_sync_over_storage(
        &crate::database::StoreDatabase::new(&db),
        owner_storage,
        StoreInitialization::OpenStore {
            expected_store_root: store.root.clone(),
        },
        Some(routing.clone()),
    )
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

    // Pre-close content, then close the epoch by removing the member. The successor
    // bootstrap and the owner's successor leaf both cover the pre-close cutoff.
    write_circle_document(
        &db,
        &routing,
        circle_id,
        "00000000-0000-4000-8000-000000000090",
        "0000000002000-0000-owner",
    )
    .await;
    components
        .run_cycle(&crate::clock::SystemClock, None, &store_dir, None)
        .await
        .expect("publish pre-close Circle content");
    components
        .remove_circle_member(circle_id, member_pubkey.clone())
        .await
        .expect("close the epoch by removing the roster member");
    publish_circle_epoch_close_response(&store.storage, &db, &signer)
        .await
        .expect("publish local Circle epoch-close response");
    components
        .run_cycle(&crate::clock::SystemClock, None, &store_dir, None)
        .await
        .expect("activate the Circle epoch-close outcome");

    // Post-close content under the successor epoch advances the frontier past the
    // close cutoff, so a snapshot over it dominates the close-cutoff candidates.
    write_circle_document(
        &db,
        &routing,
        circle_id,
        "00000000-0000-4000-8000-000000000091",
        "0000000004000-0000-owner",
    )
    .await;
    components
        .run_cycle(&crate::clock::SystemClock, None, &store_dir, None)
        .await
        .expect("publish post-close Circle content");

    // Author the dominating standalone Circle snapshot under the successor epoch.
    let (_standalone_temp, standalone_dir) = temp_store_dir();
    let standalone = crate::sync::store::push_circle_snapshots_for_test(
        &db,
        &store.storage,
        standalone_dir.as_ref().join("standalone"),
        db.schema_version(),
        &signer,
        "2026-07-24T02:00:00Z",
        &routing,
    )
    .await
    .expect("author the dominating standalone Circle snapshot");
    let standalone_image_hash = standalone.bootstrap.image.image_hash;

    // A Store snapshot covering the post-close frontier, acknowledged stable.
    let loaded_store = store
        .bind_device(&db, &signer)
        .await
        .expect("load the post-close Store snapshot");
    let mut authorized = loaded_store
        .authorize_writer()
        .await
        .expect("authorize the post-close Store snapshot");
    let (_snapshot_temp, snapshot_dir) = temp_store_dir();
    let cut = authorized
        .capture_snapshot_cut(
            snapshot_dir.as_ref().to_path_buf(),
            db.synced_tables().to_vec(),
            Some(&routing),
        )
        .await
        .expect("capture the post-close Store snapshot cut");
    let coverage = cut.coverage.clone();
    publish_store_snapshot_cut(&mut authorized, cut, "2026-07-24T02:00:01Z").await;
    loaded_store
        .stage_acknowledgement(coverage.clone(), "2026-07-24T02:00:02Z".to_string())
        .await
        .expect("stage post-close snapshot stability acknowledgement");
    loaded_store
        .drain_acknowledgements()
        .await
        .expect("activate post-close snapshot stability acknowledgement");

    // A fresh device restores from the Store snapshot.
    let membership = store
        .bind_device(&db, &signer)
        .await
        .expect("load snapshot Store")
        .membership_for_test()
        .await
        .expect("load membership for snapshot restore");
    let destination = tempfile::tempdir().expect("standalone-restore destination");
    let database_path = destination.path().join("store.db");
    let bootstrap = crate::sync::store::bootstrap_from_snapshot(
        &*store.storage,
        store.storage.store_id(),
        store.root.clone(),
        &crate::joining::MembershipFloor(membership.head_refs().to_vec()),
        db.schema_version(),
        &database_path,
        &signer,
    )
    .await
    .expect("restore the post-close Store snapshot");
    let restored = bootstrap
        .open_database(
            store.storage.store_id(),
            &database_path,
            db.synced_tables().to_vec(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            "standalone-restore-device".to_string(),
            std::sync::Arc::new(crate::clock::SystemClock),
            &circle_routing_migrations(),
            Some(&routing),
        )
        .await
        .expect("install the restored snapshot database");

    // The staged Install decision chose the dominating standalone snapshot: the
    // coverage row names its image.
    let coverage_row = restored
        .circle_bootstrap_coverage_for_test(circle_id)
        .await
        .expect("read restored Circle coverage")
        .expect("the restore installs a Circle coverage row");
    assert_eq!(
        coverage_row.bootstrap.image.image_hash, standalone_image_hash,
        "restore installs the dominating standalone snapshot's image, not a \
         close-cutoff bootstrap"
    );
}

#[tokio::test]
async fn circle_acknowledgement_stays_readable_across_epoch_rotation() {
    let fixture = rotation_fixture("rotation-ack-read").await;
    let owner_pk = keys::public_key_hex(&fixture.signer);

    // A cycle publishes the owner's Circle acknowledgement under the current
    // (soon-rotated-away) epoch.
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("cycle publishes the owner's Circle acknowledgement");
    let (old_authoring, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(fixture.circle_id, &owner_pk)
        .await
        .expect("old Circle authoring context");
    let old_control = old_authoring.control.coord.clone();
    let old_epoch = old_authoring.control.value.epoch_id();
    let acknowledgements = StoreDatabase::new(&fixture.db)
        .activated_circle_acks(fixture.circle_id)
        .await
        .expect("read activated Circle acknowledgements");
    let ack_ref = acknowledgements
        .first()
        .cloned()
        .expect("the owner published a Circle acknowledgement");
    let before = crate::sync::store::load_circle_acknowledgement_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
        &ack_ref,
    )
    .await
    .expect("read acknowledgement through its exact control");
    assert_eq!(before.epoch_id, old_epoch);
    assert_eq!(before.control, old_control);
    // The owner authored the Circle; its projection never came from an image, so
    // the acknowledgement names no seed coverage.
    assert!(before.seeded_from.is_none());

    // Remove the roster member: the old epoch closes and a successor epoch/key
    // activates.
    remove_store_member(&fixture).await;
    fixture
        .components
        .remove_circle_member(fixture.circle_id, fixture.member_pubkey.clone())
        .await
        .expect("close the epoch by removing the roster member");
    publish_circle_epoch_close_response(&fixture.store.storage, &fixture.db, &fixture.signer)
        .await
        .expect("publish local Circle epoch-close response");
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("activate the Circle epoch-close outcome");
    let (new_authoring, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(fixture.circle_id, &owner_pk)
        .await
        .expect("successor Circle authoring context");
    let new_control = new_authoring.control.coord.clone();
    assert_ne!(new_control, old_control, "the epoch rotated");

    // The pre-rotation acknowledgement, sealed under the rotated-away epoch key,
    // stays readable after the epoch rotates: the read resolves that epoch's key
    // from the retained activation of the control the acknowledgement names.
    let after = crate::sync::store::load_circle_acknowledgement_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
        &ack_ref,
    )
    .await
    .expect("read the pre-rotation acknowledgement after the epoch rotated");
    assert_eq!(after.epoch_id, old_epoch);
    assert_eq!(after.control, old_control);
}

#[tokio::test]
async fn circle_snapshot_stability_requires_every_access_device_to_acknowledge() {
    let fixture = rotation_fixture("snapshot-stability").await;
    let snapshot_temp = tempfile::tempdir().expect("snapshot temp dir");

    // Author a Circle snapshot before any device has acknowledged coverage.
    crate::sync::store::push_circle_snapshots_for_test(
        &fixture.db,
        &fixture.store.storage,
        snapshot_temp.path().to_path_buf(),
        fixture.db.schema_version(),
        &fixture.signer,
        "2026-07-23T00:00:00Z",
        &EncryptionService::from_key([42; 32]),
    )
    .await
    .expect("author Circle snapshot");
    let published = StoreDatabase::new(&fixture.db)
        .latest_local_circle_snapshot(fixture.circle_id)
        .await
        .expect("read published Circle snapshot")
        .expect("a Circle snapshot was published");

    // The owner acknowledges its own coverage past the cut. The second member's
    // device holds active Circle access but has not acknowledged, so the snapshot
    // is not stable: an access-holding device that never acknowledged keeps the
    // snapshot unusable as coverage evidence.
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("owner publishes its Circle acknowledgement");
    assert!(!crate::sync::store::circle_snapshot_is_stable_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
        fixture.circle_id,
        &published.cut,
    )
    .await
    .expect("evaluate stability before the member acknowledges"));

    // The member device installs the Circle bootstrap and publishes its own Circle
    // acknowledgement covering the cut; the owner pulls and activates it.
    let member_storage = member_storage(&fixture);
    let (_member_temp, member_store_dir) = temp_store_dir();
    member_pull(&fixture, &member_storage, &member_store_dir).await;
    member_publish_acknowledgements(&fixture, &member_storage, "2026-07-23T01:00:00Z").await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("owner activates the member's Circle acknowledgement");

    // Every access-holding device has now acknowledged coverage past the cut.
    assert!(crate::sync::store::circle_snapshot_is_stable_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
        fixture.circle_id,
        &published.cut,
    )
    .await
    .expect("evaluate stability once every access device acknowledged"));
}

#[tokio::test]
async fn circle_snapshot_stays_readable_across_epoch_rotation() {
    let fixture = rotation_fixture("rotation-snapshot-read").await;
    let owner_pk = keys::public_key_hex(&fixture.signer);
    let (old_authoring, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(fixture.circle_id, &owner_pk)
        .await
        .expect("old Circle authoring context");
    let old_control = old_authoring.control.coord.clone();
    let old_epoch = old_authoring.control.value.epoch_id();

    // Author a Circle snapshot under the current (soon-rotated-away) epoch.
    let snapshot_temp = tempfile::tempdir().expect("snapshot temp dir");
    crate::sync::store::push_circle_snapshots_for_test(
        &fixture.db,
        &fixture.store.storage,
        snapshot_temp.path().to_path_buf(),
        fixture.db.schema_version(),
        &fixture.signer,
        "2026-07-23T00:00:00Z",
        &EncryptionService::from_key([42; 32]),
    )
    .await
    .expect("author Circle snapshot");

    // Rotate the epoch by removing the roster member.
    remove_store_member(&fixture).await;
    fixture
        .components
        .remove_circle_member(fixture.circle_id, fixture.member_pubkey.clone())
        .await
        .expect("close the epoch by removing the roster member");
    publish_circle_epoch_close_response(&fixture.store.storage, &fixture.db, &fixture.signer)
        .await
        .expect("publish local Circle epoch-close response");
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("activate the Circle epoch-close outcome");

    // The old-epoch snapshot, sealed under the rotated-away key, stays readable
    // to a current member: resolve the key from the retained activation of the
    // control the snapshot names.
    let device = fixture
        .store
        .bind_device(&fixture.db, &fixture.signer)
        .await
        .expect("bind retained Circle activation Store");
    let retained = device
        .circle_package_access(fixture.circle_id, old_control.clone())
        .await
        .expect("read retained Circle activation")
        .expect("the pre-rotation control's activation is retained");
    let metas = crate::sync::store::load_circle_snapshot_metas_for_test(
        &fixture.db,
        &fixture.store.storage,
        fixture.circle_id,
        retained.into_encryption(),
        &fixture.signer,
    )
    .await
    .expect("read the pre-rotation Circle snapshot after the epoch rotated");
    let old = metas
        .iter()
        .find(|meta| meta.epoch_id == old_epoch)
        .expect("the pre-rotation snapshot remains readable");
    assert_eq!(old.control, old_control);
}

/// A standalone Circle snapshot carries row-routing ids in its image, and those
/// ids must be derived from the Store generation-one key — the key that routed
/// the rows when the host captured them — not the per-Circle epoch key that only
/// seals the published objects. This authors a snapshot over Circle content and
/// checks that a recipient authenticates its routing state against the true Store
/// routing key. When authoring derived routing from the Circle epoch key instead,
/// the projection's routes failed to authenticate against the Store-keyed rows and
/// no image could be authored at all.
#[tokio::test]
async fn standalone_circle_snapshot_authenticates_under_the_true_store_routing_key() {
    let fixture = rotation_fixture("standalone-snapshot-true-routing").await;
    let owner_pk = keys::public_key_hex(&fixture.signer);

    // Circle content captured through the host write path, routed with the Store key.
    capture_document(
        &fixture,
        "00000000-0000-4000-8000-000000000099",
        Some(fixture.circle_id),
        "0000000002000-0000-owner",
    )
    .await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("publish Circle content");

    let snapshot_temp = tempfile::tempdir().expect("snapshot temp dir");
    crate::sync::store::push_circle_snapshots_for_test(
        &fixture.db,
        &fixture.store.storage,
        snapshot_temp.path().to_path_buf(),
        fixture.db.schema_version(),
        &fixture.signer,
        "2026-07-24T00:00:00Z",
        &EncryptionService::from_key([42; 32]),
    )
    .await
    .expect("author the standalone Circle snapshot");

    // A recipient reads the image with the Circle epoch key and authenticates its
    // routing state against the true Store routing key.
    let (authoring, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(fixture.circle_id, &owner_pk)
        .await
        .expect("Circle authoring context");
    let (epoch_encryption, _) = StoreDatabase::new(&fixture.db)
        .circle_publication_context(fixture.circle_id, authoring.control.coord.clone())
        .await
        .expect("Circle publication context");
    crate::sync::store::verify_standalone_circle_snapshot_image_for_test(
        &fixture.db,
        &fixture.store.storage,
        fixture.circle_id,
        epoch_encryption,
        &EncryptionService::from_key([42; 32]),
        &fixture.signer,
    )
    .await
    .expect("standalone Circle snapshot authenticates under the true Store routing key");
}

/// Removing a roster member closes the Circle epoch and rotates its key away from
/// the generation-one founding key, so the rotated Circle key has no generation-one
/// entry to derive a row-routing key from. Standalone snapshot authoring must still
/// succeed, because it derives routing from the Store generation-one key — which an
/// epoch close never touches. Deriving routing from the rotated Circle key errored
/// `MissingGenerationOne`, killing snapshot authoring for the Circle every cycle
/// after the close.
#[tokio::test]
async fn standalone_circle_snapshot_authoring_survives_epoch_rotation() {
    let fixture = rotation_fixture("standalone-snapshot-rotation").await;

    capture_document(
        &fixture,
        "00000000-0000-4000-8000-000000000099",
        Some(fixture.circle_id),
        "0000000002000-0000-owner",
    )
    .await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("publish pre-close Circle content");

    // Close the epoch by removing the roster member, rotating the Circle key.
    fixture
        .components
        .remove_circle_member(fixture.circle_id, fixture.member_pubkey.clone())
        .await
        .expect("close the epoch by removing the roster member");
    publish_circle_epoch_close_response(&fixture.store.storage, &fixture.db, &fixture.signer)
        .await
        .expect("publish local Circle epoch-close response");
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("activate the Circle epoch-close outcome");

    // Authoring derives routing from the Store generation-one key, so the rotated
    // Circle key having no generation-one entry no longer aborts the capture.
    let snapshot_temp = tempfile::tempdir().expect("snapshot temp dir");
    crate::sync::store::push_circle_snapshots_for_test(
        &fixture.db,
        &fixture.store.storage,
        snapshot_temp.path().to_path_buf(),
        fixture.db.schema_version(),
        &fixture.signer,
        "2026-07-24T00:00:00Z",
        &EncryptionService::from_key([42; 32]),
    )
    .await
    .expect("author the standalone Circle snapshot after the epoch rotated");

    assert!(
        StoreDatabase::new(&fixture.db)
            .latest_local_circle_snapshot(fixture.circle_id)
            .await
            .expect("read the published Circle snapshot")
            .is_some(),
        "a standalone Circle snapshot is published after the rotation"
    );
}

#[tokio::test]
async fn member_circle_acknowledgement_names_its_seed_bootstrap_coverage() {
    let fixture = rotation_fixture("circle-ack-seeded-from").await;
    let circle_id = fixture.circle_id;

    // The member device installs the Circle bootstrap: its projection seeds from a
    // real coverage row the install recorded.
    let member_storage = member_storage(&fixture);
    let (_member_temp, member_store_dir) = temp_store_dir();
    member_pull(&fixture, &member_storage, &member_store_dir).await;
    let member_coverage = fixture
        .member_db
        .call(move |conn| StoreDatabase::circle_bootstrap_coverage_ref_on(conn, circle_id))
        .await
        .expect("read member Circle bootstrap coverage")
        .expect("the member's projection seeded from a real bootstrap coverage row");

    // The member publishes its Circle acknowledgement; the owner pulls and
    // activates it alongside its own.
    member_publish_acknowledgements(&fixture, &member_storage, "2026-07-23T01:00:00Z").await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("owner activates the member's Circle acknowledgement");

    // The owner reads and verifies the member's acknowledgement: its seed coverage
    // is present and names the exact bootstrap coverage row the member installed.
    let member_device_id = local_device_id(&fixture.member_db).await;
    let member_ack_ref = StoreDatabase::new(&fixture.db)
        .activated_circle_ack(circle_id, member_device_id)
        .await
        .expect("read activated member Circle acknowledgement")
        .expect("the owner activated the member's Circle acknowledgement");
    let member_ack = crate::sync::store::load_circle_acknowledgement_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
        &member_ack_ref,
    )
    .await
    .expect("owner reads the member's Circle acknowledgement");
    assert_eq!(
        member_ack.seeded_from.as_ref(),
        Some(&member_coverage),
        "the member's acknowledgement names its exact seed coverage row"
    );

    // The founder authored the Circle; its projection never came from an image, so
    // its own acknowledgement names no seed coverage.
    let owner_device_id = local_device_id(&fixture.db).await;
    let owner_ack_ref = StoreDatabase::new(&fixture.db)
        .activated_circle_ack(circle_id, owner_device_id)
        .await
        .expect("read activated owner Circle acknowledgement")
        .expect("the owner activated its own Circle acknowledgement");
    let owner_ack = crate::sync::store::load_circle_acknowledgement_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
        &owner_ack_ref,
    )
    .await
    .expect("owner reads its own Circle acknowledgement");
    assert!(
        owner_ack.seeded_from.is_none(),
        "the founder's acknowledgement names no seed coverage"
    );
}

#[tokio::test]
async fn a_removed_member_cannot_read_a_successor_epoch_circle_snapshot() {
    let fixture = rotation_fixture("successor-snapshot-unreadable").await;
    let circle_id = fixture.circle_id;
    let owner_pk = keys::public_key_hex(&fixture.signer);

    // The member installs the Circle bootstrap and holds the current epoch key.
    let member_storage = member_storage(&fixture);
    let (_member_temp, member_store_dir) = temp_store_dir();
    member_pull(&fixture, &member_storage, &member_store_dir).await;

    // Close the epoch by removing the member from the Circle; the owner drives the
    // close to successor activation. The member stays a Store member.
    fixture
        .components
        .remove_circle_member(circle_id, fixture.member_pubkey.clone())
        .await
        .expect("close the epoch by removing the Circle member");
    publish_circle_epoch_close_response(&fixture.store.storage, &fixture.db, &fixture.signer)
        .await
        .expect("publish local Circle epoch-close response");
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("activate the Circle epoch-close outcome");

    let (successor, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(circle_id, &owner_pk)
        .await
        .expect("successor Circle authoring context");
    let successor_control = successor.control.coord.clone();

    // A successor-epoch Circle snapshot is sealed under the successor epoch key.
    // The owner, a remaining member, resolves that key from its retained
    // activation — so it could author and read such a snapshot.
    let owner_device = fixture
        .store
        .bind_device(&fixture.db, &fixture.signer)
        .await
        .expect("bind owner successor Circle Store");
    assert!(
        owner_device
            .circle_package_access(circle_id, successor_control.clone())
            .await
            .expect("read owner successor access")
            .is_some(),
        "the owner resolves the successor epoch key"
    );

    // The removed member pulls the post-close state but never received the
    // successor epoch key: it cannot resolve the key that seals a successor-epoch
    // snapshot, so any such snapshot is unreadable to it.
    member_pull(&fixture, &member_storage, &member_store_dir).await;
    let member_device = fixture
        .store
        .bind_device(&fixture.member_db, &fixture.member)
        .await
        .expect("bind removed member Circle Store");
    assert!(
        member_device
            .circle_package_access(circle_id, successor_control)
            .await
            .expect("read member successor access")
            .is_none(),
        "the removed member cannot resolve the successor epoch key"
    );
}

#[tokio::test]
async fn circle_snapshot_publication_resumes_idempotently_across_upload_boundaries() {
    // Derive the number of exact object creates one Circle snapshot publication
    // makes (the image, then its metadata) from a clean run rather than hardcoding
    // it, then use the metadata create as the crash boundary.
    let baseline = rotation_fixture("snapshot-resume-baseline").await;
    let baseline_temp = tempfile::tempdir().expect("baseline snapshot temp dir");
    let before = baseline.store.home.exact_create_count();
    crate::sync::store::drive_circle_snapshot_publications_for_test(
        &baseline.db,
        &baseline.store.storage,
        baseline_temp.path().to_path_buf(),
        baseline.db.schema_version(),
        &baseline.signer,
        "2026-07-23T00:00:00Z",
        Some(&EncryptionService::from_key([42; 32])),
    )
    .await
    .expect("clean Circle snapshot publication");
    let meta_create = baseline.store.home.exact_create_count() - before;
    assert_eq!(
        meta_create, 2,
        "a blobless Circle snapshot uploads an image, then its metadata"
    );
    assert!(
        StoreDatabase::new(&baseline.db)
            .latest_local_circle_snapshot(baseline.circle_id)
            .await
            .expect("read baseline snapshot")
            .is_some(),
        "the clean publication completes"
    );

    // Boundary — image upload to metadata publication: the image is uploaded but
    // the metadata upload is interrupted before its bytes land. The publication is
    // left durable and pending with no completed snapshot; the cycle logs the
    // failure and continues. The next run resumes it and completes it exactly once,
    // the exact readback accepting the image already uploaded.
    {
        let fixture = rotation_fixture("snapshot-resume-image-meta").await;
        let circle_id = fixture.circle_id;
        let temp = tempfile::tempdir().expect("snapshot temp dir");
        fixture
            .store
            .home
            .fail_exact_create_before_call(meta_create);
        drive_circle_snapshots(&fixture, &temp, "2026-07-23T00:00:00Z")
            .await
            .expect("the cycle logs the interrupted publication and continues");
        assert!(
            latest_circle_snapshot(&fixture, circle_id).await.is_none(),
            "no snapshot completes when the metadata upload is interrupted"
        );
        assert!(
            pending_circle_snapshot(&fixture, circle_id).await.is_some(),
            "the interrupted publication remains durable for resume"
        );

        drive_circle_snapshots(&fixture, &temp, "2026-07-23T00:00:01Z")
            .await
            .expect("resume completes the pending publication");
        assert!(
            latest_circle_snapshot(&fixture, circle_id).await.is_some(),
            "the resumed publication completes"
        );
        assert!(
            pending_circle_snapshot(&fixture, circle_id).await.is_none(),
            "no durable publication remains after resume"
        );
        assert_eq!(
            latest_circle_snapshot_generation(&fixture, circle_id).await,
            Some(0),
            "the resume opens no second generation"
        );
    }

    // Boundary — metadata publication to completion: the metadata bytes are durable
    // but the upload response is lost before the publication is recorded complete.
    // The exact readback settles the lost upload within the run, so the publication
    // completes without duplication and a re-run opens no new generation.
    {
        let fixture = rotation_fixture("snapshot-resume-meta-complete").await;
        let circle_id = fixture.circle_id;
        let temp = tempfile::tempdir().expect("snapshot temp dir");
        fixture.store.home.fail_exact_create_after_call(meta_create);
        drive_circle_snapshots(&fixture, &temp, "2026-07-23T00:00:00Z")
            .await
            .expect("the lost metadata-upload response is settled by exact readback");
        assert_eq!(
            latest_circle_snapshot_generation(&fixture, circle_id).await,
            Some(0),
            "the settled publication completes exactly one generation"
        );
        assert!(
            pending_circle_snapshot(&fixture, circle_id).await.is_none(),
            "no durable publication remains after the settled upload"
        );

        drive_circle_snapshots(&fixture, &temp, "2026-07-23T00:00:01Z")
            .await
            .expect("a re-run is idempotent");
        assert_eq!(
            latest_circle_snapshot_generation(&fixture, circle_id).await,
            Some(0),
            "the re-run duplicates no generation"
        );
        assert!(
            pending_circle_snapshot(&fixture, circle_id).await.is_none(),
            "the idempotent re-run opens no new publication"
        );
    }
}

async fn drive_circle_snapshots(
    fixture: &RotationFixture,
    temp: &tempfile::TempDir,
    stamp: &str,
) -> Result<(), crate::sync::store::snapshot::SnapshotError> {
    crate::sync::store::drive_circle_snapshot_publications_for_test(
        &fixture.db,
        &fixture.store.storage,
        temp.path().to_path_buf(),
        fixture.db.schema_version(),
        &fixture.signer,
        stamp,
        Some(&EncryptionService::from_key([42; 32])),
    )
    .await
}

async fn latest_circle_snapshot(
    fixture: &RotationFixture,
    circle_id: CircleId,
) -> Option<crate::database::PublishedCircleSnapshot> {
    StoreDatabase::new(&fixture.db)
        .latest_local_circle_snapshot(circle_id)
        .await
        .expect("read latest local Circle snapshot")
}

async fn latest_circle_snapshot_generation(
    fixture: &RotationFixture,
    circle_id: CircleId,
) -> Option<u64> {
    latest_circle_snapshot(fixture, circle_id)
        .await
        .map(|snapshot| snapshot.reference.generation)
}

async fn pending_circle_snapshot(
    fixture: &RotationFixture,
    circle_id: CircleId,
) -> Option<crate::database::DurableCircleSnapshotPublication> {
    StoreDatabase::new(&fixture.db)
        .outbound_circle_snapshot_publication(circle_id)
        .await
        .expect("read pending Circle snapshot publication")
}

/// Release every retained-replay ownership record, as a superseding coverage cut
/// would in production, so a covered package becomes reclaim-eligible.
async fn release_retained_replay_ownership(fixture: &RotationFixture) {
    fixture
        .db
        .call(|connection| {
            let transaction = connection.unchecked_transaction().map_err(DbError::from)?;
            StoreDatabase::remove_retained_replay_ownership_from_snapshot_on(&transaction)?;
            transaction.commit().map_err(DbError::from)
        })
        .await
        .expect("release retained replay ownership");
}

/// Publish one Circle package and drive both devices to acknowledge coverage past
/// the Circle snapshot the owner's cycle authored over it, returning the package
/// and its exact activating commit. After this every active-access device's latest
/// acknowledgement dominates the snapshot cut, so the snapshot is stable.
async fn publish_covered_circle_package(
    fixture: &RotationFixture,
    member_storage: &Arc<crate::storage::CloudSyncStorage>,
    member_store_dir: &crate::store_dir::StoreDir,
    row_id: &str,
) -> (
    crate::protocol::store_commit::CirclePackageRef,
    crate::protocol::store_commit::StoreBatchCommitRef,
) {
    let write_id = capture_document(
        fixture,
        row_id,
        Some(fixture.circle_id),
        "2026-07-23T00:10:00Z",
    )
    .await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("publish the Circle package");
    let published = match crate::database::StoreDatabase::new(&fixture.db)
        .write_status(&write_id)
        .await
        .expect("read Circle write status")
    {
        crate::WriteStatus::Published(position) => position.commit,
        status => panic!("the Circle write must publish: {status:?}"),
    };
    let owner = fixture
        .store
        .bind_device(&fixture.db, &fixture.signer)
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

    // The member pulls the package so its acknowledgement can dominate a cut that
    // covers it. Author a Circle snapshot whose cut covers the package activation.
    member_pull(fixture, member_storage, member_store_dir).await;
    let snapshot_temp = tempfile::tempdir().expect("snapshot temp dir");
    crate::sync::store::push_circle_snapshots_for_test(
        &fixture.db,
        &fixture.store.storage,
        snapshot_temp.path().to_path_buf(),
        fixture.db.schema_version(),
        &fixture.signer,
        "2026-07-23T00:15:00Z",
        &EncryptionService::from_key([42; 32]),
    )
    .await
    .expect("author the Circle snapshot covering the package");

    // Both active-access devices acknowledge a frontier dominating the snapshot cut,
    // so the snapshot becomes acknowledgement-stable coverage evidence.
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("owner acknowledges the snapshot cut");
    member_pull(fixture, member_storage, member_store_dir).await;
    member_publish_acknowledgements(fixture, member_storage, "2026-07-23T00:20:00Z").await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("owner activates the member acknowledgement");
    (circle_package, published)
}

/// Capture a document and one blob-bearing file row under `audience`, staging the
/// file's bytes in the local store so the next cycle uploads them.
async fn capture_document_with_file(
    fixture: &RotationFixture,
    document_id: &str,
    file_id: &str,
    audience: Option<CircleId>,
    bytes: &[u8],
    stamp: &str,
) -> crate::WriteId {
    let write_id = fixture.db.new_write_id();
    let captured = write_id.clone();
    let tables = fixture.db.synced_tables().to_vec();
    let routing = EncryptionService::from_key([42; 32]);
    let insert = format!(
        "INSERT INTO documents (id, audience, _updated_at)
         VALUES ('{document_id}', {}, '{stamp}');
         INSERT INTO document_files (id, document_id, size, hash, _updated_at)
         VALUES ('{file_id}', '{document_id}', {}, '{}', '{stamp}');",
        audience
            .map(|circle_id| format!("'{circle_id}'"))
            .unwrap_or_else(|| "NULL".to_string()),
        bytes.len(),
        crate::blob::content_hash(bytes),
    );
    fixture
        .db
        .call(move |connection| {
            StoreDatabase::run_internal_store_write_transaction_on(
                connection,
                &tables,
                Some(&routing),
                captured,
                |transaction| transaction.execute_batch(&insert).map_err(DbError::from),
            )
        })
        .await
        .expect("capture document and its file row");
    crate::blob::local_files::store(&fixture.store_dir, "files", file_id, bytes)
        .await
        .expect("stage the document file bytes");
    write_id
}

/// Move a document to another audience, republishing every blob its file rows
/// carry under the destination audience's locator.
async fn move_document_audience(
    fixture: &RotationFixture,
    document_id: &str,
    audience: Option<CircleId>,
    stamp: &str,
) {
    let write_id = fixture.db.new_write_id();
    let tables = fixture.db.synced_tables().to_vec();
    let gates = fixture.db.gates();
    let blob_decls = fixture.db.blob_decls();
    let audience_value = audience.map(|circle_id| circle_id.to_string());
    let document_id = document_id.to_string();
    let stamp = stamp.to_string();
    // An audience move re-seals every blob the moved rows carry under the
    // destination audience's key, so the write needs the storage and store
    // directory that staging reads and writes.
    let staging = fixture
        .components
        .store()
        .host_write_blob_staging(tokio::runtime::Handle::current(), fixture.store_dir.clone());
    fixture
        .db
        .call(move |connection| {
            let routing = EncryptionService::from_key([42; 32]);
            StoreDatabase::run_store_write_transaction_on(
                connection,
                &tables,
                &gates,
                &blob_decls,
                Some(&routing),
                Some(&staging),
                write_id,
                |transaction| {
                    transaction
                        .execute(
                            "UPDATE documents SET audience = ?2, _updated_at = ?3 WHERE id = ?1",
                            rusqlite::params![document_id, audience_value, stamp],
                        )
                        .map(|_| ())
                        .map_err(DbError::from)
                },
            )
        })
        .await
        .expect("move the document to another audience");
}

/// Every stored blob this device holds an ownership record for, in whichever
/// audience its locator addresses.
async fn stored_blobs(fixture: &RotationFixture) -> Vec<crate::blob::locator::StoredBlobRef> {
    StoreDatabase::new(&fixture.db)
        .stored_blob_reclaim_candidates_for_test()
        .await
        .expect("read stored blob candidates")
        .into_iter()
        .map(|(stored, _)| stored)
        .collect()
}

/// Whether the exact blob ciphertext is still readable in cloud storage.
async fn blob_object_present(
    fixture: &RotationFixture,
    stored: &crate::blob::locator::StoredBlobRef,
) -> bool {
    match fixture.store.storage.verify_blob_object(stored).await {
        Ok(()) => true,
        Err(crate::storage::StorageError::NotFound(_)) => false,
        Err(error) => panic!("read the exact stored blob: {error}"),
    }
}

/// A user-initiated blob deletion writes a signed tombstone and the graced GC
/// performs the delete, but only after re-checking that no live row still
/// references the blob. That check has to be able to answer for a row whose
/// locality comes from an audience column rather than a keep gate — a Circle
/// document's attachment is exactly that shape — or the whole GC pass fails and no
/// tombstone anywhere is ever collected.
#[tokio::test]
async fn tombstone_gc_resolves_a_live_reference_through_an_audience_scoped_row() {
    let fixture = rotation_fixture("audience-scoped-tombstone-gc").await;
    let member_storage = member_storage(&fixture);
    let (_member_temp, member_store_dir) = temp_store_dir();
    member_pull(&fixture, &member_storage, &member_store_dir).await;

    capture_document_with_file(
        &fixture,
        "00000000-0000-4000-8000-0000000000e3",
        "00000000-0000-4000-8000-0000000000f3",
        Some(fixture.circle_id),
        b"circle attachment under a tombstone",
        "2026-07-23T00:10:00Z",
    )
    .await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("publish the Circle document and its file");
    let published = stored_blobs(&fixture).await;
    let [stored] = published.as_slice() else {
        panic!("the Circle document published exactly one blob: {published:?}");
    };
    let stored = stored.clone();

    // A stale tombstone over a blob whose row is still live: past the grace the GC
    // must resolve the reference, cancel the tombstone, and keep the ciphertext.
    let deleted_at = chrono::DateTime::parse_from_rfc3339("2026-07-23T00:11:00+00:00")
        .expect("valid tombstone instant")
        .with_timezone(&chrono::Utc);
    let enqueued = stored.clone();
    let store_database = StoreDatabase::new(&fixture.db);
    store_database
        .enqueue_blob_delete_for_test(enqueued, "2026-07-23T00:11:00Z".to_string())
        .await
        .expect("enqueue the blob deletion");
    let cipher = std::sync::RwLock::new(crate::storage::CloudCipher::Encrypted(
        EncryptionService::from_key([42; 32]),
    ));
    let loaded_store = fixture
        .store
        .bind_device(&fixture.db, &fixture.signer)
        .await
        .expect("load the Owner Store");
    let writer = loaded_store
        .authorize_writer()
        .await
        .expect("authorize the Owner Store");
    assert_eq!(
        writer
            .drain_tombstones(
                fixture.store.home.as_ref(),
                &cipher,
                &crate::storage::PendingRotation::default(),
                &crate::clock::FixedClock(deleted_at),
            )
            .await
            .expect("write the signed tombstone"),
        1,
        "the queued deletion writes one tombstone"
    );
    let tombstone_key = format!(
        "blob_tombstones/{}{}",
        crate::protocol::remote_object::remote_object_id(stored.object()),
        crate::storage::CloudCipher::Encrypted(EncryptionService::from_key([42; 32])).suffix(),
    );
    assert!(
        fixture.store.home.read(&tombstone_key).await.is_ok(),
        "the tombstone is written at its exact slot"
    );

    let past = crate::clock::FixedClock(
        deleted_at + crate::blob::BLOB_TOMBSTONE_GRACE + chrono::Duration::seconds(1),
    );
    let collected = writer
        .gc_tombstones(fixture.store.home.as_ref(), &cipher, &past)
        .await
        .expect("run the graced tombstone GC over an audience-scoped blob");

    assert_eq!(
        collected, 0,
        "a live row still references the blob, so nothing is collected"
    );
    assert!(
        blob_object_present(&fixture, &stored).await,
        "the referenced ciphertext stays in cloud storage"
    );
    assert!(
        fixture.store.home.read(&tombstone_key).await.is_err(),
        "the stale tombstone is canceled"
    );
}

/// An audience move re-seals every blob its rows carry under the destination
/// audience's key, which mints a new locator for content the row still binds at
/// its old `_updated_at` — and one row stamp binds one exact locator. The move
/// carries its own stamp onto the blob rows it drags along, so the new binding is
/// a new one and the host never has to know that publishing a moved subtree needs
/// its children restamped by hand.
#[tokio::test]
async fn an_audience_move_restamps_the_blob_rows_it_drags() {
    let fixture = rotation_fixture("audience-move-restamps-blob-rows").await;
    let member_storage = member_storage(&fixture);
    let (_member_temp, member_store_dir) = temp_store_dir();
    member_pull(&fixture, &member_storage, &member_store_dir).await;

    let document = "00000000-0000-4000-8000-0000000000e4";
    let file = "00000000-0000-4000-8000-0000000000f4";
    capture_document_with_file(
        &fixture,
        document,
        file,
        Some(fixture.circle_id),
        b"attachment that moves with its document",
        "2026-07-23T00:10:00Z",
    )
    .await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("publish the Circle document and its file");
    assert_eq!(
        document_file_stamp(&fixture, file).await,
        "2026-07-23T00:10:00Z",
    );

    // The move touches the document alone: the file row keeps the stamp its
    // published blob is already bound at.
    move_document_audience(&fixture, document, None, "2026-07-23T00:20:00Z").await;

    assert_eq!(
        document_file_stamp(&fixture, file).await,
        "2026-07-23T00:20:00Z",
        "the dragged blob row carries the stamp its move published it at",
    );
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("republish the document and its re-sealed file under the Store audience");
}

/// The `_updated_at` a document's file row currently carries.
async fn document_file_stamp(fixture: &RotationFixture, file_id: &str) -> String {
    let file_id = file_id.to_string();
    fixture
        .db
        .call(move |connection| {
            connection
                .query_row(
                    "SELECT _updated_at FROM document_files WHERE id = ?1",
                    [file_id],
                    |row| row.get::<_, String>(0),
                )
                .map_err(DbError::from)
        })
        .await
        .expect("read the document file row stamp")
}

/// Moving a row between audiences republishes its blob under the destination
/// audience's locator and drops the binding to the source ciphertext, which
/// nothing else ever deletes. Reclamation deletes exactly the ciphertext no live
/// row binds any more — in both directions, since a document can leave a Circle
/// for the Store audience or join one from it — and leaves the destination
/// ciphertext, which a live row does bind, alone.
#[tokio::test]
async fn audience_blob_reclaim_deletes_the_stranded_source_ciphertext() {
    let fixture = rotation_fixture("audience-blob-reclaim").await;
    let member_storage = member_storage(&fixture);
    let (_member_temp, member_store_dir) = temp_store_dir();
    member_pull(&fixture, &member_storage, &member_store_dir).await;

    let leaving = "00000000-0000-4000-8000-0000000000e1";
    let joining = "00000000-0000-4000-8000-0000000000e2";
    capture_document_with_file(
        &fixture,
        leaving,
        "00000000-0000-4000-8000-0000000000f1",
        Some(fixture.circle_id),
        b"circle attachment",
        "2026-07-23T00:10:00Z",
    )
    .await;
    capture_document_with_file(
        &fixture,
        joining,
        "00000000-0000-4000-8000-0000000000f2",
        None,
        b"store attachment",
        "2026-07-23T00:10:01Z",
    )
    .await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("publish both documents and their files");

    let sources = stored_blobs(&fixture).await;
    assert_eq!(
        sources.len(),
        2,
        "one ciphertext per audience is published: {sources:?}"
    );
    for stored in &sources {
        assert!(
            blob_object_present(&fixture, stored).await,
            "the published ciphertext is uploaded"
        );
    }

    // Nothing has moved yet: every ciphertext is still bound by a live row.
    crate::sync::store::reclaim_packages_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
    )
    .await
    .expect("run reclamation while every blob is still bound");
    for stored in &sources {
        assert!(
            blob_object_present(&fixture, stored).await,
            "a blob a live row still binds is never reclaimed"
        );
    }

    move_document_audience(&fixture, leaving, None, "2026-07-23T00:20:00Z").await;
    move_document_audience(
        &fixture,
        joining,
        Some(fixture.circle_id),
        "2026-07-23T00:20:01Z",
    )
    .await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("republish both documents under their destination audiences");

    let after_move = stored_blobs(&fixture).await;
    let destinations = after_move
        .into_iter()
        .filter(|stored| !sources.contains(stored))
        .collect::<Vec<_>>();
    assert_eq!(
        destinations.len(),
        2,
        "each move republished its blob under a new locator: {destinations:?}"
    );

    // A blob a published snapshot image lists is read by devices restoring from
    // that image, which have no rows at all — so an unbound blob an image still
    // names is held back rather than deleted.
    let mut pinned_by_an_image = Vec::new();
    for stored in &sources {
        if StoreDatabase::new(&fixture.db)
            .stored_blob_has_snapshot_owner_for_test(stored.clone())
            .await
            .expect("read the blob's snapshot ownership")
        {
            pinned_by_an_image.push(stored.clone());
        }
    }

    // A blob an accepted merge still replays is not free however its rows moved;
    // release that ownership, as a superseding coverage cut does in production.
    release_retained_replay_ownership(&fixture).await;
    crate::sync::store::reclaim_packages_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
    )
    .await
    .expect("reclaim the stranded source ciphertext");

    let mut deleted = 0;
    for stored in &sources {
        if pinned_by_an_image.contains(stored) {
            assert!(
                blob_object_present(&fixture, stored).await,
                "a blob a published snapshot image lists survives its rows moving away: {:?}",
                stored.locator().audience()
            );
            continue;
        }
        assert!(
            !blob_object_present(&fixture, stored).await,
            "the stranded source ciphertext is deleted: {:?}",
            stored.locator().audience()
        );
        deleted += 1;
    }
    assert!(
        deleted > 0,
        "at least one stranded source ciphertext was reclaimable"
    );
    for stored in &destinations {
        assert!(
            blob_object_present(&fixture, stored).await,
            "the destination ciphertext a live row binds survives: {:?}",
            stored.locator().audience()
        );
    }
}

/// Every reclaim kind rides the same durable journal, so a delete that fails
/// between authorization and deletion must leave a plan the next run finishes.
/// Driven over a stranded audience blob because its eligibility is fully
/// controlled here — the move unbinds it and releasing replay ownership frees it —
/// so the interruption lands on the delete under test rather than on whatever a
/// setup cycle happened to reclaim first.
#[tokio::test]
async fn interrupted_audience_blob_reclaim_resumes_on_restart() {
    let fixture = rotation_fixture("audience-blob-crash-resume").await;
    let member_storage = member_storage(&fixture);
    let (_member_temp, member_store_dir) = temp_store_dir();
    member_pull(&fixture, &member_storage, &member_store_dir).await;

    let document = "00000000-0000-4000-8000-0000000000e5";
    capture_document_with_file(
        &fixture,
        document,
        "00000000-0000-4000-8000-0000000000f5",
        Some(fixture.circle_id),
        b"circle attachment for the interrupted reclaim",
        "2026-07-23T00:10:00Z",
    )
    .await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("publish the Circle document and its file");
    let published = stored_blobs(&fixture).await;
    let [source] = published.as_slice() else {
        panic!("the Circle document published exactly one blob: {published:?}");
    };
    let source = source.clone();

    move_document_audience(&fixture, document, None, "2026-07-23T00:20:00Z").await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("republish the document under the Store audience");
    release_retained_replay_ownership(&fixture).await;

    // The stranded ciphertext is now eligible. Fail its delete: the reclaim
    // authorizes the deletion, the delete fails, and the run surfaces the failure
    // to its initiator with the object still present.
    fixture
        .store
        .home
        .fail_nth_exact_delete_of(&[source.object().slot()], 1);
    let interrupted = crate::sync::store::reclaim_packages_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
    )
    .await;
    assert!(
        interrupted.is_err(),
        "the delete failure fails the reclaim to its initiator: {interrupted:?}"
    );
    assert!(
        blob_object_present(&fixture, &source).await,
        "the stranded ciphertext survives the interrupted deletion"
    );

    // The journal still holds the authorized reclaim, so a restart finishes it.
    crate::sync::store::reclaim_packages_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
    )
    .await
    .expect("restart resumes the interrupted audience blob reclaim");
    assert!(
        !blob_object_present(&fixture, &source).await,
        "the restart deletes the stranded ciphertext"
    );

    // A further run is idempotent: the target is recorded as reclaimed, so nothing
    // re-authorizes or re-deletes it.
    crate::sync::store::reclaim_packages_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
    )
    .await
    .expect("a further run finds nothing left to reclaim");
    assert!(
        !blob_object_present(&fixture, &source).await,
        "the reclaimed ciphertext stays absent"
    );
}

/// The owner device's Circle snapshot stream, read the way any reader reads it:
/// from generation zero along each metadata object's create-once successor slot,
/// stopping at the first absent slot.
async fn owner_circle_snapshot_stream(
    fixture: &RotationFixture,
) -> Vec<(
    crate::protocol::store_commit::CircleSnapshotRef,
    crate::protocol::store_commit::CircleSnapshotMeta,
)> {
    let database = StoreDatabase::new(&fixture.db);
    let control = database
        .current_circle_control(fixture.circle_id)
        .await
        .expect("read the current Circle control")
        .expect("the Circle has an active control");
    let device = fixture
        .store
        .bind_device(&fixture.db, &fixture.signer)
        .await
        .expect("bind Circle snapshot access Store");
    let encryption = device
        .circle_package_access(fixture.circle_id, control)
        .await
        .expect("resolve Circle snapshot access")
        .expect("the Circle access is retained")
        .into_encryption();
    device
        .store
        .authorize_writer()
        .await
        .expect("authorize Circle snapshot stream reader")
        .circles()
        .snapshots()
        .load_circle_snapshot_refs_for_test(fixture.circle_id, encryption)
        .await
        .expect("walk the owner's Circle snapshot stream")
}

/// Whether the exact Circle snapshot image ciphertext is still readable in cloud
/// storage, read under the epoch key of the control its generation was authored
/// under.
async fn circle_snapshot_image_present(
    fixture: &RotationFixture,
    meta: &crate::protocol::store_commit::CircleSnapshotMeta,
) -> bool {
    let device = fixture
        .store
        .bind_device(&fixture.db, &fixture.signer)
        .await
        .expect("bind Circle snapshot image Store");
    let access = device
        .circle_package_access(fixture.circle_id, meta.control.clone())
        .await
        .expect("resolve Circle snapshot image access")
        .expect("the generation's control stays retained");
    let context = crate::storage::ProtocolObjectContext::circle(
        fixture.store.root.store_root_hash,
        crate::storage::ProtocolObjectDomain::CircleSnapshotImage,
        access.into_encryption(),
    );
    let prefix = crate::protocol::store_commit::semantic_prefix_from_exact_object(
        &meta.bootstrap.image.object,
        crate::storage::ProtectedObjectDomain::CircleSnapshotImage.extension(),
    )
    .expect("derive the Circle snapshot image prefix");
    match fixture
        .store
        .storage
        .read_protocol_object(&context, &meta.bootstrap.image.object, &prefix)
        .await
    {
        Ok(_) => true,
        Err(crate::storage::StorageError::NotFound(_)) => false,
        Err(error) => panic!("read the exact Circle snapshot image: {error}"),
    }
}

/// A device authors its Circle snapshots as one create-once stream of
/// generations, and a reader finds any generation only by walking that stream from
/// generation zero. Once a later generation is acknowledgement-stable and its cut
/// strictly dominates an earlier one, nobody will install the earlier image again
/// — so reclamation deletes that image while leaving the whole metadata chain, and
/// the surviving generation's own image, intact.
#[tokio::test]
async fn circle_snapshot_reclaim_deletes_a_superseded_generation_image() {
    let fixture = rotation_fixture("circle-snapshot-generation-reclaim").await;
    let member_storage = member_storage(&fixture);
    let (_member_temp, member_store_dir) = temp_store_dir();
    member_pull(&fixture, &member_storage, &member_store_dir).await;

    publish_covered_circle_package(
        &fixture,
        &member_storage,
        &member_store_dir,
        "00000000-0000-4000-8000-0000000000d1",
    )
    .await;
    let before = owner_circle_snapshot_stream(&fixture).await;
    let (_, latest) = before
        .last()
        .expect("the owner authored a Circle snapshot")
        .clone();

    // Nothing supersedes the stream's latest generation, so reclamation refuses it
    // and its image stays readable.
    crate::sync::store::reclaim_packages_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
    )
    .await
    .expect("run reclamation with no superseding Circle snapshot generation");
    assert!(
        circle_snapshot_image_present(&fixture, &latest).await,
        "the latest generation's image survives while nothing supersedes it"
    );

    // Publish Circle content past the acknowledged frontier and author a snapshot
    // over it. That generation strictly dominates the previous one, but no device
    // has acknowledged its cut — so it supersedes nothing and the earlier image
    // stays.
    capture_document(
        &fixture,
        "00000000-0000-4000-8000-0000000000d3",
        Some(fixture.circle_id),
        "2026-07-23T00:30:00Z",
    )
    .await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("publish Circle content past the acknowledged frontier");
    let unstable_temp = tempfile::tempdir().expect("unstable snapshot temp dir");
    drive_circle_snapshots(&fixture, &unstable_temp, "2026-07-23T00:35:00Z")
        .await
        .expect("author an unacknowledged Circle snapshot generation");
    let unacknowledged = owner_circle_snapshot_stream(&fixture).await;
    let (_, unstable) = unacknowledged
        .last()
        .expect("the owner authored a later Circle snapshot")
        .clone();
    assert!(
        unstable.generation > latest.generation
            && unstable
                .bootstrap
                .coverage
                .covers(&latest.bootstrap.coverage)
            && unstable.bootstrap.coverage != latest.bootstrap.coverage,
        "the unacknowledged generation's cut strictly dominates the earlier one"
    );
    crate::sync::store::reclaim_packages_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
    )
    .await
    .expect("run reclamation against an unacknowledged superseding generation");
    assert!(
        circle_snapshot_image_present(&fixture, &latest).await,
        "a superseding generation nobody acknowledged does not release the earlier image"
    );

    // A second round of Circle content drives both devices to acknowledge the later
    // generations, so the once-latest generation is superseded.
    publish_covered_circle_package(
        &fixture,
        &member_storage,
        &member_store_dir,
        "00000000-0000-4000-8000-0000000000d2",
    )
    .await;
    crate::sync::store::reclaim_packages_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
    )
    .await
    .expect("reclaim the superseded Circle snapshot image");

    let after = owner_circle_snapshot_stream(&fixture).await;
    let (_, newest) = after
        .last()
        .expect("the owner authored later Circle snapshots")
        .clone();
    assert!(
        newest.generation > latest.generation
            && newest.bootstrap.coverage.covers(&latest.bootstrap.coverage)
            && newest.bootstrap.coverage != latest.bootstrap.coverage,
        "a later generation's cut strictly dominates the earlier one"
    );
    assert!(
        !circle_snapshot_image_present(&fixture, &latest).await,
        "the superseded generation's image is deleted"
    );
    assert!(
        circle_snapshot_image_present(&fixture, &newest).await,
        "the generation no later snapshot supersedes keeps its image"
    );
    assert!(
        after
            .iter()
            .map(|(reference, _)| reference.generation)
            .collect::<Vec<_>>()
            .starts_with(
                &before
                    .iter()
                    .map(|(reference, _)| reference.generation)
                    .collect::<Vec<_>>()
            ),
        "every generation's metadata survives, so the stream stays walkable"
    );
}

#[tokio::test]
async fn circle_package_reclaim_deletes_a_snapshot_covered_package() {
    let fixture = rotation_fixture("circle-package-reclaim").await;
    let member_storage = member_storage(&fixture);
    let (_member_temp, member_store_dir) = temp_store_dir();
    member_pull(&fixture, &member_storage, &member_store_dir).await;

    let (circle_package, published) = publish_covered_circle_package(
        &fixture,
        &member_storage,
        &member_store_dir,
        "00000000-0000-4000-8000-0000000000c1",
    )
    .await;
    let owner_device = fixture
        .store
        .bind_device(&fixture.db, &fixture.signer)
        .await
        .expect("bind Circle package replay Store");

    // A freshly published Circle package is a retained replay input: reclamation
    // refuses it until a superseding cut releases that ownership.
    assert!(
        owner_device
            .circle_package_is_retained_for_replay_for_test(
                circle_package.clone(),
                published.clone(),
            )
            .await
            .expect("read Circle package replay retention"),
        "a freshly published Circle package is retained for replay"
    );
    release_retained_replay_ownership(&fixture).await;

    let result = crate::sync::store::reclaim_packages_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
    )
    .await
    .expect("reclaim the covered Circle package");
    assert!(
        result.packages_deleted >= 1,
        "reclamation deleted the snapshot-covered Circle package"
    );

    // The delete counted above required the production readback-absence check to
    // pass, so the ciphertext is gone from storage. Its ownership record is retired
    // and the materialized row is untouched.
    assert!(
        !owner_device
            .circle_package_is_retained_for_replay_for_test(
                circle_package.clone(),
                published.clone(),
            )
            .await
            .expect("read Circle package replay retention after reclaim"),
        "the reclaimed Circle package no longer has an ownership record"
    );
    let row_present = fixture
        .db
        .call(|connection| {
            connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM documents WHERE id = '00000000-0000-4000-8000-0000000000c1')",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(DbError::from)
        })
        .await
        .expect("read the owner's documents projection");
    assert!(
        row_present,
        "reclamation leaves the materialized row intact"
    );
}

#[tokio::test]
async fn circle_package_reclaim_refuses_a_replay_retained_package() {
    let fixture = rotation_fixture("circle-package-replay-retained").await;
    let member_storage = member_storage(&fixture);
    let (_member_temp, member_store_dir) = temp_store_dir();
    member_pull(&fixture, &member_storage, &member_store_dir).await;

    let (circle_package, published) = publish_covered_circle_package(
        &fixture,
        &member_storage,
        &member_store_dir,
        "00000000-0000-4000-8000-0000000000c2",
    )
    .await;
    let owner_device = fixture
        .store
        .bind_device(&fixture.db, &fixture.signer)
        .await
        .expect("bind replay-retained Circle package Store");

    // The snapshot covers the package and every device acknowledged its cut, but
    // the package is still a retained replay input: the per-Circle guard refuses
    // reclamation and the object survives.
    assert!(
        owner_device
            .circle_package_is_retained_for_replay_for_test(
                circle_package.clone(),
                published.clone(),
            )
            .await
            .expect("read Circle package replay retention"),
        "the Circle package is retained for replay"
    );
    // Reclamation may still delete the member's now-superseded seed bootstrap image
    // (the member advanced past it), but the replay-retained package itself is never
    // reclaimed while its ownership survives.
    crate::sync::store::reclaim_packages_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
    )
    .await
    .expect("run reclamation with the package still retained");
    assert!(
        owner_device
            .circle_package_is_retained_for_replay_for_test(circle_package, published)
            .await
            .expect("read Circle package replay retention after refused reclaim"),
        "the replay-retained Circle package still owns its object"
    );
}

#[tokio::test]
async fn circle_package_reclaim_verifies_a_cross_device_seeded_acknowledgement() {
    let fixture = rotation_fixture("circle-package-seeded-ack").await;
    let circle_id = fixture.circle_id;
    let member_storage = member_storage(&fixture);
    let (_member_temp, member_store_dir) = temp_store_dir();
    member_pull(&fixture, &member_storage, &member_store_dir).await;

    // The member's projection seeds from a real bootstrap coverage row the install
    // recorded — the coverage its acknowledgements will name.
    let member_coverage = fixture
        .member_db
        .call(move |conn| StoreDatabase::circle_bootstrap_coverage_ref_on(conn, circle_id))
        .await
        .expect("read member Circle bootstrap coverage")
        .expect("the member's projection seeded from a real bootstrap coverage row");

    let (circle_package, published) = publish_covered_circle_package(
        &fixture,
        &member_storage,
        &member_store_dir,
        "00000000-0000-4000-8000-0000000000c3",
    )
    .await;

    // The member's activated acknowledgement names its exact seed coverage — the
    // cross-device evidence the owner reads and dominates to prove stability.
    let member_device_id = local_device_id(&fixture.member_db).await;
    let member_ack_ref = StoreDatabase::new(&fixture.db)
        .activated_circle_ack(circle_id, member_device_id)
        .await
        .expect("read activated member acknowledgement")
        .expect("the owner activated the member acknowledgement");
    let member_ack = crate::sync::store::load_circle_acknowledgement_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
        &member_ack_ref,
    )
    .await
    .expect("owner reads the member acknowledgement");
    assert_eq!(
        member_ack.seeded_from.as_ref(),
        Some(&member_coverage),
        "the member's acknowledgement names its exact seed coverage row"
    );

    // Reclamation proceeds only because the owner could read and dominate that
    // seed-anchored cross-device acknowledgement.
    release_retained_replay_ownership(&fixture).await;
    let result = crate::sync::store::reclaim_packages_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
    )
    .await
    .expect("reclaim after cross-device verifying the seeded acknowledgement");
    assert!(
        result.packages_deleted >= 1,
        "reclamation proceeded on the strength of the member's seeded acknowledgement"
    );
    let owner_device = fixture
        .store
        .bind_device(&fixture.db, &fixture.signer)
        .await
        .expect("bind reclaimed Circle package Store");
    assert!(
        !owner_device
            .circle_package_is_retained_for_replay_for_test(circle_package, published)
            .await
            .expect("read Circle package replay retention after reclaim"),
        "the reclaimed Circle package no longer owns its object"
    );
}

/// Whether the Circle bootstrap image at `image_object` still owns a live
/// remote-object record. Reclamation deletes the record (and the ciphertext)
/// once the recipient's seed is superseded.
async fn bootstrap_image_present(
    fixture: &RotationFixture,
    image_object: &crate::storage::ExactObjectRef,
) -> bool {
    let object_id = crate::protocol::remote_object::remote_object_id(image_object);
    fixture
        .db
        .call(move |connection| {
            connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                    [object_id.to_string()],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(DbError::from)
        })
        .await
        .expect("read bootstrap image ownership presence")
}

/// The exact bootstrap image the Owner authored for the member, read from the
/// member's own installed coverage row.
async fn member_seed_image(
    fixture: &RotationFixture,
    circle_id: CircleId,
) -> crate::storage::ExactObjectRef {
    fixture
        .member_db
        .call(move |conn| StoreDatabase::circle_bootstrap_coverage_ref_on(conn, circle_id))
        .await
        .expect("read member Circle bootstrap coverage")
        .expect("the member's projection seeded from a real bootstrap coverage row")
        .bootstrap
        .image
        .object
}

/// The number of Store commits that own the bootstrap image at `image_object`.
async fn bootstrap_image_owner_count(
    fixture: &RotationFixture,
    image_object: &crate::storage::ExactObjectRef,
) -> usize {
    let object_id = crate::protocol::remote_object::remote_object_id(image_object);
    let record = fixture
        .db
        .call(move |connection| {
            connection
                .query_row(
                    "SELECT state FROM remote_objects WHERE object_id = ?1",
                    [object_id.to_string()],
                    |row| row.get::<_, String>(0),
                )
                .map_err(DbError::from)
        })
        .await
        .expect("load bootstrap image ownership");
    let record: crate::protocol::remote_object::RemoteObjectRecord =
        serde_json::from_str(&record).expect("parse bootstrap image ownership");
    let crate::protocol::remote_object::RemoteObjectRecord::SharedLiveSet(shared) = record else {
        panic!("a live bootstrap image is a shared object");
    };
    let crate::protocol::remote_object::OwnedObjectState::UploadedVerified { ownership } =
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
                crate::protocol::remote_object::SharedObjectOwner::StoreCommit(_)
            )
        })
        .count()
}

/// Every live Circle bootstrap image in the owner's ownership table, as
/// (image object, owning Store commit count).
async fn live_bootstrap_images(
    fixture: &RotationFixture,
) -> Vec<(crate::storage::ExactObjectRef, usize)> {
    let records = fixture
        .db
        .call(|connection| {
            let mut statement = connection
                .prepare("SELECT state FROM remote_objects ORDER BY object_id")
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(DbError::from)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(DbError::from)?;
            Ok(rows)
        })
        .await
        .expect("read remote object records");
    records
        .into_iter()
        .filter_map(|raw| {
            let record: crate::protocol::remote_object::RemoteObjectRecord =
                serde_json::from_str(&raw).expect("parse remote object record");
            let crate::protocol::remote_object::RemoteObjectRecord::SharedLiveSet(shared) = record
            else {
                return None;
            };
            if !matches!(
                shared.identity.domain,
                crate::protocol::remote_object::SharedLiveSetObjectDomain::CircleBootstrapImage { .. }
            ) {
                return None;
            }
            let crate::protocol::remote_object::OwnedObjectState::UploadedVerified { ownership } =
                shared.state
            else {
                return None;
            };
            let owners = ownership
                .activated
                .iter()
                .filter(|owner| {
                    matches!(
                        owner,
                        crate::protocol::remote_object::SharedObjectOwner::StoreCommit(_)
                    )
                })
                .count();
            Some((shared.identity.object.clone(), owners))
        })
        .collect()
}

#[tokio::test]
async fn two_circle_recipients_never_share_one_bootstrap_image() {
    // A bootstrap image's storage path is keyed by the recipient's slot, so two
    // recipients bootstrapped from the same underlying snapshot cut land on two
    // DISTINCT image objects, each owned by exactly the one add-member commit that
    // activated it. Sharing one image between recipients is therefore structurally
    // impossible, and the single-owner requirement in the reclaim eligibility check
    // (`validate_reclaimable_circle_bootstrap_image`) is always satisfiable: deleting
    // one recipient's seed can never remove another recipient's.
    let fixture = rotation_fixture("circle-bootstrap-two-recipients").await;
    let circle_id = fixture.circle_id;
    let member_storage = member_storage(&fixture);
    let (_member_temp, member_store_dir) = temp_store_dir();
    member_pull(&fixture, &member_storage, &member_store_dir).await;

    let first_image = member_seed_image(&fixture, circle_id).await;
    assert_eq!(
        bootstrap_image_owner_count(&fixture, &first_image).await,
        1,
        "the first recipient's seed image has exactly one activating owner"
    );

    // Onboard a second Store member and add it to the same Circle. Its bootstrap is
    // cut from the Circle's current content, the same underlying state the first
    // recipient was seeded from.
    let second = UserKeypair::generate();
    let second_pubkey = keys::public_key_hex(&second);
    fixture
        .store
        .invite_member(
            &fixture.db,
            &fixture.signer,
            &crate::sync::hlc::Hlc::new(
                "second-recipient".to_string(),
                std::sync::Arc::new(crate::clock::SystemClock),
            ),
            &second_pubkey,
            None,
            MemberRole::Member,
            &EncryptionService::from_key([42; 32]),
            "Rotation Store",
        )
        .await
        .expect("invite the second Store member");
    let second_db = open_circle_routing_test_db();
    install_active_device_fixture(
        &fixture.store,
        &fixture.db,
        &second_db,
        &second,
        "2026-07-25T02:00:00Z",
    )
    .await
    .expect("activate the second member device");
    fixture
        .components
        .add_circle_member(
            &fixture.store_dir,
            circle_id,
            second_pubkey.clone(),
            CircleRole::Member,
        )
        .await
        .expect("add the second Circle member");
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("activate the second member's access");

    // Two recipients, two distinct image objects, each with exactly one owner.
    let images = live_bootstrap_images(&fixture).await;
    assert_eq!(
        images.len(),
        2,
        "each recipient has its own bootstrap image: {images:?}"
    );
    let objects = images
        .iter()
        .map(|(object, _)| object.slot().logical_key().to_string())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        objects.len(),
        2,
        "the two recipients' bootstrap images are distinct objects: {objects:?}"
    );
    for (object, owners) in &images {
        assert_eq!(
            *owners,
            1,
            "bootstrap image {} is owned by exactly one activating commit",
            object.slot().logical_key()
        );
    }
    assert!(
        images.iter().any(|(object, _)| *object == first_image),
        "the first recipient's seed image is untouched by the second recipient's activation"
    );
}

#[tokio::test]
async fn circle_bootstrap_reclaim_unblocks_when_recipient_advances_past_its_seed() {
    let fixture = rotation_fixture("circle-bootstrap-reclaim-advance").await;
    let circle_id = fixture.circle_id;
    let member_storage = member_storage(&fixture);
    let (_member_temp, member_store_dir) = temp_store_dir();
    member_pull(&fixture, &member_storage, &member_store_dir).await;

    let image_object = member_seed_image(&fixture, circle_id).await;
    assert!(
        bootstrap_image_present(&fixture, &image_object).await,
        "the recipient's seed image exists before reclamation"
    );

    // No later Circle snapshot supersedes the recipient's seed yet, so reclamation
    // leaves the image in place.
    crate::sync::store::reclaim_packages_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
    )
    .await
    .expect("run reclamation before a later snapshot exists");
    assert!(
        bootstrap_image_present(&fixture, &image_object).await,
        "the seed image survives while no later sufficient snapshot supersedes it"
    );

    // The member pulls a later Circle package and every active-access device
    // acknowledges a stable snapshot whose cut strictly dominates the seed. The
    // recipient has moved to a later sufficient snapshot, so its seed is reclaimable.
    publish_covered_circle_package(
        &fixture,
        &member_storage,
        &member_store_dir,
        "00000000-0000-4000-8000-0000000000b1",
    )
    .await;
    crate::sync::store::reclaim_packages_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
    )
    .await
    .expect("reclaim the superseded seed image");
    assert!(
        !bootstrap_image_present(&fixture, &image_object).await,
        "the seed image is reclaimed once a later sufficient snapshot supersedes it"
    );
}

#[tokio::test]
async fn circle_bootstrap_reclaim_unblocks_when_recipient_loses_authority() {
    let fixture = rotation_fixture("circle-bootstrap-reclaim-removed").await;
    let circle_id = fixture.circle_id;
    let member_storage = member_storage(&fixture);
    let (_member_temp, member_store_dir) = temp_store_dir();
    member_pull(&fixture, &member_storage, &member_store_dir).await;

    let image_object = member_seed_image(&fixture, circle_id).await;

    // The member acknowledges its seed (naming its seed coverage) and the Owner
    // activates that acknowledgement — the exact evidence the Owner reads after the
    // member is removed to prove which seed the member held. No later snapshot is
    // authored, so while the member still holds access its seed is not superseded
    // and the automatic reclamation in the cycle leaves the image in place.
    member_publish_acknowledgements(&fixture, &member_storage, "2026-07-23T01:00:00Z").await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("owner activates the member acknowledgement");
    assert!(
        bootstrap_image_present(&fixture, &image_object).await,
        "an active member's seed survives while no later snapshot supersedes it"
    );

    // Remove the member from the Circle; the epoch closes and a successor control
    // activates whose roster excludes the member.
    fixture
        .components
        .remove_circle_member(circle_id, fixture.member_pubkey.clone())
        .await
        .expect("close the epoch by removing the Circle member");
    publish_circle_epoch_close_response(&fixture.store.storage, &fixture.db, &fixture.signer)
        .await
        .expect("publish local Circle epoch-close response");
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("activate the Circle epoch-close outcome");
    assert!(
        !StoreDatabase::new(&fixture.db)
            .circle_current_roster_members(circle_id)
            .await
            .expect("read successor roster")
            .contains(&fixture.member_pubkey),
        "the removed member is absent from the successor roster"
    );

    // The removed member lost authority under the activated successor control: its
    // seed image is reclaimed, re-verified from its own signed acknowledgement.
    crate::sync::store::reclaim_packages_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
    )
    .await
    .expect("reclaim the removed member's seed image");
    assert!(
        !bootstrap_image_present(&fixture, &image_object).await,
        "the removed member's seed image is reclaimed under the successor control"
    );
}

#[tokio::test]
async fn store_membership_revocation_cascades_into_bootstrap_reclaim() {
    // Revoking a recipient's STORE membership does not by itself exclude it from the
    // Circle roster: it marks the Circle rotation-required and waits. The operator's
    // Circle-member removal then closes the epoch, and the successor roster — the one
    // piece of evidence the lost-authority arm reads — omits the identity. This proves
    // the Store-revocation trigger reaches the same roster-exclusion evidence rather
    // than a second, separate authority path.
    let fixture = rotation_fixture("circle-bootstrap-store-revocation").await;
    let circle_id = fixture.circle_id;
    let member_storage = member_storage(&fixture);
    let (_member_temp, member_store_dir) = temp_store_dir();
    member_pull(&fixture, &member_storage, &member_store_dir).await;
    let image_object = member_seed_image(&fixture, circle_id).await;

    // The recipient acknowledges its seed: the signed evidence naming the exact
    // coverage the Owner will later delete.
    member_publish_acknowledgements(&fixture, &member_storage, "2026-07-25T03:00:00Z").await;
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("owner activates the member acknowledgement");

    // Revoke the recipient's Store membership. The Circle becomes rotation-required
    // but its roster still names the identity, so no lost-authority evidence exists
    // yet and the seed image survives.
    remove_store_member(&fixture).await;
    assert!(
        list_circles(&fixture)
            .await
            .iter()
            .find(|circle| circle.id() == circle_id)
            .expect("affected Circle listed after Store removal")
            .rotation_required(),
        "revoking Store membership marks the Circle rotation-required"
    );
    assert!(
        StoreDatabase::new(&fixture.db)
            .circle_current_roster_members(circle_id)
            .await
            .expect("read roster after Store revocation")
            .contains(&fixture.member_pubkey),
        "Store revocation alone does not yet exclude the identity from the Circle roster"
    );
    crate::sync::store::reclaim_packages_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
    )
    .await
    .expect("run reclamation while the roster still names the identity");
    assert!(
        bootstrap_image_present(&fixture, &image_object).await,
        "Store revocation alone does not reclaim the recipient's seed image"
    );

    // Completing the cascade — the Circle-member removal that clears rotation —
    // activates a successor control whose roster omits the identity. That is the same
    // evidence the lost-authority arm consumes, and the seed image is now reclaimed.
    fixture
        .components
        .remove_circle_member(circle_id, fixture.member_pubkey.clone())
        .await
        .expect("close the epoch by removing the Circle member");
    publish_circle_epoch_close_response(&fixture.store.storage, &fixture.db, &fixture.signer)
        .await
        .expect("publish local Circle epoch-close response");
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("activate the Circle epoch-close outcome");
    assert!(
        !StoreDatabase::new(&fixture.db)
            .circle_current_roster_members(circle_id)
            .await
            .expect("read successor roster")
            .contains(&fixture.member_pubkey),
        "the cascade excludes the revoked identity from the successor roster"
    );
    assert!(
        !bootstrap_image_present(&fixture, &image_object).await,
        "the revoked identity's seed image is reclaimed once the cascade completes"
    );
}

#[tokio::test]
async fn circle_package_reclaim_reads_an_acknowledgement_sealed_under_a_rotated_epoch() {
    // Each acknowledgement reference names the control that resolves its epoch
    // key. After rotation, a pre-rotation acknowledgement remains readable through
    // that retained exact control.
    let fixture = rotation_fixture("circle-reclaim-rotated-ack").await;
    let circle_id = fixture.circle_id;
    let owner_pk = keys::public_key_hex(&fixture.signer);

    // The owner publishes its Circle acknowledgement under the current (soon-rotated)
    // epoch.
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("owner acknowledges under the pre-rotation epoch");
    let (old_authoring, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(circle_id, &owner_pk)
        .await
        .expect("pre-rotation Circle authoring context");
    let old_control = old_authoring.control.coord.clone();
    let old_epoch = old_authoring.control.value.epoch_id();
    let owner_device_id = local_device_id(&fixture.db).await;
    let owner_ack_ref = StoreDatabase::new(&fixture.db)
        .activated_circle_ack(circle_id, owner_device_id)
        .await
        .expect("read owner activated acknowledgement")
        .expect("the owner published a Circle acknowledgement");

    // Rotate the epoch: remove the roster member and finalize the close.
    remove_store_member(&fixture).await;
    fixture
        .components
        .remove_circle_member(circle_id, fixture.member_pubkey.clone())
        .await
        .expect("close the epoch by removing the roster member");
    publish_circle_epoch_close_response(&fixture.store.storage, &fixture.db, &fixture.signer)
        .await
        .expect("publish local Circle epoch-close response");
    fixture
        .components
        .run_cycle(&crate::clock::SystemClock, None, &fixture.store_dir, None)
        .await
        .expect("activate the successor epoch");
    let (new_authoring, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(circle_id, &owner_pk)
        .await
        .expect("successor Circle authoring context");
    let new_control = new_authoring.control.coord.clone();
    assert_ne!(new_control, old_control, "the epoch rotated");

    // The reclaim ack reader resolves each acknowledgement's epoch key from the
    // retained activation of the control it names — not from a live keyring. After
    // the epoch rotates, the old control stays retained, so the reader (the exact
    // path reclaim stability uses) still reads the pre-rotation acknowledgement.
    let acknowledgement = crate::sync::store::load_circle_acknowledgement_for_test(
        &fixture.db,
        &fixture.store.storage,
        &fixture.signer,
        &owner_ack_ref,
    )
    .await
    .expect("reclaim reads a rotated-epoch acknowledgement via its retained control");
    assert_eq!(
        acknowledgement.epoch_id, old_epoch,
        "the acknowledgement was sealed under the rotated-away epoch"
    );
}
