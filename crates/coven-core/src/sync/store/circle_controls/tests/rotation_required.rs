use super::*;
use std::collections::BTreeSet;

use crate::sync::cycle::{init_sync_over_storage, StoreInitialization, SyncComponents};

fn circle_routing_migrations() -> Vec<crate::migration::Migration> {
    vec![crate::migration::Migration::sql(
        1,
        "Circle routing schema",
        "CREATE TABLE documents (
                 id TEXT PRIMARY KEY,
                 audience TEXT,
                 _updated_at TEXT NOT NULL
             ) STRICT;",
    )]
}

fn open_circle_routing_test_db() -> Database {
    crate::sync::test_helpers::open_test_db_schema(
        vec![crate::sync::session::SyncedTable::new(
            "documents",
            crate::sync::session::RowIdentity::IndependentUuid,
        )
        .scoped_by("audience")],
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
    custody: crate::sync::test_helpers::TestCustody,
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
    crate::sync::store::invite_member(
        &store.storage,
        store.home.as_ref(),
        &signer,
        &crate::sync::hlc::Hlc::new(format!("{label}-owner")),
        &member_pubkey,
        None,
        MemberRole::Member,
        &EncryptionService::from_key([42; 32]),
        store.storage.store_id(),
        "Rotation Store",
        &StoreDatabase::new(&db),
    )
    .await
    .expect("invite Store member");
    let member_db = open_circle_routing_test_db();
    install_active_device_fixture(&store, &db, &member_db, &member, "2026-07-23T00:00:00Z")
        .await
        .expect("activate Store member device");

    let (store_temp, store_dir) = temp_store_dir();
    let owner_storage = crate::sync::cloud_storage::CloudSyncStorage::new(
        store.home.clone(),
        crate::sync::cloud_storage::CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
        crate::sync::cloud_storage::BlobPathScheme::Hashed,
        store.storage.store_id(),
        signer.clone(),
    )
    .expect("open Circle owner storage");
    let components = init_sync_over_storage(
        &StoreDatabase::new(&db),
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
        custody,
    }
}

async fn remove_store_member(fixture: &RotationFixture) {
    fixture
        .components
        .remove_member(&fixture.member_pubkey, &fixture.custody)
        .await
        .expect("remove Store member");
}

/// Cloud storage for the fixture's second member device, over the shared home.
fn member_storage(fixture: &RotationFixture) -> crate::sync::cloud_storage::CloudSyncStorage {
    crate::sync::cloud_storage::CloudSyncStorage::new(
        fixture.store.home.clone(),
        crate::sync::cloud_storage::CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
        crate::sync::cloud_storage::BlobPathScheme::Hashed,
        fixture.store.storage.store_id(),
        fixture.member.clone(),
    )
    .expect("open Circle member storage")
}

/// The member device installs the Circle bootstrap and returns to an active
/// projection holding the current epoch key.
async fn member_pull(
    fixture: &RotationFixture,
    storage: &crate::sync::cloud_storage::CloudSyncStorage,
    store_dir: &crate::store_dir::StoreDir,
) {
    crate::sync::store::Store::authorize_borrowed(storage, &fixture.member_db)
        .await
        .expect("authorize Circle member Store")
        .pull(
            store_dir,
            &fixture.member,
            Some(&EncryptionService::from_key([42; 32])),
        )
        .await
        .expect("member installs the Circle bootstrap");
}

/// Stage and publish the member device's Store and Circle acknowledgements at its
/// current accepted frontier, riding one Store commit.
async fn member_publish_acknowledgements(
    fixture: &RotationFixture,
    storage: &crate::sync::cloud_storage::CloudSyncStorage,
    stamp: &str,
) {
    let frontier = crate::sync::store_commit::CommitFrontier::from_refs(
        StoreDatabase::new(&fixture.member_db)
            .materialized_frontier()
            .await
            .expect("read member frontier"),
    )
    .expect("shape member frontier");
    crate::sync::store::stage_store_acknowledgement_for_test(
        &fixture.member_db,
        storage,
        frontier.clone(),
        stamp.to_string(),
        &fixture.member,
    )
    .await
    .expect("stage member Store acknowledgement");
    crate::sync::store::stage_circle_acknowledgements_for_test(
        &fixture.member_db,
        storage,
        &frontier,
        stamp,
        &fixture.member,
    )
    .await
    .expect("stage member Circle acknowledgement");
    crate::sync::store::drain_store_acknowledgements_for_test(
        &fixture.member_db,
        storage,
        &fixture.member,
    )
    .await
    .expect("publish member acknowledgements");
}

async fn local_device_id(db: &Database) -> crate::sync::store_commit::StoreDeviceId {
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
    let membership = crate::sync::store::pull::load_cycle_membership(
        &fixture.store.storage,
        &StoreDatabase::new(&fixture.db),
    )
    .await
    .expect("load cycle membership");
    membership
        .chain
        .expect("membership chain")
        .current_members()
        .into_iter()
        .map(|(pubkey, _)| pubkey)
        .collect()
}

async fn list_circles(fixture: &RotationFixture) -> Vec<crate::sync::circle::CircleInfo> {
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
        fixture
            .db
            .write_status(&store_write)
            .await
            .expect("read Store write status"),
        crate::WriteStatus::Published(_)
    ));
    assert!(matches!(
        fixture
            .db
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
    match fixture
        .db
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
    assert!(
        add.to_string().contains("requires rotation"),
        "add-member failure must name the rotation requirement: {add}"
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

    crate::sync::store::invite_member(
        &fixture.store.storage,
        fixture.store.home.as_ref(),
        &fixture.signer,
        &crate::sync::hlc::Hlc::new("rotation-readd".to_string()),
        &fixture.member_pubkey,
        None,
        MemberRole::Member,
        &fixture
            .store
            .storage
            .cipher_state()
            .encryption()
            .expect("live Store keyring"),
        fixture.store.storage.store_id(),
        "Rotation Store",
        &StoreDatabase::new(&fixture.db),
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
    let authorized_store =
        crate::sync::store::Store::authorize_borrowed(&fixture.store.storage, &fixture.db)
            .await
            .expect("authorize Circle close response");
    authorized_store
        .publish_circle_epoch_close_responses(&fixture.signer)
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
        fixture
            .db
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
        fixture
            .db
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
    let authorized_store =
        crate::sync::store::Store::authorize_borrowed(&fixture.store.storage, &fixture.db)
            .await
            .expect("authorize Circle close response");
    authorized_store
        .publish_circle_epoch_close_responses(&fixture.signer)
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
        fixture
            .db
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
    let published = match fixture
        .db
        .write_status(&blocked)
        .await
        .expect("read republished write status")
    {
        crate::WriteStatus::Published(position) => position.commit,
        status => panic!("formerly blocked write must publish under the successor: {status:?}"),
    };
    let published_commit = crate::sync::store::pull::load_commit_with_author(
        &fixture.store.storage,
        &fixture.store.root,
        &published,
    )
    .await
    .expect("load the successor-epoch commit")
    .0;
    let [circle_package] = published_commit.circle_packages() else {
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
    let membership = fixture
        .store
        .open_into(&fixture.member_db)
        .await
        .expect("open the Store as the removed member");
    let (_member_temp, member_store_dir) = temp_store_dir();
    let routing = EncryptionService::from_key([42; 32]);
    let member_pull = crate::sync::store::pull_store_commits(
        &StoreDatabase::new(&fixture.member_db),
        fixture.member_db.synced_tables(),
        &fixture.store.storage,
        fixture.store.root.store_root_hash,
        &member_store_dir,
        &membership,
        Some(&fixture.member),
        Some(&routing),
    )
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
            let refs =
                crate::sync::store::database::StoreDatabase::materialized_frontier_on(conn, None)?;
            crate::sync::store_commit::CommitFrontier::from_refs(refs)
                .map_err(|error| DbError::Message(error.to_string()))
        })
        .await
        .expect("read the accepted materialized frontier");
    let authorized =
        crate::sync::store::Store::authorize_borrowed(&fixture.store.storage, &fixture.db)
            .await
            .expect("authorize the successor bootstrap cut");
    let (image_temp, image_dir) = temp_store_dir();
    let cut = authorized
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
    let authorized =
        crate::sync::store::Store::authorize_borrowed(&fixture.store.storage, &fixture.db)
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
    crate::sync::store::invite_member(
        &fixture.store.storage,
        fixture.store.home.as_ref(),
        &fixture.signer,
        &crate::sync::hlc::Hlc::new("rotation-outsider".to_string()),
        &outsider_pubkey,
        None,
        MemberRole::Member,
        &EncryptionService::from_key([42; 32]),
        fixture.store.storage.store_id(),
        "Rotation Store",
        &StoreDatabase::new(&fixture.db),
    )
    .await
    .expect("invite a Store member who joins no Circle");

    fixture
        .components
        .remove_member(&outsider_pubkey, &fixture.custody)
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
        fixture
            .db
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
    crate::sync::store::Store::authorize_borrowed(&fixture.store.storage, &fixture.db)
        .await
        .expect("authorize Circle close response")
        .publish_circle_epoch_close_responses(&fixture.signer)
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
    let pull = crate::sync::store::Store::authorize_borrowed(&fixture.store.storage, &joined_db)
        .await
        .expect("authorize the joined device pull")
        .pull(
            &joined_dir,
            &fixture.signer,
            Some(&EncryptionService::from_key([42; 32])),
        )
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
    circle_id: crate::sync::circle::CircleId,
    membership: crate::sync::membership::MembershipChain,
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
    circle_id: crate::sync::circle::CircleId,
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
    crate::sync::store::invite_member(
        &store.storage,
        store.home.as_ref(),
        &signer,
        &crate::sync::hlc::Hlc::new(format!("{name}-owner")),
        &member_pubkey,
        None,
        MemberRole::Member,
        &routing,
        store.storage.store_id(),
        "Restore Store",
        &StoreDatabase::new(&db),
    )
    .await
    .expect("invite Store member");
    let (_store_temp, store_dir) = temp_store_dir();
    let owner_storage = crate::sync::cloud_storage::CloudSyncStorage::new(
        store.home.clone(),
        crate::sync::cloud_storage::CloudCipher::Encrypted(routing.clone()),
        crate::sync::cloud_storage::BlobPathScheme::Hashed,
        store.storage.store_id(),
        signer.clone(),
    )
    .expect("open Circle owner storage");
    let components = init_sync_over_storage(
        &StoreDatabase::new(&db),
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
        crate::sync::store::Store::authorize_borrowed(&store.storage, &db)
            .await
            .expect("authorize Circle close response")
            .publish_circle_epoch_close_responses(&signer)
            .await
            .expect("publish local Circle epoch-close response");
        components
            .run_cycle(&crate::clock::SystemClock, None, &store_dir, None)
            .await
            .expect("activate the Circle epoch-close outcome");
    }

    let authorized = crate::sync::store::Store::authorize_borrowed(&store.storage, &db)
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
    authorized
        .push_snapshot(
            cut.snapshot,
            cut.coverage,
            db.schema_version(),
            &signer,
            "2026-07-24T01:00:00Z".to_string(),
        )
        .await
        .expect("publish the Store snapshot");
    crate::sync::store::stage_store_acknowledgement_for_test(
        &db,
        &store.storage,
        coverage.clone(),
        "2026-07-24T01:00:01Z".to_string(),
        &signer,
    )
    .await
    .expect("stage snapshot stability acknowledgement");
    crate::sync::store::drain_store_acknowledgements_for_test(&db, &store.storage, &signer)
        .await
        .expect("activate snapshot stability acknowledgement");

    let membership =
        crate::sync::store::pull::load_cycle_membership(&store.storage, &StoreDatabase::new(&db))
            .await
            .expect("load membership for snapshot restore")
            .chain
            .expect("membership chain");

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
        &*store.storage,
        store.storage.store_id(),
        store.root.clone(),
        &crate::join_code::MembershipFloor(membership.head_refs().to_vec()),
        db.schema_version(),
        &database_path,
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
            &circle_routing_migrations(),
            Some(&routing),
            &*store.storage,
            &signer,
        )
        .await
        .expect("a Circle with active access but no image restores without error");

    let coverage = restored
        .call(move |conn| StoreDatabase::circle_bootstrap_coverage_ref_on(conn, circle_id))
        .await
        .expect("read restored Circle coverage");
    assert!(
        coverage.is_none(),
        "selection stages no coverage row for a Circle it holds no image for"
    );

    let control_count: i64 = restored
        .call(move |conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM circle_control_activations WHERE circle_id = ?1",
                [circle_id.to_string()],
                |row| row.get(0),
            )
            .map_err(DbError::from)
        })
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
        &*store.storage,
        store.storage.store_id(),
        store.root.clone(),
        &crate::join_code::MembershipFloor(membership.head_refs().to_vec()),
        db.schema_version(),
        &database_path,
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
            &circle_routing_migrations(),
            Some(&routing),
            &*store.storage,
            &signer,
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
        &*store.storage,
        store.storage.store_id(),
        store.root.clone(),
        &crate::join_code::MembershipFloor(membership.head_refs().to_vec()),
        db.schema_version(),
        &database_path,
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
            &circle_routing_migrations(),
            Some(&routing),
            &*store.storage,
            &member,
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
    crate::sync::store::invite_member(
        &store.storage,
        store.home.as_ref(),
        &signer,
        &crate::sync::hlc::Hlc::new("snapshot-restore-owner".to_string()),
        &member_pubkey,
        None,
        MemberRole::Member,
        &routing,
        store.storage.store_id(),
        "Restore Store",
        &StoreDatabase::new(&db),
    )
    .await
    .expect("invite Store member");
    let (_store_temp, store_dir) = temp_store_dir();
    let owner_storage = crate::sync::cloud_storage::CloudSyncStorage::new(
        store.home.clone(),
        crate::sync::cloud_storage::CloudCipher::Encrypted(routing.clone()),
        crate::sync::cloud_storage::BlobPathScheme::Hashed,
        store.storage.store_id(),
        signer.clone(),
    )
    .expect("open Circle owner storage");
    let components = init_sync_over_storage(
        &StoreDatabase::new(&db),
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
    crate::sync::store::Store::authorize_borrowed(&store.storage, &db)
        .await
        .expect("authorize Circle close response")
        .publish_circle_epoch_close_responses(&signer)
        .await
        .expect("publish local Circle epoch-close response");
    components
        .run_cycle(&crate::clock::SystemClock, None, &store_dir, None)
        .await
        .expect("activate the Circle epoch-close outcome");

    // Publish a Store snapshot covering the post-close frontier; the single owner
    // device acknowledges it stable. Its image prunes the old-epoch retained rows
    // now covered by the successor bootstrap.
    let authorized = crate::sync::store::Store::authorize_borrowed(&store.storage, &db)
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
    authorized
        .push_snapshot(
            cut.snapshot,
            cut.coverage,
            db.schema_version(),
            &signer,
            "2026-07-24T01:00:00Z".to_string(),
        )
        .await
        .expect("publish the post-close Store snapshot");
    crate::sync::store::stage_store_acknowledgement_for_test(
        &db,
        &store.storage,
        coverage.clone(),
        "2026-07-24T01:00:01Z".to_string(),
        &signer,
    )
    .await
    .expect("stage post-close snapshot stability acknowledgement");
    crate::sync::store::drain_store_acknowledgements_for_test(&db, &store.storage, &signer)
        .await
        .expect("activate post-close snapshot stability acknowledgement");

    // A device is restored from the snapshot. Installation validates the image's
    // retained inputs against the retention rule; the successor bootstrap's
    // coverage keeps retained rows a Store snapshot of a Circle store legitimately
    // carries, which the validator must accept.
    let membership =
        crate::sync::store::pull::load_cycle_membership(&store.storage, &StoreDatabase::new(&db))
            .await
            .expect("load membership for snapshot restore")
            .chain
            .expect("membership chain");
    let destination = tempfile::tempdir().expect("snapshot restore destination");
    let database_path = destination.path().join("store.db");
    let bootstrap = crate::sync::store::bootstrap_from_snapshot(
        &*store.storage,
        store.storage.store_id(),
        store.root.clone(),
        &crate::join_code::MembershipFloor(membership.head_refs().to_vec()),
        db.schema_version(),
        &database_path,
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
            "restored-device".to_string(),
            &circle_routing_migrations(),
            Some(&routing),
            &*store.storage,
            &signer,
        )
        .await
        .expect("install the restored snapshot database");

    // The restored device pulls and converges to the owner's accepted Store
    // frontier: the installed snapshot represents the closed epoch exactly, so
    // nothing is held and the projections agree.
    let (_restored_temp, restored_dir) = temp_store_dir();
    let pull = crate::sync::store::pull_store_commits(
        &StoreDatabase::new(&restored),
        restored.synced_tables(),
        &*store.storage,
        store.root.store_root_hash,
        &restored_dir,
        &membership,
        Some(&signer),
        Some(&routing),
    )
    .await
    .expect("the restored device pulls the close without a foreign-key violation");
    assert!(
        pull.held_positions.is_empty(),
        "the restored device holds no positions after the close: {:?}",
        pull.held_positions
    );
    let owner_frontier = StoreDatabase::new(&db)
        .materialized_frontier()
        .await
        .expect("read owner Store frontier");
    let restored_frontier = StoreDatabase::new(&restored)
        .materialized_frontier()
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
        &crate::join_code::MembershipFloor(membership.head_refs().to_vec()),
        db.schema_version(),
        &removed_path,
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
            &circle_routing_migrations(),
            Some(&routing),
            &*store.storage,
            &member,
        )
        .await
        .expect("install the removed-member restore database");

    let removed_coverage = removed_db
        .call(move |conn| StoreDatabase::circle_bootstrap_coverage_ref_on(conn, circle_id))
        .await
        .expect("read removed-member Circle coverage");
    assert!(
        removed_coverage.is_none(),
        "the removed member retains no Circle coverage row"
    );

    let control_count: i64 = removed_db
        .call(move |conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM circle_control_activations WHERE circle_id = ?1",
                [circle_id.to_string()],
                |row| row.get(0),
            )
            .map_err(DbError::from)
        })
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
        .call(StoreDatabase::circle_bootstrap_replay_inputs_on)
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
    crate::sync::store::invite_member(
        &store.storage,
        store.home.as_ref(),
        &signer,
        &crate::sync::hlc::Hlc::new("standalone-dominates-owner".to_string()),
        &member_pubkey,
        None,
        MemberRole::Member,
        &routing,
        store.storage.store_id(),
        "Restore Store",
        &StoreDatabase::new(&db),
    )
    .await
    .expect("invite Store member");
    let (_store_temp, store_dir) = temp_store_dir();
    let owner_storage = crate::sync::cloud_storage::CloudSyncStorage::new(
        store.home.clone(),
        crate::sync::cloud_storage::CloudCipher::Encrypted(routing.clone()),
        crate::sync::cloud_storage::BlobPathScheme::Hashed,
        store.storage.store_id(),
        signer.clone(),
    )
    .expect("open Circle owner storage");
    let components = init_sync_over_storage(
        &StoreDatabase::new(&db),
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
    crate::sync::store::Store::authorize_borrowed(&store.storage, &db)
        .await
        .expect("authorize Circle close response")
        .publish_circle_epoch_close_responses(&signer)
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
        &*store.storage,
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
    let authorized = crate::sync::store::Store::authorize_borrowed(&store.storage, &db)
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
    authorized
        .push_snapshot(
            cut.snapshot,
            cut.coverage,
            db.schema_version(),
            &signer,
            "2026-07-24T02:00:01Z".to_string(),
        )
        .await
        .expect("publish the post-close Store snapshot");
    crate::sync::store::stage_store_acknowledgement_for_test(
        &db,
        &store.storage,
        coverage.clone(),
        "2026-07-24T02:00:02Z".to_string(),
        &signer,
    )
    .await
    .expect("stage post-close snapshot stability acknowledgement");
    crate::sync::store::drain_store_acknowledgements_for_test(&db, &store.storage, &signer)
        .await
        .expect("activate post-close snapshot stability acknowledgement");

    // A fresh device restores from the Store snapshot.
    let membership =
        crate::sync::store::pull::load_cycle_membership(&store.storage, &StoreDatabase::new(&db))
            .await
            .expect("load membership for snapshot restore")
            .chain
            .expect("membership chain");
    let destination = tempfile::tempdir().expect("standalone-restore destination");
    let database_path = destination.path().join("store.db");
    let bootstrap = crate::sync::store::bootstrap_from_snapshot(
        &*store.storage,
        store.storage.store_id(),
        store.root.clone(),
        &crate::join_code::MembershipFloor(membership.head_refs().to_vec()),
        db.schema_version(),
        &database_path,
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
            &circle_routing_migrations(),
            Some(&routing),
            &*store.storage,
            &signer,
        )
        .await
        .expect("install the restored snapshot database");

    // The staged Install decision chose the dominating standalone snapshot: the
    // coverage row names its image.
    let coverage_row = restored
        .call(move |conn| StoreDatabase::circle_bootstrap_coverage_ref_on(conn, circle_id))
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
        &ack_ref,
        &old_control,
    )
    .await
    .expect("read acknowledgement under the current control");
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
    crate::sync::store::Store::authorize_borrowed(&fixture.store.storage, &fixture.db)
        .await
        .expect("authorize Circle close response")
        .publish_circle_epoch_close_responses(&fixture.signer)
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
        &ack_ref,
        &old_control,
    )
    .await
    .expect("read the pre-rotation acknowledgement after the epoch rotated");
    assert_eq!(after.epoch_id, old_epoch);
    assert_eq!(after.control, old_control);
}

#[tokio::test]
async fn circle_snapshot_stability_requires_every_access_device_to_acknowledge() {
    let fixture = rotation_fixture("snapshot-stability").await;
    let owner_pk = keys::public_key_hex(&fixture.signer);
    let (authoring, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(fixture.circle_id, &owner_pk)
        .await
        .expect("Circle authoring context");
    let control = authoring.control.coord.clone();
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
        fixture.circle_id,
        &control,
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
        fixture.circle_id,
        &control,
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
    crate::sync::store::Store::authorize_borrowed(&fixture.store.storage, &fixture.db)
        .await
        .expect("authorize Circle close response")
        .publish_circle_epoch_close_responses(&fixture.signer)
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
    let retained = StoreDatabase::new(&fixture.db)
        .circle_package_access(fixture.circle_id, old_control.clone())
        .await
        .expect("read retained Circle activation")
        .expect("the pre-rotation control's activation is retained");
    let metas = crate::sync::store::load_circle_snapshot_metas_for_test(
        &fixture.db,
        &fixture.store.storage,
        fixture.circle_id,
        retained.encryption,
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
    crate::sync::store::Store::authorize_borrowed(&fixture.store.storage, &fixture.db)
        .await
        .expect("authorize Circle close response")
        .publish_circle_epoch_close_responses(&fixture.signer)
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
    let owner_pk = keys::public_key_hex(&fixture.signer);

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

    let control = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(circle_id, &owner_pk)
        .await
        .expect("owner Circle authoring context")
        .0
        .control
        .coord;

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
        &member_ack_ref,
        &control,
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
        &owner_ack_ref,
        &control,
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
    crate::sync::store::Store::authorize_borrowed(&fixture.store.storage, &fixture.db)
        .await
        .expect("authorize Circle close response")
        .publish_circle_epoch_close_responses(&fixture.signer)
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
    assert!(
        StoreDatabase::new(&fixture.db)
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
    assert!(
        StoreDatabase::new(&fixture.member_db)
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
