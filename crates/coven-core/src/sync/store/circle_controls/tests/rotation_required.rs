use super::*;
use std::collections::BTreeSet;

use crate::sync::cycle::{init_sync_over_storage, StoreInitialization, SyncComponents};

fn open_circle_routing_test_db() -> Database {
    crate::sync::test_helpers::open_test_db_schema(
        vec![crate::sync::session::SyncedTable::new(
            "documents",
            crate::sync::session::RowIdentity::IndependentUuid,
        )
        .scoped_by("audience")],
        vec![crate::migration::Migration::sql(
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
        .find(|circle| circle.id == fixture.circle_id)
        .expect("affected Circle is listed");
    assert!(
        affected.rotation_required,
        "removing a roster member makes the Circle rotation-required"
    );
    let other = circles
        .iter()
        .find(|circle| circle.id == unaffected)
        .expect("unaffected Circle is listed");
    assert!(
        !other.rotation_required,
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
    assert!(
        list_circles(&fixture)
            .await
            .iter()
            .find(|circle| circle.id == fixture.circle_id)
            .expect("affected Circle listed after removal")
            .rotation_required
    );

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
            .find(|circle| circle.id == fixture.circle_id)
            .expect("affected Circle listed after re-add")
            .rotation_required,
        "a re-added Store member's roster entry is active again, clearing rotation"
    );
}

#[tokio::test]
async fn closing_the_epoch_clears_rotation_and_resumes_publication() {
    let fixture = rotation_fixture("rotation-close-clears").await;
    remove_store_member(&fixture).await;
    assert!(
        list_circles(&fixture)
            .await
            .iter()
            .find(|circle| circle.id == fixture.circle_id)
            .expect("affected Circle listed after removal")
            .rotation_required
    );

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
            .find(|circle| circle.id == fixture.circle_id)
            .expect("Circle listed after close")
            .rotation_required,
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
    assert!(
        !list_circles(&fixture)
            .await
            .iter()
            .find(|circle| circle.id == fixture.circle_id)
            .expect("Circle listed after close")
            .rotation_required
    );
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
            .find(|circle| circle.id == fixture.circle_id)
            .expect("Circle listed after unrelated removal")
            .rotation_required,
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
