use super::*;
use crate::sync::store::pull::{
    insert_latest_acknowledgement, merge_retained_merge_history, readiness,
    verified_merge_membership_prefix, verify_merge_history_refs, Readiness,
    VerifiedMergePrefixHeadStatus,
};
use crate::sync::store_commit::{OpenedRetainedMergeHistorySummary, OwnerRecoveryNodeRef};
use rusqlite::OptionalExtension;

mod effective_access_failure;

async fn one_retained_checkpoint() -> (
    Database,
    crate::sync::test_helpers::TestStore,
    MembershipChain,
    OpenedRetainedMergeHistorySummary,
) {
    let db = crate::sync::test_helpers::open_test_db();
    let store = crate::sync::test_helpers::TestStore::create(
        &db,
        "retained-checkpoint-conflict",
        crate::keys::UserKeypair::generate(),
    )
    .await
    .expect("create retained-checkpoint Store");
    let database = StoreDatabase::new(&db);
    let membership = crate::sync::store::pull::load_cycle_membership(
        &store.storage,
        &crate::sync::store::database::StoreDatabase::new(&db),
    )
    .await
    .expect("load checkpoint membership")
    .chain
    .expect("Merge Store has membership");
    crate::sync::test_helpers::host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('checkpoint-conflict', 'checkpoint', NULL, 1, \
                 '0000000001000-0000-checkpoint', '2026-07-21')",
    )
    .await;
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("load checkpoint device id")
        .expect("checkpoint device id exists");
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    assert!(crate::sync::store::preparation::prepare_store_write(
        &database,
        &store.storage,
        &device_id,
        "2026-07-21T00:00:00Z",
        &store.signer,
        &store_dir,
        &membership,
    )
    .await
    .expect("prepare checkpoint commit"));
    assert_eq!(
        crate::sync::store::publication::drain_store_writes(&database, &store.storage)
            .await
            .expect("publish checkpoint commit"),
        1,
    );
    let reference = database
        .latest_local_store_position()
        .await
        .expect("load checkpoint position")
        .expect("checkpoint position exists");
    let mut retained = database
        .retained_merge_history_frontier(vec![reference])
        .await
        .expect("open retained checkpoint");
    assert_eq!(retained.len(), 1);
    (db, store, membership, retained.remove(0))
}

#[tokio::test]
async fn retained_checkpoint_merge_rejects_same_coordinate_competitors() {
    let (_db, store, membership, checkpoint) = Box::pin(one_retained_checkpoint()).await;

    let mut conflicting_commit = checkpoint.clone();
    let (coordinate, reference) = conflicting_commit
        .summary
        .causal_cut
        .first_key_value()
        .map(|(coordinate, reference)| (coordinate.clone(), reference.clone()))
        .expect("checkpoint causal cut is nonempty");
    let mut replacement = reference;
    replacement.commit_hash = ObjectHash::digest(b"same-coordinate competing commit");
    conflicting_commit
        .summary
        .causal_cut
        .insert(coordinate, replacement);
    assert!(merge_retained_merge_history(
        &store.root,
        &membership,
        vec![checkpoint.clone(), conflicting_commit],
    )
    .is_err());

    let mut conflicting_head = checkpoint.clone();
    let announcement = conflicting_head
        .announcement_frontier
        .values_mut()
        .next()
        .expect("opened checkpoint has an announcement frontier");
    announcement.reference.head_hash = ObjectHash::digest(b"same-stream competing head");
    assert!(merge_retained_merge_history(
        &store.root,
        &membership,
        vec![checkpoint, conflicting_head],
    )
    .is_err());
}

#[tokio::test]
async fn retained_checkpoint_merge_rejects_different_sequence_acknowledgement_forks() {
    let (db, store, _membership, checkpoint) = Box::pin(one_retained_checkpoint()).await;
    let coverage = CommitFrontier::from_refs(
        crate::sync::store::database::StoreDatabase::new(&db)
            .materialized_frontier()
            .await
            .expect("load acknowledgement coverage"),
    )
    .expect("derive acknowledgement coverage");
    crate::sync::test_helpers::publish_store_ack_fixture(
        &db,
        &store.storage,
        coverage,
        &store.signer,
    )
    .await
    .expect("publish retained acknowledgement");
    let acknowledgement_commit = crate::sync::store::database::StoreDatabase::new(&db)
        .latest_local_store_position()
        .await
        .expect("load acknowledgement commit")
        .expect("acknowledgement commit exists");
    let mut retained = crate::sync::store::database::StoreDatabase::new(&db)
        .retained_merge_history_frontier(vec![acknowledgement_commit])
        .await
        .expect("open acknowledgement checkpoint");
    let acknowledgement = retained
        .remove(0)
        .summary
        .acknowledgements
        .into_values()
        .next()
        .expect("checkpoint retains its acknowledgement");
    let mut forged_higher_fork = acknowledgement.clone();
    let (latest_ref, latest_value) = acknowledgement
        .latest()
        .expect("acknowledgement proof chain has a latest entry");
    let device_id = latest_ref.registration.device_id;
    let mut forked_at_same_sequence = (latest_ref.clone(), latest_value.clone());
    forked_at_same_sequence.0.ack_hash = ObjectHash::digest(b"forked acknowledgement");
    forged_higher_fork
        .chain
        .insert(latest_ref.sequence, forked_at_same_sequence.clone());
    let higher_sequence = latest_ref.sequence + 1;
    forked_at_same_sequence.0.sequence = higher_sequence;
    forked_at_same_sequence.1.sequence = higher_sequence;
    forged_higher_fork
        .chain
        .insert(higher_sequence, forked_at_same_sequence);

    let mut merged = checkpoint.summary.acknowledgements;
    insert_latest_acknowledgement(&mut merged, device_id, acknowledgement)
        .expect("first acknowledgement establishes the retained stream");
    assert!(insert_latest_acknowledgement(&mut merged, device_id, forged_higher_fork,).is_err());
}

async fn local_store_stream_id(
    database: &Database,
    store: &crate::sync::test_helpers::TestStore,
    identity: &crate::keys::UserKeypair,
) -> crate::sync::membership::AuthorStreamId {
    let device_id = database
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("load local Store device id")
        .expect("local Store device id exists");
    let (_, registration, _, _) = crate::sync::store::load_local_store_authority_for_test(
        &StoreDatabase::new(database),
        &device_id,
        identity,
    )
    .await
    .expect("load local Store authority");
    crate::sync::store_commit::StreamActivation::device_authorized_stream_id(
        store.root.store_root_hash,
        &registration,
        crate::sync::store_commit::StreamAnchorDomain::StoreAnnouncements,
    )
}

#[tokio::test]
async fn progressive_discovery_replays_same_history_in_canonical_order() {
    let founder = crate::sync::test_helpers::open_test_db();
    let identity = crate::keys::UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &founder,
        "progressive-canonical-replay",
        identity.clone(),
    )
    .await
    .expect("create canonical replay Store");
    crate::sync::test_helpers::host_exec(
        &founder,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
         VALUES ('canonical-row', 'c0', 'b0', 1,
                 '0000000001000-0000-base', '2026-07-21')",
    )
    .await;
    let (_founder_temp, founder_store_dir) = crate::sync::test_helpers::temp_store_dir();
    assert!(store
        .publish_pending(&founder, &founder_store_dir)
        .await
        .expect("publish canonical replay base"));

    let writer = crate::sync::test_helpers::open_test_db();
    crate::sync::test_helpers::install_active_device_fixture(
        &store,
        &founder,
        &writer,
        &identity,
        "2026-07-21T00:00:00Z",
    )
    .await
    .expect("activate concurrent writer");
    let mut producers = Vec::new();
    for database in [founder.clone(), writer] {
        let stream_id = local_store_stream_id(&database, &store, &identity).await;
        producers.push((stream_id, database));
    }
    producers.sort_by_key(|producer| producer.0);

    let progressive = crate::sync::test_helpers::open_test_db();
    let canonical = crate::sync::test_helpers::open_test_db();
    let (_progressive_temp, progressive_store_dir) = crate::sync::test_helpers::temp_store_dir();
    let (_canonical_temp, canonical_store_dir) = crate::sync::test_helpers::temp_store_dir();
    crate::sync::test_helpers::pull_into(&progressive, &store, &progressive_store_dir).await;
    crate::sync::test_helpers::pull_into(&canonical, &store, &canonical_store_dir).await;

    let x2_producer = &producers[0].1;
    let chain_producer = &producers[1].1;
    for update in [
        "UPDATE notes SET title = 'c1', _updated_at = '0000000003000-0000-x1'
         WHERE id = 'canonical-row'",
        "UPDATE notes SET body = 'bM', _updated_at = '0000000009000-0000-m'
         WHERE id = 'canonical-row'",
    ] {
        crate::sync::test_helpers::host_exec(chain_producer, update).await;
        let (_producer_temp, producer_store_dir) = crate::sync::test_helpers::temp_store_dir();
        assert!(store
            .publish_pending(chain_producer, &producer_store_dir)
            .await
            .unwrap_or_else(|error| panic!("publish chained concurrent update: {error}")));
        crate::sync::test_helpers::pull_into(&progressive, &store, &progressive_store_dir).await;
    }
    crate::sync::test_helpers::host_exec(
        x2_producer,
        "UPDATE notes SET title = 'c2', _updated_at = '0000000004000-0000-x2'
         WHERE id = 'canonical-row'",
    )
    .await;
    let (_x2_temp, x2_store_dir) = crate::sync::test_helpers::temp_store_dir();
    assert!(store
        .publish_pending(x2_producer, &x2_store_dir)
        .await
        .unwrap_or_else(|error| panic!("publish independent concurrent update: {error}")));
    crate::sync::test_helpers::pull_into(&progressive, &store, &progressive_store_dir).await;
    crate::sync::test_helpers::pull_into(&canonical, &store, &canonical_store_dir).await;

    let progressive_title = crate::sync::test_helpers::query_text(
        &progressive,
        "SELECT title FROM notes WHERE id = 'canonical-row'",
    )
    .await;
    let canonical_title = crate::sync::test_helpers::query_text(
        &canonical,
        "SELECT title FROM notes WHERE id = 'canonical-row'",
    )
    .await;
    assert_eq!(progressive_title, canonical_title);
}

fn scoped_replay_schema() -> (
    Vec<crate::sync::session::SyncedTable>,
    Vec<crate::migration::Migration>,
) {
    (
        vec![crate::sync::session::SyncedTable::new(
            "notes",
            crate::sync::session::RowIdentity::IndependentUuid,
        )
        .scoped_by("audience")],
        vec![crate::migration::Migration::sql(
            1,
            "scoped replay schema",
            "CREATE TABLE notes (
                 id TEXT PRIMARY KEY,
                 audience TEXT,
                 body TEXT NOT NULL,
                 _updated_at TEXT NOT NULL
             ) STRICT;",
        )],
    )
}

fn open_scoped_replay_database() -> Database {
    let (tables, migrations) = scoped_replay_schema();
    crate::sync::test_helpers::open_test_db_schema(tables, migrations)
}

fn open_scoped_replay_database_at(path: &std::path::Path) -> Database {
    let (tables, migrations) = scoped_replay_schema();
    Database::open(
        path,
        tables,
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        "scoped-replay-device".to_string(),
        &migrations,
    )
    .expect("open scoped replay database")
    .0
}

async fn scoped_host_exec(database: &Database, sql: String) {
    let tables = database.synced_tables().to_vec();
    let gates = database.gates();
    let blob_decls = database.blob_decls();
    let write_id = database.new_write_id();
    database
        .call(move |connection| {
            let routing = crate::encryption::EncryptionService::from_key([42; 32]);
            StoreDatabase::run_store_write_transaction_on(
                connection,
                &tables,
                &gates,
                &blob_decls,
                Some(&routing),
                None,
                write_id,
                |transaction| {
                    transaction
                        .execute_batch(&sql)
                        .map_err(crate::database::DbError::from)
                },
            )
        })
        .await
        .expect("commit scoped host write");
}

async fn pull_scoped(
    database: &Database,
    store: &crate::sync::test_helpers::TestStore,
    identity: &crate::keys::UserKeypair,
    store_dir: &crate::store_dir::StoreDir,
) -> StorePullResult {
    let membership = store
        .open_into(database)
        .await
        .expect("open scoped replay Store");
    let routing = crate::encryption::EncryptionService::from_key([42; 32]);
    crate::sync::store::pull_store_commits(
        &StoreDatabase::new(database),
        database.synced_tables(),
        &store.storage,
        store.root.store_root_hash,
        store_dir,
        &membership,
        Some(identity),
        Some(&routing),
    )
    .await
    .expect("pull scoped replay Store")
}

async fn pull_scoped_with(
    database: &Database,
    store: &crate::sync::test_helpers::TestStore,
    storage: &dyn SyncStorage,
    membership: &MembershipChain,
    identity: &crate::keys::UserKeypair,
    store_dir: &crate::store_dir::StoreDir,
) -> Result<StorePullResult, StorePullError> {
    let routing = crate::encryption::EncryptionService::from_key([42; 32]);
    crate::sync::store::pull_store_commits(
        &StoreDatabase::new(database),
        database.synced_tables(),
        storage,
        store.root.store_root_hash,
        store_dir,
        membership,
        Some(identity),
        Some(&routing),
    )
    .await
}

fn exact_circle_package_slot(commit: &StoreBatchCommit) -> crate::storage::cloud::ObjectSlot {
    let [reference] = commit.circle_packages() else {
        panic!("test commit must contain one Circle package");
    };
    reference.package.object.slot().clone()
}

async fn current_membership(database: &Database, storage: &dyn SyncStorage) -> MembershipChain {
    load_cycle_membership(storage, &StoreDatabase::new(database))
        .await
        .expect("load current Store membership")
        .chain
        .expect("initialized Store has a membership chain")
}

struct EffectiveAccessFixture {
    owner_database: Database,
    owner: crate::keys::UserKeypair,
    member: crate::keys::UserKeypair,
    member_storage: std::sync::Arc<crate::sync::cloud_storage::CloudSyncStorage>,
    store: crate::sync::test_helpers::TestStore,
    circle_id: crate::sync::circle::CircleId,
}

async fn effective_access_fixture(
    label: &str,
    member_database: &Database,
    owner_store_dir: &crate::store_dir::StoreDir,
    member_store_dir: &crate::store_dir::StoreDir,
) -> EffectiveAccessFixture {
    let owner_database = open_scoped_replay_database();
    let owner = crate::keys::UserKeypair::generate();
    let member = crate::keys::UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(&owner_database, label, owner.clone())
        .await
        .expect("create effective-access Store");
    store
        .open_into(&owner_database)
        .await
        .expect("open effective-access owner Store");
    let preinvite_membership = current_membership(&owner_database, &store.storage).await;
    crate::sync::store::membership::invite_member(
        &store.storage,
        store.home.as_ref(),
        &owner,
        &crate::sync::hlc::Hlc::new(format!("{label}-owner")),
        &crate::keys::public_key_hex(&member),
        None,
        crate::sync::membership::MemberRole::Member,
        &crate::encryption::EncryptionService::from_key([42; 32]),
        store.storage.store_id(),
        "Effective Access Store",
        &StoreDatabase::new(&owner_database),
    )
    .await
    .expect("invite effective-access Store member");
    crate::sync::test_helpers::install_active_device_fixture(
        &store,
        &owner_database,
        member_database,
        &member,
        "2026-07-23T00:00:00Z",
    )
    .await
    .expect("activate effective-access member device");

    let owner_store = store
        .loaded_store(&owner_database)
        .await
        .expect("load effective-access owner Store");
    let owner_device = owner_database
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("load effective-access owner device")
        .expect("effective-access owner device is active");
    let circle_id = owner_store
        .create_circle(
            &owner_device,
            "0000000001000-0000-owner",
            "Effective Access",
            &owner,
        )
        .await
        .expect("create effective-access Circle");
    let owner_storage = crate::sync::cloud_storage::CloudSyncStorage::new(
        store.home.clone(),
        crate::sync::cloud_storage::CloudCipher::Encrypted(
            crate::encryption::EncryptionService::from_key([42; 32]),
        ),
        crate::sync::cloud_storage::BlobPathScheme::Hashed,
        store.storage.store_id(),
        owner.clone(),
    )
    .expect("open effective-access owner storage");
    let components = crate::sync::cycle::init_sync_over_storage(
        &StoreDatabase::new(&owner_database),
        owner_storage,
        crate::sync::cycle::StoreInitialization::OpenStore {
            expected_store_root: store.root.clone(),
        },
        Some(crate::encryption::EncryptionService::from_key([42; 32])),
    )
    .await
    .expect("initialize effective-access owner sync");
    components
        .add_circle_member(
            owner_store_dir,
            circle_id,
            crate::keys::public_key_hex(&member),
            crate::sync::circle::CircleRole::Member,
        )
        .await
        .expect("add effective-access Circle member");

    let member_storage = std::sync::Arc::new(
        crate::sync::cloud_storage::CloudSyncStorage::new(
            store.home.clone(),
            crate::sync::cloud_storage::CloudCipher::Encrypted(
                crate::encryption::EncryptionService::from_key([42; 32]),
            ),
            crate::sync::cloud_storage::BlobPathScheme::Hashed,
            store.storage.store_id(),
            member.clone(),
        )
        .expect("open effective-access member storage"),
    );
    store
        .open_into(member_database)
        .await
        .expect("open effective-access member Store");
    let initial_pull = pull_scoped_with(
        member_database,
        &store,
        member_storage.as_ref(),
        &preinvite_membership,
        &member,
        member_store_dir,
    )
    .await
    .expect("pull effective-access Circle activation");
    assert!(initial_pull.held_positions.is_empty(), "{initial_pull:?}");

    EffectiveAccessFixture {
        owner_database,
        owner,
        member,
        member_storage,
        store,
        circle_id,
    }
}

const EFFECTIVE_ACCESS_ROW_ID: &str = "01890a5d-ac96-774b-bcce-b302099c3f75";
const READD_EFFECTIVE_ACCESS_ROW_ID: &str = "01890a5d-ac96-774b-bcce-b302099c3f76";

async fn publish_effective_access_row(
    fixture: &EffectiveAccessFixture,
    owner_store_dir: &crate::store_dir::StoreDir,
    body: &str,
    stamp: &str,
) -> StoreBatchCommitRef {
    publish_effective_access_row_with_id(
        fixture,
        owner_store_dir,
        EFFECTIVE_ACCESS_ROW_ID,
        body,
        stamp,
    )
    .await
}

async fn publish_effective_access_row_with_id(
    fixture: &EffectiveAccessFixture,
    owner_store_dir: &crate::store_dir::StoreDir,
    row_id: &str,
    body: &str,
    stamp: &str,
) -> StoreBatchCommitRef {
    let statement = if scoped_routing_state(&fixture.owner_database, row_id)
        .await
        .row
        .is_some()
    {
        format!(
            "UPDATE notes
             SET body = '{body}', _updated_at = '{stamp}'
             WHERE id = '{row_id}';"
        )
    } else {
        format!(
            "INSERT INTO notes (id, audience, body, _updated_at)
             VALUES ('{row_id}', '{}', '{body}', '{stamp}');",
            fixture.circle_id
        )
    };
    scoped_host_exec(&fixture.owner_database, statement).await;
    assert!(fixture
        .store
        .publish_pending(&fixture.owner_database, owner_store_dir)
        .await
        .expect("publish effective-access Circle row"));
    StoreDatabase::new(&fixture.owner_database)
        .latest_local_store_position()
        .await
        .expect("load effective-access row position")
        .expect("effective-access row has a Store position")
}

async fn load_commit(
    fixture: &EffectiveAccessFixture,
    reference: &StoreBatchCommitRef,
) -> StoreBatchCommit {
    load_commit_with_author(&fixture.store.storage, &fixture.store.root, reference)
        .await
        .expect("load effective-access commit")
        .0
}

#[test]
fn later_removal_blocks_historical_circle_access() {
    assert_eq!(
        super::materialization::historical_local_store_membership(
            LocalStoreMembership::Removed,
            LocalStoreMembership::Current,
        ),
        LocalStoreMembership::Removed
    );
}

#[test]
fn later_admission_does_not_grant_pre_admission_circle_access() {
    assert_eq!(
        super::materialization::historical_local_store_membership(
            LocalStoreMembership::Current,
            LocalStoreMembership::NotYetMember,
        ),
        LocalStoreMembership::NotYetMember
    );
}

#[test]
fn later_readd_does_not_grant_removed_interval_circle_access() {
    assert_eq!(
        super::materialization::historical_local_store_membership(
            LocalStoreMembership::Current,
            LocalStoreMembership::Removed,
        ),
        LocalStoreMembership::Removed
    );
}

#[tokio::test]
async fn newly_discovered_store_admission_activates_circle_access() {
    let member_database = open_scoped_replay_database();
    let (_owner_temp, owner_store_dir) = crate::sync::test_helpers::temp_store_dir();
    let (_member_store_temp, member_store_dir) = crate::sync::test_helpers::temp_store_dir();
    let fixture = effective_access_fixture(
        "newly-admitted-member-effective-access",
        &member_database,
        &owner_store_dir,
        &member_store_dir,
    )
    .await;

    assert_eq!(
        StoreDatabase::new(&member_database)
            .get_circles(
                &crate::keys::public_key_hex(&fixture.member),
                std::collections::BTreeSet::from([
                    crate::keys::public_key_hex(&fixture.owner),
                    crate::keys::public_key_hex(&fixture.member),
                ]),
            )
            .await
            .expect("list Circles after newly discovered Store admission")
            .into_iter()
            .map(|circle| circle.name)
            .collect::<Vec<_>>(),
        vec!["Effective Access".to_string()]
    );
}

#[tokio::test]
async fn removed_store_member_skips_late_circle_package_and_atomically_prunes_rows() {
    let member_temp = tempfile::tempdir().expect("create effective-access database directory");
    let member_path = member_temp.path().join("member.sqlite3");
    let member_database = open_scoped_replay_database_at(&member_path);
    let (_owner_temp, owner_store_dir) = crate::sync::test_helpers::temp_store_dir();
    let (_member_store_temp, member_store_dir) = crate::sync::test_helpers::temp_store_dir();
    let fixture = effective_access_fixture(
        "removed-member-effective-access",
        &member_database,
        &owner_store_dir,
        &member_store_dir,
    )
    .await;

    let first = publish_effective_access_row(
        &fixture,
        &owner_store_dir,
        "visible before removal",
        "0000000002000-0000-owner",
    )
    .await;
    let membership = current_membership(&member_database, fixture.member_storage.as_ref()).await;
    let first_pull = pull_scoped_with(
        &member_database,
        &fixture.store,
        fixture.member_storage.as_ref(),
        &membership,
        &fixture.member,
        &member_store_dir,
    )
    .await
    .expect("pull pre-removal Circle row");
    assert!(first_pull.held_positions.is_empty(), "{first_pull:?}");
    assert_eq!(
        scoped_routing_state(&member_database, EFFECTIVE_ACCESS_ROW_ID)
            .await
            .row
            .as_ref()
            .map(|row| row.1.as_str()),
        Some("visible before removal")
    );
    let hidden_before_removal = publish_effective_access_row(
        &fixture,
        &owner_store_dir,
        "private immediately before removal",
        "0000000002500-0000-owner",
    )
    .await;
    let hidden_before_removal_commit = load_commit(&fixture, &hidden_before_removal).await;
    let hidden_before_removal_package_slot =
        exact_circle_package_slot(&hidden_before_removal_commit);

    // The last Circle package the owner authors before the removal. Once the
    // removal is materialized the owner may no longer publish new Circle content
    // (the Circle is rotation-required), so this models the newest package the
    // removed member must still be pruned from.
    let late = publish_effective_access_row(
        &fixture,
        &owner_store_dir,
        "private just before removal",
        "0000000002800-0000-owner",
    )
    .await;
    let late_commit = load_commit(&fixture, &late).await;
    let late_package_slot = exact_circle_package_slot(&late_commit);

    let custody = crate::sync::test_helpers::TestCustody::default();
    custody.set_initial_key([42; 32]);
    crate::sync::store::remove_member(
        &fixture.store.storage,
        fixture.store.home.as_ref(),
        &fixture.owner,
        &crate::sync::hlc::Hlc::new("removed-member-owner".to_string()),
        &crate::keys::public_key_hex(&fixture.member),
        &crate::encryption::EncryptionService::from_key([42; 32]),
        &custody,
        fixture.store.storage.cipher_state().as_ref(),
        &crate::sync::cloud_storage::PendingRotation::none(),
        &StoreDatabase::new(&fixture.owner_database),
    )
    .await
    .expect("remove effective-access Store member");
    let removal = StoreDatabase::new(&fixture.owner_database)
        .latest_local_store_position()
        .await
        .expect("load Store removal position")
        .expect("Store removal has a position");
    let latest_membership =
        current_membership(&member_database, fixture.member_storage.as_ref()).await;
    assert!(!latest_membership
        .current_members()
        .iter()
        .any(|(member, _)| member == &crate::keys::public_key_hex(&fixture.member)));

    fixture.store.home.clear_exact_reads();
    member_database.fail_next_merge_materialization_at(
        crate::database::MergeMaterializationFailurePoint::SummaryMaterialization,
    );
    pull_scoped_with(
        &member_database,
        &fixture.store,
        fixture.member_storage.as_ref(),
        &latest_membership,
        &fixture.member,
        &member_store_dir,
    )
    .await
    .expect_err("injected transaction failure interrupts removed-member materialization");
    assert_eq!(
        scoped_routing_state(&member_database, EFFECTIVE_ACCESS_ROW_ID)
            .await
            .row
            .as_ref()
            .map(|row| row.1.as_str()),
        Some("visible before removal")
    );
    assert!(StoreDatabase::new(&member_database)
        .exact_materialized_ref(&commit_stream_id(&late.coord), late.coord.sequence(),)
        .await
        .expect("check rolled-back late position")
        .is_none());

    fixture
        .store
        .home
        .remove_exact_object(&hidden_before_removal_package_slot);
    fixture.store.home.remove_exact_object(&late_package_slot);
    fixture.store.home.clear_exact_reads();
    let pull = pull_scoped_with(
        &member_database,
        &fixture.store,
        fixture.member_storage.as_ref(),
        &latest_membership,
        &fixture.member,
        &member_store_dir,
    )
    .await
    .expect("pull Store state after membership removal");
    assert!(pull.held_positions.is_empty(), "{pull:?}");
    assert!(!fixture
        .store
        .home
        .exact_reads()
        .contains(&hidden_before_removal_package_slot));
    assert!(!fixture
        .store
        .home
        .exact_reads()
        .contains(&late_package_slot));
    let state = scoped_routing_state(&member_database, EFFECTIVE_ACCESS_ROW_ID).await;
    assert_eq!(state.row, None);
    assert_eq!(state.route, None);
    assert!(StoreDatabase::new(&member_database)
        .get_circles(
            &crate::keys::public_key_hex(&fixture.member),
            std::collections::BTreeSet::from([crate::keys::public_key_hex(&fixture.owner)]),
        )
        .await
        .expect("list Circles after Store membership removal")
        .is_empty());
    assert!(StoreDatabase::new(&member_database)
        .circle_authoring_context(
            fixture.circle_id,
            &crate::keys::public_key_hex(&fixture.member),
        )
        .await
        .is_err());
    let (public_circle_state, private_circle_state): (i64, i64) = member_database
        .call(|connection| {
            connection
                .query_row(
                    "SELECT
                         (SELECT COUNT(*) FROM circle_current_state),
                         (SELECT COUNT(*) FROM circle_access_cache)
                       + (SELECT COUNT(*) FROM circle_roster_cache)
                       + (SELECT COUNT(*) FROM circle_metadata_cache)",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(DbError::from)
        })
        .await
        .expect("count Circle state after Store membership removal");
    assert_eq!(public_circle_state, 1);
    assert_eq!(private_circle_state, 0);
    assert_eq!(
        state.mirror,
        Some((
            Some(fixture.circle_id.to_string()),
            "0000000002000-0000-owner".to_string(),
        ))
    );
    for reference in [&first, &hidden_before_removal, &late, &removal] {
        assert_eq!(
            StoreDatabase::new(&member_database)
                .exact_materialized_ref(
                    &commit_stream_id(&reference.coord),
                    reference.coord.sequence(),
                )
                .await
                .expect("load effective-access materialized position"),
            Some(reference.clone())
        );
    }

    std::thread::spawn(move || drop(member_database))
        .join()
        .expect("close effective-access member database");
    let reopened = open_scoped_replay_database_at(&member_path);
    let reopened_state = scoped_routing_state(&reopened, EFFECTIVE_ACCESS_ROW_ID).await;
    assert_eq!(reopened_state.row, None);
    assert_eq!(reopened_state.route, None);
    assert_eq!(
        reopened_state.mirror,
        Some((
            Some(fixture.circle_id.to_string()),
            "0000000002000-0000-owner".to_string(),
        ))
    );
    assert_eq!(
        StoreDatabase::new(&reopened)
            .exact_materialized_ref(&commit_stream_id(&removal.coord), removal.coord.sequence(),)
            .await
            .expect("load reopened removal position"),
        Some(removal)
    );
    assert!(StoreDatabase::new(&reopened)
        .get_circles(
            &crate::keys::public_key_hex(&fixture.member),
            std::collections::BTreeSet::from([crate::keys::public_key_hex(&fixture.owner)]),
        )
        .await
        .expect("list reopened Circles after Store membership removal")
        .is_empty());
    let reopened_public_circle_state: i64 = reopened
        .call(|connection| {
            connection
                .query_row("SELECT COUNT(*) FROM circle_current_state", [], |row| {
                    row.get(0)
                })
                .map_err(DbError::from)
        })
        .await
        .expect("count reopened public Circle state");
    assert_eq!(reopened_public_circle_state, 1);
}

#[tokio::test]
async fn readded_store_member_restores_circle_access_from_a_stale_removed_membership() {
    let member_database = open_scoped_replay_database();
    let (_owner_temp, owner_store_dir) = crate::sync::test_helpers::temp_store_dir();
    let (_member_store_temp, member_store_dir) = crate::sync::test_helpers::temp_store_dir();
    let fixture = effective_access_fixture(
        "readded-member-effective-access",
        &member_database,
        &owner_store_dir,
        &member_store_dir,
    )
    .await;

    publish_effective_access_row(
        &fixture,
        &owner_store_dir,
        "visible before removal",
        "0000000002000-0000-owner",
    )
    .await;
    let initial_membership =
        current_membership(&member_database, fixture.member_storage.as_ref()).await;
    let initial_pull = pull_scoped_with(
        &member_database,
        &fixture.store,
        fixture.member_storage.as_ref(),
        &initial_membership,
        &fixture.member,
        &member_store_dir,
    )
    .await
    .expect("pull Circle row before Store removal");
    assert!(initial_pull.held_positions.is_empty(), "{initial_pull:?}");

    // A Circle package the owner authors before the removal that the member has
    // not yet pulled. The removal pull applies it under the removed membership,
    // exercising the prune of the member's Circle rows.
    let pre_removal = publish_effective_access_row(
        &fixture,
        &owner_store_dir,
        "private just before removal",
        "0000000002500-0000-owner",
    )
    .await;
    let pre_removal_commit = load_commit(&fixture, &pre_removal).await;
    let pre_removal_package_slot = exact_circle_package_slot(&pre_removal_commit);

    let custody = crate::sync::test_helpers::TestCustody::default();
    custody.set_initial_key([42; 32]);
    crate::sync::store::remove_member(
        &fixture.store.storage,
        fixture.store.home.as_ref(),
        &fixture.owner,
        &crate::sync::hlc::Hlc::new("readded-member-removal".to_string()),
        &crate::keys::public_key_hex(&fixture.member),
        &crate::encryption::EncryptionService::from_key([42; 32]),
        &custody,
        fixture.store.storage.cipher_state().as_ref(),
        &crate::sync::cloud_storage::PendingRotation::none(),
        &StoreDatabase::new(&fixture.owner_database),
    )
    .await
    .expect("remove Store member before re-add");
    // Once the removal is materialized the owner can no longer publish new
    // Circle content (the Circle is rotation-required until it is closed and
    // rotated), so no package is authored during the removed interval; the
    // re-add restores access to the Circle's current state alone.
    let removed_membership =
        current_membership(&member_database, fixture.member_storage.as_ref()).await;
    fixture.store.home.clear_exact_reads();
    let removal_pull = pull_scoped_with(
        &member_database,
        &fixture.store,
        fixture.member_storage.as_ref(),
        &removed_membership,
        &fixture.member,
        &member_store_dir,
    )
    .await
    .expect("pull Store membership removal");
    assert!(removal_pull.held_positions.is_empty(), "{removal_pull:?}");
    assert!(
        !fixture
            .store
            .home
            .exact_reads()
            .contains(&pre_removal_package_slot),
        "a removed member does not fetch the unpulled pre-removal Circle package"
    );
    assert_eq!(
        scoped_routing_state(&member_database, EFFECTIVE_ACCESS_ROW_ID)
            .await
            .row,
        None
    );

    crate::sync::store::membership::invite_member(
        &fixture.store.storage,
        fixture.store.home.as_ref(),
        &fixture.owner,
        &crate::sync::hlc::Hlc::new("readded-member-invitation".to_string()),
        &crate::keys::public_key_hex(&fixture.member),
        None,
        crate::sync::membership::MemberRole::Member,
        &crate::encryption::EncryptionService::from_key([42; 32]),
        fixture.store.storage.store_id(),
        "Effective Access Store",
        &StoreDatabase::new(&fixture.owner_database),
    )
    .await
    .expect("re-add effective-access Store member");
    let rotated_store_encryption = fixture
        .store
        .storage
        .cipher_state()
        .encryption()
        .expect("re-added encrypted Store has a live keyring");
    crate::sync::store::apply_key_rotation(
        rotated_store_encryption,
        &custody,
        fixture.member_storage.cipher_state().as_ref(),
    )
    .expect("adopt the Store key wrapped by the re-add");
    let owner_store = fixture
        .store
        .loaded_store(&fixture.owner_database)
        .await
        .expect("load owner Store for Circle successor");
    let owner_device = fixture
        .owner_database
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("load owner device for Circle successor")
        .expect("owner device exists for Circle successor");
    owner_store
        .rename_circle(
            &owner_device,
            "0000000004000-0000-owner",
            fixture.circle_id,
            "Effective Access Restored",
            &fixture.owner,
        )
        .await
        .expect("publish Circle successor after Store re-add");
    publish_effective_access_row_with_id(
        &fixture,
        &owner_store_dir,
        READD_EFFECTIVE_ACCESS_ROW_ID,
        "visible after re-add",
        "0000000005000-0000-owner",
    )
    .await;

    fixture.store.home.clear_exact_reads();
    let readd_pull = pull_scoped_with(
        &member_database,
        &fixture.store,
        fixture.member_storage.as_ref(),
        &removed_membership,
        &fixture.member,
        &member_store_dir,
    )
    .await
    .expect("pull Store re-add and Circle successor");
    assert!(readd_pull.held_positions.is_empty(), "{readd_pull:?}");
    assert_eq!(
        scoped_routing_state(&member_database, READD_EFFECTIVE_ACCESS_ROW_ID)
            .await
            .row
            .as_ref()
            .map(|row| row.1.as_str()),
        Some("visible after re-add")
    );
    assert_eq!(
        StoreDatabase::new(&member_database)
            .get_circles(
                &crate::keys::public_key_hex(&fixture.member),
                std::collections::BTreeSet::from([
                    crate::keys::public_key_hex(&fixture.owner),
                    crate::keys::public_key_hex(&fixture.member),
                ]),
            )
            .await
            .expect("list restored Circles")
            .into_iter()
            .map(|circle| circle.name)
            .collect::<Vec<_>>(),
        vec!["Effective Access Restored".to_string()]
    );
}

#[derive(Debug, PartialEq, Eq)]
struct ScopedRoutingState {
    row: Option<(Option<String>, String, String)>,
    route: Option<(String, String)>,
    mirror: Option<(Option<String>, String)>,
}

async fn scoped_routing_state(database: &Database, row_id: &str) -> ScopedRoutingState {
    let row_id = row_id.to_string();
    database
        .call(move |connection| {
            let routing_id = crate::sync::test_helpers::test_row_routing_id(
                connection, [42; 32], "notes", &row_id,
            )
            .to_string();
            let row = connection
                .query_row(
                    "SELECT audience, body, _updated_at FROM notes WHERE id = ?1",
                    [&row_id],
                    |row| {
                        Ok((
                            row.get::<_, Option<String>>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(DbError::from)?;
            let route = connection
                .query_row(
                    "SELECT routing_id, _updated_at
                     FROM _coven_row_routes
                     WHERE table_name = 'notes' AND row_id = ?1",
                    [&row_id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()
                .map_err(DbError::from)?;
            let mirror = connection
                .query_row(
                    "SELECT circle_id, _updated_at
                     FROM _coven_audience
                     WHERE routing_id = ?1",
                    [&routing_id],
                    |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()
                .map_err(DbError::from)?;
            Ok(ScopedRoutingState { row, route, mirror })
        })
        .await
        .expect("read scoped routing state")
}

#[derive(Clone, Copy, Debug)]
enum RoutingConflict {
    MoveMove,
    MoveEdit,
    DeleteMove,
    MoveLocal,
}

impl RoutingConflict {
    fn store_id(self) -> &'static str {
        match self {
            Self::MoveMove => "routing-replay-move-move",
            Self::MoveEdit => "routing-replay-move-edit",
            Self::DeleteMove => "routing-replay-delete-move",
            Self::MoveLocal => "routing-replay-move-local",
        }
    }
}

#[tokio::test]
async fn routing_conflicts_converge_after_progressive_and_complete_discovery() {
    const ROW_ID: &str = "01890a5d-ac96-774b-bcce-b302099c3f74";

    for conflict in [
        RoutingConflict::MoveMove,
        RoutingConflict::MoveEdit,
        RoutingConflict::DeleteMove,
        RoutingConflict::MoveLocal,
    ] {
        let founder = open_scoped_replay_database();
        let identity = crate::keys::UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &founder,
            conflict.store_id(),
            identity.clone(),
        )
        .await
        .expect("create scoped replay Store");
        store.home.sort_listings();
        store
            .open_into(&founder)
            .await
            .expect("open founder scoped replay Store");
        let founder_device = founder
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("load scoped replay founder device")
            .expect("scoped replay founder device exists");
        let loaded = store
            .loaded_store(&founder)
            .await
            .expect("load founder Store operations");
        let first_circle = loaded
            .create_circle(
                &founder_device,
                "0000000001000-0000-owner",
                "First",
                &identity,
            )
            .await
            .expect("create first routing-conflict Circle");
        let second_circle = loaded
            .create_circle(
                &founder_device,
                "0000000001001-0000-owner",
                "Second",
                &identity,
            )
            .await
            .expect("create second routing-conflict Circle");
        scoped_host_exec(
            &founder,
            format!(
                "INSERT INTO notes VALUES (
                     '{ROW_ID}', NULL, 'base', '0000000002000-0000-base'
                 );"
            ),
        )
        .await;
        let (_founder_temp, founder_dir) = crate::sync::test_helpers::temp_store_dir();
        assert!(store
            .publish_pending(&founder, &founder_dir)
            .await
            .expect("publish scoped replay base"));

        let first_writer = open_scoped_replay_database();
        let second_writer = open_scoped_replay_database();
        let progressive = open_scoped_replay_database();
        let complete = open_scoped_replay_database();
        for participant in [&first_writer, &second_writer, &progressive, &complete] {
            crate::sync::test_helpers::install_active_device_fixture(
                &store,
                &founder,
                participant,
                &identity,
                "2026-07-22T00:00:00Z",
            )
            .await
            .expect("activate scoped replay device");
        }
        let (_first_temp, first_dir) = crate::sync::test_helpers::temp_store_dir();
        let (_second_temp, second_dir) = crate::sync::test_helpers::temp_store_dir();
        let (_progressive_temp, progressive_dir) = crate::sync::test_helpers::temp_store_dir();
        let (_complete_temp, complete_dir) = crate::sync::test_helpers::temp_store_dir();
        for (participant, directory) in [
            (&first_writer, &first_dir),
            (&second_writer, &second_dir),
            (&progressive, &progressive_dir),
            (&complete, &complete_dir),
        ] {
            let pulled = pull_scoped(participant, &store, &identity, directory).await;
            assert!(pulled.held_positions.is_empty(), "{conflict:?}: {pulled:?}");
        }

        let mut writers = [
            (
                local_store_stream_id(&first_writer, &store, &identity).await,
                &first_writer,
                &first_dir,
            ),
            (
                local_store_stream_id(&second_writer, &store, &identity).await,
                &second_writer,
                &second_dir,
            ),
        ];
        writers.sort_by_key(|writer| writer.0);
        let (_, canonical_earlier, canonical_earlier_dir) = writers[0];
        let (_, canonical_later, canonical_later_dir) = writers[1];

        let (canonical_later_sql, canonical_earlier_sql) = match conflict {
            RoutingConflict::MoveMove => (
                format!(
                    "UPDATE notes
                     SET audience = '{first_circle}', body = 'first move',
                         _updated_at = '0000000003000-0000-first'
                     WHERE id = '{ROW_ID}';"
                ),
                format!(
                    "UPDATE notes
                     SET audience = '{second_circle}', body = 'second move',
                         _updated_at = '0000000004000-0000-second'
                     WHERE id = '{ROW_ID}';"
                ),
            ),
            RoutingConflict::MoveEdit => (
                format!(
                    "UPDATE notes
                     SET audience = '{first_circle}', body = 'moved',
                         _updated_at = '0000000003000-0000-move'
                     WHERE id = '{ROW_ID}';"
                ),
                format!(
                    "UPDATE notes
                     SET body = 'edited', _updated_at = '0000000004000-0000-edit'
                     WHERE id = '{ROW_ID}';"
                ),
            ),
            RoutingConflict::DeleteMove => (
                format!(
                    "UPDATE notes
                     SET audience = '{first_circle}', body = 'moved',
                         _updated_at = '0000000003000-0000-move'
                     WHERE id = '{ROW_ID}';"
                ),
                format!("DELETE FROM notes WHERE id = '{ROW_ID}';"),
            ),
            RoutingConflict::MoveLocal => (
                format!(
                    "UPDATE notes
                     SET audience = '{first_circle}', body = 'moved',
                         _updated_at = '0000000003000-0000-move'
                     WHERE id = '{ROW_ID}';"
                ),
                format!(
                    "UPDATE notes
                     SET audience = 'local', body = 'local',
                         _updated_at = '0000000004000-0000-local'
                     WHERE id = '{ROW_ID}';"
                ),
            ),
        };

        scoped_host_exec(canonical_later, canonical_later_sql).await;
        assert!(store
            .publish_pending(canonical_later, canonical_later_dir)
            .await
            .expect("publish canonical-later routing conflict"));
        let first_pull = pull_scoped(&progressive, &store, &identity, &progressive_dir).await;
        assert!(
            first_pull.held_positions.is_empty(),
            "{conflict:?}: {first_pull:?}"
        );

        scoped_host_exec(canonical_earlier, canonical_earlier_sql).await;
        assert!(store
            .publish_pending(canonical_earlier, canonical_earlier_dir)
            .await
            .expect("publish canonical-earlier routing conflict"));
        let progressive_pull = pull_scoped(&progressive, &store, &identity, &progressive_dir).await;
        let complete_pull = pull_scoped(&complete, &store, &identity, &complete_dir).await;
        assert!(
            progressive_pull.held_positions.is_empty(),
            "{conflict:?}: {progressive_pull:?}"
        );
        assert!(
            complete_pull.held_positions.is_empty(),
            "{conflict:?}: {complete_pull:?}"
        );

        let progressive_state = scoped_routing_state(&progressive, ROW_ID).await;
        let complete_state = scoped_routing_state(&complete, ROW_ID).await;
        assert_eq!(
            progressive_state, complete_state,
            "{conflict:?} must converge regardless of discovery grouping"
        );
        match conflict {
            RoutingConflict::MoveMove => {
                assert_eq!(
                    progressive_state.row.as_ref().map(|row| row.0.clone()),
                    Some(Some(second_circle.to_string()))
                );
            }
            RoutingConflict::MoveEdit => {
                assert_eq!(
                    progressive_state.row.as_ref().map(|row| row.0.clone()),
                    Some(Some(first_circle.to_string()))
                );
            }
            RoutingConflict::DeleteMove | RoutingConflict::MoveLocal => {
                assert_eq!(
                    progressive_state,
                    ScopedRoutingState {
                        row: None,
                        route: None,
                        mirror: None,
                    },
                    "{conflict:?} must remove every remote routing representation"
                );
            }
        }
    }
}

#[test]
fn recovery_cursor_requires_the_exact_origin_activation_pair() {
    let recovery_id = crate::sync::store_commit::DeviceRecoveryId::from_hash(ObjectHash::digest(
        b"recovery cursor id",
    ));
    let owner_grant = crate::sync::causal_grants::MembershipGrantId(ObjectHash::digest(
        b"recovery cursor owner grant",
    ));
    let recovery_slot = crate::storage::cloud::ObjectSlot::opaque(
        "store-v1/test/recovery.json".to_string(),
        "recovery-cursor-slot".to_string(),
    )
    .expect("construct recovery cursor slot");
    let node = OwnerRecoveryNodeRef {
        owner_pubkey: "recovery-owner".to_string(),
        owner_grant: owner_grant.clone(),
        sequence: 1,
        node_hash: ObjectHash::digest(b"recovery cursor node"),
        object: ExactObjectRef::new(
            recovery_slot.clone(),
            1,
            ObjectHash::digest(b"recovery cursor bytes"),
        ),
    };
    let origin = StoreDeviceRegistrationOrigin::Recovery {
        recovery_id,
        recovery_slot,
        owner_grant: owner_grant.clone(),
    };
    let activation = StoreDeviceRegistrationActivation::Recovery {
        recovery_id,
        node: node.clone(),
    };

    assert_eq!(
        registration_recovery_cursor(&origin, &activation).expect("derive exact recovery cursor"),
        Some(OwnerRecoveryCursor {
            owner_grant,
            position: OwnerRecoveryPosition::At { node: node.clone() },
        })
    );

    let wrong_activation = StoreDeviceRegistrationActivation::Recovery {
        recovery_id: crate::sync::store_commit::DeviceRecoveryId::from_hash(ObjectHash::digest(
            b"another recovery cursor id",
        )),
        node,
    };
    assert!(registration_recovery_cursor(&origin, &wrong_activation).is_err());
}

#[tokio::test]
async fn merge_outbound_projects_membership_to_the_commits_predecessors() {
    let founder = crate::sync::test_helpers::user_keypair_from_seed([42; 32]);
    let founder_db = crate::sync::test_helpers::open_test_db();
    let store = crate::sync::test_helpers::TestStore::create(
        &founder_db,
        "causal-membership-proof",
        founder.clone(),
    )
    .await
    .expect("create Merge Store");
    let founder_database = StoreDatabase::new(&founder_db);
    let candidate = crate::sync::test_helpers::user_keypair_from_seed([43; 32]);
    let encryption = crate::encryption::EncryptionService::from_key([73; 32]);
    crate::sync::store::membership::invite_member(
        &store.storage,
        store.home.as_ref(),
        &founder,
        &crate::sync::hlc::Hlc::new("causal-membership-proof".to_string()),
        &crate::sync::test_helpers::pubkey_hex(&candidate),
        None,
        crate::sync::membership::MemberRole::Member,
        &encryption,
        "causal-membership-proof",
        "Causal Membership Proof",
        &founder_database,
    )
    .await
    .expect("invite exact Store member");

    let candidate_db = crate::sync::test_helpers::open_test_db();
    crate::sync::test_helpers::install_active_device_fixture(
        &store,
        &founder_db,
        &candidate_db,
        &candidate,
        "2026-07-21T00:00:00Z",
    )
    .await
    .expect("activate candidate device");
    crate::sync::test_helpers::promote_active_member_fixture(
        &store,
        &founder_db,
        &candidate_db,
        &founder,
        &candidate,
        &encryption,
    )
    .await
    .expect("promote candidate Owner");
    let candidate_membership = crate::sync::store::pull::load_cycle_membership(
        &store.storage,
        &crate::sync::store::database::StoreDatabase::new(&candidate_db),
    )
    .await
    .expect("load candidate Owner membership");
    let (_candidate_temp, candidate_store_dir) = crate::sync::test_helpers::temp_store_dir();
    let candidate_pull = Box::pin(crate::sync::store::pull_store_commits(
        &StoreDatabase::new(&candidate_db),
        candidate_db.synced_tables(),
        &store.storage,
        store.root.store_root_hash,
        &candidate_store_dir,
        candidate_membership
            .chain
            .as_ref()
            .expect("candidate membership chain exists"),
        Some(&candidate),
        None,
    ))
    .await
    .expect("pull candidate Owner to the common Store history");
    assert!(candidate_pull.held_positions.is_empty());

    let earlier_db = &candidate_db;
    let earlier_owner = &candidate;
    let later_db = &founder_db;
    let later_owner = &founder;

    let mut earlier_membership = crate::sync::store::pull::load_cycle_membership(
        &store.storage,
        &crate::sync::store::database::StoreDatabase::new(earlier_db),
    )
    .await
    .expect("load earlier Owner membership")
    .chain
    .expect("initialized Store has membership");
    let _rotated = crate::sync::store::membership::revoke_member_durable(
        &store.storage,
        store.home.as_ref(),
        store.root.store_root_hash,
        &mut earlier_membership,
        earlier_owner,
        &crate::sync::test_helpers::pubkey_hex(&candidate),
        &store.root.store_root_id.to_string(),
        "0000000003000-0000-causal-proof",
        &encryption,
        &crate::sync::cloud_storage::PendingRotation::none(),
        &StoreDatabase::new(earlier_db),
    )
    .await
    .expect("publish traversal-earlier Owner removal control");
    let earlier_control = crate::sync::store::database::StoreDatabase::new(earlier_db)
        .latest_local_store_position()
        .await
        .expect("load earlier Owner position")
        .expect("earlier Owner published the membership control");
    let (earlier_value, _) = load_commit_with_author(&store.storage, &store.root, &earlier_control)
        .await
        .expect("load traversal-earlier control");
    let Some(crate::sync::store_commit::StoreControl { transition }) = earlier_value.control()
    else {
        panic!("earlier Owner position is not a Merge membership control");
    };

    let changeset = crate::sync::test_helpers::capture_bytes(
        &crate::sync::test_helpers::open_test_db(),
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('causal-proof-row', 'causal proof', NULL, \
                   '0000000001000-0000-causal-proof', '2026-07-21')",
        ],
    )
    .await;
    crate::sync::store::database::StoreDatabase::new(later_db)
        .enqueue_store_changeset_for_test(changeset)
        .await
        .expect("enqueue later concurrent write");
    let later_membership = crate::sync::store::pull::load_cycle_membership(
        &store.storage,
        &crate::sync::store::database::StoreDatabase::new(later_db),
    )
    .await
    .expect("load membership containing the concurrent control");
    let caller_membership = later_membership
        .chain
        .as_ref()
        .expect("initialized Store has membership");
    let earlier_head_ref = caller_membership
        .head_refs()
        .iter()
        .find(|head| head.coord == transition.body.entry.coord)
        .expect("caller membership contains the concurrent control")
        .clone();
    let earlier_head = crate::sync::store::membership::load_exact_membership_head(
        &store.storage,
        &store.root,
        &earlier_head_ref,
    )
    .await
    .expect("load concurrent membership head");
    let later_device_id = later_db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("load later Owner device id")
        .expect("later Owner device is activated");
    let (_later_temp, later_store_dir) = crate::sync::test_helpers::temp_store_dir();
    assert!(crate::sync::store::preparation::prepare_store_write(
        &StoreDatabase::new(later_db),
        &store.storage,
        &later_device_id,
        "2026-07-21T00:02:00Z",
        later_owner,
        &later_store_dir,
        later_membership
            .chain
            .as_ref()
            .expect("later Merge membership chain"),
    )
    .await
    .expect("prepare later concurrent write"));
    crate::sync::store::publication::drain_store_writes(
        &StoreDatabase::new(later_db),
        &store.storage,
    )
    .await
    .expect("publish later concurrent write");
    let later_commit = crate::sync::store::database::StoreDatabase::new(later_db)
        .latest_local_store_position()
        .await
        .expect("load later Owner position")
        .expect("later Owner published the data commit");

    let (later_value, _) = load_commit_with_author(&store.storage, &store.root, &later_commit)
        .await
        .expect("load later concurrent commit");
    let later_predecessors = commit_predecessor_references(&later_value);
    assert!(!later_predecessors.contains(&earlier_control));
    let signed_membership = &later_value.membership_state;
    assert!(!signed_membership
        .heads
        .iter()
        .any(|head| head.coord == transition.body.entry.coord));

    let verified = verify_merge_history_refs(
        &store.storage,
        &store.root,
        [later_commit.clone(), earlier_control.clone()],
    )
    .await
    .expect("verify both concurrent commits");
    let later_prefix = verified_merge_membership_prefix(&verified.commits, later_predecessors)
        .expect("derive the later commit's exact membership prefix");
    assert_eq!(
        later_prefix
            .classify_head(&earlier_head_ref, &earlier_head, &earlier_control,)
            .expect("classify concurrent control against later prefix"),
        VerifiedMergePrefixHeadStatus::OutsidePrefix,
    );
}

#[tokio::test]
async fn merge_gap_reports_the_exact_signed_predecessor() {
    let source = crate::sync::test_helpers::open_test_db();
    let store = crate::sync::test_helpers::TestStore::create(
        &source,
        "exact-predecessor-test",
        crate::keys::UserKeypair::generate(),
    )
    .await
    .expect("create exact predecessor test Store");
    let changeset = crate::sync::test_helpers::capture_bytes(
        &crate::sync::test_helpers::open_test_db(),
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('gap-row', 'gap', NULL, '0000000001000-0000-gap', '2026-01-01')",
        ],
    )
    .await;
    let first = store
        .publish_changeset("founder", 1, &changeset, source.schema_version())
        .await
        .expect("publish first exact commit");
    let second = store
        .publish_changeset("founder", 2, &changeset, source.schema_version())
        .await
        .expect("publish second exact commit");
    let third = store
        .publish_changeset("founder", 3, &changeset, source.schema_version())
        .await
        .expect("publish third exact commit");
    let (_, founder, _) = store
        .founder_device_authority()
        .await
        .expect("load founder authority");
    let commit = crate::sync::store_objects::load_commit_ref(
        &store.storage,
        store.root.store_root_hash,
        &third,
        &founder,
    )
    .await
    .expect("load third exact commit")
    .value;
    let stream_id = commit_stream_id(&first.coord);
    let frontier = BTreeMap::from([(stream_id.clone(), first.clone())]);
    let coverage = CommitFrontier::from_refs(frontier.clone()).expect("build exact frontier");
    let device_cut = coverage.commits().clone();
    let source_database = StoreDatabase::new(&source);
    let (_, device_state) = source_database
        .store_device_state_for_history_cut(&StoreHistoryCut(device_cut))
        .await
        .expect("load exact device state");
    let target = crate::sync::test_helpers::open_test_db();
    let target_database = StoreDatabase::new(&target);

    let readiness = readiness(
        &target_database,
        &store.storage,
        &store.root,
        &coverage,
        &frontier,
        &device_state,
        &[],
        &third,
        &commit,
    )
    .await
    .expect("evaluate exact predecessor gap");

    assert!(matches!(
        readiness,
        Readiness::Held(HeldStorePosition {
            reason: HeldStorePositionReason::MissingPredecessor(missing),
            ..
        }) if missing == second
    ));
}
