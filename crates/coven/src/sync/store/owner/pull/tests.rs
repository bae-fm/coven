use super::*;
use crate::database::Database;
use crate::keys::MasterKeyCustody;
use crate::protocol::store_commit::OpenedRetainedMergeHistorySummary;
use crate::sync::store::owner::pull::{
    insert_latest_acknowledgement, merge_retained_merge_history, Readiness,
    VerifiedMergePrefixHeadStatus,
};

#[path = "tests/effective_access_failure.rs"]
mod effective_access_failure;

async fn one_retained_checkpoint() -> (
    Database,
    std::sync::Arc<crate::sync::test_helpers::TestStore>,
    crate::keys::UserKeypair,
    MembershipChain,
    OpenedRetainedMergeHistorySummary,
) {
    let db = crate::sync::test_helpers::open_test_db();
    let signer = crate::keys::UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &db,
        "retained-checkpoint-conflict",
        signer.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create retained-checkpoint Store");
    let loaded_store = store
        .bind_device(&db, &signer)
        .await
        .expect("load checkpoint Store");
    let membership = loaded_store
        .membership_for_test()
        .await
        .expect("load checkpoint membership");
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('checkpoint-conflict', 'checkpoint', NULL, 1, \
                 '0000000001000-0000-checkpoint', '2026-07-21')",
    )
    .await;
    let mut writer = loaded_store
        .authorize_writer()
        .await
        .expect("authorize checkpoint writer");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare checkpoint commit"));
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("publish checkpoint commit"),
        1,
    );
    let reference = loaded_store
        .latest_local_store_position()
        .await
        .expect("load checkpoint position")
        .expect("checkpoint position exists");
    let mut retained = loaded_store
        .retained_merge_history_frontier_for_test(vec![reference])
        .await
        .expect("open retained checkpoint");
    assert_eq!(retained.len(), 1);
    (db, store, signer, membership, retained.remove(0))
}

#[tokio::test]
async fn retained_checkpoint_merge_rejects_same_coordinate_competitors() {
    let (_db, store, _signer, membership, checkpoint) = Box::pin(one_retained_checkpoint()).await;

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
    let (db, store, signer, _membership, checkpoint) = Box::pin(one_retained_checkpoint()).await;
    let coverage = CommitFrontier::from_refs(
        crate::database::StoreDatabase::new(&db)
            .materialized_frontier()
            .await
            .expect("load acknowledgement coverage"),
    )
    .expect("derive acknowledgement coverage");
    let device = store
        .bind_device(&db, &signer)
        .await
        .expect("bind retained-checkpoint device");
    device
        .publish_acknowledgement(coverage)
        .await
        .expect("publish retained acknowledgement");
    let acknowledgement_commit = device
        .latest_local_store_position()
        .await
        .expect("load acknowledgement commit")
        .expect("acknowledgement commit exists");
    let mut retained = device
        .retained_merge_history_frontier_for_test(vec![acknowledgement_commit])
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
    forked_at_same_sequence.1.body_mut().sequence = higher_sequence;
    forged_higher_fork
        .chain
        .insert(higher_sequence, forked_at_same_sequence);

    let mut merged = checkpoint.summary.acknowledgements;
    insert_latest_acknowledgement(&mut merged, device_id, acknowledgement)
        .expect("first acknowledgement establishes the retained stream");
    assert!(insert_latest_acknowledgement(&mut merged, device_id, forged_higher_fork,).is_err());
}

#[tokio::test]
async fn progressive_discovery_replays_same_history_in_canonical_order() {
    let founder = crate::sync::test_helpers::open_test_db();
    let identity = crate::keys::UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &founder,
        "progressive-canonical-replay",
        identity.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create canonical replay Store");
    founder
        .execute_test_host_write(
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
    store
        .activate_joined_device(&founder, &writer, &identity, "2026-07-21T00:00:00Z")
        .await
        .expect("activate concurrent writer");
    let mut producers = Vec::new();
    for database in [founder.clone(), writer] {
        let stream_id = store
            .bind_device(&database, &identity)
            .await
            .expect("bind canonical replay Store device")
            .announcement_stream_id_for_test()
            .await
            .expect("derive canonical replay Store stream through writer authority");
        producers.push((stream_id, database));
    }
    producers.sort_by_key(|producer| producer.0);

    let progressive = crate::sync::test_helpers::open_test_db();
    let canonical = crate::sync::test_helpers::open_test_db();
    let (_progressive_temp, progressive_store_dir) = crate::sync::test_helpers::temp_store_dir();
    let (_canonical_temp, canonical_store_dir) = crate::sync::test_helpers::temp_store_dir();
    store.pull_into(&progressive, &progressive_store_dir).await;
    store.pull_into(&canonical, &canonical_store_dir).await;

    let x2_producer = &producers[0].1;
    let chain_producer = &producers[1].1;
    for update in [
        "UPDATE notes SET title = 'c1', _updated_at = '0000000003000-0000-x1'
         WHERE id = 'canonical-row'",
        "UPDATE notes SET body = 'bM', _updated_at = '0000000009000-0000-m'
         WHERE id = 'canonical-row'",
    ] {
        chain_producer.execute_test_host_write(update).await;
        let (_producer_temp, producer_store_dir) = crate::sync::test_helpers::temp_store_dir();
        assert!(store
            .publish_pending(chain_producer, &producer_store_dir)
            .await
            .unwrap_or_else(|error| panic!("publish chained concurrent update: {error}")));
        store.pull_into(&progressive, &progressive_store_dir).await;
    }
    x2_producer
        .execute_test_host_write(
            "UPDATE notes SET title = 'c2', _updated_at = '0000000004000-0000-x2'
         WHERE id = 'canonical-row'",
        )
        .await;
    let (_x2_temp, x2_store_dir) = crate::sync::test_helpers::temp_store_dir();
    assert!(store
        .publish_pending(x2_producer, &x2_store_dir)
        .await
        .unwrap_or_else(|error| panic!("publish independent concurrent update: {error}")));
    store.pull_into(&progressive, &progressive_store_dir).await;
    store.pull_into(&canonical, &canonical_store_dir).await;

    let progressive_title = progressive
        .query_test_text("SELECT title FROM notes WHERE id = 'canonical-row'")
        .await;
    let canonical_title = canonical
        .query_test_text("SELECT title FROM notes WHERE id = 'canonical-row'")
        .await;
    assert_eq!(progressive_title, canonical_title);
}

fn scoped_replay_schema() -> (
    Vec<crate::protocol::synced_schema::SyncedTable>,
    Vec<crate::Migration>,
) {
    (
        vec![crate::protocol::synced_schema::SyncedTable::new(
            "notes",
            crate::protocol::synced_schema::RowIdentity::IndependentUuid,
        )
        .scoped_by("audience")],
        vec![crate::Migration::sql(
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
        crate::protocol::blob::BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "scoped-replay-device".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &migrations,
    )
    .expect("open scoped replay database")
}

fn exact_circle_package_slot(commit: &StoreBatchCommit) -> crate::protocol::objects::ObjectSlot {
    let [reference] = commit.circle_packages() else {
        panic!("test commit must contain one Circle package");
    };
    reference.package.object.slot().clone()
}

struct EffectiveAccessFixture {
    owner_database: Database,
    owner_device: crate::sync::test_helpers::TestDevice,
    member_device: crate::sync::test_helpers::TestDevice,
    owner: crate::keys::UserKeypair,
    member: crate::keys::UserKeypair,
    store: std::sync::Arc<crate::sync::test_helpers::TestStore>,
    home: std::sync::Arc<crate::InMemoryCloudHome>,
    circle_id: crate::protocol::circle::CircleId,
}

impl EffectiveAccessFixture {
    fn effective_access_members(&self) -> std::collections::BTreeSet<String> {
        std::collections::BTreeSet::from([
            crate::keys::public_key_hex(&self.owner),
            crate::keys::public_key_hex(&self.member),
        ])
    }

    async fn load_commit(&self, reference: &StoreBatchCommitRef) -> VerifiedStoreBatchCommit {
        self.owner_device
            .load_commit_for_test(reference)
            .await
            .expect("load effective-access commit")
    }

    async fn delete_circle(&self) {
        self.owner_device
            .delete_circle(self.circle_id)
            .await
            .expect("delete the Circle");
    }

    async fn pull_member(
        &self,
        store_dir: &crate::store_dir::StoreDir,
    ) -> Result<StorePullResult, crate::sync::test_helpers::TestPullError> {
        self.member_device
            .pull_store(store_dir)
            .await
            .map(|(_, result)| result)
    }

    async fn publish_row(&self, row_id: &str, body: &str, stamp: &str) -> StoreBatchCommitRef {
        let statement = if self
            .owner_database
            .scoped_routing_state_for_test(row_id)
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
                self.circle_id
            )
        };
        self.owner_database
            .run_scoped_host_write_for_test(statement)
            .await;
        let mut writer = self
            .owner_device
            .authorize_writer()
            .await
            .expect("authorize effective-access owner writer");
        assert!(writer
            .prepare_pending_store_write()
            .await
            .expect("prepare effective-access Circle row"));
        assert_eq!(
            writer
                .drain_store_writes()
                .await
                .expect("publish effective-access Circle row"),
            1
        );
        self.owner_device
            .latest_local_store_position()
            .await
            .expect("load effective-access row position")
            .expect("effective-access row has a Store position")
    }

    async fn create(
        label: &str,
        member_database: &Database,
        owner_store_dir: &crate::store_dir::StoreDir,
        member_store_dir: &crate::store_dir::StoreDir,
    ) -> Self {
        let owner_database = open_scoped_replay_database();
        let owner = crate::keys::UserKeypair::generate();
        let member = crate::keys::UserKeypair::generate();
        let home = crate::sync::test_helpers::test_cloud_home();
        let store = crate::sync::test_helpers::TestStore::create(
            &owner_database,
            label,
            owner.clone(),
            home.clone(),
        )
        .await
        .expect("create effective-access Store");
        store
            .open_into(&owner_database)
            .await
            .expect("open effective-access owner Store");
        store
            .invite_member(
                &owner_database,
                &owner,
                &crate::keys::public_key_hex(&member),
                None,
                crate::protocol::membership::MemberRole::Member,
                &crate::encryption::EncryptionService::from_key([42; 32]),
                "Effective Access Store",
            )
            .await
            .expect("invite effective-access Store member");
        let member_device = store
            .activate_joined_device(
                &owner_database,
                member_database,
                &member,
                "2026-07-23T00:00:00Z",
            )
            .await
            .expect("activate effective-access member device");

        let owner_store = store
            .bind_device(&owner_database, &owner)
            .await
            .expect("load effective-access owner Store");
        let circle_id = owner_store
            .create_circle("0000000001000-0000-owner", "Effective Access")
            .await
            .expect("create effective-access Circle");
        let owner_storage = crate::storage::CloudSyncStorage::new(
            home.clone(),
            crate::storage::CloudCipher::Encrypted(crate::encryption::EncryptionService::from_key(
                [42; 32],
            )),
            crate::storage::BlobPathScheme::Hashed,
            label,
            owner.clone(),
        )
        .expect("open effective-access owner storage");
        let components = crate::sync::cycle::PreparedSyncComponents::prepare(
            StoreDatabase::new(&owner_database),
            owner_store_dir.clone(),
            crate::sync::test_owner_graph::local_blob_access(
                StoreDatabase::new(&owner_database),
                owner_store_dir.clone(),
            ),
            owner_storage,
            owner.clone(),
            crate::sync::cycle::StoreInitialization::OpenStore {
                expected_store_root: store.root.clone(),
            },
            Some(crate::encryption::EncryptionService::from_key([42; 32])),
        )
        .await
        .expect("prepare effective-access owner sync")
        .initialize()
        .await
        .expect("initialize effective-access owner sync");
        components
            .add_circle_member(
                circle_id,
                crate::keys::public_key_hex(&member),
                crate::protocol::circle::CircleRole::Member,
            )
            .await
            .expect("add effective-access Circle member");

        let initial_pull = member_device
            .pull_store(member_store_dir)
            .await
            .expect("pull effective-access Circle activation")
            .1;
        assert!(initial_pull.held_positions.is_empty(), "{initial_pull:?}");

        Self {
            owner_database,
            owner_device: owner_store,
            member_device,
            owner,
            member,
            store,
            home,
            circle_id,
        }
    }
}

const EFFECTIVE_ACCESS_ROW_ID: &str = "01890a5d-ac96-774b-bcce-b302099c3f75";
const READD_EFFECTIVE_ACCESS_ROW_ID: &str = "01890a5d-ac96-774b-bcce-b302099c3f76";

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
    let fixture = EffectiveAccessFixture::create(
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
            .map(|circle| circle.name().expect("listed Circle is active").to_string())
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
    let fixture = EffectiveAccessFixture::create(
        "removed-member-effective-access",
        &member_database,
        &owner_store_dir,
        &member_store_dir,
    )
    .await;

    let first = fixture
        .publish_row(
            EFFECTIVE_ACCESS_ROW_ID,
            "visible before removal",
            "0000000002000-0000-owner",
        )
        .await;
    let first_pull = fixture
        .pull_member(&member_store_dir)
        .await
        .expect("pull pre-removal Circle row");
    assert!(first_pull.held_positions.is_empty(), "{first_pull:?}");
    assert_eq!(
        member_database
            .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
            .await
            .row
            .as_ref()
            .map(|row| row.1.as_str()),
        Some("visible before removal")
    );
    let hidden_before_removal = fixture
        .publish_row(
            EFFECTIVE_ACCESS_ROW_ID,
            "private immediately before removal",
            "0000000002500-0000-owner",
        )
        .await;
    let hidden_before_removal_commit = fixture.load_commit(&hidden_before_removal).await;
    let hidden_before_removal_package_slot =
        exact_circle_package_slot(&hidden_before_removal_commit);

    // The last Circle package the owner authors before the removal. Once the
    // removal is materialized the owner may no longer publish new Circle content
    // (the Circle is rotation-required), so this models the newest package the
    // removed member must still be pruned from.
    let late = fixture
        .publish_row(
            EFFECTIVE_ACCESS_ROW_ID,
            "private just before removal",
            "0000000002800-0000-owner",
        )
        .await;
    let late_commit = fixture.load_commit(&late).await;
    let late_package_slot = exact_circle_package_slot(&late_commit);

    let custody = crate::sync::test_helpers::TestCustody::default();
    custody.set_initial_key([42; 32]);
    fixture
        .store
        .remove_member(
            &fixture.owner_database,
            &fixture.owner,
            &crate::keys::public_key_hex(&fixture.member),
            &crate::encryption::EncryptionService::from_key([42; 32]),
            &custody,
        )
        .await
        .expect("remove effective-access Store member");
    let removal = fixture
        .owner_device
        .latest_local_store_position()
        .await
        .expect("load Store removal position")
        .expect("Store removal has a position");
    let latest_membership = fixture
        .member_device
        .membership()
        .await
        .expect("load current removed-member Store membership");
    assert!(!latest_membership
        .current_members()
        .iter()
        .any(|(member, _)| member == &crate::keys::public_key_hex(&fixture.member)));

    fixture.home.clear_exact_reads();
    member_database.fail_next_merge_materialization_at(
        crate::database::MergeMaterializationFailurePoint::SummaryMaterialization,
    );
    fixture
        .pull_member(&member_store_dir)
        .await
        .expect_err("injected transaction failure interrupts removed-member materialization");
    assert_eq!(
        member_database
            .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
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
        .home
        .remove_exact_object(&hidden_before_removal_package_slot);
    fixture.home.remove_exact_object(&late_package_slot);
    fixture.home.clear_exact_reads();
    let pull = fixture
        .pull_member(&member_store_dir)
        .await
        .expect("pull Store state after membership removal");
    assert!(pull.held_positions.is_empty(), "{pull:?}");
    assert!(!fixture
        .home
        .exact_reads()
        .contains(&hidden_before_removal_package_slot));
    assert!(!fixture.home.exact_reads().contains(&late_package_slot));
    let state = member_database
        .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
        .await;
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
        .test_sql(|database| database.circle_state_table_counts())
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

    let circle_id = fixture.circle_id;
    let member_pubkey = crate::keys::public_key_hex(&fixture.member);
    let owner_pubkey = crate::keys::public_key_hex(&fixture.owner);
    drop(fixture);
    std::thread::spawn(move || drop(member_database))
        .join()
        .expect("close effective-access member database");
    let reopened = open_scoped_replay_database_at(&member_path);
    let reopened_state = reopened
        .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
        .await;
    assert_eq!(reopened_state.row, None);
    assert_eq!(reopened_state.route, None);
    assert_eq!(
        reopened_state.mirror,
        Some((
            Some(circle_id.to_string()),
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
            &member_pubkey,
            std::collections::BTreeSet::from([owner_pubkey]),
        )
        .await
        .expect("list reopened Circles after Store membership removal")
        .is_empty());
    let reopened_public_circle_state: i64 = reopened
        .test_sql(|database| {
            database.table_row_count(crate::database::DatabaseTestTable::named(
                "circle_current_state",
            ))
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
    let fixture = EffectiveAccessFixture::create(
        "readded-member-effective-access",
        &member_database,
        &owner_store_dir,
        &member_store_dir,
    )
    .await;

    fixture
        .publish_row(
            EFFECTIVE_ACCESS_ROW_ID,
            "visible before removal",
            "0000000002000-0000-owner",
        )
        .await;
    let initial_pull = fixture
        .pull_member(&member_store_dir)
        .await
        .expect("pull Circle row before Store removal");
    assert!(initial_pull.held_positions.is_empty(), "{initial_pull:?}");

    // A Circle package the owner authors before the removal that the member has
    // not yet pulled. The removal pull applies it under the removed membership,
    // exercising the prune of the member's Circle rows.
    let pre_removal = fixture
        .publish_row(
            EFFECTIVE_ACCESS_ROW_ID,
            "private just before removal",
            "0000000002500-0000-owner",
        )
        .await;
    let pre_removal_commit = fixture.load_commit(&pre_removal).await;
    let pre_removal_package_slot = exact_circle_package_slot(&pre_removal_commit);

    let custody = crate::sync::test_helpers::TestCustody::default();
    custody.set_initial_key([42; 32]);
    fixture
        .store
        .remove_member(
            &fixture.owner_database,
            &fixture.owner,
            &crate::keys::public_key_hex(&fixture.member),
            &crate::encryption::EncryptionService::from_key([42; 32]),
            &custody,
        )
        .await
        .expect("remove Store member before re-add");
    // Once the removal is materialized the owner can no longer publish new
    // Circle content (the Circle is rotation-required until it is closed and
    // rotated), so no package is authored during the removed interval; the
    // re-add restores access to the Circle's current state alone.
    fixture.home.clear_exact_reads();
    let removal_pull = fixture
        .pull_member(&member_store_dir)
        .await
        .expect("pull Store membership removal");
    assert!(removal_pull.held_positions.is_empty(), "{removal_pull:?}");
    assert!(
        !fixture
            .home
            .exact_reads()
            .contains(&pre_removal_package_slot),
        "a removed member does not fetch the unpulled pre-removal Circle package"
    );
    assert_eq!(
        member_database
            .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
            .await
            .row,
        None
    );

    fixture
        .store
        .invite_member(
            &fixture.owner_database,
            &fixture.owner,
            &crate::keys::public_key_hex(&fixture.member),
            None,
            crate::protocol::membership::MemberRole::Member,
            &crate::encryption::EncryptionService::from_key([42; 32]),
            "Effective Access Store",
        )
        .await
        .expect("re-add effective-access Store member");
    let rotated_store_encryption = crate::encryption::EncryptionService::from(
        custody
            .unlock()
            .expect("load rotated Store keyring")
            .expect("scoped Store has an established keyring"),
    );
    fixture
        .member_device
        .adopt_key_rotation(&rotated_store_encryption, &custody)
        .expect("adopt the Store key wrapped by the re-add");
    let owner_store = fixture
        .store
        .bind_device(&fixture.owner_database, &fixture.owner)
        .await
        .expect("load owner Store for Circle successor");
    owner_store
        .rename_circle(
            "0000000004000-0000-owner",
            fixture.circle_id,
            "Effective Access Restored",
        )
        .await
        .expect("publish Circle successor after Store re-add");
    fixture
        .publish_row(
            READD_EFFECTIVE_ACCESS_ROW_ID,
            "visible after re-add",
            "0000000005000-0000-owner",
        )
        .await;

    fixture.home.clear_exact_reads();
    let readd_pull = fixture
        .pull_member(&member_store_dir)
        .await
        .expect("pull Store re-add and Circle successor");
    assert!(readd_pull.held_positions.is_empty(), "{readd_pull:?}");
    assert_eq!(
        member_database
            .scoped_routing_state_for_test(READD_EFFECTIVE_ACCESS_ROW_ID)
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
            .map(|circle| circle.name().expect("listed Circle is active").to_string())
            .collect::<Vec<_>>(),
        vec!["Effective Access Restored".to_string()]
    );
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
        let home = crate::sync::test_helpers::test_cloud_home();
        let store = crate::sync::test_helpers::TestStore::create(
            &founder,
            conflict.store_id(),
            identity.clone(),
            home.clone(),
        )
        .await
        .expect("create scoped replay Store");
        home.sort_listings();
        store
            .open_into(&founder)
            .await
            .expect("open founder scoped replay Store");
        let loaded = store
            .bind_device(&founder, &identity)
            .await
            .expect("load founder Store operations");
        let first_circle = loaded
            .create_circle("0000000001000-0000-owner", "First")
            .await
            .expect("create first routing-conflict Circle");
        let second_circle = loaded
            .create_circle("0000000001001-0000-owner", "Second")
            .await
            .expect("create second routing-conflict Circle");
        founder
            .run_scoped_host_write_for_test(format!(
                "INSERT INTO notes VALUES (
                     '{ROW_ID}', NULL, 'base', '0000000002000-0000-base'
                 );"
            ))
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
            store
                .activate_joined_device(&founder, participant, &identity, "2026-07-22T00:00:00Z")
                .await
                .expect("activate scoped replay device");
        }
        let (_first_temp, first_dir) = crate::sync::test_helpers::temp_store_dir();
        let (_second_temp, second_dir) = crate::sync::test_helpers::temp_store_dir();
        let (_progressive_temp, progressive_dir) = crate::sync::test_helpers::temp_store_dir();
        let (_complete_temp, complete_dir) = crate::sync::test_helpers::temp_store_dir();
        let first_writer_device = store
            .bind_device(&first_writer, &identity)
            .await
            .expect("bind first routing-conflict writer");
        let second_writer_device = store
            .bind_device(&second_writer, &identity)
            .await
            .expect("bind second routing-conflict writer");
        let progressive_device = store
            .bind_device(&progressive, &identity)
            .await
            .expect("bind progressive routing-conflict reader");
        let complete_device = store
            .bind_device(&complete, &identity)
            .await
            .expect("bind complete routing-conflict reader");
        for (device, directory) in [
            (&first_writer_device, &first_dir),
            (&second_writer_device, &second_dir),
            (&progressive_device, &progressive_dir),
            (&complete_device, &complete_dir),
        ] {
            let pulled = device
                .pull_store(directory)
                .await
                .expect("pull scoped replay Store")
                .1;
            assert!(pulled.held_positions.is_empty(), "{conflict:?}: {pulled:?}");
        }

        let mut writers = [
            (
                first_writer_device
                    .announcement_stream_id_for_test()
                    .await
                    .expect("derive first routing-conflict writer stream"),
                &first_writer,
                &first_dir,
            ),
            (
                second_writer_device
                    .announcement_stream_id_for_test()
                    .await
                    .expect("derive second routing-conflict writer stream"),
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

        canonical_later
            .run_scoped_host_write_for_test(canonical_later_sql)
            .await;
        assert!(store
            .publish_pending(canonical_later, canonical_later_dir)
            .await
            .expect("publish canonical-later routing conflict"));
        let first_pull = progressive_device
            .pull_store(&progressive_dir)
            .await
            .expect("pull canonical-later routing conflict")
            .1;
        assert!(
            first_pull.held_positions.is_empty(),
            "{conflict:?}: {first_pull:?}"
        );

        canonical_earlier
            .run_scoped_host_write_for_test(canonical_earlier_sql)
            .await;
        assert!(store
            .publish_pending(canonical_earlier, canonical_earlier_dir)
            .await
            .expect("publish canonical-earlier routing conflict"));
        let progressive_pull = progressive_device
            .pull_store(&progressive_dir)
            .await
            .expect("pull complete progressive routing history")
            .1;
        let complete_pull = complete_device
            .pull_store(&complete_dir)
            .await
            .expect("pull complete routing history")
            .1;
        assert!(
            progressive_pull.held_positions.is_empty(),
            "{conflict:?}: {progressive_pull:?}"
        );
        assert!(
            complete_pull.held_positions.is_empty(),
            "{conflict:?}: {complete_pull:?}"
        );

        let progressive_state = progressive.scoped_routing_state_for_test(ROW_ID).await;
        let complete_state = complete.scoped_routing_state_for_test(ROW_ID).await;
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
                    crate::database::ScopedRoutingStateForTest {
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

#[tokio::test]
async fn merge_outbound_projects_membership_to_the_commits_predecessors() {
    let founder = crate::sync::test_helpers::user_keypair_from_seed([42; 32]);
    let founder_db = crate::sync::test_helpers::open_test_db();
    let store = crate::sync::test_helpers::TestStore::create(
        &founder_db,
        "causal-membership-proof",
        founder.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Merge Store");
    let candidate = crate::sync::test_helpers::user_keypair_from_seed([43; 32]);
    let encryption = crate::encryption::EncryptionService::from_key([73; 32]);
    store
        .invite_member(
            &founder_db,
            &founder,
            &crate::sync::test_helpers::pubkey_hex(&candidate),
            None,
            crate::protocol::membership::MemberRole::Member,
            &encryption,
            "Causal Membership Proof",
        )
        .await
        .expect("invite exact Store member");

    let candidate_db = crate::sync::test_helpers::open_test_db();
    store
        .activate_joined_device(
            &founder_db,
            &candidate_db,
            &candidate,
            "2026-07-21T00:00:00Z",
        )
        .await
        .expect("activate candidate device");
    store
        .promote_active_member_fixture(
            &founder_db,
            &candidate_db,
            &founder,
            &candidate,
            &encryption,
        )
        .await
        .expect("promote candidate Owner");
    let candidate_device = store
        .bind_device(&candidate_db, &candidate)
        .await
        .expect("bind candidate Owner");
    let mut candidate_writer = candidate_device
        .authorize_writer()
        .await
        .expect("authorize candidate Owner");
    let candidate_pull = candidate_writer
        .pull(None)
        .await
        .expect("pull candidate Owner to the common Store history");
    assert!(candidate_pull.held_positions.is_empty());

    let earlier_db = &candidate_db;
    let earlier_owner = &candidate;
    let later_db = &founder_db;
    let later_owner = &founder;

    let earlier_device = store
        .bind_device(earlier_db, earlier_owner)
        .await
        .expect("bind earlier Owner device");
    let mut earlier_writer = earlier_device
        .authorize_writer()
        .await
        .expect("authorize earlier Owner device");
    let _rotated = earlier_writer
        .revoke_member_without_local_adoption_for_test(
            &crate::sync::test_helpers::pubkey_hex(&candidate),
            "0000000003000-0000-causal-proof",
            &encryption,
            &crate::storage::PendingRotation::none(),
        )
        .await
        .expect("publish traversal-earlier Owner removal control");
    let earlier_control = earlier_device
        .latest_local_store_position()
        .await
        .expect("load earlier Owner position")
        .expect("earlier Owner published the membership control");
    let earlier_value = earlier_device
        .load_commit_for_test(&earlier_control)
        .await
        .expect("load traversal-earlier control");
    let Some(crate::protocol::store_commit::StoreControl { transition }) =
        earlier_value.value().control()
    else {
        panic!("earlier Owner position is not a Merge membership control");
    };

    let changeset = crate::sync::test_helpers::open_test_db()
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('causal-proof-row', 'causal proof', NULL, \
                   '0000000001000-0000-causal-proof', '2026-07-21')",
        ])
        .await;
    crate::database::StoreDatabase::new(later_db)
        .enqueue_store_changeset_for_test(changeset)
        .await
        .expect("enqueue later concurrent write");
    let later_membership = store
        .bind_device(later_db, later_owner)
        .await
        .expect("bind later Owner Store")
        .membership()
        .await
        .expect("load later Owner membership");
    let caller_membership = &later_membership;
    let earlier_head_ref = caller_membership
        .head_refs()
        .iter()
        .find(|head| head.coord == transition.body.entry.coord)
        .expect("caller membership contains the concurrent control")
        .clone();
    let earlier_head = earlier_device
        .load_membership_head_for_test(&earlier_head_ref)
        .await
        .expect("load concurrent membership head");
    let later_device = store
        .bind_device(later_db, later_owner)
        .await
        .expect("bind later Owner device");
    let mut later_writer = later_device
        .authorize_writer()
        .await
        .expect("authorize later Owner writer");
    assert!(later_writer
        .prepare_pending_store_write()
        .await
        .expect("prepare later concurrent write"));
    later_writer
        .drain_store_writes()
        .await
        .expect("publish later concurrent write");
    let later_commit = later_device
        .latest_local_store_position()
        .await
        .expect("load later Owner position")
        .expect("later Owner published the data commit");

    let later_value = later_device
        .load_commit_for_test(&later_commit)
        .await
        .expect("load later concurrent commit");
    let later_predecessors = commit_predecessor_references(later_value.value());
    assert!(!later_predecessors.contains(&earlier_control));
    let signed_membership = &later_value.value().membership_state;
    assert!(!signed_membership
        .heads
        .iter()
        .any(|head| head.coord == transition.body.entry.coord));

    let later_prefix = later_device
        .verified_merge_membership_prefix_for_test(
            [later_commit.clone(), earlier_control.clone()],
            later_predecessors,
        )
        .await
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
    let signer = crate::keys::UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &source,
        "exact-predecessor-test",
        signer.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact predecessor test Store");
    let changeset = crate::sync::test_helpers::open_test_db()
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('gap-row', 'gap', NULL, '0000000001000-0000-gap', '2026-01-01')",
        ])
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
    let founder_authority = store
        .founder_device_authority()
        .await
        .expect("load founder authority");
    let source_device = store
        .bind_device(&source, &signer)
        .await
        .expect("bind source Store device");
    let commit = source_device
        .load_commit_for_test(&third)
        .await
        .expect("load third exact commit");
    assert_eq!(commit.author(), founder_authority.registration());
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
    let target_device = store
        .open_into(&target)
        .await
        .expect("open target Store device");

    let readiness = target_device
        .pull_readiness_for_test(
            &coverage,
            &frontier,
            &device_state,
            &[],
            &third,
            commit.value(),
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

#[tokio::test]
async fn deleting_a_circle_prunes_receivers_and_refuses_new_writes() {
    let member_temp = tempfile::tempdir().expect("create effective-access database directory");
    let member_path = member_temp.path().join("member.sqlite3");
    let member_database = open_scoped_replay_database_at(&member_path);
    let (_owner_temp, owner_store_dir) = crate::sync::test_helpers::temp_store_dir();
    let (_member_store_temp, member_store_dir) = crate::sync::test_helpers::temp_store_dir();
    let fixture = EffectiveAccessFixture::create(
        "delete-circle-prunes",
        &member_database,
        &owner_store_dir,
        &member_store_dir,
    )
    .await;

    fixture
        .publish_row(
            EFFECTIVE_ACCESS_ROW_ID,
            "before deletion",
            "0000000002000-0000-owner",
        )
        .await;
    fixture
        .pull_member(&member_store_dir)
        .await
        .expect("member pulls the pre-deletion Circle row");
    assert_eq!(
        member_database
            .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
            .await
            .row
            .as_ref()
            .map(|row| row.1.as_str()),
        Some("before deletion")
    );

    // The Circle row carries a blob. Both the owner (host author) and the member
    // (recipient) hold a `row_blob_locators` binding for it, which the deletion
    // must prune along with the row.
    fixture
        .owner_database
        .bind_circle_row_blob_for_test(EFFECTIVE_ACCESS_ROW_ID)
        .await;
    member_database
        .bind_circle_row_blob_for_test(EFFECTIVE_ACCESS_ROW_ID)
        .await;
    assert_eq!(
        fixture
            .owner_database
            .row_blob_binding_count_for_test(EFFECTIVE_ACCESS_ROW_ID)
            .await,
        1,
        "the owner holds the Circle row's blob binding before deletion"
    );
    assert_eq!(
        member_database
            .row_blob_binding_count_for_test(EFFECTIVE_ACCESS_ROW_ID)
            .await,
        1,
        "the member holds the Circle row's blob binding before deletion"
    );

    // A pre-deletion Circle package the member has not yet pulled.
    fixture
        .publish_row(
            READD_EFFECTIVE_ACCESS_ROW_ID,
            "private before deletion",
            "0000000002500-0000-owner",
        )
        .await;

    fixture.delete_circle().await;

    // The owner converges to Deleted: rows pruned, control spine retained.
    assert!(fixture
        .owner_database
        .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
        .await
        .row
        .is_none());
    assert!(
        fixture
            .owner_database
            .circle_control_activation_count_for_test(fixture.circle_id)
            .await
            > 0,
        "the owner retains the control authority spine after deletion"
    );
    assert_eq!(
        fixture
            .owner_database
            .row_blob_binding_count_for_test(EFFECTIVE_ACCESS_ROW_ID)
            .await,
        0,
        "the owner's Circle row blob binding is pruned on deletion"
    );
    let owner_circles = StoreDatabase::new(&fixture.owner_database)
        .get_circles(
            &crate::keys::public_key_hex(&fixture.owner),
            fixture.effective_access_members(),
        )
        .await
        .expect("list owner Circles after deletion");
    assert!(
        matches!(owner_circles.as_slice(),
            [crate::protocol::circle::CircleInfo::Deleted { id }] if *id == fixture.circle_id),
        "the owner reports the Circle as deleted: {owner_circles:?}"
    );

    // The member pulls the deletion (and the late pre-deletion package) and
    // converges identically: rows, routes, and the late package are gone.
    fixture
        .pull_member(&member_store_dir)
        .await
        .expect("member pulls the deletion");
    let pruned = member_database
        .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
        .await;
    assert!(pruned.row.is_none(), "the member's Circle row is pruned");
    assert!(
        pruned.route.is_none(),
        "the member's private route is pruned"
    );
    assert!(
        member_database
            .scoped_routing_state_for_test(READD_EFFECTIVE_ACCESS_ROW_ID)
            .await
            .row
            .is_none(),
        "the late pre-deletion package is omitted"
    );
    assert!(
        member_database
            .circle_control_activation_count_for_test(fixture.circle_id)
            .await
            > 0,
        "the member retains the control authority spine after deletion"
    );
    assert_eq!(
        member_database
            .row_blob_binding_count_for_test(EFFECTIVE_ACCESS_ROW_ID)
            .await,
        0,
        "the member's Circle row blob binding is pruned on deletion"
    );
    let member_circles = StoreDatabase::new(&member_database)
        .get_circles(
            &crate::keys::public_key_hex(&fixture.member),
            fixture.effective_access_members(),
        )
        .await
        .expect("list member Circles after deletion");
    assert!(
        matches!(member_circles.as_slice(),
            [crate::protocol::circle::CircleInfo::Deleted { id }] if *id == fixture.circle_id),
        "the member reports the Circle as deleted: {member_circles:?}"
    );

    // A new host write destined to the deleted Circle is refused at capture.
    let circle_id = fixture.circle_id;
    let error = StoreDatabase::new(&fixture.owner_database)
        .run_host_store_write_for_test(
            Some(crate::encryption::EncryptionService::from_key([42; 32])),
            None,
            move |transaction| {
                transaction
                    .execute_batch(&format!(
                        "INSERT INTO notes (id, audience, body, _updated_at)
                             VALUES ('01890a5d-ac96-774b-bcce-b302099c3f99', '{circle_id}',
                                     'after deletion', '0000000003000-0000-owner');"
                    ))
                    .map_err(DbError::from)
            },
        )
        .await
        .expect_err("a host write into a deleted Circle is refused");
    assert!(error.to_string().contains("deleted"), "{error}");
}

#[tokio::test]
async fn a_non_owner_is_refused_circle_deletion() {
    let member_temp = tempfile::tempdir().expect("create effective-access database directory");
    let member_path = member_temp.path().join("member.sqlite3");
    let member_database = open_scoped_replay_database_at(&member_path);
    let (_owner_temp, owner_store_dir) = crate::sync::test_helpers::temp_store_dir();
    let (_member_store_temp, member_store_dir) = crate::sync::test_helpers::temp_store_dir();
    let fixture = EffectiveAccessFixture::create(
        "delete-non-owner",
        &member_database,
        &owner_store_dir,
        &member_store_dir,
    )
    .await;

    // The member holds active access but is not the Circle Owner.
    let member_store = fixture
        .store
        .bind_device(&member_database, &fixture.member)
        .await
        .expect("load member Store");
    let refused = member_store
        .delete_circle(fixture.circle_id)
        .await
        .expect_err("a non-owner cannot delete a Circle");
    assert!(
        format!("{refused}").to_lowercase().contains("owner"),
        "the refusal names the missing Owner authority: {refused:?}"
    );

    // The Circle is untouched — still active on the member.
    let circles = StoreDatabase::new(&member_database)
        .get_circles(
            &crate::keys::public_key_hex(&fixture.member),
            fixture.effective_access_members(),
        )
        .await
        .expect("list member Circles after the refused deletion");
    assert!(
        matches!(circles.as_slice(),
            [crate::protocol::circle::CircleInfo::Active { id, .. }] if *id == fixture.circle_id),
        "the refused deletion leaves the Circle active: {circles:?}"
    );
}

#[tokio::test]
async fn a_pre_deletion_package_applied_then_pruned_converges_with_the_omitted_order() {
    let member_temp = tempfile::tempdir().expect("create effective-access database directory");
    let member_path = member_temp.path().join("member.sqlite3");
    let member_database = open_scoped_replay_database_at(&member_path);
    let (_owner_temp, owner_store_dir) = crate::sync::test_helpers::temp_store_dir();
    let (_member_store_temp, member_store_dir) = crate::sync::test_helpers::temp_store_dir();
    let fixture = EffectiveAccessFixture::create(
        "delete-two-order",
        &member_database,
        &owner_store_dir,
        &member_store_dir,
    )
    .await;

    // First arrival order: the member applies a pre-deletion package before the
    // deletion is authored, so the row is materialized locally.
    fixture
        .publish_row(
            EFFECTIVE_ACCESS_ROW_ID,
            "applied then pruned",
            "0000000002000-0000-owner",
        )
        .await;
    fixture
        .pull_member(&member_store_dir)
        .await
        .expect("member applies the pre-deletion package");
    assert_eq!(
        member_database
            .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
            .await
            .row
            .as_ref()
            .map(|row| row.1.as_str()),
        Some("applied then pruned")
    );

    // The deletion arrives later and prunes the already-applied row. The
    // terminal state matches the order where the package arrives together with
    // (or after) the deletion and is omitted: rows gone, Circle deleted.
    fixture.delete_circle().await;
    fixture
        .pull_member(&member_store_dir)
        .await
        .expect("member pulls the later deletion");
    assert!(member_database
        .scoped_routing_state_for_test(EFFECTIVE_ACCESS_ROW_ID)
        .await
        .row
        .is_none());
    let circles = StoreDatabase::new(&member_database)
        .get_circles(
            &crate::keys::public_key_hex(&fixture.member),
            fixture.effective_access_members(),
        )
        .await
        .expect("list member Circles after the later deletion");
    assert!(
        matches!(circles.as_slice(),
            [crate::protocol::circle::CircleInfo::Deleted { id }] if *id == fixture.circle_id),
        "the applied-then-pruned order converges to deleted: {circles:?}"
    );
}

#[tokio::test]
async fn a_deleted_circles_authority_spine_retains_historical_controls() {
    let member_temp = tempfile::tempdir().expect("create effective-access database directory");
    let member_path = member_temp.path().join("member.sqlite3");
    let member_database = open_scoped_replay_database_at(&member_path);
    let (_owner_temp, owner_store_dir) = crate::sync::test_helpers::temp_store_dir();
    let (_member_store_temp, member_store_dir) = crate::sync::test_helpers::temp_store_dir();
    let fixture = EffectiveAccessFixture::create(
        "delete-historical-spine",
        &member_database,
        &owner_store_dir,
        &member_store_dir,
    )
    .await;

    // A real Circle package the owner authored before deletion: its encrypted
    // payload is the retained historical package the authority spine must still
    // decrypt and verify against the frozen epoch's key.
    let package_commit_ref = fixture
        .publish_row(
            EFFECTIVE_ACCESS_ROW_ID,
            "authored before deletion",
            "0000000002000-0000-owner",
        )
        .await;
    let package_commit = fixture.load_commit(&package_commit_ref).await;
    let [package_ref] = package_commit.circle_packages() else {
        panic!("the pre-deletion Store commit carries one Circle package");
    };
    let package_ref = package_ref.clone();
    let historical_control = package_ref.control.clone();

    fixture.delete_circle().await;

    // Live materialization is gone, but the authority spine still resolves the
    // historical control the package was signed under.
    assert!(StoreDatabase::new(&fixture.owner_database)
        .circle_is_deleted(fixture.circle_id)
        .await
        .expect("read deleted state"));
    let retained = fixture
        .owner_device
        .verified_circle_activation_for_test(fixture.circle_id, historical_control.clone())
        .await
        .expect("query the retained activation")
        .expect("the historical control is retained after deletion");
    assert_eq!(retained.control.coord, historical_control);
    assert!(
        retained.control.verify(),
        "the retained historical control still verifies against the authority spine"
    );

    // The historical-keyring path reconstructs the frozen epoch's key from the
    // retained authority spine — the caches are gone, but the retained replay
    // still yields the package access. Decrypting and verifying the actual
    // retained package with it proves the spine keeps historical commits
    // readable after deletion.
    let access = retained
        .epoch_access()
        .expect("reconstruct retained package access after deletion")
        .expect("the retained historical activation carries package access");
    let decrypted = fixture
        .owner_device
        .open_circle_package_for_test(&access, &package_commit, &package_ref)
        .await
        .expect("decrypt and verify the retained package against the frozen epoch key");
    assert!(
        !decrypted.is_empty(),
        "the retained package decrypts to its signed payload"
    );
}
