use super::*;
use crate::sync::store::pull::{
    insert_latest_acknowledgement, merge_retained_merge_history, readiness,
    verified_merge_membership_prefix, verify_merge_history_refs, Readiness,
    VerifiedMergePrefixHeadStatus,
};
use crate::sync::store_commit::{OpenedRetainedMergeHistorySummary, OwnerRecoveryNodeRef};
use rusqlite::OptionalExtension;

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

fn open_scoped_replay_database() -> Database {
    crate::sync::test_helpers::open_test_db_schema(
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
