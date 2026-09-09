use super::{store_database, HistoryPublisher, PublishedHistory};
use crate::sync::test_helpers::TestStore;
use coven_keys::keys::UserKeypair;

#[path = "snapshot_device_history_tests.rs"]
mod snapshot_device_history_tests;

/// Two devices, each acknowledging the other's commits.
///
/// The single-device fixture cannot reach this: a lone device publishes an
/// acknowledgement only now and then, so almost none of its retained commits
/// activate one. With two devices nearly every commit does, and the retained
/// path's per-commit cost lives behind exactly that — which is why a fix
/// measured only against `PublishedHistory` looked complete and was not.
struct AcknowledgedHistory {
    db: coven_database::Database,
    home: std::sync::Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
    device: crate::sync::test_helpers::TestDevice,
    peer_db: coven_database::Database,
    peer: crate::sync::test_helpers::TestDevice,
    /// Held for the fixture's life, not read: the Store outlives every device
    /// bound against it.
    _store: std::sync::Arc<crate::sync::test_helpers::TestStore>,
}

impl AcknowledgedHistory {
    async fn publish(rounds: u64) -> Self {
        let founder = UserKeypair::generate();
        let member = UserKeypair::generate();
        let member_pubkey = hex::encode(member.public_key());
        let store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(store_dir.clone());
        let home = crate::sync::test_helpers::test_cloud_home();
        let (store, _storage) = TestStore::create_with_connection(
            &db,
            store_dir.clone(),
            "acknowledged-history",
            founder.clone(),
            home.clone(),
        )
        .await
        .expect("create Merge Store");
        let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
        store
            .admit_member(
                &db,
                store_dir.clone(),
                &founder,
                &member_pubkey,
                None,
                coven_protocol::membership::MemberRole::Member,
                &encryption,
                "Acknowledged Store",
            )
            .await
            .expect("admit the peer as a Member");
        let peer_store_dir = crate::sync::test_helpers::test_store_dir();
        let peer_db = crate::sync::test_helpers::open_test_db(peer_store_dir.clone());
        let peer = store
            .activate_joined_device(
                &db,
                store_dir.clone(),
                &peer_db,
                peer_store_dir.clone(),
                &member,
                "2026-03-01T00:00:45Z",
            )
            .await
            .expect("activate the peer device");
        let device = store
            .bind_device_in(&db, store_dir.clone(), &founder)
            .await
            .expect("bind the local device");

        let fixture = Self {
            db,
            home,
            device,
            peer_db,
            peer,
            _store: store,
        };
        for round in 1..=rounds {
            fixture.publish_round(round).await;
        }
        fixture
    }

    /// One note from each side, with a cycle each way afterwards so both devices
    /// see and acknowledge the other's commit.
    async fn publish_round(&self, round: u64) {
        HistoryPublisher::new(&self.db, &self.device)
            .publish_note(round * 2 - 1)
            .await;
        self.peer
            .run_cycle(None)
            .await
            .expect("peer pulls and acknowledges");
        HistoryPublisher::new(&self.peer_db, &self.peer)
            .publish_note(round * 2)
            .await;
        self.device
            .run_cycle(None)
            .await
            .expect("local device pulls and acknowledges");
    }

    async fn retained_history(&self) -> Vec<coven_database::OwnedVerifiedMergeMaterialization> {
        self.device
            .retained_merge_replay_inputs_for_test()
            .await
            .expect("load retained verified Merge history")
    }

    /// Publish a Store snapshot over the device's current frontier.
    async fn publish_snapshot_now(&self) {
        let image_dir = tempfile::tempdir().expect("snapshot image dir");
        let image = coven_database::StoreDatabase::new(&self.db)
            .capture_snapshot_image_for_test(
                self._store.root().clone(),
                image_dir.path().to_path_buf(),
                None,
            )
            .await
            .expect("capture a snapshot image");
        let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
            coven_database::StoreDatabase::new(&self.db)
                .materialized_frontier()
                .await
                .expect("materialized frontier"),
        )
        .expect("frontier");
        self.device
            .publish_snapshot(image, coverage)
            .await
            .expect("publish the snapshot");
    }

    /// Provider operations a cycle with nothing new to do asks for — every
    /// call, not only the reads, which is the unit the cycle log budgets in.
    async fn settled_cycle_requests(&self) -> u64 {
        self.device.run_cycle(None).await.expect("settle the cycle");
        self.device
            .run_cycle(None)
            .await
            .expect("settle the acknowledgement it just made");
        self.home.clear_exact_reads();
        self.home.clear_exact_listings();
        let before = self._store.provider_requests_issued();
        self.device
            .run_cycle(None)
            .await
            .expect("run a settled cycle");
        let after = self._store.provider_requests_issued();
        after - before
    }

    /// Provider reads made by a cycle that has nothing new to do. The first
    /// settling cycle publishes this device's own acknowledgement of what it
    /// just pulled; the one measured after that is the steady state.
    async fn settled_cycle_reads(&self) -> usize {
        self.device.run_cycle(None).await.expect("settle the cycle");
        self.home.clear_exact_reads();
        self.device
            .run_cycle(None)
            .await
            .expect("run a settled cycle");
        self.home.exact_reads().len()
    }
    /// Publication requests excluding exact checks of inherited deletion obligations.
    async fn snapshot_publication_requests_without_retirement(&self) -> u64 {
        let database = store_database(&self.db);
        let baseline = database
            .installed_replay_baseline()
            .await
            .expect("installed baseline");
        let snapshot = baseline
            .snapshot()
            .expect("the fixture stands on its Join snapshot");
        let publication = database
            .store_current_publication()
            .await
            .expect("accepted publication");
        assert_eq!(
            publication
                .record()
                .latest_snapshot()
                .map(|accepted| &accepted.snapshot),
            Some(&snapshot.reference),
            "the measured operation starts from the latest accepted snapshot",
        );
        let retirement = &snapshot.meta.history_summary.reclaim;
        let retirement_slots = retirement
            .snapshots
            .values()
            .flat_map(|owned| owned.objects())
            .chain(retirement.publications.values().map(|entry| &entry.object))
            .map(|object| object.slot().clone())
            .collect::<std::collections::BTreeSet<_>>();
        let image_dir = tempfile::tempdir().expect("snapshot image dir");
        let image = coven_database::StoreDatabase::new(&self.db)
            .capture_snapshot_image_for_test(
                self.device.store_root().clone(),
                image_dir.path().to_path_buf(),
                None,
            )
            .await
            .expect("capture a snapshot image");
        let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
            coven_database::StoreDatabase::new(&self.db)
                .materialized_frontier()
                .await
                .expect("materialized frontier"),
        )
        .expect("frontier");
        self.home.clear_exact_reads();
        let before = self._store.provider_requests_issued();
        self.device
            .publish_snapshot(image, coverage)
            .await
            .expect("publish the snapshot");
        let after = self._store.provider_requests_issued();
        let issued = after - before;
        let reads = self.home.exact_reads();
        // Preparation confirms absence, then candidate verification independently
        // checks every omitted obligation. No other historical reads are exempt.
        for slot in &retirement_slots {
            assert_eq!(
                reads.iter().filter(|read| *read == slot).count(),
                2,
                "a settled deletion obligation is checked once at each boundary: {slot:?}",
            );
        }
        issued
            .checked_sub(2 * retirement_slots.len() as u64)
            .expect("retirement checks are counted provider operations")
    }

    /// Let both devices consume the accepted snapshot and each other's
    /// pending acknowledgement and cleanup work before measuring another operation.
    async fn settle_onto_the_published_snapshot(&self) {
        self.device.run_cycle(None).await.expect("settle the cycle");
        self.peer.run_cycle(None).await.expect("the peer settles");
        self.device
            .run_cycle(None)
            .await
            .expect("stand on the snapshot just acknowledged");
        self.peer
            .run_cycle(None)
            .await
            .expect("the peer stands on the snapshot it acknowledged");
        self.device
            .run_cycle(None)
            .await
            .expect("the device observes the peer acknowledgement");
    }
}

/// The across-cycle claim, on the history shape that actually occurs in the
/// field: a device that has already verified two-device history with
/// acknowledgement evidence reaches the provider for none of it again.
#[tokio::test]
async fn repeat_cycles_over_acknowledged_history_read_none_of_it() {
    let fixture = AcknowledgedHistory::publish(4).await;
    let retained = fixture.retained_history().await;
    assert!(
        retained
            .iter()
            .filter(|entry| entry.history_evidence().acknowledgement.is_some())
            .count()
            >= 2,
        "the fixture must retain commits that activate acknowledgements, or it \
         cannot exercise the ack path at all",
    );
    let retained_slots = retained
        .iter()
        .flat_map(|entry| {
            let mut slots = vec![
                entry.commit_ref().object.slot().clone(),
                entry
                    .acceptance()
                    .exact_publication()
                    .expect("uncompacted history has exact acceptance")
                    .reference()
                    .object
                    .slot()
                    .clone(),
            ];
            if let Some(acknowledgement) = &entry.history_evidence().acknowledgement {
                slots.push(acknowledgement.acknowledgement().0.object.slot().clone());
            }
            slots
        })
        .collect::<Vec<_>>();

    for cycle in 1..=3 {
        fixture.home.clear_exact_reads();
        fixture
            .device
            .run_cycle(None)
            .await
            .expect("pull acknowledged retained history");
        let reread = fixture
            .home
            .exact_reads()
            .into_iter()
            .filter(|slot| retained_slots.contains(slot))
            .collect::<Vec<_>>();
        assert!(
            reread.is_empty(),
            "cycle {cycle} reread {} retained commit/publication/acknowledgement objects it had \
             already verified: {reread:?}",
            reread.len(),
        );
    }
}

/// The assertion above names the object kinds a two-device history retains, so
/// it only catches a re-read of something already thought of. This one does not
/// look at kinds at all: whatever a settled cycle reads, reading it must not
/// depend on how much history the device has behind it. A per-commit cost of any
/// shape shows up here.
#[tokio::test]
async fn acknowledged_history_depth_does_not_change_what_a_settled_cycle_reads() {
    let shallow = AcknowledgedHistory::publish(2)
        .await
        .settled_cycle_reads()
        .await;
    let deep = AcknowledgedHistory::publish(6)
        .await
        .settled_cycle_reads()
        .await;

    assert_eq!(
        shallow, deep,
        "a settled cycle's provider reads grew with two-device history depth: \
         two rounds read {shallow}, six read {deep}",
    );
}

/// A settled cycle checks for new accepted work without reading its history
/// again. Accepted membership controls come from retained authority rather than
/// a separate discovery walk over membership streams.
///
/// The number is exact on purpose. A budget written as "not too many" is one
/// nobody notices doubling.
#[tokio::test]
async fn a_settled_cycle_asks_the_provider_only_for_what_could_be_new() {
    let settled = AcknowledgedHistory::publish(4)
        .await
        .settled_cycle_requests()
        .await;

    // Five exact authority reads, two versioned reads, one provider
    // binding and one listing. The retained history adds no requests.
    assert_eq!(
        settled, 9,
        "a settled two-device cycle asked the provider for {settled} operations",
    );
}

/// And the budget is a property of the store's shape, not of how much has
/// happened in it.
#[tokio::test]
async fn history_depth_does_not_change_what_a_settled_cycle_asks_for() {
    let shallow = AcknowledgedHistory::publish(2)
        .await
        .settled_cycle_requests()
        .await;
    let deep = AcknowledgedHistory::publish(6)
        .await
        .settled_cycle_requests()
        .await;

    assert_eq!(
        shallow, deep,
        "a settled cycle's provider operations grew with history depth: two \
         rounds asked for {shallow}, six asked for {deep}",
    );
}

#[tokio::test]
async fn settled_authorization_uses_accepted_history_without_listing_membership_streams() {
    let fixture = AcknowledgedHistory::publish(2).await;
    fixture.settled_cycle_requests().await;
    fixture.home.clear_exact_listings();
    fixture
        .device
        .run_cycle(None)
        .await
        .expect("authorize and run the settled Store cycle");
    assert_eq!(
        fixture.home.exact_listed_prefixes(),
        Vec::<String>::new(),
        "accepted Store history already identifies its membership controls",
    );
}

/// A Store snapshot changes the accepted publication boundary without changing
/// the Store cut or device state that a standing acknowledgement asserts.
#[tokio::test]
async fn snapshot_publication_does_not_repeat_a_standing_store_acknowledgement() {
    for rounds in [2, 6] {
        let fixture = AcknowledgedHistory::publish(rounds).await;
        let database = store_database(&fixture.db);
        for generation in 0..5 {
            fixture.settle_onto_the_published_snapshot().await;
            let before = database
                .latest_local_store_ack()
                .await
                .expect("read the standing acknowledgement")
                .expect("the device has acknowledged accepted work");
            assert!(before.standing.is_some());
            let previous = database
                .store_current_publication()
                .await
                .expect("read accepted publication before snapshot");
            fixture.publish_snapshot_now().await;
            let published = database
                .store_current_publication()
                .await
                .expect("read accepted publication after snapshot");
            assert_ne!(
                previous.record().latest_snapshot(),
                published.record().latest_snapshot(),
                "the test must accept a new snapshot",
            );
            assert!(
                fixture
                    .device
                    .stage_current_acknowledgement_if_new("2026-03-02T00:00:00Z")
                    .await
                    .expect("evaluate the actual acknowledgement publisher")
                    .is_none(),
                "snapshot {generation} after {rounds} rounds repeated a standing assertion",
            );
            fixture
                .device
                .run_cycle(None)
                .await
                .expect("consume the snapshot");
            let after = database
                .latest_local_store_ack()
                .await
                .expect("read acknowledgement after snapshot")
                .expect("the standing acknowledgement remains");
            assert_eq!(before.reference, after.reference);
        }
    }
}

/// A retained row holds one acknowledgement or none — never a chain of them.
///
/// This replaces `retained_materialization_rows_do_not_repeat_predecessor_
/// history`, which compared sequence 2 against sequence 12 with a 1 024-byte
/// tolerance. The row was growing about 6 KB per acknowledging commit the whole
/// time that test was green: over ten commits the growth fit inside the
/// tolerance, and over three hundred it was a 223 MB table.
///
/// Two assertions, because neither alone is enough. The structural one is exact
/// and is the invariant: a row carries at most one acknowledgement. The size one
/// catches anything else that might start accumulating, and its bound is derived
/// rather than picked — the spread across a whole history must stay under the
/// size of a single acknowledgement, so a row cannot have gained even one extra.
/// An exact byte equality is not available and would be false: an
/// acknowledgement names a cut, a cut names sequence numbers, and JSON writes
/// those in decimal, so the same shape encodes three bytes wider once sequences
/// reach two digits.
async fn retained_acknowledgement_evidence(rounds: u64) -> Vec<(u64, usize, usize)> {
    let fixture = AcknowledgedHistory::publish(0).await;
    let mut rows = std::collections::BTreeMap::new();
    for round in 1..=rounds {
        fixture.publish_round(round).await;
        // Sample accepted rows before a later snapshot retires them. Retirement
        // must not make a history-size assertion vacuous.
        for input in fixture.retained_history().await {
            if input.commit().author_registration.device_id != fixture.device.typed_device_id() {
                continue;
            }
            let sequence = input.commit_ref().coord.sequence();
            let bytes = fixture
                .db
                .retained_canonical_input_for_test(
                    input.commit_ref().coord.stream_id.to_string(),
                    sequence,
                )
                .await
                .expect("read the retained row");
            let row: serde_json::Value = serde_json::from_slice(&bytes).expect("row is JSON");
            let evidence = &row["history_evidence"];
            let acknowledgements = match &evidence["acknowledgement"] {
                serde_json::Value::Null => 0,
                activated => {
                    assert!(
                        activated.get("chain").is_none(),
                        "a retained row carries an acknowledgement chain at sequence {sequence}"
                    );
                    activated["acknowledgement"]
                        .as_array()
                        .map(|_| 1)
                        .expect("an activated acknowledgement is one reference and value")
                }
            };
            let measured = (
                acknowledgements,
                serde_json::to_vec(evidence).expect("evidence").len(),
            );
            if let Some(previous) = rows.insert(sequence, measured) {
                assert_eq!(previous, measured, "retained evidence is immutable");
            }
        }
    }
    rows.into_iter()
        .map(|(sequence, (count, bytes))| (sequence, count, bytes))
        .collect()
}

/// One acknowledgement per row is the length of the chain a row may carry, and
/// it does not change with how much history the row sits on top of.
#[tokio::test]
async fn a_retained_row_never_carries_an_acknowledgement_chain() {
    for rounds in [4_u64, 30] {
        let rows = retained_acknowledgement_evidence(rounds).await;
        assert!(
            rows.iter().all(|(_, count, _)| *count <= 1),
            "a retained row carried more than one acknowledgement at {rounds} rounds",
        );
        assert!(
            rows.iter().any(|(_, count, _)| *count == 1),
            "the fixture must retain acknowledging commits to mean anything",
        );
    }
}

#[tokio::test]
async fn a_retained_row_costs_the_same_at_every_sequence() {
    let rows = retained_acknowledgement_evidence(25).await;
    // A stream's first commits introduce the device and have a different shape.
    let acknowledging = rows[2..]
        .iter()
        .filter(|(_, count, _)| *count == 1)
        .collect::<Vec<_>>();
    assert!(
        acknowledging.len() >= 8,
        "the fixture must retain enough acknowledging commits to compare: {acknowledging:?}",
    );
    let smallest = acknowledging
        .iter()
        .map(|(_, _, bytes)| *bytes)
        .min()
        .expect("acknowledging rows exist");
    let largest = acknowledging
        .iter()
        .map(|(_, _, bytes)| *bytes)
        .max()
        .expect("acknowledging rows exist");
    // One acknowledgement is the unit that used to accumulate, so the whole
    // history's spread staying under one of them is the statement that none of
    // these rows gained a second.
    assert!(
        largest - smallest < smallest / 2,
        "a retained row's evidence grew across the history: smallest {smallest}, \
         largest {largest} — {acknowledging:?}",
    );

    let plain = rows[2..]
        .iter()
        .filter(|(_, count, _)| *count == 0)
        .collect::<Vec<_>>();
    let first_plain = plain[0].2;
    assert!(
        plain.iter().all(|(_, _, bytes)| *bytes == first_plain),
        "a row with no acknowledgement stopped being a fixed size: {plain:?}",
    );
}

/// A Store where nothing is happening stops growing.
///
/// Publishing an acknowledgement appends a commit, and that commit moves the
/// frontier the next acknowledgement would name — so a device that acknowledges
/// whatever it currently sees acknowledges its own acknowledgement, and an idle
/// Store gains a commit per device per cycle without end. The live store this
/// was found on carried 385 commits behind 16 host writes.
#[tokio::test]
async fn an_idle_cycle_appends_no_commit() {
    let fixture = AcknowledgedHistory::publish(2).await;
    // The fixture's last round leaves this device with the peer's commit to
    // acknowledge. This cycle says that; every cycle after it has nothing to add.
    fixture
        .device
        .run_cycle(None)
        .await
        .expect("settle the cycle");
    let settled = fixture.retained_history().await.len();

    for cycle in 1..=4 {
        fixture
            .device
            .run_cycle(None)
            .await
            .expect("run an idle cycle");
        assert_eq!(
            fixture.retained_history().await.len(),
            settled,
            "idle cycle {cycle} appended a commit to a Store where nothing happened",
        );
    }
}

/// And starts again the moment there is something to say. The guard withholds an
/// acknowledgement that repeats itself, never one that carries news.
#[tokio::test]
async fn a_cycle_with_something_to_say_acknowledges_it() {
    let fixture = AcknowledgedHistory::publish(2).await;
    fixture
        .device
        .run_cycle(None)
        .await
        .expect("settle the cycle");
    fixture
        .device
        .run_cycle(None)
        .await
        .expect("confirm the settled cycle is idle");
    let settled = fixture.retained_history().await.len();

    HistoryPublisher::new(&fixture.peer_db, &fixture.peer)
        .publish_note(99)
        .await;
    fixture
        .device
        .run_cycle(None)
        .await
        .expect("pull and acknowledge the peer's commit");
    assert_eq!(
        fixture.retained_history().await.len(),
        settled + 2,
        "the peer's commit and this device's acknowledgement of it both land",
    );

    fixture
        .device
        .run_cycle(None)
        .await
        .expect("run an idle cycle again");
    assert_eq!(
        fixture.retained_history().await.len(),
        settled + 2,
        "and the Store settles again once the news is acknowledged",
    );
}

/// Snapshot publication does not reload history in proportion to its depth.
/// Inherited deletion obligations require independent absence checks during
/// preparation and verification; those exact slots are accounted for separately.
#[tokio::test]
async fn publishing_a_snapshot_asks_for_what_it_writes_and_nothing_per_commit() {
    // The first publication also finalizes the initial admission evidence.
    // Later generations reuse the verified proof retained by the baseline.
    // Each generation still verifies the predecessor's exact signed result.
    const PUBLICATION_REQUESTS: [u64; 4] = [32, 28, 28, 28];

    let mut measurements = Vec::new();
    for rounds in [1u64, 4, 8] {
        let fixture = AcknowledgedHistory::publish(rounds).await;
        fixture.settle_onto_the_published_snapshot().await;
        for snapshot in 0..4 {
            measurements.push((
                rounds,
                snapshot,
                fixture
                    .snapshot_publication_requests_without_retirement()
                    .await,
            ));
            fixture.settle_onto_the_published_snapshot().await;
        }
    }
    assert!(
        measurements
            .iter()
            .all(|(_, generation, requests)| { *requests == PUBLICATION_REQUESTS[*generation] }),
        "publication requests excluding exact inherited retirement checks by \
         (history rounds, generation, requests): {measurements:?}",
    );
}

/// The owner's journal for a join is retired when the joined device arrives,
/// and not before.
///
/// The owner's half of a join ended at a published activation commit and stayed
/// there for the life of the store: `ActivationPrepared` is permanently a
/// "hand the activation over" action, and the owner has no artifact by which it
/// could learn the joining device took it — the same asymmetry that makes the
/// joiner delete the attempt's transport slots. So every join a device ever
/// hosted left a row behind, holding the completion and the activation.
///
/// What the owner can see is the joined device's own first commit. Everything
/// else about the join is something the owner wrote — the registration goes
/// Active from the owner's own activation commit, so it says nothing about
/// whether that device ever ran. A stream under the joined registration's
/// announcement stream id appearing in the materialized frontier is a commit
/// that device signed and this one verified.
#[tokio::test]
async fn an_owner_retires_a_join_when_the_joined_device_arrives() {
    use crate::sync::store::DeviceJoinAction;

    let founder = UserKeypair::generate();
    let member = UserKeypair::generate();
    let member_pubkey = hex::encode(member.public_key());
    let store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(store_dir.clone());
    let home = crate::sync::test_helpers::test_cloud_home();
    let (store, _storage) = TestStore::create_with_connection(
        &db,
        store_dir.clone(),
        "join-retirement",
        founder.clone(),
        home.clone(),
    )
    .await
    .expect("create Merge Store");
    let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
    store
        .admit_member(
            &db,
            store_dir.clone(),
            &founder,
            &member_pubkey,
            None,
            coven_protocol::membership::MemberRole::Member,
            &encryption,
            "Join Retirement Store",
        )
        .await
        .expect("admit the peer as a Member");
    let peer_store_dir = crate::sync::test_helpers::test_store_dir();
    let peer_db = crate::sync::test_helpers::open_test_db(peer_store_dir.clone());
    let peer = store
        .activate_joined_device(
            &db,
            store_dir.clone(),
            &peer_db,
            peer_store_dir.clone(),
            &member,
            "2026-03-01T00:00:45Z",
        )
        .await
        .expect("activate the peer device");
    let owner = store
        .bind_device_in(&db, store_dir.clone(), &founder)
        .await
        .expect("bind the owner device");
    let owner_db = coven_database::StoreDatabase::new(&db);

    let awaiting_activation = |actions: &[DeviceJoinAction]| {
        actions.iter().any(|action| {
            matches!(
                action,
                DeviceJoinAction::TransferActivation(_)
                    | DeviceJoinAction::TransferSamePrincipalJoin(_)
            )
        })
    };
    assert!(
        awaiting_activation(
            &owner_db
                .device_join_actions()
                .await
                .expect("read the owner's join actions")
        ),
        "the owner holds the join it published for the joining device",
    );

    // A cycle before the joined device has published anything of its own must
    // leave the row alone: the registration is already Active, because the
    // owner activated it, and that is exactly the evidence that proves nothing.
    owner
        .run_cycle(None)
        .await
        .expect("run a cycle before the joined device arrives");
    assert!(
        awaiting_activation(
            &owner_db
                .device_join_actions()
                .await
                .expect("read the owner's join actions again")
        ),
        "a device that has published nothing has not arrived",
    );

    HistoryPublisher::new(&peer_db, &peer).publish_note(1).await;
    owner
        .run_cycle(None)
        .await
        .expect("pull the joined device's first commit");

    assert!(
        !awaiting_activation(
            &owner_db
                .device_join_actions()
                .await
                .expect("read the owner's join actions after arrival")
        ),
        "the joined device published its own commit, so the owner's journal is retired",
    );
}
