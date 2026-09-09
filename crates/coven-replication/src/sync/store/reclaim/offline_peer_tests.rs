use super::*;
struct OfflinePeerFixture {
    store: std::sync::Arc<crate::sync::test_helpers::TestStore>,
    owner_db: coven_database::Database,
    owner_dir: coven_foundation::store_dir::StoreDir,
    owner: UserKeypair,
    member: Option<UserKeypair>,
    owner_device: crate::sync::test_helpers::TestDevice,
    covering: coven_protocol::store_commit::AcceptedStoreSnapshotRef,
    peer: Option<StoreDeviceRegistrationRef>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PeerJoin {
    BeforeCoverage,
    AfterCoverage,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PeerPrincipal {
    SamePrincipal,
    SeparateMember,
}

impl OfflinePeerFixture {
    async fn build(store_id: &str, join: PeerJoin, principal: PeerPrincipal) -> Self {
        let signer = UserKeypair::generate();
        let member = match principal {
            PeerPrincipal::SamePrincipal => None,
            PeerPrincipal::SeparateMember => Some(UserKeypair::generate()),
        };
        let owner_dir = crate::sync::test_helpers::test_store_dir();
        let owner_db = crate::sync::test_helpers::open_test_db(owner_dir.clone());
        let store = Box::pin(crate::sync::test_helpers::TestStore::create(
            &owner_db,
            owner_dir.clone(),
            store_id,
            signer.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        ))
        .await
        .expect("create two-device reclaim Store");
        let owner_device = Box::pin(store.open_into(&owner_db, owner_dir.clone()))
            .await
            .expect("open owner Store device");

        let changeset =
            crate::sync::test_helpers::open_test_db(crate::sync::test_helpers::test_store_dir())
                .capture_test_changeset(&[
                    "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                     VALUES ('unanimity-row', 'unanimity', NULL, \
                     '0000000001000-0000-unanimity', '2026-01-01')",
                ])
                .await;
        store
            .publish_changeset("founder", 1, &changeset, owner_db.schema_version())
            .await
            .expect("publish Store history to snapshot");

        // Capture before or after the join so the snapshot's device state
        // determines whether the peer participates in its reclaim decision.
        let latest_snapshot = || async {
            coven_database::StoreDatabase::new(&owner_db)
                .store_current_publication()
                .await
                .expect("read accepted publication")
                .record()
                .latest_snapshot()
                .expect("an accepted snapshot exists")
                .clone()
        };
        let mut covering = None;
        if join == PeerJoin::AfterCoverage {
            publish_current_snapshot(&owner_device).await;
            covering = Some(latest_snapshot().await);
        }
        let peer_dir = crate::sync::test_helpers::test_store_dir();
        let peer_db = crate::sync::test_helpers::open_test_db(peer_dir.clone());
        match &member {
            None => {
                Box::pin(store.activate_joined_device(
                    &owner_db,
                    owner_dir.clone(),
                    &peer_db,
                    peer_dir,
                    &signer,
                    "2026-07-18T00:00:00Z",
                ))
                .await
                .expect("activate peer Store device");
            }
            Some(member) => {
                // Admitting first is what makes this peer a member in its own
                // right; the activation that follows is the same join the
                // same-principal peer does, so the choreography above still
                // holds and only the author of the registration differs.
                Box::pin(store.admit_and_activate_peer(
                    &owner_db,
                    owner_dir.clone(),
                    &peer_db,
                    peer_dir,
                    member,
                ))
                .await
                .expect("admit and activate a second member's device");
            }
        }
        let covering = match covering {
            Some(reference) => reference,
            None => {
                // The join acknowledged its installation snapshot. Capture the
                // accepted frontier after activation without acknowledging this
                // new snapshot on the peer's behalf.
                publish_current_snapshot(&owner_device).await;
                latest_snapshot().await
            }
        };

        let acknowledged_at = owner_device
            .latest_local_store_position()
            .await
            .expect("read the owner's Store position")
            .expect("the Store has published history");
        let StoreCommitCoord { stream_id, .. } = acknowledged_at.coord;
        owner_device
            .publish_acknowledgement(CommitFrontier(BTreeMap::from([(
                stream_id,
                acknowledged_at,
            )])))
            .await
            .expect("owner acknowledges the covering snapshot");

        let local_device_id = owner_device.device_id().clone();
        let registrations = coven_database::StoreDatabase::new(&owner_db)
            .activated_store_device_registration_records()
            .await
            .expect("list active Store registrations");
        let peer = registrations
            .iter()
            .map(|registration| registration.reference().clone())
            .find(|reference| reference.device_id.to_string() != local_device_id);

        Self {
            store,
            owner_db,
            owner_dir,
            owner: signer,
            member,
            owner_device,
            covering,
            peer,
        }
    }

    async fn remove_peer_member(&self) {
        let member = self
            .member
            .as_ref()
            .expect("only a separate member can be removed as one");
        self.store
            .remove_member(
                &self.owner_db,
                self.owner_dir.clone(),
                &self.owner,
                &crate::sync::test_helpers::pubkey_hex(member),
                &coven_keys::encryption::EncryptionService::from_key([42; 32]),
                &crate::sync::test_helpers::TestCustody::default(),
            )
            .await
            .expect("remove the peer's member");
    }

    async fn chosen_snapshot(&self) -> coven_protocol::store_commit::AcceptedStoreSnapshotRef {
        let mut writer = self
            .owner_device
            .authorize_writer()
            .await
            .expect("authorize reclaim writer");
        writer
            .reclaim()
            .choose_snapshot()
            .await
            .expect("reclaim selects some snapshot")
            .reference()
    }
}

#[tokio::test]
async fn an_idle_device_does_not_block_accepted_snapshot_reclaim() {
    Box::pin(async {
        let fixture = OfflinePeerFixture::build(
            "reclaim-unanimity-idle",
            PeerJoin::BeforeCoverage,
            PeerPrincipal::SamePrincipal,
        )
        .await;
        assert!(
            fixture.peer.is_some(),
            "the peer joined before the coverage"
        );

        let chosen = fixture.chosen_snapshot().await;
        assert!(
            chosen.publication.position.get() >= fixture.covering.publication.position.get(),
            "an idle member does not block accepted snapshot retirement: publication {} against {}",
            chosen.publication.position.get(),
            fixture.covering.publication.position.get(),
        );
    })
    .await;
}

#[tokio::test]
async fn a_device_excluded_after_the_coverage_does_not_block_reclaim() {
    Box::pin(async {
        let fixture = OfflinePeerFixture::build(
            "reclaim-unanimity-excluded",
            PeerJoin::BeforeCoverage,
            PeerPrincipal::SamePrincipal,
        )
        .await;
        let peer = fixture.peer.clone().expect("the peer joined");
        fixture.owner_device.finalize_peer_exclusion(&peer).await;

        // At or past, not equal: excluding a device publishes history of its own,
        // which can produce a newer snapshot that is also selectable. What
        // matters is that reclaim is no longer held below the one the excluded
        // device was blocking.
        let chosen = fixture.chosen_snapshot().await;
        assert!(
            chosen.publication.position.get() >= fixture.covering.publication.position.get(),
            "an excluded device is excused, so reclaim reaches its snapshot: chose \
             generation {} against {}",
            chosen.publication.position.get(),
            fixture.covering.publication.position.get(),
        );
    })
    .await;
}

#[tokio::test]
async fn a_device_that_joined_after_the_coverage_does_not_block_reclaim() {
    Box::pin(async {
        let fixture = OfflinePeerFixture::build(
            "reclaim-unanimity-joined-after",
            PeerJoin::AfterCoverage,
            PeerPrincipal::SamePrincipal,
        )
        .await;
        assert!(fixture.peer.is_some(), "the peer joined after the coverage");

        let chosen = fixture.chosen_snapshot().await;
        assert!(
            chosen.publication.position.get() >= fixture.covering.publication.position.get(),
            "a device that joined after the coverage is excused, so reclaim reaches its \
             snapshot: chose generation {} against {}",
            chosen.publication.position.get(),
            fixture.covering.publication.position.get(),
        );
    })
    .await;
}

#[tokio::test]
async fn a_removed_members_device_does_not_block_reclaim() {
    Box::pin(async {
        let fixture = OfflinePeerFixture::build(
            "reclaim-unanimity-removed-member",
            PeerJoin::BeforeCoverage,
            PeerPrincipal::SeparateMember,
        )
        .await;
        assert!(
            fixture.peer.is_some(),
            "the second member's device joined before the coverage"
        );

        let before = fixture.chosen_snapshot().await;
        assert!(
            before.publication.position.get() >= fixture.covering.publication.position.get(),
            "a current idle member does not block accepted snapshot retirement: publication {} against {}",
            before.publication.position.get(),
            fixture.covering.publication.position.get(),
        );

        fixture.remove_peer_member().await;

        // At or past, not equal: removing a member publishes history of its own,
        // which can produce a newer snapshot that is also selectable. What
        // matters is that reclaim is no longer held below the one the removed
        // member's device was blocking.
        let after = fixture.chosen_snapshot().await;
        assert!(
            after.publication.position.get() >= fixture.covering.publication.position.get(),
            "a removed member's device is excused, so reclaim reaches its snapshot: chose \
             generation {} against {}",
            after.publication.position.get(),
            fixture.covering.publication.position.get(),
        );
    })
    .await;
}

#[tokio::test]
async fn membership_after_snapshot_coverage_preserves_the_installed_baseline() {
    Box::pin(async {
        let fixture = OfflinePeerFixture::build(
            "retirement-membership-cut",
            PeerJoin::BeforeCoverage,
            PeerPrincipal::SeparateMember,
        )
        .await;
        let database = coven_database::StoreDatabase::new(&fixture.owner_db);
        let frontier_before_removal = coven_protocol::store_commit::CommitFrontier::from_refs(
            database
                .materialized_frontier()
                .await
                .expect("read the Store frontier before member removal"),
        )
        .expect("shape the Store frontier before member removal");

        fixture.remove_peer_member().await;

        let frontier_after_removal = coven_protocol::store_commit::CommitFrontier::from_refs(
            database
                .materialized_frontier()
                .await
                .expect("read the Store frontier after member removal"),
        )
        .expect("shape the Store frontier after member removal");
        assert_ne!(frontier_after_removal, frontier_before_removal);
        assert!(
            frontier_after_removal.covers(&frontier_before_removal),
            "member removal advances accepted Store history beyond the snapshot",
        );

        let baseline_before = database
            .installed_replay_baseline()
            .await
            .expect("read settled snapshot baseline");
        let expected_snapshot = baseline_before
            .snapshot()
            .expect("fixture retired its snapshot")
            .reference
            .clone();
        let outcome = fixture
            .owner_device
            .stand_on_accepted_snapshot()
            .await
            .expect("evaluate retirement from the accepted snapshot boundary");
        assert_eq!(
            outcome,
            crate::sync::store::ReplayBaselineAdvance::Declined(
                crate::sync::store::ReplayBaselineDecline::BaselineAtCoverage {
                    snapshot: expected_snapshot.clone()
                },
            )
        );
        let baseline_after = database
            .installed_replay_baseline()
            .await
            .expect("read unchanged baseline");
        assert_eq!(
            baseline_after
                .snapshot()
                .expect("snapshot remains installed")
                .reference,
            expected_snapshot
        );
        assert_eq!(baseline_after.coverage(), baseline_before.coverage());
        assert_eq!(
            coven_protocol::store_commit::CommitFrontier::from_refs(
                database
                    .materialized_frontier()
                    .await
                    .expect("read preserved removal frontier")
            )
            .expect("shape preserved removal frontier"),
            frontier_after_removal,
            "standing on prior coverage preserves the accepted removal suffix",
        );
        assert!(!fixture
            .owner_device
            .membership_for_test()
            .await
            .expect("read preserved membership")
            .is_member_now(&keys::public_key_hex(
                fixture.member.as_ref().expect("separate member")
            )));
    })
    .await;
}
