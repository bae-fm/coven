use super::*;
use coven_protocol::membership::LocalStoreMembership;

fn open_scoped_database(store_dir: coven_foundation::store_dir::StoreDir) -> Database {
    crate::sync::test_helpers::open_test_db_schema(
        store_dir,
        vec![coven_protocol::synced_schema::SyncedTable::new(
            "documents",
            coven_protocol::synced_schema::RowIdentity::IndependentUuid,
        )
        .scoped_by("audience")],
        vec![coven_database::Migration::sql(
            1,
            "Circle package schema",
            "CREATE TABLE documents (
                id TEXT PRIMARY KEY,
                audience TEXT,
                _updated_at TEXT NOT NULL
            ) STRICT;",
        )],
    )
}

#[tokio::test]
async fn staged_successor_access_reads_the_same_historical_package_as_installed_access() {
    assert_staged_package_access(false).await;
}

#[tokio::test]
async fn reversed_prepared_controls_restore_historical_package_access() {
    assert_staged_package_access(true).await;
}

async fn assert_staged_package_access(prepare_historical_control: bool) {
    let name = "staged-historical-circle-package";
    let owner_dir = crate::sync::test_helpers::test_store_dir();
    let owner_db = open_scoped_database(owner_dir.clone());
    let (store, home, owner_identity, founder) =
        persist_merge_operation(&owner_db, owner_dir.clone(), name).await;
    let circle_id = founder.circle_id();
    let owner = store
        .bind_device_in(&owner_db, owner_dir.clone(), &owner_identity)
        .await
        .expect("bind Circle owner");
    owner
        .resume_circle_operations()
        .await
        .expect("publish the founder Circle");

    let recipient_identity = UserKeypair::generate();
    let recipient_pubkey = keys::public_key_hex(&recipient_identity);
    let routing = EncryptionService::from_key([42; 32]);
    store
        .admit_member(
            &owner_db,
            owner_dir.clone(),
            &owner_identity,
            &recipient_pubkey,
            None,
            MemberRole::Member,
            &routing,
            name,
        )
        .await
        .expect("admit the recipient to the Store");
    let recipient_dir = crate::sync::test_helpers::test_store_dir();
    let recipient_db = open_scoped_database(recipient_dir.clone());
    store
        .activate_joined_device(
            &owner_db,
            owner_dir.clone(),
            &recipient_db,
            recipient_dir.clone(),
            &recipient_identity,
            "2026-07-24T01:00:00Z",
        )
        .await
        .expect("activate the recipient device without Circle access");
    let recipient = store
        .bind_device_in(&recipient_db, recipient_dir.clone(), &recipient_identity)
        .await
        .expect("bind the recipient device");
    let recipient_store = store
        .open_store_with_identity(&recipient_db, recipient_dir, &recipient_identity)
        .await
        .expect("open the recipient's activation reader");
    let components = prepare_owner_sync_components(
        &owner_db,
        &store,
        &home,
        &owner_dir,
        &owner_identity,
        name,
        circle_test_custody(),
    )
    .await;

    let historical_activation = if prepare_historical_control {
        owner
            .rename_circle(
                &StoreDatabase::new(&owner_db).stamp(),
                circle_id,
                "Before the access grant",
            )
            .await
            .expect("publish a historical control the recipient has not installed");
        let reference = owner
            .latest_local_store_position()
            .await
            .expect("read the historical control position")
            .expect("the historical control is published");
        let commit = owner
            .load_commit_for_test(&reference)
            .await
            .expect("load the historical control commit");
        Some(
            recipient
                .load_circle_activations(&reference, commit.value(), commit.author())
                .await
                .expect("verify the historical control without materializing it"),
        )
    } else {
        None
    };

    let write = owner_db
        .capture_circle_document_for_test(
            "00000000-0000-4000-8000-000000000001",
            circle_id,
            "0000000003000-0000-owner",
        )
        .await
        .expect("capture a document under the original Circle control");
    assert!(owner
        .prepare_pending_store_write()
        .await
        .expect("prepare the Circle document"));
    assert_eq!(
        owner
            .drain_store_writes()
            .await
            .expect("publish the Circle document"),
        1
    );
    let package_commit_ref = match StoreDatabase::new(&owner_db)
        .write_status(&write)
        .await
        .expect("read the published document position")
    {
        coven_protocol::write::WriteStatus::Published(position) => position
            .exact_commit()
            .expect("published Circle row has an exact commit")
            .clone(),
        status => panic!("the Circle document was not published: {status:?}"),
    };
    let package_commit = owner
        .load_commit_for_test(&package_commit_ref)
        .await
        .expect("load the accepted Circle document commit");
    if !prepare_historical_control {
        let (_, pulled) = recipient
            .pull_store()
            .await
            .expect("observe the document commit before receiving Circle access");
        assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    }
    let historical_prepared = historical_activation.iter().collect::<Vec<_>>();
    let before = recipient
        .load_applicable_circle_packages_for_test(
            &package_commit,
            &historical_prepared,
            package_commit.author(),
            LocalStoreMembership::Current,
        )
        .await
        .expect("classify the inaccessible historical package");
    assert!(before.is_empty(), "the recipient has not joined the Circle");

    components
        .add_circle_member(circle_id, recipient_pubkey, CircleRole::Member)
        .await
        .expect("grant the recipient access in the same Circle epoch");
    let successor = StoreDatabase::new(&owner_db)
        .circle_authoring_context(circle_id, &keys::public_key_hex(&owner_identity))
        .await
        .expect("read the successor Circle control")
        .0
        .control;
    let successor_commit_ref = StoreDatabase::new(&owner_db)
        .retained_circle_activation_commit_ref(circle_id, successor.coord.clone())
        .await
        .expect("read the successor activation position")
        .expect("the successor activation is retained");
    let successor_commit = owner
        .load_commit_for_test(&successor_commit_ref)
        .await
        .expect("load the accepted member-add commit");
    let staged = load_prepared_activations(
        &recipient_store,
        &successor_commit,
        &recipient_identity,
        &historical_prepared,
    )
    .await
    .expect("verify the recipient's successor access without installing it");
    let [activation] = staged.circles() else {
        panic!("the member-add commit must carry one Circle activation");
    };
    let [package_ref] = package_commit.circle_packages() else {
        panic!("the document commit must carry one Circle package");
    };
    assert_ne!(activation.control.coord, package_ref.control);
    assert_eq!(
        activation.control.value.key_fingerprint(),
        package_ref.key_fingerprint,
        "the successor grants access to the historical package's epoch key"
    );
    assert!(activation
        .epoch_access()
        .expect("verify staged epoch access")
        .is_some());
    let later_activation = if prepare_historical_control {
        owner
            .rename_circle(
                &StoreDatabase::new(&owner_db).stamp(),
                circle_id,
                "After the access grant",
            )
            .await
            .expect("publish another control after the access grant");
        let reference = owner
            .latest_local_store_position()
            .await
            .expect("read the later control position")
            .expect("the later control is published");
        let commit = owner
            .load_commit_for_test(&reference)
            .await
            .expect("load the later control commit");
        let prepared = std::iter::once(&staged)
            .chain(historical_activation.iter())
            .collect::<Vec<_>>();
        let later =
            load_prepared_activations(&recipient_store, &commit, &recipient_identity, &prepared)
                .await
                .expect("verify the later control without materializing its predecessors");
        let mut history = recipient_store
            .authorize_history_for_test()
            .await
            .expect("authorize predecessor verification");
        history
            .prepare_store_publication_history_for_test()
            .await
            .expect("verify the accepted history including the later control");
        let activating = staged.stream_activations().activating_commit();
        history
            .verify_prepared_circle_predecessor_for_test(commit.value(), activating, activation)
            .expect("the later control observes the staged access grant");
        assert!(
            history
                .verify_prepared_circle_predecessor_for_test(
                    package_commit.value(),
                    activating,
                    activation,
                )
                .is_err(),
            "knowing a later accepted control does not put it in an earlier commit's history"
        );
        assert!(
            history
                .verify_prepared_circle_predecessor_for_test(
                    commit.value(),
                    package_commit.reference(),
                    activation,
                )
                .is_err(),
            "an earlier package commit did not activate the Circle control"
        );
        history
            .verify_prepared_circle_predecessor_for_test(commit.value(), activating, activation)
            .expect("rejection leaves exact predecessor verification available");
        assert_eq!(
            history
                .verified_circle_predecessors_for_test(commit.value(), circle_id, &[&staged])
                .expect("select the exact prepared predecessor"),
            vec![activation.clone()],
        );
        let [later_control] = later.circles() else {
            panic!("the later rename carries one Circle control");
        };
        coven_protocol::circle_activation::verify_control_context_for_verified_commit(
            &later_control.reference,
            &later_control.control,
            &successor_commit,
        )
        .expect("matching author and root alone do not establish exact control containment");
        let substituted =
            coven_protocol::circle_activation::VerifiedCircleActivations::from_verified_parts(
                later.circles().to_vec(),
                staged.stream_activations().clone(),
                later.bootstraps().to_vec(),
                later.local_exclusions().to_vec(),
                later.bootstrap_pending_exclusions().to_vec(),
            );
        let error = history
            .verified_circle_predecessors_for_test(commit.value(), circle_id, &[&substituted])
            .expect_err("an earlier commit cannot authorize another same-author Circle control");
        assert!(
            error
                .to_string()
                .contains("absent from its exact activating commit"),
            "{error}",
        );
        assert_eq!(
            history
                .verified_circle_predecessors_for_test(commit.value(), circle_id, &[&staged])
                .expect("rejection preserves the verified predecessor"),
            vec![activation.clone()],
        );
        Some(later)
    } else {
        None
    };
    let prepared = later_activation
        .iter()
        .chain(std::iter::once(&staged))
        .chain(historical_activation.iter())
        .collect::<Vec<_>>();
    let staged_packages = recipient
        .load_applicable_circle_packages_for_test(
            &package_commit,
            &prepared,
            package_commit.author(),
            LocalStoreMembership::Current,
        )
        .await
        .expect("read the historical package through staged successor access");

    let (_, pulled) = recipient
        .pull_store()
        .await
        .expect("install the same verified successor access");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    let installed_packages = recipient
        .load_applicable_circle_packages_for_test(
            &package_commit,
            &[],
            package_commit.author(),
            LocalStoreMembership::Current,
        )
        .await
        .expect("read the historical package through installed successor access");
    assert_eq!(installed_packages.len(), 1);
    assert_eq!(installed_packages[0].reference, *package_ref);
    assert_eq!(
        staged_packages.len(),
        installed_packages.len(),
        "staging verified access must not omit a package that installing it makes readable"
    );
    assert_eq!(staged_packages[0].bytes, installed_packages[0].bytes);

    let exact_write = owner_db
        .capture_circle_document_for_test(
            "00000000-0000-4000-8000-000000000002",
            circle_id,
            &StoreDatabase::new(&owner_db).stamp(),
        )
        .await
        .expect("capture a package under the recipient's exact active control");
    assert!(owner
        .prepare_pending_store_write()
        .await
        .expect("prepare the active-control document"));
    assert_eq!(
        owner
            .drain_store_writes()
            .await
            .expect("publish the active-control document"),
        1
    );
    let exact_commit_ref = match StoreDatabase::new(&owner_db)
        .write_status(&exact_write)
        .await
        .expect("read the active-control document position")
    {
        coven_protocol::write::WriteStatus::Published(position) => position
            .exact_commit()
            .expect("published Circle row has an exact commit")
            .clone(),
        status => panic!("the active-control document was not published: {status:?}"),
    };
    let exact_commit = owner
        .load_commit_for_test(&exact_commit_ref)
        .await
        .expect("load the active-control document commit");
    assert!(recipient
        .circle_epoch_access(circle_id, exact_commit.circle_packages()[0].control.clone())
        .await
        .expect("read exact installed package access")
        .is_some());

    owner
        .delete_circle(circle_id)
        .await
        .expect("publish a deletion after the access grant");
    let deletion_ref = owner
        .latest_local_store_position()
        .await
        .expect("read the deletion position")
        .expect("the deletion is published");
    let deletion_commit = owner
        .load_commit_for_test(&deletion_ref)
        .await
        .expect("load the accepted deletion");
    let deletion = recipient
        .load_circle_activations(
            &deletion_ref,
            deletion_commit.value(),
            deletion_commit.author(),
        )
        .await
        .expect("verify the deletion before installing it");
    let deletion_prepared = std::iter::once(&deletion)
        .chain(prepared.iter().copied())
        .collect::<Vec<_>>();
    for commit in [&package_commit, &exact_commit] {
        let deleted_packages = recipient
            .load_applicable_circle_packages_for_test(
                commit,
                &deletion_prepared,
                commit.author(),
                LocalStoreMembership::Current,
            )
            .await
            .expect("classify package access after a staged deletion");
        assert!(
            deleted_packages.is_empty(),
            "exact and historical access must not survive a staged deletion"
        );
    }

    let (_, pulled) = recipient.pull_store().await.expect("install the deletion");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    let stale_packages = recipient
        .load_applicable_circle_packages_for_test(
            &package_commit,
            &prepared,
            package_commit.author(),
            LocalStoreMembership::Current,
        )
        .await
        .expect("classify staged predecessor access after the installed deletion");
    assert!(
        stale_packages.is_empty(),
        "stale prepared access must not undo an installed deletion"
    );
}

#[tokio::test]
async fn concurrent_accepted_control_is_not_a_prepared_circle_predecessor() {
    let name = "concurrent-circle-predecessor";
    let owner_dir = crate::sync::test_helpers::test_store_dir();
    let owner_db = open_scoped_database(owner_dir.clone());
    let (store, _home, identity, founder) =
        persist_merge_operation(&owner_db, owner_dir.clone(), name).await;
    let circle_id = founder.circle_id();
    let owner = store
        .bind_device_in(&owner_db, owner_dir.clone(), &identity)
        .await
        .expect("bind the first Owner device");
    owner
        .resume_circle_operations()
        .await
        .expect("publish the founder");
    let peer_dir = crate::sync::test_helpers::test_store_dir();
    let peer_db = open_scoped_database(peer_dir.clone());
    let peer = store
        .activate_joined_device(
            &owner_db,
            owner_dir.clone(),
            &peer_db,
            peer_dir,
            &identity,
            "2026-07-24T01:00:00Z",
        )
        .await
        .expect("activate the second Owner device");
    let write = owner_db
        .capture_circle_document_for_test(
            "00000000-0000-4000-8000-000000000001",
            circle_id,
            &StoreDatabase::new(&owner_db).stamp(),
        )
        .await
        .expect("capture a row before the peer control exists");
    assert!(owner
        .prepare_pending_store_write()
        .await
        .expect("prepare the original row commit"));
    let pending = StoreDatabase::new(&owner_db)
        .oldest_prepared_store_write()
        .await
        .expect("read the prepared row")
        .expect("the original row commit is retained");
    peer.rename_circle(
        &StoreDatabase::new(&peer_db).stamp(),
        circle_id,
        "Concurrent control",
    )
    .await
    .expect("publish the peer control before the original row");
    let peer_ref = peer
        .latest_local_store_position()
        .await
        .expect("read the accepted peer control")
        .expect("the peer control is published");
    let peer_commit = peer
        .load_commit_for_test(&peer_ref)
        .await
        .expect("load the peer control commit");
    let peer_controls = peer
        .load_circle_activations(&peer_ref, peer_commit.value(), peer_commit.author())
        .await
        .expect("verify the real peer Circle activation");
    assert_eq!(
        owner
            .drain_store_writes()
            .await
            .expect("accept the original row through publication retry"),
        1
    );
    let candidate_ref = match StoreDatabase::new(&owner_db)
        .write_status(&write)
        .await
        .expect("read the accepted row")
    {
        coven_protocol::write::WriteStatus::Published(position) => position
            .exact_commit()
            .expect("published Circle row has an exact commit")
            .clone(),
        status => panic!("the original row must be accepted: {status:?}"),
    };
    let candidate = owner
        .load_commit_for_test(&candidate_ref)
        .await
        .expect("load the original row commit");
    assert_eq!(
        candidate.value().to_bytes(),
        pending.commit.bytes,
        "retry must preserve the original signed causal observations"
    );
    let owner_store = store
        .open_store_with_identity(&owner_db, owner_dir, &identity)
        .await
        .expect("open the verifier owner");
    let mut history = owner_store
        .authorize_history_for_test()
        .await
        .expect("authorize accepted history");
    history
        .prepare_store_publication_history_for_test()
        .await
        .expect("verify both accepted commits");
    assert!(
        history
            .verified_circle_predecessors_for_test(candidate.value(), circle_id, &[&peer_controls])
            .expect("classify the concurrent prepared control")
            .is_empty(),
        "earlier publication does not make a concurrent control a causal predecessor"
    );
    assert!(
        history
            .verified_circle_predecessors_for_test(
                peer_commit.value(),
                circle_id,
                &[&peer_controls]
            )
            .expect("classify the current control")
            .is_empty(),
        "a commit cannot supply its own predecessor authority"
    );
}

async fn load_prepared_activations(
    receiver: &crate::sync::store::Store,
    commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    identity: &UserKeypair,
    prepared: &[&coven_protocol::circle_activation::VerifiedCircleActivations],
) -> Result<coven_protocol::circle_activation::VerifiedCircleActivations, CircleOperationError> {
    let mut history = receiver.authorize_history_for_test().await?;
    history.prepare_store_publication_history_for_test().await?;
    let predecessors = crate::sync::store::pull::commit_predecessor_references(commit.value());
    let membership_prefix = history
        .verified_merge_membership_prefix_for_test(predecessors.clone(), predecessors)
        .await?;
    let mut prefix = coven_protocol::circle_activation::VerifiedStreamActivationPrefix::empty();
    for group in prepared {
        prefix.include(group.stream_activations())?;
    }
    let routing = coven_protocol::circle::derive_row_routing_key(
        &EncryptionService::from_key([42; 32]),
        commit.store_root_hash(),
    )?;
    history
        .circles()
        .activations()
        .load_payload(
            commit,
            Some(identity),
            Some(&routing),
            &prefix,
            &membership_prefix,
            prepared,
        )
        .await
}
