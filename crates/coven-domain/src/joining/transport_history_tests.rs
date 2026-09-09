use super::*;

/// The stream and sequence a Store package object's key names, from
/// `package_semantic_prefix`: `.../packages/{stream}/{sequence}/{hash}`.
fn store_package_coordinate(logical_key: &str) -> Option<(String, u64)> {
    let (_, tail) = logical_key.split_once("/packages/")?;
    let mut parts = tail.split('/');
    let stream = parts.next()?.to_string();
    let sequence = parts.next()?.parse::<u64>().ok()?;
    Some((stream, sequence))
}

/// A joining device installs the owner's snapshot image and then owes only the
/// history published after it. The rows behind the coverage came with the
/// image, so reading a package per covered commit buys nothing — and it is what
/// made a live join over a hundred commits spend minutes reinstalling history
/// it already held.
#[test]
fn a_join_resolves_only_the_history_its_snapshot_does_not_cover() {
    on_a_deep_stack(run_a_join_resolves_only_the_history_its_snapshot_does_not_cover);
}

async fn run_a_join_resolves_only_the_history_its_snapshot_does_not_cover() {
    let fixture = TransportFixture::build("device-join-snapshot-coverage").await;
    for index in 0..8 {
        fixture.publish_owner_row(&format!("covered-{index}")).await;
    }
    // Two announcement streams, so the coverage names a tip for each and the
    // bootstrap has to credit both.
    fixture.publish_second_stream(3).await;
    fixture.publish_owner_snapshot().await;
    let coverage = fixture
        .owner_database
        .latest_local_store_snapshot()
        .await
        .expect("read the owner's published snapshot")
        .expect("the owner published a snapshot")
        .meta
        .coverage
        .commits()
        .iter()
        .map(|(stream, reference)| (stream.to_string(), reference.coord.sequence()))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert!(
        coverage.len() > 1,
        "the snapshot has to cover more than one stream: {coverage:?}",
    );
    for index in 0..3 {
        fixture
            .publish_owner_row(&format!("uncovered-{index}"))
            .await;
    }

    let bundle = fixture.begin().await;
    fixture.home.clear_exact_reads();
    let cancel = never_cancelled();
    let joiner = fixture.client();
    let (config, activation) = tokio::join!(
        Box::pin(joiner.join_via_transport(&bundle, timing(), no_join_progress(), &cancel)),
        Box::pin(fixture.drive_owner(&bundle)),
    );
    activated(activation);
    joined(config);

    let package_reads = fixture
        .home
        .exact_reads()
        .into_iter()
        .filter_map(|slot| store_package_coordinate(slot.logical_key()))
        .collect::<Vec<_>>();
    let (covered, uncovered): (Vec<_>, Vec<_>) = package_reads
        .iter()
        .partition(|(stream, sequence)| coverage.get(stream).is_some_and(|tip| sequence <= tip));
    assert!(
        covered.is_empty(),
        "the join read packages for commits its installed snapshot already covers: \
         {covered:?} against coverage {coverage:?}",
    );
    assert!(
        !uncovered.is_empty(),
        "the join read no package at all, so it proves nothing about coverage",
    );
}

/// What one same-provider join actually handed the joining device: the closure
/// the owner carried, and the size of the journal row it sits in.
struct CarriedJoin {
    /// The Store root the closure belongs to.
    root: coven_protocol::store_commit::StoreRootRef,
    /// The cut the attempt's activation commit names, which the joining device
    /// has to land on however little the closure carries.
    bootstrap_cut: coven_protocol::store_commit::StoreHistoryCut,
    /// The snapshot's coverage, as a tip sequence per stream.
    coverage: std::collections::BTreeMap<String, u64>,
    closure: coven_protocol::store_commit::device_join_exchange::DeviceJoinBootstrapClosure,
    previous: coven_protocol::store_commit::StoreCurrentPublicationRecord,
    /// The owner's journal row for the attempt, serialized as it is stored.
    journal_bytes: usize,
}

impl CarriedJoin {
    /// The stream and sequence of every commit the closure carried.
    fn carried(&self) -> Vec<(String, u64)> {
        self.closure
            .commits
            .iter()
            .map(|commit| {
                (
                    commit.reference.coord.stream_id.to_string(),
                    commit.reference.coord.sequence(),
                )
            })
            .collect()
    }
}

/// Run one whole same-provider join over a store whose history runs `covered`
/// owner commits deep behind its snapshot and three commits past it, and report
/// what the owner carried.
async fn carried_join_over(store_id: &str, covered: usize) -> CarriedJoin {
    let fixture = TransportFixture::build(store_id).await;
    for index in 0..covered {
        fixture.publish_owner_row(&format!("covered-{index}")).await;
    }
    // Two announcement streams, so the coverage names a tip for each and the
    // trimmed walk has to start from both.
    fixture.publish_second_stream(3).await;
    fixture.publish_owner_snapshot().await;
    let covered_packages = fixture
        .home
        .keys()
        .into_iter()
        .filter(|key| store_package_coordinate(key).is_some())
        .collect::<Vec<_>>();
    assert!(
        !covered_packages.is_empty(),
        "the covered history has packages to retire"
    );
    fixture
        .owner_store
        .reclaim_packages()
        .await
        .expect("retire the history represented by the first snapshot");
    for key in &covered_packages {
        assert!(
            fixture.home.get(key).is_none(),
            "covered package remains: {key}"
        );
    }
    // The first snapshot retains the physical work it authorizes. Its
    // successor can omit that completed work before becoming a Join handoff.
    fixture.publish_owner_snapshot().await;
    let coverage = fixture
        .owner_database
        .latest_local_store_snapshot()
        .await
        .expect("read the owner's published snapshot")
        .expect("the owner published a snapshot")
        .meta
        .coverage
        .commits()
        .iter()
        .map(|(stream, reference)| (stream.to_string(), reference.coord.sequence()))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert!(
        coverage.len() > 1,
        "the snapshot has to cover more than one stream: {coverage:?}",
    );
    for index in 0..3 {
        fixture
            .publish_owner_row(&format!("uncovered-{index}"))
            .await;
    }

    let bundle = fixture.begin().await;
    let cancel = never_cancelled();
    let joiner = fixture.client();
    assert_joiner_waited_for(
        Box::pin(joiner.join_via_transport(&bundle, one_shot(), no_join_progress(), &cancel)).await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );
    activated(fixture.drive_owner(&bundle).await);
    let accepted = fixture
        .owner_database
        .store_current_publication()
        .await
        .expect("read the accepted join publication before the joiner advances it");
    joined(
        Box::pin(joiner.join_via_transport(&bundle, timing(), no_join_progress(), &cancel)).await,
    );

    let record = fixture
        .owner_database
        .load_device_join(
            bundle.offer.attempt_id,
            coven_replication::sync::store::DeviceJoinRole::Owner,
        )
        .await
        .expect("read the owner's join journal")
        .expect("the owner journalled the attempt it completed");
    let journal_bytes = serde_json::to_string(&record)
        .expect("the journal row serializes as it is stored")
        .len();
    let join = match *record.progress {
        coven_protocol::store_commit::device_join_journal::DeviceJoinRoleProgress::Owner(
            coven_protocol::store_commit::device_join_journal::OwnerJoinProgress::
                SamePrincipalCompleted { join, .. },
        ) => join,
        other => panic!("the owner's journal is at {other:?}, not a completed same-provider join"),
    };
    assert_eq!(
        accepted.record(),
        &join.installation.bootstrap.publication.current
    );
    CarriedJoin {
        previous: join
            .installation
            .authority
            .metadata
            .publication_predecessor
            .clone(),
        root: join.installation.authority.store_root.clone(),
        // The cut the attempt named is the activation commit's own predecessor
        // cut; the closure carries that commit, so the test reads it from there
        // rather than from a separate attempt file.
        bootstrap_cut: {
            let activation = join
                .installation
                .bootstrap
                .commits
                .iter()
                .find(|commit| commit.reference == join.activation.outcome_activation)
                .expect("the closure carries the attempt's activation commit");
            let commit: coven_protocol::store_commit::StoreBatchCommit =
                serde_json::from_slice(&activation.canonical_commit)
                    .expect("the carried activation commit parses");
            commit
                .order
                .predecessor_cut()
                .expect("the activation commit names a predecessor cut")
        },
        coverage,
        closure: join.installation.bootstrap,
        journal_bytes,
    }
}

/// The owner hands the joining device a snapshot image and the commits
/// published after it. Carrying the history behind that snapshot as well makes
/// the joiner parse and signature-check every commit of the store's life only
/// to discard the result at installation, and writes the whole history into the
/// owner's journal row on every attempt — which is what put a live join over
/// two hundred commits into the minutes and megabytes.
#[test]
fn a_join_carries_only_the_history_its_snapshot_does_not_cover() {
    on_a_deep_stack(run_a_join_carries_only_the_history_its_snapshot_does_not_cover);
}

async fn run_a_join_carries_only_the_history_its_snapshot_does_not_cover() {
    let shallow = carried_join_over("device-join-carry-shallow", 2).await;
    let deep = carried_join_over("device-join-carry-deep", 14).await;

    for (name, join) in [("shallow", &shallow), ("deep", &deep)] {
        let carried = join.carried();
        let covered = carried
            .iter()
            .filter(|(stream, sequence)| {
                join.coverage.get(stream).is_some_and(|tip| sequence <= tip)
            })
            .collect::<Vec<_>>();
        assert!(
            covered.is_empty(),
            "the {name} join carried commits its snapshot already covers: {covered:?} \
             against coverage {:?}",
            join.coverage,
        );
        assert!(
            !carried.is_empty(),
            "the {name} join carried no commit at all, so it proves nothing",
        );
        // The activation commit still names the whole bootstrap cut, and the
        // joining device still checks it against that cut — the second stream's
        // tip sits behind the snapshot, so the closure does not carry it and
        // the check passes on what the installed image supplies instead. The
        // join above ran to membership, which is that check passing.
        let uncarried_tips = join
            .bootstrap_cut
            .commits()
            .values()
            .filter(|reference| {
                !carried.contains(&(
                    reference.coord.stream_id.to_string(),
                    reference.coord.sequence(),
                ))
            })
            .count();
        assert!(
            uncarried_tips > 0,
            "the {name} join carried every tip of its bootstrap cut, so the cut check \
             was never asked to reach past the closure",
        );
    }

    assert_eq!(
        shallow.carried().len(),
        deep.carried().len(),
        "twelve more commits behind the snapshot changed what the join carries: \
         {:?} against {:?}",
        shallow.carried(),
        deep.carried(),
    );
    assert!(
        deep.journal_bytes < shallow.journal_bytes + shallow.journal_bytes / 10,
        "the owner's journal row grew with history behind the snapshot: \
         {} bytes over {covered_deep} covered commits against {} over {covered_shallow}",
        deep.journal_bytes,
        shallow.journal_bytes,
        covered_deep = 14,
        covered_shallow = 2,
    );
}

/// Trimming the carry does not trim the checking. Every commit still in the
/// closure is parsed against its exact reference and signature-checked against
/// its author before the joining device will build a plan from it, so an
/// altered body, a body swapped with a sibling's, and a body lifted from
/// another Store are all refused.
#[test]
fn a_trimmed_closure_is_still_verified_commit_by_commit() {
    on_a_deep_stack(run_a_trimmed_closure_is_still_verified_commit_by_commit);
}

async fn run_a_trimmed_closure_is_still_verified_commit_by_commit() {
    let join = carried_join_over("device-join-carry-verified", 2).await;
    let foreign = carried_join_over("device-join-carry-foreign", 2).await;
    coven_database::DeviceJoinBootstrapPlan::from_closure(
        &join.root,
        join.previous.clone(),
        join.closure.clone(),
    )
    .expect("the closure the owner carried builds a plan");
    assert!(
        join.closure.commits.len() > 1,
        "the refusals below need at least two carried commits: {:?}",
        join.carried(),
    );

    let mut altered = join.closure.clone();
    altered.commits[0].canonical_commit[0] ^= 0xff;
    assert!(
        coven_database::DeviceJoinBootstrapPlan::from_closure(
            &join.root,
            join.previous.clone(),
            altered,
        )
        .is_err(),
        "a closure with an altered commit body built a plan",
    );

    let mut swapped = join.closure.clone();
    swapped.commits[0].canonical_commit = swapped.commits[1].canonical_commit.clone();
    assert!(
        coven_database::DeviceJoinBootstrapPlan::from_closure(
            &join.root,
            join.previous.clone(),
            swapped,
        )
        .is_err(),
        "a closure whose commit body belongs to a different reference built a plan",
    );

    let mut borrowed = join.closure.clone();
    borrowed.commits[0].canonical_commit = foreign.closure.commits[0].canonical_commit.clone();
    assert!(
        coven_database::DeviceJoinBootstrapPlan::from_closure(
            &join.root,
            join.previous.clone(),
            borrowed,
        )
        .is_err(),
        "a closure carrying another Store's commit body built a plan",
    );

    let mut foreign_root = join.closure.clone();
    foreign_root.commits = foreign.closure.commits.clone();
    assert!(
        coven_database::DeviceJoinBootstrapPlan::from_closure(
            &join.root,
            join.previous.clone(),
            foreign_root,
        )
        .is_err(),
        "a closure of another Store's commits built a plan for this Store's root",
    );
}

/// A membership stream is a hash-linked list: each head names the slot of the
/// next, so a walk always starts at the founder anchor and runs to the end, and
/// no part of it can be fetched in parallel or skipped. That makes walking it
/// twice exactly twice the round-trips — which is what a joining device used to
/// do, once to open its cloud home and once to install its owner anchor, for
/// about twenty-four seconds of a two-minute live join.
///
/// The two sides run one at a time here because they share one cloud home in
/// this fixture, and the claim is about what the joining device reads.
#[test]
fn a_join_walks_the_membership_chain_once() {
    on_a_deep_stack(run_a_join_walks_the_membership_chain_once);
}

async fn run_a_join_walks_the_membership_chain_once() {
    let fixture = TransportFixture::build("device-join-membership-once").await;
    // More than one membership stream, and more than one head on each, so a
    // repeated walk shows up as a repeated read rather than as a single one.
    fixture.publish_second_stream(1).await;
    fixture.publish_owner_snapshot().await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();

    let join_once = |timing| {
        let client = fixture.client();
        let bundle = &bundle;
        let cancel = &cancel;
        async move {
            client
                .join_via_transport(bundle, timing, no_join_progress(), cancel)
                .await
        }
    };

    // The joining device publishes its request and dies; the owner then admits
    // it without needing the joining device back.
    assert_joiner_waited_for(
        Box::pin(join_once(one_shot())).await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );
    activated(fixture.drive_owner_with(&bundle, one_shot()).await);

    // From here to the end of the join, every read is the joining device's.
    fixture.home.clear_exact_reads();
    joined(Box::pin(join_once(timing())).await);

    let head_reads = fixture
        .home
        .exact_reads()
        .into_iter()
        .filter(|slot| slot.logical_key().contains("/membership/heads/"))
        .map(|slot| slot.logical_key().to_string())
        .collect::<Vec<_>>();
    let distinct = head_reads.iter().collect::<std::collections::BTreeSet<_>>();
    assert!(
        distinct.len() > 1,
        "the joining device read at most one membership head, so this proves \
         nothing about walking the chain twice",
    );
    // Installing the carried registration control uses the pending join's own
    // history verifier, so its head is read once more. The pinned Founder is a
    // distinct head; a repeated traversal would read every covered head again.
    assert!(
        head_reads.len() <= distinct.len() + 1,
        "the joining device read {} membership heads over {} distinct ones, \
         which is a second walk rather than a single re-read: {head_reads:?}",
        head_reads.len(),
        distinct.len(),
    );
}

/// What a joining device spends on membership does not grow with the Store's
/// membership history.
///
/// A device join opens the Store keyring out of the membership chain, so the
/// chain is the first thing it reads and none of it can come from behind the
/// keyring. Reading it change by change made "open Store storage" most of a
/// live join: two provider round trips per membership change, back to the
/// Store's founding entry. The newest published snapshot names a rollup
/// carrying all of it, so the joining device reads that instead and walks the
/// provider only for what the snapshot does not cover.
///
/// Counted here are the operations under the membership, rollup, and
/// snapshot-metadata prefixes — everything the join spends deciding what the
/// membership is. The snapshot *image* is deliberately not among them: a
/// joining device downloads one on purpose, and it is the one large transfer
/// the round-trip budget allows.
#[test]
fn a_join_reads_the_same_membership_however_deep_the_chain() {
    on_a_deep_stack(run_a_join_reads_the_same_membership_however_deep_the_chain);
}

async fn run_a_join_reads_the_same_membership_however_deep_the_chain() {
    let shallow = join_membership_operations("join-rollup-shallow", 0).await;
    let deep = join_membership_operations("join-rollup-deep", 8).await;

    assert_eq!(
        shallow, deep,
        "a join over a chain with eight more members spent {deep:?} membership \
         operations against {shallow:?}, so it still follows the history",
    );
    // The exact snapshot locator selects metadata and its rollup. The anchored
    // walk pins the covered terminal, reads the later registration head and its
    // absent successor, and verifies both terminal results. Installation loads
    // the pinned Founder and verifies the registration control in its own owner.
    assert_eq!(
        deep,
        MembershipOperations {
            snapshots: 1,
            rollups: 1,
            heads: 5,
            entries: 3,
            acceptances: 2,
            listings: 0,
        },
        "a cold join must retain its exact membership request profile",
    );
}

/// The membership operations one whole join issues over a Store with
/// `extra_members` admitted beyond its founder.
#[derive(Debug, Default, PartialEq, Eq)]
struct MembershipOperations {
    snapshots: usize,
    rollups: usize,
    heads: usize,
    entries: usize,
    acceptances: usize,
    listings: usize,
}

async fn join_membership_operations(store_id: &str, extra_members: usize) -> MembershipOperations {
    let fixture = TransportFixture::build(store_id).await;
    for _ in 0..extra_members {
        fixture.admit_extra_member().await;
    }
    fixture.publish_owner_snapshot().await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();

    let join_once = |timing| {
        let client = fixture.client();
        let bundle = &bundle;
        let cancel = &cancel;
        async move {
            client
                .join_via_transport(bundle, timing, no_join_progress(), cancel)
                .await
        }
    };

    // The joining device publishes its request and dies; the owner then admits
    // it without needing the joining device back, so that from here on every
    // operation counted is the joining device's own.
    assert_joiner_waited_for(
        Box::pin(join_once(one_shot())).await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );
    activated(fixture.drive_owner_with(&bundle, one_shot()).await);

    fixture.home.clear_exact_reads();
    fixture.home.clear_exact_listings();
    joined(Box::pin(join_once(timing())).await);

    let counted = |key: &str| {
        key.starts_with("store-v1/membership/")
            || key.starts_with("store-v1/membership-rollups/")
            || key.starts_with("store-v1/snapshots/")
    };
    let mut operations = MembershipOperations::default();
    for slot in fixture.home.exact_reads() {
        let key = slot.logical_key();
        if key.starts_with("store-v1/snapshots/") {
            operations.snapshots += 1;
        } else if key.starts_with("store-v1/membership-rollups/") {
            operations.rollups += 1;
        } else if key.starts_with("store-v1/membership/heads/") {
            operations.heads += 1;
        } else if key.starts_with("store-v1/membership/entries/") {
            operations.entries += 1;
        } else if key.starts_with("store-v1/membership/acceptances/") {
            operations.acceptances += 1;
        } else {
            assert!(!counted(key), "unclassified membership read: {key}");
        }
    }
    operations.listings = fixture
        .home
        .exact_listed_prefixes()
        .iter()
        .filter(|prefix| counted(prefix))
        .count();
    operations
}

/// What a join spends on the transport when it never waits.
///
/// A device join's transport operations divide into two kinds, and only one of
/// them is the protocol. The artifacts are fixed: each one the exchange moves
/// is read once, and a joining device that resumes republishes the artifact it
/// had already published, because the joiner journal advances when the artifact
/// is *prepared* and a crash between preparing and publishing has to be
/// recoverable — so the republish is the recovery path, and the read that
/// follows its slot collision is what confirms the slot holds the same
/// transfer. The rest is polling, and polling is time, not protocol.
///
/// This is the shape with no waiting in it at all: the joining device publishes
/// its request and dies, the owner admits it without needing the joiner back,
/// and the joining device then runs once with every artifact it asks for
/// already at its slot. Whatever it spends here it spends on every join.
///
/// Only the artifacts are counted here, and exactly: a new read in the exchange
/// is a change to the protocol and should have to be written down. What the
/// watch spends is the join's duration divided by its cadence, which no count
/// taken over a leg this short can tell apart from a watch that never backs
/// off — `the_cancellation_watch_backs_off_instead_of_polling_flat_out` settles
/// that on a clock it controls.
#[test]
fn a_join_that_never_waits_spends_a_fixed_number_of_transport_operations() {
    on_a_deep_stack(run_a_join_that_never_waits_spends_a_fixed_number_of_transport_operations);
}

async fn run_a_join_that_never_waits_spends_a_fixed_number_of_transport_operations() {
    let fixture = TransportFixture::build("device-join-no-wait-budget").await;
    fixture.publish_owner_snapshot().await;
    let bundle = fixture.begin().await;
    let cancel = never_cancelled();
    let join_once = |timing| {
        let client = fixture.client();
        let bundle = &bundle;
        let cancel = &cancel;
        async move {
            client
                .join_via_transport(bundle, timing, no_join_progress(), cancel)
                .await
        }
    };

    assert_joiner_waited_for(
        Box::pin(join_once(one_shot())).await,
        DeviceJoinTransportKind::SamePrincipalJoin,
    );
    activated(fixture.drive_owner_with(&bundle, one_shot()).await);

    // From here the owner is done, so nothing the joining device asks for is
    // still on its way and every operation it makes is its own.
    fixture.home.clear_exact_reads();
    joined(Box::pin(join_once(timing())).await);

    let artifacts = fixture
        .home
        .exact_reads()
        .into_iter()
        .filter_map(|slot| {
            slot.logical_key()
                .strip_prefix("store-v1/device-join-transport/")
                .map(|tail| tail.to_string())
        })
        .collect::<Vec<_>>();

    // Six artifacts the exchange moves, the request this resume republishes and
    // the read that confirms its slot, the step waits' first look at the
    // abandonment slot, and the teardown's probe of every slot an attempt can
    // hold before it deletes the ones it finds — which is now one listing and a
    // read per object that is really there, rather than a probe for every name
    // the build knows.
    assert_eq!(
        artifacts.len(),
        9,
        "the joining device made {} artifact operations on a join with no waiting in it: {artifacts:?}",
        artifacts.len(),
    );
    // Every one of those is a step of the exchange. A joining device used to
    // spend the whole join watching a slot for the owner cancelling, which now
    // cannot happen: nothing here is a watch.
    assert!(
        artifacts
            .iter()
            .all(|tail| !tail.ends_with("/cancellation.json")),
        "the joining device watched for a cancellation that cannot come: {artifacts:?}",
    );
}
