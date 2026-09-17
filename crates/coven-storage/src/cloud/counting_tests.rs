//! What the counting home counts, and what it must not.

use super::counting::CountingCloudHome;
use super::test_utils::InMemoryCloudHome;
use super::{create_exact_bytes, CloudHome, ExactCloudHome, UploadProgress};
use coven_foundation::stage_timing::{ProviderRequests, StageTimings};
use coven_protocol::objects::ObjectSlot;
use std::sync::Arc;

fn counting_home() -> (Arc<dyn ExactCloudHome>, Arc<dyn ProviderRequests>) {
    let home: Arc<dyn ExactCloudHome> =
        Arc::new(CountingCloudHome::new(Arc::new(InMemoryCloudHome::new())));
    let requests = home
        .provider_requests()
        .expect("a counting home reports its counter");
    (home, requests)
}

fn no_progress() -> UploadProgress {
    Arc::new(|_| {})
}

fn counts(timings: &StageTimings) -> String {
    timings
        .counted_stages()
        .map(|(name, requests)| format!("{name} {requests}req"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The choreography a device join's stages are made of, counted stage by stage.
///
/// This is the assertion the whole mechanism exists for: a run's line has to
/// say which stage spent the operations, so a stage over the join budget — one
/// snapshot download and a handful of small operations — convicts itself
/// instead of inviting another round of instrumentation. Every count below is
/// what this choreography asks for, so a change that adds a round trip to one
/// of these stages breaks this test rather than quietly costing a live join.
#[tokio::test]
async fn a_join_choreography_reports_its_operations_by_stage() {
    let (home, requests) = counting_home();
    let mut slots = std::collections::HashMap::new();
    for (key, bytes) in [
        ("root", b"root".to_vec()),
        ("founder", b"founder".to_vec()),
        ("snapshot", vec![0_u8; 64]),
    ] {
        let slot = home.allocate_slot(key).await.unwrap();
        create_exact_bytes(home.as_ref(), &slot, &bytes, &no_progress())
            .await
            .unwrap();
        slots.insert(key, slot);
    }
    let mut timings = StageTimings::counting("device join", Some(requests));

    // Pin the Store root: one read of the root, one of the founder behind it.
    timings
        .stage("pin the Store root", async {
            home.read_at(&slots["root"]).await.unwrap();
            home.read_at(&slots["founder"]).await.unwrap();
        })
        .await;

    // Walk the membership chain: a read per entry, ending on the one that
    // misses. This is the stage that grows with the store's history, and the
    // count is what says so.
    timings
        .stage("walk the membership chain", async {
            for entry in 0..3 {
                let slot = ObjectSlot::logical(format!("membership/{entry}")).unwrap();
                let _ = home.read_at(&slot).await;
            }
        })
        .await;

    // Download the snapshot: one operation however many bytes it moves. A
    // stage whose count is one and whose time is large is a transfer problem,
    // which is exactly what telling the two apart is for.
    timings
        .stage("download the snapshot", async {
            home.read_at(&slots["snapshot"]).await.unwrap();
        })
        .await;

    // Installing it is local work over bytes already in hand.
    timings.mark("install the snapshot", || {});

    assert_eq!(
        counts(&timings),
        "pin the Store root 2req, walk the membership chain 3req, \
         download the snapshot 1req, install the snapshot 0req",
    );
}

/// Operations between the stages belong to no stage, and the run's total says
/// so — the same way its wall time already exceeds the sum of its stages.
#[tokio::test]
async fn the_run_total_exceeds_its_stages_by_what_they_did_not_name() {
    let (home, requests) = counting_home();
    let mut timings = StageTimings::counting("device join", Some(Arc::clone(&requests)));
    let root = ObjectSlot::logical("root".to_string()).unwrap();
    let stray = ObjectSlot::logical("stray".to_string()).unwrap();

    timings
        .stage("pin the Store root", async {
            let _ = home.read_at(&root).await;
        })
        .await;
    let _ = home.read_at(&stray).await;

    assert_eq!(counts(&timings), "pin the Store root 1req");
    assert_eq!(requests.issued(), 2, "the stray read is still the home's");
}

/// Naming a slot, looking at it, and listing for it are each one thing asked
/// of the provider, and the counter says three — a decorator that inherited a
/// trait default instead of forwarding it would lose the operation it skipped.
#[tokio::test]
async fn every_exact_operation_counts() {
    let (home, requests) = counting_home();

    let slot = home.allocate_slot("commit/1").await.unwrap();
    home.observe_at(&slot).await.unwrap();
    home.list_slots("commit/").await.unwrap();

    assert_eq!(requests.issued(), 3);
}

/// An unwrapped home answers `None`, which is what keeps a run over it
/// reporting its times alone instead of a column of zeroes nobody measured.
#[test]
fn an_unwrapped_home_counts_nothing() {
    assert!(CloudHome::provider_requests(&InMemoryCloudHome::new()).is_none());
}
