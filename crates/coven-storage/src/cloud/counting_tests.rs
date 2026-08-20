//! What the counting home counts, and what it must not.

use super::counting::CountingCloudHome;
use super::test_utils::InMemoryCloudHome;
use super::{BlobBody, CloudHome, ExactCloudHome, UploadProgress};
use coven_foundation::stage_timing::{ProviderRequests, StageTimings};
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
    home.put_object("root", b"root".to_vec()).await.unwrap();
    home.put_object("founder", b"founder".to_vec())
        .await
        .unwrap();
    home.put_object("snapshot", vec![0_u8; 64]).await.unwrap();
    let mut timings = StageTimings::counting("device join", Some(requests));

    // Pin the Store root: one read of the root, one of the founder behind it.
    timings
        .stage("pin the Store root", async {
            home.read("root").await.unwrap();
            home.read("founder").await.unwrap();
        })
        .await;

    // Walk the membership chain: a read per entry, ending on the one that
    // misses. This is the stage that grows with the store's history, and the
    // count is what says so.
    timings
        .stage("walk the membership chain", async {
            for entry in 0..3 {
                let _ = home.read(&format!("membership/{entry}")).await;
            }
        })
        .await;

    // Download the snapshot: one operation however many bytes it moves. A
    // stage whose count is one and whose time is large is a transfer problem,
    // which is exactly what telling the two apart is for.
    timings
        .stage("download the snapshot", async {
            home.read("snapshot").await.unwrap();
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

    timings
        .stage("pin the Store root", async {
            let _ = home.read("root").await;
        })
        .await;
    let _ = home.read("stray").await;

    assert_eq!(counts(&timings), "pin the Store root 1req");
    assert_eq!(requests.issued(), 2, "the stray read is still the home's");
}

/// A getter is not a request. `multipart_threshold` decides how a write will be
/// shaped and touches no provider, so counting it would inflate every upload
/// stage by one and make the budget unreadable.
#[tokio::test]
async fn asking_the_home_about_itself_costs_nothing() {
    let (home, requests) = counting_home();

    assert!(home.multipart_threshold() > 0);
    assert_eq!(requests.issued(), 0, "a getter is not a request");

    home.write("blob", BlobBody::from_bytes(vec![0_u8; 8]), &no_progress())
        .await
        .unwrap();
    assert_eq!(
        requests.issued(),
        1,
        "one write is one operation however the provider shapes it underneath",
    );
}

/// The slot side of the home counts too. Both traits cross the same provider
/// boundary, so a caller working in slots must not be invisible to a budget
/// written against a caller working in keys.
#[tokio::test]
async fn slot_operations_count_the_same_as_key_operations() {
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
