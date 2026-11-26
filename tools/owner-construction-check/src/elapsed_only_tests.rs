use std::collections::BTreeSet;

use crate::syntax::RustFile;

fn kinds(source: &str) -> BTreeSet<String> {
    let file = RustFile::fixture("crates/app-sync/src/timing.rs", source);
    let policy = crate::gated_policy();
    let code = crate::crate_code::CrateCode::fixture(&[file], &[], &policy);
    crate::capability_boundaries::find_capability_boundary_violations(&code, &policy)
        .into_iter()
        .map(|finding| finding.message)
        .collect()
}

#[test]
fn a_duration_that_only_logs_or_counts_is_no_use_of_the_clock() {
    for source in [
        "fn run() { let started = Instant::now(); work(); log(started.elapsed()); }",
        "fn run() { let started = std::time::Instant::now(); work(); tracing::debug!(took = ?started.elapsed()); }",
        "fn run() -> Duration { Instant::now().elapsed() }",
        "fn run() { let a = Instant::now(); let b = Instant::now(); (a.elapsed(), b.elapsed()); }",
    ] {
        assert!(kinds(source).is_empty(), "{source}");
    }
}

#[test]
fn an_instant_that_reaches_anything_but_elapsed_is_a_use_of_the_clock() {
    for source in [
        "fn run() { let deadline = Instant::now(); store(deadline); }",
        "fn run() { let started = Instant::now(); if started > other { stop(); } }",
        "fn run() { let started = Instant::now(); work(); log(started.elapsed()); keep(started); }",
        "fn run() { let started = Instant::now(); tracing::debug!(?started); }",
        "fn run() -> Instant { Instant::now() }",
        "fn run() { Holder { at: Instant::now() }; }",
        "fn run() { let _ = Instant::now() < other; }",
        "fn run() { let started = SystemTime::now(); started.elapsed(); }",
    ] {
        assert_eq!(kinds(source).len(), 1, "{source}");
    }
}
