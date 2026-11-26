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
        "fn run() { let started = Instant::now(); work(); tracing::info!(took = ?started.elapsed()); }",
        "fn run() { let started = std::time::Instant::now(); work(); tracing::debug!(took = ?started.elapsed()); }",
        "fn run() { let started = Instant::now(); let took = started.elapsed(); tracing::debug!(?took); }",
        "fn run() { let started = Instant::now(); count.time += started.elapsed(); }",
        "fn run() { let a = Instant::now(); let b = Instant::now(); tracing::debug!(?a, took = ?b.elapsed()); let c = a.elapsed(); tracing::debug!(?c); }",
    ] {
        assert!(kinds(source).is_empty(), "{source}");
    }
}

#[test]
fn an_instant_that_reaches_anything_but_elapsed_is_a_use_of_the_clock() {
    for source in [
        "fn run() { let deadline = Instant::now(); store(deadline); }",
        "fn run() { let started = Instant::now(); if started > other { stop(); } }",
        "fn run() { let started = Instant::now(); work(); keep(started); }",
        "fn run() { let started = Instant::now(); log(started.elapsed()); }",
        "fn run() -> Instant { Instant::now() }",
        "fn run() { Holder { at: Instant::now() }; }",
        "fn run() { let _ = Instant::now() < other; }",
        "fn run() { let started = Instant::now(); let took = started.elapsed(); if took > limit { stop(); } }",
        "fn run() { let started = Instant::now(); keep(started.elapsed()); }",
        "fn run() { let started = Instant::now(); Holder { at: started.elapsed() }; }",
        "fn run() { let started = Instant::now(); assert!(started.elapsed() < limit); }",
        "fn run() { let _ = Instant::now().elapsed() > limit; }",
        "fn run() { let started = SystemTime::now(); started.elapsed(); }",
        "fn run() { let started = Instant::now(); if started.elapsed() > limit { stop(); } }",
        "fn run() { let started = Instant::now(); while started.elapsed() < limit { work(); } }",
        "fn run() { let started = Instant::now(); match started.elapsed().as_secs() { 0 => {} _ => stop() } }",
    ] {
        assert_eq!(kinds(source).len(), 1, "{source}");
    }
}
