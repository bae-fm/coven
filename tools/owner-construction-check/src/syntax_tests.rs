use super::*;

#[test]
fn test_sources_are_exactly_sibling_test_files_and_integration_tests() {
    for test_source in [
        "crates/coven-sync/src/pull_tests.rs",
        "crates/coven-sync/src/sync/loop_tests.rs",
        "crates/coven-sync/tests/two_devices.rs",
        "crates/coven-sync/tests/support/fixture.rs",
        "tools/owner-construction-check/src/main_tests.rs",
    ] {
        assert!(is_test_source(test_source), "{test_source}");
    }
    for production in [
        "crates/coven-database/src/test_support/synthetic_store.rs",
        "crates/coven-sync/src/test_helpers.rs",
        "crates/coven-sync/src/tests.rs",
        "crates/coven-sync/src/test_owner_graph.rs",
        "crates/coven-sync/src/pull_tests/case.rs",
        "crates/coven-sync/src/sync/tests/case.rs",
        "crates/coven-sync/src/pull_test.rs",
    ] {
        assert!(!is_test_source(production), "{production}");
    }
}

#[test]
fn only_items_that_compile_solely_into_tests_are_test_only() {
    let item = |source: &str| syn::parse_str::<syn::ItemFn>(source).expect("parse fixture item");
    for test_only in [
        "#[test] fn f() {}",
        "#[tokio::test] async fn f() {}",
        "#[cfg(test)] fn f() {}",
        "#[cfg(all(test, unix))] fn f() {}",
        "#[cfg(any(test, all(test, unix)))] fn f() {}",
    ] {
        assert!(is_test_only(&item(test_only).attrs), "{test_only}");
    }
    for production in [
        "fn f() {}",
        "#[cfg(not(test))] fn f() {}",
        "#[cfg(feature = \"test-utils\")] fn f() {}",
        "#[cfg(any(test, feature = \"test-utils\"))] fn f() {}",
        "#[cfg(unix)] fn f() {}",
    ] {
        assert!(!is_test_only(&item(production).attrs), "{production}");
    }
}
