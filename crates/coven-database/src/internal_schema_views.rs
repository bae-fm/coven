//! The views a live query reads whole, and which of coven's objects app SQL may read.
//! Their SQL is in coven-schema.

/// The views whose rows also change with what this process keeps in memory:
/// the transfers in flight, the device-only copies and the network
/// (`memory_facts.rs`). A live query reads each whole. It narrows every other
/// view through the view's own SELECT, those that read these ones included.
const MEMORY_VIEWS: [&str; 6] = [
    "coven_uploads",
    "coven_sending",
    "coven_downloading",
    "coven_device_only_downloading",
    "coven_device_only_placed",
    "coven_network",
];

/// The views a live query reads whole (`MEMORY_VIEWS`).
pub(crate) fn whole_views() -> std::sync::Arc<sqlive::WholeViews> {
    std::sync::Arc::new(sqlive::WholeViews::new(MEMORY_VIEWS))
}

/// Whether `name` is one of coven's views or tables app SQL may read.
pub(crate) fn app_readable(name: &str) -> bool {
    coven_schema::READABLE.iter().any(|object| object.eq_ignore_ascii_case(name))
}

#[cfg(test)]
#[path = "internal_schema_views_tests.rs"]
mod tests;
