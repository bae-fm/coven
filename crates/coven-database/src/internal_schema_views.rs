//! The views a live query reads whole, and which of coven's objects app SQL may read.
//! Their SQL is in coven-schema.

/// The views a live query reads whole: those whose rows also change with
/// what this process keeps in memory (`memory_facts.rs`). It narrows every
/// other view through the view's own SELECT, those that read these ones
/// included.
pub(crate) fn whole_views() -> std::sync::Arc<sqlive::WholeViews> {
    std::sync::Arc::new(sqlive::WholeViews::new(crate::memory_facts::views()))
}

/// Whether `name` is one of coven's views or tables app SQL may read.
pub(crate) fn app_readable(name: &str) -> bool {
    coven_schema::READABLE.iter().any(|object| object.eq_ignore_ascii_case(name))
}

#[cfg(test)]
#[path = "internal_schema_views_tests.rs"]
mod tests;
