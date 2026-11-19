// Read code that calls the earlier crates' helpers (`helpers.rs`, `relay.rs`):
// each line that calls or passes on a helper whose record holds findings
// ends in a comment naming their lints.

use reads_base::{LiveQuery, SqlReadContext};
use reads_helpers::{Album, Years, album, newest_first, oldest_first};
use reads_relay::newest;

pub fn read_newest(read: &SqlReadContext) -> Vec<Album> {
    newest_first(read.query("SELECT id, year FROM albums", album)) // read_sort
}

pub fn read_years(read: &SqlReadContext, ids: &[i64]) -> Vec<Option<i64>> {
    let years = Years { sql: read };
    let _first = years.of_first(); // read_keyed_statement
    years.of_each(ids) // read_query_per_row
}

/// Calls a helper of the helpers' crate through the relay's crate.
pub fn read_relayed(read: &SqlReadContext) -> Vec<Album> {
    newest(read.query("SELECT id, year FROM albums", album)) // read_sort
}

/// Passes a helper on to run on what the read returned.
pub fn read_passed(read: &SqlReadContext) -> Vec<Album> {
    Some(read.query("SELECT id, year FROM albums", album)).map(newest_first).unwrap_or_default() // read_sort
}

/// The helpers' crate finds the work of `oldest_first`, which its own read
/// code reaches.
pub fn read_oldest(read: &SqlReadContext) -> Vec<Album> {
    oldest_first(read.query("SELECT id, year FROM albums", album))
}

/// A mark at the call holds what the helper does.
pub fn read_marked(read: &SqlReadContext) -> Vec<Album> {
    #[expect(clippy::read_sort, reason = "the fixture's mark")]
    let albums = newest_first(read.query("SELECT id, year FROM albums", album));
    albums
}

/// No read code: it calls the helper on albums no read here returned.
pub fn sort_given(albums: Vec<Album>) -> Vec<Album> {
    newest_first(albums)
}

/// No read code: only a live query's `process` runs the helper.
pub fn watch(query: LiveQuery<Vec<Album>>) -> Vec<Album> {
    query.process(newest_first)
}
