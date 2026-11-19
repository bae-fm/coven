// The crate the read rule's lints check under the policy of this folder:
// each line that holds a finding ends in a comment naming its lints.

use std::collections::HashMap;

use reads_base::{FromColumn, LiveQuery, Row, SqlRead, SqlReadContext};

#[derive(Clone)]
pub struct Album {
    pub id: i64,
    pub year: i64,
    pub cover: Option<String>,
}

/// The row mapper, which makes `Album` a row type.
fn album(row: &Row) -> Result<Album, ()> {
    Ok(Album { id: row.get(0), year: row.get(1), cover: None })
}

const KINDS: [(&str, u8); 2] = [("lp", 1), ("cd", 2)];

/// Albums of its own, whose `filter` is no library method.
pub struct Albums(Vec<Album>);

impl Albums {
    pub fn filter(&self, year: i64) -> bool {
        year > 0 && self.0.capacity() > 0
    }
}

pub fn read_albums(read: &SqlReadContext, at: usize) -> Vec<Album> {
    let mut albums = read.query("SELECT id, year FROM albums", album);
    albums.sort_by_key(|album| album.year); // read_sort
    albums.retain(|album| album.year > 0); // read_filter
    let _newest = albums.iter().max_by_key(|album| album.year); // read_aggregate
    let _found = albums.iter().any(|album| album.id == 1); // read_pick
    let _many = albums.len() > 2; // read_length_branch
    let _count = albums.len(); // read_aggregate
    let _by_year: HashMap<i64, Vec<&Album>> = HashMap::new(); // read_group
    let _first = if let [first, ..] = albums.as_slice() { first.id } else { 0 }; // read_length_branch
    let mut total = 0;
    for album in &albums {
        total += album.year; // read_aggregate
    }
    let _ = total;
    let _joined = albums.get(at).is_some(); // read_join
    let _cover = albums.first().cloned().and_then(|album| album.cover.map(|cover| cover.len())); // read_drop_row
    #[expect(clippy::read_sort, reason = "the fixture's mark")]
    let _marked = {
        let mut copy = albums.clone();
        copy.sort_by_key(|album| album.id);
        copy
    };
    let _words = "a b".split(' ').filter(|word| !word.is_empty()).count();
    let _kind = KINDS.iter().find(|(name, _)| *name == "lp");
    let _title =
        albums.first().and_then(|album| album.cover.clone()).filter(|cover| !cover.is_empty());
    let _own = Albums(Vec::new()).filter(1);
    albums
}

/// Sorts what `read_titles` read: read code reaches it.
fn sorted(mut albums: Vec<Album>) -> Vec<Album> {
    albums.sort_by_key(|album| album.id); // read_sort
    albums
}

pub fn read_titles(read: &SqlReadContext) -> Vec<Album> {
    sorted(read.query("SELECT id, year FROM albums", album))
}

/// No read code reaches it.
pub fn elsewhere(mut albums: Vec<Album>) -> Vec<Album> {
    albums.sort_by_key(|album| album.id);
    albums
}

/// Only a live query's `process` runs it.
fn shown(mut albums: Vec<Album>) -> usize {
    albums.sort_by_key(|album| album.year);
    albums.len()
}

pub fn watch(query: LiveQuery<Vec<Album>>) -> usize {
    query.process(shown)
}

pub fn watch_years(query: LiveQuery<Vec<Album>>) -> usize {
    query.process(|albums| albums.iter().filter(|album| album.year > 0).count())
}

/// Holds a live query: its methods read what the query read.
pub struct Shelf {
    query: LiveQuery<Vec<Album>>,
}

impl Shelf {
    pub fn newest(&self) -> Option<i64> {
        self.query.latest().iter().map(|album| album.year).max() // read_aggregate
    }
}

fn detail_of(query: &LiveQuery<Vec<Album>>, resolve: impl Fn(&Vec<Album>) -> usize) -> usize {
    resolve(query.latest())
}

pub fn run(query: &LiveQuery<Vec<Album>>) -> usize {
    detail_of(query, |albums| albums.iter().filter(|album| album.year > 0).count()) // read_filter read_aggregate
}

/// Reads through the read trait, with closures for its row mappers: a
/// tuple of columns is a row type, and the rows of a read of one column are
/// the collections that flow from what the read returns.
pub fn read_ids(sql: &impl SqlRead, mut wanted: Vec<i64>) -> Vec<(i64, i64)> {
    let mut pairs = sql.query("SELECT id, year FROM albums", |row| Ok((row.get(0), row.get(1))));
    pairs.sort(); // read_sort
    let ids = sql.query("SELECT id FROM albums", |row| Ok(row.get(0)));
    let mut kept = ids;
    kept.sort(); // read_sort
    let _known = kept.iter().filter(|id| **id > 0).count(); // read_filter read_aggregate
    let [_first, ..] = kept.as_slice() else { return pairs }; // read_length_branch
    let _any = matches!(kept.as_slice(), [_, ..]); // read_length_branch
    let count = sql.query_row("SELECT count(*) FROM albums", |row| Ok(row.get(0)));
    let _many = count.is_some_and(|count: i64| count > 2);
    wanted.sort();
    let _wanted = wanted.len();
    pairs
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct Key(i64);

impl FromColumn for Key {
    fn from_column(value: i64) -> Self {
        Key(value)
    }
}

/// Maps each row to a key, one column's value, through a generic column
/// read; keys the read did not return are no rows.
pub fn read_keys(sql: &impl SqlRead, mut known: Vec<Key>) -> Result<Vec<Key>, ()> {
    let mut keys = sql.query("SELECT id FROM albums", |row| row.column::<Key>(0));
    keys.sort(); // read_sort
    known.sort();
    Ok(keys)
}
