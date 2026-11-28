// The crate the read rule's lints check under the policy of this folder:
// each line that holds a finding ends in a comment naming its lints.

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Mutex;

use reads_base::{
    FromColumn, LiveQuery, LiveRequest, Returning, Row, SqlRead, SqlReadContext, ValueStream,
};

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
    let _many = albums.len() > 2; // read_length_branch read_cutoff
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
    let _many = count.is_some_and(|count: i64| count > 2); // read_cutoff
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

/// Runs statements for each row, and statements keyed by what another
/// returned.
pub fn read_each(sql: &impl SqlRead, ids: &[i64]) -> i64 {
    let albums = sql.query("SELECT id, year FROM albums", album);
    for album in &albums {
        let _ = sql.query_row("SELECT 1", |row| Ok(row.get(0) + album.id)); // read_query_per_row
    }
    let _years = ids.iter().map(|id| sql.query_row("SELECT 2", |row| Ok(row.get(0) + id))); // read_query_per_row
    let _nested =
        sql.query("SELECT id", |row| sql.query_row("SELECT 3", |_| Ok(row.get(0))).ok_or(())); // read_query_per_row
    let first = albums.first().map(|album| album.id).unwrap_or(0);
    let _ = sql.query_row(&format!("SELECT {first}"), |row| Ok(row.get(0))); // read_keyed_statement
    let _ = year_of(sql, first); // read_keyed_statement
    let _year = albums.first().map(|album| year_of(sql, album.id)); // read_keyed_statement
    let _ = sql.query_row("SELECT 4", |row| Ok(row.get(0) + first));
    let _ = sql.query_row("SELECT 5", |row| Ok(row.get(0)));
    year_of(sql, ids.len() as i64)
}

fn year_of(sql: &impl SqlRead, id: i64) -> i64 {
    sql.query_row("SELECT year", |row| Ok(row.get(0) + id)).unwrap_or(0)
}

/// Holds a live query and state beside it.
pub struct Panel {
    query: LiveQuery<Vec<Album>>,
    shown: Mutex<usize>, // read_kept_state
    title: String,
}

impl Panel {
    pub fn show(&mut self) -> usize {
        self.title = String::new(); // read_kept_state
        self.title.push('a'); // read_kept_state
        self.query.latest().capacity()
    }

    pub async fn clear(&mut self) {
        self.title.clear(); // read_kept_state
    }
}

/// Holds a value stream and the value it delivered last.
pub struct Progress {
    stream: ValueStream<u64>,
    last: Cell<u64>, // read_kept_state
}

/// Keeps a count it reads through, beside no live query.
pub struct Counter {
    count: Cell<usize>,
}

impl Counter {
    pub fn read_count_of_every_album(
        &self, // read_kept_state
        sql: &SqlReadContext,
    ) -> usize {
        let ids = sql.query("SELECT id FROM albums", |row| Ok(row.get(0)));
        self.count.get() + ids.capacity()
    }
}

static SEEN: Mutex<usize> = Mutex::new(0);

pub fn read_seen(read: &SqlReadContext) -> usize {
    let albums = read.query("SELECT id, year FROM albums", album);
    let seen = SEEN.lock().map(|seen| *seen).unwrap_or(0); // read_kept_state
    let _over = albums.capacity() > LIMIT; // read_cutoff
    let _few = albums.capacity() < limit(); // read_cutoff
    let _some = albums.capacity() > 0;
    seen
}

const LIMIT: usize = 50;

fn limit() -> usize {
    LIMIT * 2
}

pub fn count_deliveries(query: &LiveQuery<Vec<Album>>) {
    let mut total = 0;
    query.subscribe(move |_| {
        total += 1; // read_kept_state
    });
}

/// What a live query is asked.
pub struct Request {
    pub generation: u64,
    pub name: String,
}

pub fn rerun(query: &mut LiveRequest<Request>, mut request: Request) {
    request.generation += 1; // read_request_bump
    request.generation = request.generation.wrapping_add(1); // read_request_bump
    let next = Request { generation: request.generation + 1, name: request.name }; // read_request_bump
    query.ask(next);
}

/// A screen's object, which holds a live query and converts each value it
/// pulls to its bridge type: by a function path or by a closure, as a
/// bridge macro's `converted by` line names either.
pub struct Pulled {
    query: LiveQuery<Vec<Album>>,
}

/// A bridge type, converted from what a read returned.
pub struct Shown(Vec<Album>);

impl Shown {
    fn from_core(mut albums: Vec<Album>) -> Self {
        albums.sort_by_key(|album| album.year); // read_sort
        Shown(albums)
    }
}

impl Pulled {
    pub fn next_by_path(&self) -> Shown {
        (Shown::from_core)(self.query.latest().clone())
    }

    pub fn next_by_closure(&self) -> Shown {
        (|mut albums: Vec<Album>| {
            albums.sort_by_key(|album| album.id); // read_sort
            Shown(albums)
        })(self.query.latest().clone())
    }
}

/// What a statement macro generates for a statement of singles: its request,
/// its row type and the row's mapper.
pub struct SinglesParams;

/// A row of the singles statement.
pub struct SinglesRow {
    pub id: i64,
    pub year: i64,
}

impl Returning for SinglesParams {
    type Row = SinglesRow;

    fn read(row: &Row) -> Result<SinglesRow, ()> {
        Ok(SinglesRow { id: row.get(0), year: row.get(1) })
    }
}

/// A single, which read code makes of each row of the singles statement.
pub struct Single {
    pub id: i64,
    pub year: i64,
}

/// Reads the singles through their generated statement: its rows, and the
/// singles made of them, are a read's rows.
pub fn read_singles(read: &SqlReadContext) -> Vec<Single> {
    let mut rows = read.statement(&SinglesParams);
    rows.retain(|row| row.year > 0); // read_filter
    let mut singles: Vec<Single> =
        rows.into_iter().map(|row| Single { id: row.id, year: row.year }).collect();
    singles.retain(|single| single.id > 0); // read_filter
    singles
}

/// Keeps the recent singles of what it is given: read code passes it the
/// singles it made of a read's rows.
fn recent(mut singles: Vec<Single>) -> Vec<Single> {
    singles.retain(|single| single.year > 1); // read_filter
    singles
}

pub fn read_recent(read: &SqlReadContext) -> Vec<Single> {
    recent(
        read.statement(&SinglesParams)
            .into_iter()
            .map(|row| Single { id: row.id, year: row.year })
            .collect(),
    )
}

/// Singles a caller asks about, and singles made of other values, are no
/// read's rows, though singles are made of a read's rows elsewhere.
pub fn read_known(read: &SqlReadContext, asked: &[Single], ids: &[i64]) -> usize {
    let _asked = asked.iter().filter(|single| single.year > 0).count();
    let made: Vec<Single> = ids.iter().map(|id| Single { id: *id, year: 0 }).collect();
    let _made = made.iter().any(|single| single.id == 1);
    read.statement(&SinglesParams).capacity()
}
