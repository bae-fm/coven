// Helpers of the read rule's fixture that a later crate's read code calls
// (`helped.rs`). Read code of this crate reaches only `oldest_first`, so this
// crate's compile finds the work of that one alone, on the line that ends in
// a comment naming its lint. It records what read code would find through
// the others, and the later crate's compile finds that at each call.

use reads_base::{Row, SqlRead, SqlReadContext};

#[derive(Clone)]
pub struct Album {
    pub id: i64,
    pub year: i64,
}

/// The row mapper, which makes `Album` a row type.
pub fn album(row: &Row) -> Result<Album, ()> {
    Ok(Album { id: row.get(0), year: row.get(1) })
}

/// Sorts a read's rows through a function the crate keeps to itself.
pub fn newest_first(mut albums: Vec<Album>) -> Vec<Album> {
    by_year(&mut albums);
    albums
}

fn by_year(albums: &mut [Album]) {
    albums.sort_by_key(|album| album.year);
}

/// Runs statements through a read value it holds: its inputs name no read
/// type, so it is no read code here.
pub struct Years<'a> {
    pub sql: &'a SqlReadContext,
}

impl Years<'_> {
    pub fn of_each(&self, ids: &[i64]) -> Vec<Option<i64>> {
        ids.iter().map(|id| self.sql.query_row("SELECT year", |row| Ok(row.get(0) + id))).collect()
    }

    pub fn of_first(&self) -> Option<i64> {
        let first: i64 = self.sql.query_row("SELECT id", |row| Ok(row.get(0)))?;
        self.sql.query_row(&format!("SELECT year WHERE id = {first}"), |row| Ok(row.get(0)))
    }
}

/// Read code of this crate.
pub fn read_oldest(sql: &impl SqlRead) -> Vec<Album> {
    oldest_first(sql.query("SELECT id, year FROM albums", album))
}

/// Read code of this crate reaches it, so this crate finds its work, and a
/// later crate's call of it finds nothing more.
pub fn oldest_first(mut albums: Vec<Album>) -> Vec<Album> {
    albums.sort_by_key(|album| album.year); // read_sort
    albums
}
