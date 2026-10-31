//! What a synced insert costs through coven (§4). The library is built here
//! as it ships, without its test-only statement profile; the same statement
//! and triggers in plain SQLite are measured by `a_triggered_statements_cost`
//! inside the crate. Run explicitly, in the release profile:
//!
//!   cargo test --release -p coven-database --test insert_cost -- --ignored --nocapture --test-threads=1

use coven_database::{
    Database, DatabaseBuilder, FileColumn, Migration, Namespace, Place, Provenance, RowIdentity,
    SyncedTable,
};
use coven_foundation::{
    files::StoreDirectories,
    id_source::{IdSource, SequentialIds, StoreId},
};
use std::time::{Duration, Instant};

/// A scanned folder listed through a trigger that reads a view of six
/// tables, as bae's import list keeps its rows.
const TRIGGERED: &str = "CREATE TABLE scans(id TEXT NOT NULL PRIMARY KEY, path TEXT NOT NULL, title TEXT NOT NULL);
CREATE TABLE edits(id TEXT NOT NULL PRIMARY KEY, title TEXT);
CREATE TABLE verdicts(id TEXT NOT NULL PRIMARY KEY, kind TEXT, position INTEGER);
CREATE TABLE matches(id TEXT NOT NULL, position INTEGER NOT NULL, pressing TEXT, failure TEXT, PRIMARY KEY(id, position));
CREATE TABLE origins(id TEXT NOT NULL PRIMARY KEY, created_at INTEGER);
CREATE TABLE skipped(path TEXT NOT NULL PRIMARY KEY);
CREATE TABLE listing(id TEXT NOT NULL PRIMARY KEY, tab TEXT NOT NULL, state TEXT, title TEXT NOT NULL, unit_path TEXT NOT NULL);
CREATE VIEW placed AS
  SELECT s.id, CASE WHEN o.id IS NOT NULL THEN 'done' WHEN k.path IS NOT NULL THEN 'skipped' ELSE 'pending' END AS tab,
         CASE WHEN v.kind IS NULL THEN 'not_looked_up'
              WHEN (SELECT count(DISTINCT pressing) FROM matches m WHERE m.id=s.id) <> 1 THEN 'several'
              WHEN EXISTS(SELECT 1 FROM matches m WHERE m.id=s.id AND m.failure IS NOT NULL) THEN 'unread'
              WHEN v.kind = 'found' THEN 'identified' ELSE 'needs_you' END AS state,
         coalesce(e.title, s.title) AS title,
         CASE WHEN instr(s.path,'/')>0 THEN substr(s.path,1,instr(s.path,'/')-1) ELSE s.path END AS unit_path
  FROM scans s LEFT JOIN edits e ON e.id=s.id LEFT JOIN verdicts v ON v.id=s.id
  LEFT JOIN origins o ON o.id=s.id LEFT JOIN skipped k ON k.path=s.path;
CREATE TRIGGER scans_listed AFTER INSERT ON scans BEGIN
  INSERT OR REPLACE INTO listing SELECT * FROM placed WHERE id=NEW.id; END;";

const INSERT: &str = "INSERT INTO scans VALUES (?1, ?2, ?3)";
/// Rows per write, and writes measured.
const ROWS: usize = 500;
const WRITES: usize = 40;

fn row(i: usize) -> (String, String, String) {
    (format!("id-{i}"), format!("Artist/Album {i}"), format!("title {i}"))
}

/// The median write's time per row.
fn median(mut times: Vec<Duration>) -> Duration {
    times.sort();
    times[times.len() / 2] / ROWS as u32
}

async fn open(directory: &tempfile::TempDir) -> Database {
    let ids = SequentialIds::new();
    let store = StoreDirectories::new(directory.path().into())
        .create_store_dir(StoreId(ids.new_id()), &ids)
        .unwrap();
    DatabaseBuilder::new(store)
        .synced_tables(vec![SyncedTable::new("scans", RowIdentity::SharedKey)])
        .migrations(vec![Migration::addition(1, "scans", TRIGGERED)])
        .open()
        .await
        .unwrap()
}

/// A library shaped as bae imports into it: releases of albums, each with a
/// medium, tracks and their files. Rows reference their parents, a medium
/// and a track hold a unique position in their release, CHECKs read JSON,
/// each file row references a file coven stores, and a local trigger keeps
/// an FTS5 index of track titles.
const LIBRARY: &str = "CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, created_at TEXT NOT NULL) STRICT;
CREATE TABLE releases(id TEXT NOT NULL PRIMARY KEY, album_id TEXT NOT NULL REFERENCES albums(id) ON DELETE CASCADE,
  album_order REAL NOT NULL, year INTEGER,
  labels TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(labels) AND json_type(labels) = 'array'),
  barcodes TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(barcodes) AND json_type(barcodes) = 'array'),
  country TEXT CHECK (country IS NULL OR (length(country) = 2 AND country = upper(country))),
  created_at TEXT NOT NULL) STRICT;
CREATE INDEX releases_album ON releases(album_id, album_order, id);
CREATE TABLE release_mediums(id TEXT NOT NULL PRIMARY KEY, release_id TEXT NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
  position INTEGER NOT NULL CHECK (position >= 1), side_count INTEGER NOT NULL CHECK (side_count >= 1),
  created_at TEXT NOT NULL, UNIQUE (release_id, position)) STRICT;
CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, release_id TEXT NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
  title TEXT NOT NULL, medium_id TEXT NOT NULL REFERENCES release_mediums(id),
  track_number INTEGER NOT NULL CHECK (track_number >= 1), duration_ms INTEGER NOT NULL CHECK (duration_ms >= 0),
  created_at TEXT NOT NULL, position INTEGER NOT NULL DEFAULT 0 CHECK (position >= 0)) STRICT;
CREATE INDEX tracks_medium ON tracks(medium_id);
CREATE UNIQUE INDEX tracks_release_position ON tracks(release_id, position);
CREATE TABLE release_files(id TEXT NOT NULL PRIMARY KEY, release_id TEXT NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
  original_filename TEXT NOT NULL, file_size INTEGER NOT NULL, content_type TEXT NOT NULL, created_at TEXT NOT NULL,
  file TEXT) STRICT;
CREATE INDEX release_files_release ON release_files(release_id, original_filename, id);
CREATE TABLE track_search_entry(entry INTEGER PRIMARY KEY, track_id TEXT NOT NULL, UNIQUE (track_id)) STRICT;
CREATE VIRTUAL TABLE track_search USING fts5(title, track_id UNINDEXED, tokenize = 'trigram case_sensitive 1');
CREATE TRIGGER track_search_added AFTER INSERT ON tracks BEGIN
  INSERT INTO track_search_entry (track_id) VALUES (NEW.id);
  INSERT INTO track_search (rowid, title, track_id)
  VALUES ((SELECT entry FROM track_search_entry WHERE track_id = NEW.id), lower(NEW.title), NEW.id); END;";

/// Releases per write, and tracks per release, each track with its file.
const RELEASES: usize = 10;
const TRACKS: usize = 12;
/// A release's rows: its album, itself, its medium, and each track's row
/// and file row.
const RELEASE_ROWS: usize = 3 + 2 * TRACKS;

/// The `n`th id of `kind`, a version 4 UUID.
fn uuid(kind: u32, n: usize) -> String {
    format!("{kind:08x}-0000-4000-8000-{n:012x}")
}

async fn open_library(directory: &tempfile::TempDir) -> Database {
    let ids = SequentialIds::new();
    let store = StoreDirectories::new(directory.path().into())
        .create_store_dir(StoreId(ids.new_id()), &ids)
        .unwrap();
    DatabaseBuilder::new(store)
        .synced_tables(vec![
            SyncedTable::new("albums", RowIdentity::SharedKey),
            SyncedTable::new("releases", RowIdentity::IndependentUuid),
            SyncedTable::new("release_mediums", RowIdentity::SharedKey),
            SyncedTable::new("tracks", RowIdentity::IndependentUuid),
            SyncedTable::new("release_files", RowIdentity::SharedKey)
                .file_column(FileColumn::new("file")),
        ])
        .namespaces(vec![Namespace::new("audio", Provenance::AppProvided, 1 << 30)])
        .migrations(vec![Migration::addition(1, "library", LIBRARY)])
        .open()
        .await
        .unwrap()
}

/// Live queries an album's screens keep open while an import runs, each
/// joining tables the import writes: the album's tracks, its files, how
/// many tracks each of its mediums holds through a chain of joins, and its
/// title beside a correlated count of its tracks.
const JOINED: [&str; 4] = [
    "SELECT t.title FROM tracks t JOIN releases r ON t.release_id = r.id WHERE r.album_id = ?1 ORDER BY t.position",
    "SELECT f.original_filename FROM releases r JOIN release_files f ON f.release_id = r.id WHERE r.album_id = ?1 ORDER BY f.original_filename",
    "SELECT count(t.id) FROM releases r JOIN release_mediums m ON m.release_id = r.id LEFT JOIN tracks t ON t.medium_id = m.id WHERE r.album_id = ?1 GROUP BY m.id ORDER BY m.position",
    "SELECT a.title || (SELECT count(*) FROM tracks t WHERE t.release_id = r.id) FROM albums a JOIN releases r ON r.album_id = a.id WHERE a.id = ?1",
];

/// Write releases `releases`, each with its album, medium, and tracks and
/// their file rows, each file row referencing a staged file when `files`
/// is set and none when not.
async fn write_releases(db: &Database, files: bool, releases: std::ops::Range<usize>) {
    let (start, end) = (releases.start, releases.end);
    db.write_with_files::<_, _, _, _, coven_database::DbError>(
        move |batch| {
            Ok((0..(end - start) * TRACKS)
                .map(|_| files.then(|| batch.put_file("audio", vec![7; 64], Place::Cloud).to_string()))
                .collect::<Vec<_>>())
        },
        move |sql, files| {
            for r in start..end {
                let (album, release, medium) = (uuid(1, r), uuid(2, r), uuid(3, r));
                sql.execute(
                    "INSERT INTO albums VALUES (?1, ?2, '2026-10-30')",
                    (&album, format!("album {r}")),
                )?;
                sql.execute(
                    "INSERT INTO releases(id,album_id,album_order,year,labels,barcodes,country,created_at)
                     VALUES (?1, ?2, 0, 1999, '[{\"name\":\"label\"}]', '[\"0123\"]', 'GB', '2026-10-30')",
                    (&release, &album),
                )?;
                sql.execute(
                    "INSERT INTO release_mediums VALUES (?1, ?2, 1, 1, '2026-10-30')",
                    (&medium, &release),
                )?;
                for t in 0..TRACKS {
                    let n = r * TRACKS + t;
                    sql.execute(
                        "INSERT INTO tracks VALUES (?1, ?2, ?3, ?4, ?5, 200000, '2026-10-30', ?6)",
                        (uuid(4, n), &release, format!("track {n}"), &medium, t as i64 + 1, t as i64),
                    )?;
                    sql.execute(
                        "INSERT INTO release_files VALUES (?1, ?2, ?3, 64, 'audio/flac', '2026-10-30', ?4)",
                        (
                            uuid(5, n),
                            &release,
                            format!("{t:02} track.flac"),
                            &files[(r - start) * TRACKS + t],
                        ),
                    )?;
                }
            }
            Ok(())
        },
    )
    .await
    .unwrap();
}

/// The median write's time per row of `RELEASES` releases a write, each
/// file row referencing a staged file when `files` is set and none when
/// not. With `live`, the `JOINED` queries watch an album written first,
/// which no measured write changes: every write judges its rows for each
/// of them, and none runs again.
async fn import(files: bool, live: bool) -> Duration {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let directory = tempfile::tempdir().unwrap();
    let db = open_library(&directory).await;
    let runs = std::sync::Arc::new(AtomicUsize::new(0));
    let mut queries = Vec::new();
    if live {
        let watched = WRITES * RELEASES;
        write_releases(&db, files, watched..watched + 1).await;
        for sql in JOINED {
            let (runs, album) = (runs.clone(), uuid(1, watched));
            let mut query = db.subscribe(move |read| {
                runs.fetch_add(1, Ordering::SeqCst);
                Ok(read.query(sql, [&album], |r| r.get::<_, coven_database::types::Value>(0))?)
            });
            assert!(!query.next().await.unwrap().unwrap().is_empty(), "{sql}");
            queries.push(query);
        }
    }
    let mut times = Vec::new();
    for write in 0..WRITES {
        let started = Instant::now();
        write_releases(&db, files, write * RELEASES..(write + 1) * RELEASES).await;
        times.push(started.elapsed());
    }
    // A query the writes marked would run again as it's asked for its next
    // result.
    let ran = runs.load(Ordering::SeqCst);
    for query in &mut queries {
        assert!(tokio::time::timeout(Duration::from_millis(100), query.next()).await.is_err());
    }
    assert_eq!(runs.load(Ordering::SeqCst), ran);
    drop(queries);
    let indexed: i64 = db
        .read(|sql| {
            Ok(sql.query_row(
                "SELECT count(*) FROM track_search WHERE track_search MATCH 'track'",
                [],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(indexed, (WRITES * RELEASES * TRACKS + if live { TRACKS } else { 0 }) as i64);
    db.close().await.unwrap();
    times.sort();
    times[times.len() / 2] / (RELEASES * RELEASE_ROWS) as u32
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "one timing measurement, run explicitly in the release profile"]
async fn a_release_imports_cost() {
    eprintln!(
        "per row of a release, {} rows a write, no files: {:?}",
        RELEASES * RELEASE_ROWS,
        import(false, false).await
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "one timing measurement, run explicitly in the release profile"]
async fn a_release_imports_cost_with_joined_live_queries_open() {
    eprintln!(
        "per row of a release, {} rows a write, no files, {} joined live queries open: {:?}",
        RELEASES * RELEASE_ROWS,
        JOINED.len(),
        import(false, true).await
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "one timing measurement, run explicitly in the release profile"]
async fn a_release_with_files_imports_cost() {
    eprintln!(
        "per row of a release, {} rows a write, a file staged per track: {:?}",
        RELEASES * RELEASE_ROWS,
        import(true, false).await
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "one timing measurement, run explicitly in the release profile"]
async fn a_synced_inserts_cost() {
    let directory = tempfile::tempdir().unwrap();
    let db = open(&directory).await;
    let mut coven_times = Vec::new();
    for write in 0..WRITES {
        let started = Instant::now();
        db.write(move |sql| {
            for i in write * ROWS..(write + 1) * ROWS {
                sql.execute(INSERT, row(i))?;
            }
            Ok(())
        })
        .await
        .unwrap();
        coven_times.push(started.elapsed());
    }
    let listed: i64 = db
        .read(|sql| Ok(sql.query_row("SELECT count(*) FROM listing", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(listed, (ROWS * WRITES) as i64);
    db.close().await.unwrap();
    eprintln!("per synced insert, committed in writes of {ROWS}: {:?}", median(coven_times));
}
