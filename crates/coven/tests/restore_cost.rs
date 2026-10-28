//! What restoring a library-sized store on a new device costs locally, with
//! in-memory storage, so every millisecond is coven's own work: key
//! derivation, decryption, SQLite and the app's triggers. Run explicitly, in
//! the release profile, with coven's info lines:
//!
//!   RUST_LOG=info cargo test --release -p coven --features test-utils \
//!     --test restore_cost -- --ignored --nocapture --test-threads 1
//!
//! The store holds a music library's rows: 3,000 releases, 39,000 tracks and
//! 39,000 files, written as an import writes them, ten releases a write, with
//! a sync pass after each, then 200 small edits. One run gives the tables
//! listing triggers and an index each, as an app's device-local projections
//! have; the other has none, so the difference is the triggers' cost. Each
//! runs with a floor, as on a store whose snapshots are a week old, and
//! without one, as on a younger store.
#![cfg(feature = "test-utils")]

use coven::{
    Clock, CovenHandle, MemoryStorage, Migration, RowIdentity, SecretText, Storage, StoreLayout,
    SyncStatus, SyncedTable, TestCoven,
};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const RELEASES: usize = 3_000;
const TRACKS_PER_RELEASE: usize = 13;
const RELEASES_PER_WRITE: usize = 10;
const EDITS: usize = 200;

macro_rules! tables {
    () => {
        "
CREATE TABLE releases(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, artist TEXT NOT NULL,
  year INTEGER, label TEXT, catalog TEXT, created_at INTEGER NOT NULL);
CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, release_id TEXT NOT NULL, title TEXT NOT NULL,
  position INTEGER NOT NULL, duration_ms INTEGER NOT NULL, created_at INTEGER NOT NULL);
CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY, track_id TEXT NOT NULL, path TEXT NOT NULL,
  size INTEGER NOT NULL, format TEXT NOT NULL, created_at INTEGER NOT NULL);"
    };
}

/// Device-local projections kept by triggers on every row, as a library's
/// listing and search tables are.
macro_rules! projections {
    () => {
        "
CREATE TABLE release_listing(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL,
  artist TEXT NOT NULL, tracks INTEGER NOT NULL, duration_ms INTEGER NOT NULL);
CREATE INDEX release_listing_artist ON release_listing(artist, title);
CREATE TABLE track_search(track_id TEXT NOT NULL PRIMARY KEY, words TEXT NOT NULL);
CREATE INDEX track_search_words ON track_search(words);
CREATE INDEX tracks_release ON tracks(release_id, position);
CREATE INDEX files_track ON files(track_id);
CREATE TRIGGER releases_listed AFTER INSERT ON releases BEGIN
  INSERT OR REPLACE INTO release_listing VALUES(NEW.id, NEW.title, NEW.artist,
    (SELECT count(*) FROM tracks WHERE release_id = NEW.id),
    (SELECT coalesce(sum(duration_ms), 0) FROM tracks WHERE release_id = NEW.id));
END;
CREATE TRIGGER releases_renamed AFTER UPDATE ON releases BEGIN
  UPDATE release_listing SET title = NEW.title, artist = NEW.artist WHERE id = NEW.id;
END;
CREATE TRIGGER tracks_listed AFTER INSERT ON tracks BEGIN
  UPDATE release_listing SET tracks = tracks + 1, duration_ms = duration_ms + NEW.duration_ms
    WHERE id = NEW.release_id;
  INSERT OR REPLACE INTO track_search VALUES(NEW.id, lower(NEW.title || ' ' ||
    coalesce((SELECT title || ' ' || artist FROM releases WHERE id = NEW.release_id), '')));
END;"
    };
}

/// The fixture's wall clock moves with real time and can jump ahead, so
/// snapshots age past retention's grace; elapsed time is real, so coven's
/// timing lines report real durations.
struct BenchClock {
    origin: Instant,
    ahead: AtomicU64,
}

impl Clock for BenchClock {
    fn now(&self) -> SystemTime {
        UNIX_EPOCH
            + Duration::from_secs(1_000_000 + self.ahead.load(Ordering::SeqCst))
            + self.origin.elapsed()
    }
    fn elapsed(&self) -> Duration {
        self.origin.elapsed()
    }
}

fn tables() -> Vec<SyncedTable> {
    ["releases", "tracks", "files"]
        .into_iter()
        .map(|table| SyncedTable::new(table, RowIdentity::SharedKey))
        .collect()
}

fn migrations(triggers: bool) -> Vec<Migration> {
    let sql = if triggers {
        concat!(tables!(), projections!())
    } else {
        tables!()
    };
    vec![Migration::addition(1, "library", sql)]
}

/// One sync pass that starts after this call.
async fn pass(handle: &CovenHandle, clock: &BenchClock) {
    let after = clock.now();
    let mut status = handle.subscribe_sync_status();
    status.borrow_and_update();
    handle.sync_now();
    loop {
        status.changed().await.unwrap();
        match &*status.borrow_and_update() {
            SyncStatus::Synced { finished_at } if *finished_at >= after => return,
            SyncStatus::Failed { error } => panic!("{error:?}"),
            _ => {}
        }
    }
}

async fn import(handle: &CovenHandle, clock: &BenchClock) {
    for write in 0..RELEASES / RELEASES_PER_WRITE {
        handle
            .write(move |sql| {
                for release in write * RELEASES_PER_WRITE..(write + 1) * RELEASES_PER_WRITE {
                    let id = format!("release-{release:05}");
                    for track in 0..TRACKS_PER_RELEASE {
                        let track_id = format!("{id}-track-{track:02}");
                        sql.execute(
                            "INSERT INTO tracks VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
                            (
                                &track_id,
                                &id,
                                format!("Track {track} of release {release}"),
                                track as i64 + 1,
                                180_000_i64 + track as i64,
                                1_700_000_000_i64 + release as i64,
                            ),
                        )?;
                        sql.execute(
                            "INSERT INTO files VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
                            (
                                format!("{track_id}-file"),
                                &track_id,
                                format!("Artist {}/Album {release}/{track:02}.flac", release % 400),
                                30_000_000_i64,
                                "flac",
                                1_700_000_000_i64 + release as i64,
                            ),
                        )?;
                    }
                    sql.execute(
                        "INSERT INTO releases VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        (
                            &id,
                            format!("Album {release}"),
                            format!("Artist {}", release % 400),
                            1960_i64 + (release % 60) as i64,
                            "Label",
                            format!("CAT-{release}"),
                            1_700_000_000_i64 + release as i64,
                        ),
                    )?;
                }
                Ok(())
            })
            .await
            .unwrap();
        pass(handle, clock).await;
    }
}

async fn edit(handle: &CovenHandle, clock: &BenchClock) {
    for edit in 0..EDITS {
        handle
            .write(move |sql| {
                sql.execute(
                    "UPDATE releases SET title = ?1 WHERE id = ?2",
                    (
                        format!("Album {edit} (remastered)"),
                        format!("release-{edit:05}"),
                    ),
                )?;
                Ok(())
            })
            .await
            .unwrap();
        pass(handle, clock).await;
    }
}

async fn restore_cost(triggers: bool, floor: bool) {
    let clock = Arc::new(BenchClock {
        origin: Instant::now(),
        ahead: AtomicU64::new(0),
    });
    let storage = Arc::new(
        MemoryStorage::builder()
            .clock(clock.clone())
            .build()
            .unwrap(),
    );
    let owner_root = tempfile::tempdir().unwrap();
    let owner = TestCoven::new();
    let handle = owner
        .builder(StoreLayout::new(owner_root.path().into()))
        .synced_tables(tables())
        .migrations(migrations(triggers))
        .clock(clock.clone())
        .storage_connector(storage.clone())
        .create_store("Library")
        .await
        .unwrap();
    handle
        .setup_s3_storage(
            storage.config(),
            "Ana's laptop",
            "key".into(),
            SecretText::new("secret".into()),
        )
        .await
        .unwrap();
    handle.start_sync().await.unwrap();
    let building = Instant::now();
    import(&handle, &clock).await;
    if floor {
        // Past retention's week of grace, the next pass writes the floor.
        clock.ahead.fetch_add(8 * 24 * 60 * 60, Ordering::SeqCst);
        pass(&handle, &clock).await;
        pass(&handle, &clock).await;
    }
    edit(&handle, &clock).await;
    eprintln!(
        "built the store in {:?} (triggers {triggers}, floor {floor})",
        building.elapsed()
    );
    let code = handle.restore_code_for_transfer().await.unwrap();
    handle.close().await.unwrap();

    let device_root = tempfile::tempdir().unwrap();
    let (_cancelling, cancel) = tokio::sync::watch::channel(false);
    let restoring = Instant::now();
    let restored = coven::restore_from_code(
        TestCoven::new()
            .builder(StoreLayout::new(device_root.path().into()))
            .synced_tables(tables())
            .migrations(migrations(triggers))
            .clock(clock.clone())
            .storage_connector(Arc::new(storage.for_device())),
        &code,
        None,
        "Ana's phone",
        |_| {},
        &cancel,
    )
    .await
    .unwrap();
    let restored_in = restoring.elapsed();
    let tracks = restored
        .read(|sql| {
            let count = sql.query_row("SELECT count(*) FROM tracks", [], |row| {
                row.get::<_, i64>(0)
            })?;
            Ok(count)
        })
        .await
        .unwrap();
    assert_eq!(tracks, (RELEASES * TRACKS_PER_RELEASE) as i64);
    eprintln!("restored {tracks} tracks in {restored_in:?} (triggers {triggers}, floor {floor})");
    restored.close().await.unwrap();
}

fn logs() {
    let installed = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .try_init();
    // An earlier measurement in this process installed the subscriber.
    if let Err(error) = installed {
        eprintln!("keeping the installed log subscriber: {error}");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "one timing measurement, run explicitly in the release profile"]
async fn a_restore_with_triggers_and_a_floor_costs() {
    logs();
    restore_cost(true, true).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "one timing measurement, run explicitly in the release profile"]
async fn a_restore_without_triggers_with_a_floor_costs() {
    logs();
    restore_cost(false, true).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "one timing measurement, run explicitly in the release profile"]
async fn a_restore_with_triggers_and_no_floor_costs() {
    logs();
    restore_cost(true, false).await;
}
