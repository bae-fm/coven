//! The SQLite settings an app may choose for its store's connections, the ones
//! coven keeps fixed, and the WAL commands an app may run (§4, api.md E1).

use std::num::NonZeroU32;
use std::time::Duration;

use crate::sqlite::DatabaseConnection;
use crate::DbError;

/// SQLite settings for every connection coven opens to the store's database:
/// the writer and the readers of a writable handle, and every connection of a
/// read-only one. A setting left `None` keeps SQLite's own default. Coven
/// applies them as each connection opens, before any of its own statements.
///
/// The settings coven's guarantees rest on have no field here: the journal
/// mode, `synchronous`, the locking mode, foreign keys and the rest stay
/// fixed (§4).
///
/// ```compile_fail
/// let _ = coven_database::SqliteSettings {
///     journal_mode: None,
///     ..Default::default()
/// };
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SqliteSettings {
    /// How long a connection waits for a lock another connection holds before
    /// failing with `SQLITE_BUSY` (`PRAGMA busy_timeout`), at most
    /// `i32::MAX` milliseconds. Unset keeps the 5 seconds rusqlite sets when
    /// it opens a connection. RESTART and TRUNCATE checkpoints wait this long
    /// for readers.
    pub busy_timeout: Option<Duration>,
    /// The size of each connection's page cache (`PRAGMA cache_size`).
    pub cache_size: Option<CacheSize>,
    /// Whether, and past how many cached pages, a transaction writes changed
    /// pages to the file before it commits (`PRAGMA cache_spill`).
    pub cache_spill: Option<CacheSpill>,
    /// Bytes of the database read through memory-mapped I/O (`PRAGMA
    /// mmap_size`), at most `i64::MAX`. SQLite lowers it to the largest size
    /// it was built to map.
    pub mmap_size: Option<u64>,
    /// Where temporary tables and indices are kept (`PRAGMA temp_store`).
    /// On Android, coven's SQLite is built to keep them in memory whatever
    /// this says.
    pub temp_store: Option<TempStore>,
    /// Helper threads one statement may use to sort (`PRAGMA threads`), at
    /// most `i32::MAX`. SQLite lowers it to the most it was built to allow.
    pub threads: Option<u32>,
    /// The size SQLite truncates the WAL file to at the first commit after
    /// a checkpoint lets the WAL start over (`PRAGMA journal_size_limit`).
    pub journal_size_limit: Option<JournalSizeLimit>,
    /// The WAL size at which a commit runs a passive checkpoint (`PRAGMA
    /// wal_autocheckpoint`).
    pub wal_autocheckpoint: Option<WalAutocheckpoint>,
    /// Whether the query planner may build a temporary index for one query
    /// (`PRAGMA automatic_index`).
    pub automatic_index: Option<bool>,
    /// Whether SQLite checks each page's cells for damage as it reads them
    /// (`PRAGMA cell_size_check`).
    pub cell_size_check: Option<bool>,
    /// Whether deleted content is overwritten in the file (`PRAGMA
    /// secure_delete`).
    pub secure_delete: Option<SecureDelete>,
    /// Whether a commit's sync on macOS and iOS also flushes the drive's own
    /// cache, `F_FULLFSYNC` (`PRAGMA fullfsync`). Other platforms ignore it.
    pub fullfsync: Option<bool>,
    /// The same for checkpoints (`PRAGMA checkpoint_fullfsync`).
    pub checkpoint_fullfsync: Option<bool>,
    /// How many rows of each index `ANALYZE` reads (`PRAGMA analysis_limit`).
    pub analysis_limit: Option<AnalysisLimit>,
    /// The page size a store's database is created with (`PRAGMA
    /// page_size`). It is fixed once the file has its first page: an
    /// existing store keeps the size it was created with.
    pub page_size: Option<PageSize>,
    /// The auto-vacuum mode a store's database is created with (`PRAGMA
    /// auto_vacuum`). Like the page size, an existing store keeps its own.
    pub auto_vacuum: Option<AutoVacuum>,
}

/// The SQLite settings one connection to the store's database has in effect,
/// as SQLite reports them. Every connection has the same.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqliteSettingsInEffect {
    /// `PRAGMA busy_timeout`, to the millisecond.
    pub busy_timeout: Duration,
    /// `PRAGMA cache_size`.
    pub cache_size: CacheSize,
    /// `PRAGMA cache_spill`: when on, the page count past which a
    /// transaction spills, which is never below the cache's own size.
    pub cache_spill: CacheSpill,
    /// `PRAGMA mmap_size`.
    pub mmap_size: u64,
    /// `PRAGMA temp_store`.
    pub temp_store: TempStore,
    /// `PRAGMA threads`.
    pub threads: u32,
    /// `PRAGMA journal_size_limit`.
    pub journal_size_limit: JournalSizeLimit,
    /// `PRAGMA wal_autocheckpoint`.
    pub wal_autocheckpoint: WalAutocheckpoint,
    /// `PRAGMA automatic_index`.
    pub automatic_index: bool,
    /// `PRAGMA cell_size_check`.
    pub cell_size_check: bool,
    /// `PRAGMA secure_delete`.
    pub secure_delete: SecureDelete,
    /// `PRAGMA fullfsync`.
    pub fullfsync: bool,
    /// `PRAGMA checkpoint_fullfsync`.
    pub checkpoint_fullfsync: bool,
    /// `PRAGMA analysis_limit`.
    pub analysis_limit: AnalysisLimit,
    /// `PRAGMA page_size`.
    pub page_size: PageSize,
    /// `PRAGMA auto_vacuum`.
    pub auto_vacuum: AutoVacuum,
}

/// A page cache's size, which SQLite counts in pages or in kibibytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheSize {
    /// This many pages, at most `i32::MAX`.
    Pages(u32),
    /// As many pages as fit in this many kibibytes, at most `i32::MAX`.
    Kibibytes(u32),
}

/// Whether a transaction may spill changed pages before it commits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheSpill {
    /// Changed pages stay in memory until commit.
    Off,
    /// Spill once the cache holds this many pages, at most `i32::MAX`.
    Pages(NonZeroU32),
}

/// Where temporary tables and indices are kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TempStore {
    /// Where SQLite was built to keep them.
    Default,
    /// In a temporary file.
    File,
    /// In memory.
    Memory,
}

/// How large the WAL file may stay after a checkpoint resets it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalSizeLimit {
    /// The file keeps its size.
    Unlimited,
    /// The file is truncated to this many bytes, at most `i64::MAX`.
    Bytes(u64),
}

/// When a commit runs a passive checkpoint by itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalAutocheckpoint {
    /// Never: the WAL grows until the app checkpoints it.
    Off,
    /// Once the WAL holds this many pages, at most `i32::MAX`.
    Pages(NonZeroU32),
}

/// Whether deleted content is overwritten.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecureDelete {
    /// Deleted content stays in free pages until they are reused.
    Off,
    /// Deleted content is overwritten with zeros.
    On,
    /// Overwritten only where that adds no I/O.
    Fast,
}

/// How many rows of each index `ANALYZE` reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnalysisLimit {
    /// Every row.
    Unlimited,
    /// About this many rows, at most `i32::MAX`.
    Rows(NonZeroU32),
}

/// A database page size: a power of two from 512 bytes to 64 KiB.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageSize {
    /// 512 bytes.
    Bytes512,
    /// 1 KiB.
    Bytes1024,
    /// 2 KiB.
    Bytes2048,
    /// 4 KiB, SQLite's default.
    Bytes4096,
    /// 8 KiB.
    Bytes8192,
    /// 16 KiB.
    Bytes16384,
    /// 32 KiB.
    Bytes32768,
    /// 64 KiB.
    Bytes65536,
}

/// What happens to the pages deleted content frees.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AutoVacuum {
    /// They stay in the file for reuse.
    Off,
    /// Each commit moves them to the end of the file and truncates it.
    Full,
    /// They are given back only when the app runs an incremental vacuum.
    Incremental,
}

/// SQLite's checkpoint modes (`PRAGMA wal_checkpoint`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointMode {
    /// Copy what no reader still needs, without waiting.
    Passive,
    /// Wait for the writer lock and for readers, then copy the whole WAL.
    Full,
    /// As `Full`, then wait for readers to leave the WAL, so the next commit
    /// writes the WAL from its start.
    Restart,
    /// As `Restart`, then truncate the WAL file to zero bytes.
    Truncate,
}

/// What a checkpoint did, as SQLite reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalCheckpoint {
    /// A `Full`, `Restart` or `Truncate` checkpoint could not finish within
    /// the busy timeout, because a reader or another writer held on.
    pub busy: bool,
    /// Frames in the WAL.
    pub log_frames: u32,
    /// Frames copied into the database, this time or before.
    pub checkpointed_frames: u32,
}

/// How many free pages an incremental vacuum gives back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IncrementalVacuum {
    /// Every free page.
    All,
    /// At most this many, at most `i32::MAX`.
    Pages(NonZeroU32),
}

impl PageSize {
    fn bytes(self) -> u32 {
        match self {
            PageSize::Bytes512 => 512,
            PageSize::Bytes1024 => 1024,
            PageSize::Bytes2048 => 2048,
            PageSize::Bytes4096 => 4096,
            PageSize::Bytes8192 => 8192,
            PageSize::Bytes16384 => 16384,
            PageSize::Bytes32768 => 32768,
            PageSize::Bytes65536 => 65536,
        }
    }

    fn from_bytes(bytes: i64) -> Result<Self, DbError> {
        Ok(match bytes {
            512 => PageSize::Bytes512,
            1024 => PageSize::Bytes1024,
            2048 => PageSize::Bytes2048,
            4096 => PageSize::Bytes4096,
            8192 => PageSize::Bytes8192,
            16384 => PageSize::Bytes16384,
            32768 => PageSize::Bytes32768,
            65536 => PageSize::Bytes65536,
            _ => return Err(unreported()),
        })
    }
}

impl SqliteSettings {
    /// The PRAGMAs that set these choices on any connection.
    fn connection_pragmas(&self) -> Result<Vec<String>, DbError> {
        let mut pragmas = Vec::new();
        if let Some(timeout) = self.busy_timeout {
            let milliseconds = int("busy_timeout", timeout.as_millis())?;
            pragmas.push(format!("busy_timeout = {milliseconds}"));
        }
        if let Some(size) = self.cache_size {
            pragmas.push(match size {
                CacheSize::Pages(pages) => format!("cache_size = {}", int("cache_size", pages)?),
                CacheSize::Kibibytes(kib) => format!("cache_size = -{}", int("cache_size", kib)?),
            });
        }
        if let Some(spill) = self.cache_spill {
            pragmas.push(match spill {
                CacheSpill::Off => "cache_spill = OFF".into(),
                CacheSpill::Pages(pages) => {
                    format!("cache_spill = {}", int("cache_spill", pages.get())?)
                }
            });
        }
        if let Some(bytes) = self.mmap_size {
            pragmas.push(format!("mmap_size = {}", int64("mmap_size", bytes)?));
        }
        if let Some(store) = self.temp_store {
            let store = match store {
                TempStore::Default => "DEFAULT",
                TempStore::File => "FILE",
                TempStore::Memory => "MEMORY",
            };
            pragmas.push(format!("temp_store = {store}"));
        }
        if let Some(threads) = self.threads {
            pragmas.push(format!("threads = {}", int("threads", threads)?));
        }
        if let Some(limit) = self.journal_size_limit {
            let bytes = match limit {
                JournalSizeLimit::Unlimited => -1,
                JournalSizeLimit::Bytes(bytes) => int64("journal_size_limit", bytes)?,
            };
            pragmas.push(format!("journal_size_limit = {bytes}"));
        }
        if let Some(checkpoint) = self.wal_autocheckpoint {
            let pages = match checkpoint {
                WalAutocheckpoint::Off => 0,
                WalAutocheckpoint::Pages(pages) => int("wal_autocheckpoint", pages.get())?,
            };
            pragmas.push(format!("wal_autocheckpoint = {pages}"));
        }
        for (name, value) in [
            ("automatic_index", self.automatic_index),
            ("cell_size_check", self.cell_size_check),
            ("fullfsync", self.fullfsync),
            ("checkpoint_fullfsync", self.checkpoint_fullfsync),
        ] {
            if let Some(value) = value {
                pragmas.push(format!("{name} = {}", if value { "ON" } else { "OFF" }));
            }
        }
        if let Some(delete) = self.secure_delete {
            let delete = match delete {
                SecureDelete::Off => "OFF",
                SecureDelete::On => "ON",
                SecureDelete::Fast => "FAST",
            };
            pragmas.push(format!("secure_delete = {delete}"));
        }
        if let Some(limit) = self.analysis_limit {
            let rows = match limit {
                AnalysisLimit::Unlimited => 0,
                AnalysisLimit::Rows(rows) => int("analysis_limit", rows.get())?,
            };
            pragmas.push(format!("analysis_limit = {rows}"));
        }
        Ok(pragmas)
    }

    /// The PRAGMAs that shape a new database file. The page size comes first:
    /// setting the auto-vacuum mode writes the file's first page, which fixes it.
    fn creation_pragmas(&self) -> Vec<String> {
        let mut pragmas = Vec::new();
        if let Some(size) = self.page_size {
            pragmas.push(format!("page_size = {}", size.bytes()));
        }
        if let Some(vacuum) = self.auto_vacuum {
            let vacuum = match vacuum {
                AutoVacuum::Off => "NONE",
                AutoVacuum::Full => "FULL",
                AutoVacuum::Incremental => "INCREMENTAL",
            };
            pragmas.push(format!("auto_vacuum = {vacuum}"));
        }
        pragmas
    }
}

impl DatabaseConnection {
    /// Set the settings coven fixes, then the app's choices, on a connection
    /// that has run no statement yet. Changing `temp_store` drops temporary
    /// objects, so this precedes coven's temporary triggers. A writable
    /// connection to an empty file also gets its creation settings, before
    /// anything writes the file.
    pub(crate) fn configure(
        &self,
        settings: &SqliteSettings,
        read_only: bool,
    ) -> Result<(), DbError> {
        self.batch(
            "PRAGMA foreign_keys = ON; PRAGMA recursive_triggers = ON; PRAGMA trusted_schema = OFF;",
        )?;
        let mut pragmas = settings.connection_pragmas()?;
        if !read_only {
            let pages: i64 = self.query_row("PRAGMA page_count", [], |r| r.get(0))?;
            if pages == 0 {
                pragmas.extend(settings.creation_pragmas());
            }
        }
        if pragmas.is_empty() {
            return Ok(());
        }
        self.batch(&format!("PRAGMA {};", pragmas.join("; PRAGMA ")))
    }

    /// Put the database in WAL mode and make every commit durable (§4).
    pub(crate) fn enable_wal(&self) -> Result<(), DbError> {
        let mode: String = self.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(DbError::WalUnavailable { mode });
        }
        self.batch("PRAGMA synchronous = FULL;")
    }

    pub(crate) fn sqlite_settings(&self) -> Result<SqliteSettingsInEffect, DbError> {
        let read = |pragma: &str| -> Result<i64, DbError> {
            self.query_row(&format!("PRAGMA {pragma}"), [], |r| r.get(0))
        };
        let positive = |value: i64| u32::try_from(value).ok().and_then(NonZeroU32::new);
        let cache_size = read("cache_size")?;
        Ok(SqliteSettingsInEffect {
            busy_timeout: Duration::from_millis(unsigned(read("busy_timeout")?)?),
            cache_size: if cache_size < 0 {
                CacheSize::Kibibytes(unsigned(-cache_size)?)
            } else {
                CacheSize::Pages(unsigned(cache_size)?)
            },
            cache_spill: match read("cache_spill")? {
                0 => CacheSpill::Off,
                spill => CacheSpill::Pages(positive(spill).ok_or_else(unreported)?),
            },
            mmap_size: unsigned(read("mmap_size")?)?,
            temp_store: match read("temp_store")? {
                0 => TempStore::Default,
                1 => TempStore::File,
                2 => TempStore::Memory,
                _ => return Err(unreported()),
            },
            threads: unsigned(read("threads")?)?,
            journal_size_limit: match read("journal_size_limit")? {
                -1 => JournalSizeLimit::Unlimited,
                bytes => JournalSizeLimit::Bytes(unsigned(bytes)?),
            },
            wal_autocheckpoint: match read("wal_autocheckpoint")? {
                0 => WalAutocheckpoint::Off,
                checkpoint => {
                    WalAutocheckpoint::Pages(positive(checkpoint).ok_or_else(unreported)?)
                }
            },
            automatic_index: read("automatic_index")? != 0,
            cell_size_check: read("cell_size_check")? != 0,
            secure_delete: match read("secure_delete")? {
                0 => SecureDelete::Off,
                1 => SecureDelete::On,
                2 => SecureDelete::Fast,
                _ => return Err(unreported()),
            },
            fullfsync: read("fullfsync")? != 0,
            checkpoint_fullfsync: read("checkpoint_fullfsync")? != 0,
            analysis_limit: match read("analysis_limit")? {
                0 => AnalysisLimit::Unlimited,
                rows => AnalysisLimit::Rows(positive(rows).ok_or_else(unreported)?),
            },
            page_size: PageSize::from_bytes(read("page_size")?)?,
            auto_vacuum: match read("auto_vacuum")? {
                0 => AutoVacuum::Off,
                1 => AutoVacuum::Full,
                2 => AutoVacuum::Incremental,
                _ => return Err(unreported()),
            },
        })
    }

    /// Checkpoint the WAL outside any transaction. This connection is the
    /// writer, always in WAL mode, so SQLite reports both frame counts.
    pub(crate) fn wal_checkpoint(&self, mode: CheckpointMode) -> Result<WalCheckpoint, DbError> {
        let mode = match mode {
            CheckpointMode::Passive => "PASSIVE",
            CheckpointMode::Full => "FULL",
            CheckpointMode::Restart => "RESTART",
            CheckpointMode::Truncate => "TRUNCATE",
        };
        let (busy, log, checkpointed): (i64, i64, i64) =
            self.query_row(&format!("PRAGMA main.wal_checkpoint({mode})"), [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })?;
        let frames = |count: i64| {
            u32::try_from(count).map_err(|_overflow| DbError::Invariant {
                what: "a checkpoint of the writer's WAL reports its frames",
            })
        };
        Ok(WalCheckpoint {
            busy: busy != 0,
            log_frames: frames(log)?,
            checkpointed_frames: frames(checkpointed)?,
        })
    }

    /// Give back free pages of an incrementally vacuumed database. SQLite
    /// frees one page per result row, so every row is stepped.
    pub(crate) fn incremental_vacuum(&self, pages: IncrementalVacuum) -> Result<(), DbError> {
        let sql = match pages {
            IncrementalVacuum::All => "PRAGMA main.incremental_vacuum".to_owned(),
            IncrementalVacuum::Pages(pages) => format!(
                "PRAGMA main.incremental_vacuum({})",
                int("incremental_vacuum", pages.get())?
            ),
        };
        self.query(&sql, [], |_| Ok(()))?;
        Ok(())
    }
}

/// `value` as one of SQLite's 32-bit integer settings.
fn int(setting: &'static str, value: impl Into<u128>) -> Result<i32, DbError> {
    i32::try_from(value.into()).map_err(|_overflow| DbError::SqliteSettingOutOfRange {
        setting,
        maximum: i32::MAX as u64,
    })
}

/// `value` as one of SQLite's 64-bit integer settings.
fn int64(setting: &'static str, value: u64) -> Result<i64, DbError> {
    i64::try_from(value).map_err(|_overflow| DbError::SqliteSettingOutOfRange {
        setting,
        maximum: i64::MAX as u64,
    })
}

fn unsigned<T: TryFrom<i64>>(value: i64) -> Result<T, DbError> {
    T::try_from(value).map_err(|_overflow| unreported())
}

/// SQLite reported a setting outside the values it documents.
fn unreported() -> DbError {
    DbError::Invariant {
        what: "SQLite reports a setting within its documented values",
    }
}

#[cfg(test)]
#[path = "sqlite_settings_tests.rs"]
mod tests;
