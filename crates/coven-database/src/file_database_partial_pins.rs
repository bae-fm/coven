//! A pin's download that stopped (§17.8): the first bytes of a file's whole
//! encrypted copy, synced and kept apart from the cache, so the next prepare
//! of the file continues from them. The cache neither counts nor evicts them.
//! They go when the app unpins the file, when the pin they became publishes,
//! or when the stored bytes they are of leave this device.

use super::{cache, BytesWriting, CacheReservation, FileDatabase};
use crate::{sqlite::DatabaseConnection, DbError, FileRef};
use coven_foundation::files::FileName;

impl FileDatabase {
    /// The cache file a prepare of `file` writes its whole copy to, and how
    /// many of its bytes are already kept: the partial copy an earlier
    /// prepare left, reserved again, or else `fresh`, reserved as a new
    /// file, with none. The bytes kept are whole chunks after the header,
    /// synced.
    #[expect(clippy::map_err_ignore, reason = "a stored name that won't decode is DamagedDatabase")]
    pub async fn reserve_pin_download(
        &self,
        file: &FileRef,
        fresh: FileName,
    ) -> Result<(CacheReservation, u64), DbError> {
        let database = &self.database;
        let writing = BytesWriting::of(database).await;
        let held = {
            let file = file.clone();
            database
                .call(move |inner| {
                    let found = inner
                        .access
                        .with_writer(|writer| writer.read_transaction(|| partial(writer, &file)))?;
                    let Some((path, size)) = found else {
                        return Ok(None);
                    };
                    let name = FileName::new(&path).map_err(|_| DbError::DamagedDatabase)?;
                    inner.staging.reserve([name.clone()]);
                    Ok::<_, DbError>(Some((
                        CacheReservation {
                            reserved: inner.staging.clone(),
                            file,
                            name,
                            writing: Some(writing),
                            partial: true,
                        },
                        size,
                    )))
                })
                .await?
        };
        match held {
            Some(held) => Ok(held),
            None => Ok((self.reserve_cache_file(file, fresh).await?, 0)),
        }
    }

    /// Keep the first `size` bytes of the reservation's cache file, which
    /// are synced, as the partial copy the next prepare continues from.
    /// `NoLongerStored` when no record here names the file's stored bytes
    /// any more, and `PinChanged` when the app unpinned the file since the
    /// copy was reserved, which ended the partial (§17.8).
    #[expect(
        clippy::map_err_ignore,
        reason = "a size past SQLite's integer is TooLarge, which names the field and size"
    )]
    pub async fn keep_pin_download(
        &self,
        reservation: &mut CacheReservation,
        size: u64,
    ) -> Result<(), DbError> {
        let (file, name, kept) =
            (reservation.file.clone(), reservation.name.clone(), reservation.partial);
        let size = i64::try_from(size).map_err(|_| DbError::TooLarge {
            field: "partial pin download",
            actual: size,
            maximum: i64::MAX as u64,
        })?;
        self.run(move |db, _, _, _| db.transaction(|db| record(db, &file, &name, size, kept)))
            .await?;
        reservation.partial = true;
        Ok(())
    }

    /// The cache file of the partial download kept for `file`, and how many
    /// of its bytes are kept, for a test to look at.
    #[cfg(any(test, feature = "test-utils"))]
    #[expect(clippy::map_err_ignore, reason = "a stored name that won't decode is DamagedDatabase")]
    pub async fn pin_download_kept(
        &self,
        file: &FileRef,
    ) -> Result<Option<(FileName, u64)>, DbError> {
        let file = file.clone();
        let found = self.read(move |db| partial(db, &file)).await?;
        found
            .map(|(path, size)| {
                Ok((FileName::new(&path).map_err(|_| DbError::DamagedDatabase)?, size))
            })
            .transpose()
    }

    /// End the partial copy of `file`, whose bytes are not the file's:
    /// they go once the writer is free.
    pub async fn discard_pin_download(&self, file: &FileRef) -> Result<(), DbError> {
        let file = file.clone();
        let chosen = self
            .run(move |db, _, _, _| {
                let object = cache::object_path_of(&file)?;
                let paths = remove(db, file.namespace(), &object)?;
                Ok::<_, DbError>(crate::file_removals::choose_cache(&paths))
            })
            .await?;
        self.removed(chosen).await
    }
}

/// The partial copy kept of `file`: its cache file and how many bytes of
/// it are kept.
#[expect(clippy::map_err_ignore, reason = "a stored size that won't convert is DamagedDatabase")]
fn partial(db: &DatabaseConnection, file: &FileRef) -> Result<Option<(String, u64)>, DbError> {
    let found = db
        .query(
            "SELECT path,size FROM _coven_cache_partials WHERE namespace=?1 AND object_path=?2",
            (file.namespace(), cache::object_path_of(file)?),
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
        )?
        .pop();
    found
        .map(|(path, size)| Ok((path, u64::try_from(size).map_err(|_| DbError::DamagedDatabase)?)))
        .transpose()
}

/// Record that `name` holds `size` synced bytes of `file`'s copy: its row
/// updated when `kept`, which an unpin may have taken, and else made, naming
/// the file's copy as a partial for the first time.
fn record(
    db: &DatabaseConnection,
    file: &FileRef,
    name: &FileName,
    size: i64,
    kept: bool,
) -> Result<(), DbError> {
    if !cache::named(db, file)? {
        return Err(DbError::NoLongerStored { file: file.id() });
    }
    let object = cache::object_path_of(file)?;
    if kept {
        let updated = db.internal_execute(
            "UPDATE _coven_cache_partials SET size=?4
             WHERE namespace=?1 AND object_path=?2 AND path=?3",
            rusqlite::params![file.namespace(), object, name.as_str(), size],
        )?;
        return if updated == 1 { Ok(()) } else { Err(DbError::PinChanged { file: file.id() }) };
    }
    // The reservation's cleanup record goes before the row names its bytes.
    crate::file_removals::keep(db, "cache", name.as_str())?;
    db.internal_execute(
        "INSERT INTO _coven_cache_partials(namespace,object_path,path,size) VALUES(?1,?2,?3,?4)",
        rusqlite::params![file.namespace(), object, name.as_str(), size],
    )?;
    Ok(())
}

/// Delete the partial copy of the stored bytes at `object` and record its
/// cache file for removal; returns the name.
pub(crate) fn remove(
    db: &DatabaseConnection,
    namespace: &str,
    object: &str,
) -> Result<Vec<String>, DbError> {
    let paths = db.query(
        "DELETE FROM _coven_cache_partials WHERE namespace=?1 AND object_path=?2 RETURNING path",
        (namespace, object),
        |r| r.get::<_, String>(0),
    )?;
    for path in &paths {
        crate::file_removals::record_cached(db, path)?;
    }
    Ok(paths)
}
