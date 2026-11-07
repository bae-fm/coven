//! Where a pin's whole copy of a file begins: continuing the partial copy an
//! earlier prepare left, once its bytes check, or from the header (§17.8).

use super::*;
use coven_foundation::files::{FileArea, FileWriter, ObservationError};

/// What checking a partial copy's kept bytes found.
enum Checked {
    /// Header and `chunks` whole chunks, every one authenticated.
    Whole { hash: ContentHasher, chunks: u64 },
    /// The bytes are not the file's start as the stored layout fixes it, or
    /// some chunk fails authentication.
    Damaged,
}

impl UploadedFile {
    /// Reserve the copy's cache file and open it for writing: the partial
    /// copy an earlier prepare left, continued from its kept bytes, or else
    /// a new one holding the header. A partial that doesn't check is
    /// deleted and the copy begins anew.
    ///
    /// Returns the reservation and its writer, then what the copy holds
    /// already: the content hash of its plaintext, the first chunk it lacks
    /// and the bytes in it, its header and the chunks before that one.
    pub(super) async fn begin_whole(
        &self,
    ) -> Result<(CacheReservation, FileWriter, ContentHasher, u64, u64), TransferError> {
        let database = &self.owner.database;
        let fresh = || FileName::new(self.owner.ids.new_id().to_string()).expect("UUID cache name");
        let (mut reservation, mut kept) =
            database.reserve_pin_download(&self.file, fresh()).await?;
        let mut checked = None;
        if kept > 0 {
            match self.check_partial(&reservation, kept).await? {
                Checked::Whole { hash, chunks } => checked = Some((hash, chunks)),
                Checked::Damaged => {
                    drop(reservation);
                    database.discard_pin_download(&self.file).await?;
                    (reservation, kept) =
                        database.reserve_pin_download(&self.file, fresh()).await?;
                    debug_assert_eq!(kept, 0, "a discarded partial leaves a new copy");
                }
            }
        }
        match checked {
            Some((hash, next)) => {
                let (reservation, writer) = self.open_writer(reservation, Some(kept)).await?;
                Ok((reservation, writer, hash, next, kept))
            }
            None => {
                let (reservation, mut writer) = self.open_writer(reservation, None).await?;
                let header = self.header.encode();
                writer.append(&header).await?;
                Ok((reservation, writer, ContentHasher::new(), 0, header.len() as u64))
            }
        }
    }

    /// The reservation's cache file, created, or reopened after its first
    /// `kept` bytes. The reservation travels through the blocking task, so
    /// the name stays reserved while it runs after a cancelled await.
    async fn open_writer(
        &self,
        reservation: CacheReservation,
        kept: Option<u64>,
    ) -> Result<(CacheReservation, FileWriter), FileReadError> {
        let directory = self.owner.directory.clone();
        let name = reservation.name().clone();
        let (reservation, writer) = coven_foundation::task::blocking_completing(
            "open a cache file",
            self.owner.database.bugs().clone(),
            move || {
                let writer = match kept {
                    Some(kept) => directory.resume_writer(FileArea::Cache, &name, kept),
                    None => directory.create_writer(FileArea::Cache, &name),
                }
                .map_err(FileReadError::from);
                Ok::<_, std::convert::Infallible>((reservation, writer))
            },
        )
        .await
        .map_err(coven_foundation::files::FileError::from)?
        .unwrap_or_else(|never| match never {});
        Ok((reservation, writer?))
    }

    /// Read the partial copy's `kept` bytes back and authenticate them: the
    /// header the file's size fixes, then whole chunks, each opened under
    /// its place in the object, feeding the content hash as they go.
    async fn check_partial(
        &self,
        reservation: &CacheReservation,
        kept: u64,
    ) -> Result<Checked, FileReadError> {
        let file = self.owner.directory.file(FileArea::Cache, reservation.name());
        let (header, location, path) = (self.header, self.location.clone(), self.path.clone());
        coven_foundation::task::blocking_completing(
            "check a partial cache file",
            self.owner.database.bugs().clone(),
            move || -> Result<Checked, FileReadError> {
                let reader = match file.open_reader() {
                    Ok(reader) => reader,
                    Err(ObservationError::File(error)) => return Err(error.into()),
                    Err(ObservationError::Missing(_) | ObservationError::Changed(_)) => {
                        return Ok(Checked::Damaged)
                    }
                };
                let read = |offset: u64, length: usize| match reader.read_at(offset, length) {
                    Ok(bytes) => Ok(Some(bytes)),
                    Err(ObservationError::File(error)) => Err(FileReadError::from(error)),
                    Err(ObservationError::Missing(_) | ObservationError::Changed(_)) => Ok(None),
                };
                if reader.size() < kept {
                    return Ok(Checked::Damaged);
                }
                let encoded = header.encode();
                if read(0, encoded.len())?.as_deref() != Some(encoded.as_slice()) {
                    return Ok(Checked::Damaged);
                }
                let coven_format::file_reference::FileReference { key, .. } =
                    location.reference()?;
                let mut hash = ContentHasher::new();
                let (mut end, mut chunks) = (encoded.len() as u64, 0);
                while end < kept {
                    let Ok(chunk) = header.chunk(chunks) else {
                        return Ok(Checked::Damaged);
                    };
                    if chunk.offset != end || chunk.end() > kept {
                        return Ok(Checked::Damaged);
                    }
                    let Some(sealed) = read(chunk.offset, chunk.stored_length())? else {
                        return Ok(Checked::Damaged);
                    };
                    let Ok(plain) =
                        header.open_chunk(&key, path.as_str(), chunks, sealed.as_slice())
                    else {
                        return Ok(Checked::Damaged);
                    };
                    hash.update(&plain);
                    end = chunk.end();
                    chunks += 1;
                }
                Ok(if end == kept { Checked::Whole { hash, chunks } } else { Checked::Damaged })
            },
        )
        .await
        .map_err(coven_foundation::files::FileError::from)?
    }
}
