//! Completing a stored upload. A file this device must keep cached takes
//! coven's own copy as its cache copy, rather than the device downloading the
//! bytes it just stored (§17.6, §17.7).

use super::FileTransfer;
use crate::{files::file_error::TransferError, files::file_failure::file_subject, FileReadError};
use coven_crypto::FILE_CHUNK_TAG_LEN;
use coven_database::CacheReservation;
use coven_format::{chunks::CHUNK_SIZE, file::FILE_HEADER_LEN};

/// Chunks sealed per piece when a cache copy is written from the source.
const COPY_CHUNKS: usize = 16;

impl FileTransfer<'_> {
    /// Retire the upload storage confirmed. A file that must stay cached
    /// here first gets its cache file, which the completing transaction
    /// publishes. Failing to write it fails this attempt; the retry finds
    /// the stored object and writes it again. `sent` is what a single
    /// request sent, when one did.
    pub(super) async fn complete(&self, sent: Option<Vec<u8>>) -> Result<(), TransferError> {
        let owner = self.owner;
        let copy = if owner.database.upload_keeps_copy(&self.item.file).await? {
            Some(self.keep_copy(sent).await?)
        } else {
            None
        };
        let kept = copy.is_some();
        {
            // Publishing a whole cache file excludes fetches into the cache.
            let _cache = if kept {
                Some(owner.cache.write().await)
            } else {
                None
            };
            owner
                .database
                .complete_upload(self.item.id, file_subject(&self.item.file), copy)
                .await
                .map_err(FileReadError::from)?;
        }
        if kept {
            owner.trim(self.item.file.namespace()).await;
        }
        Ok(())
    }

    /// Write the whole encrypted file to a reserved cache file: the bytes a
    /// single request sent, or the source sealed again in chunk-aligned
    /// pieces, each chunk checked against its first-read hash as for
    /// sending. Chunk nonces are their indexes, so the copy is byte for byte
    /// what storage holds.
    async fn keep_copy(&self, sent: Option<Vec<u8>>) -> Result<CacheReservation, TransferError> {
        let (reservation, mut writer) = self.owner.create_cache_file(&self.item.file).await?;
        match sent {
            Some(bytes) => writer.append(&bytes).await?,
            None => {
                let step = (COPY_CHUNKS * (CHUNK_SIZE + FILE_CHUNK_TAG_LEN)) as u64;
                let mut offset = 0;
                while offset < self.total {
                    let end = (offset.max(FILE_HEADER_LEN as u64) + step).min(self.total);
                    // At most the header and one step of chunks.
                    let piece = self.read_encrypted(offset, (end - offset) as usize).await?;
                    writer.append(&piece).await?;
                    offset = end;
                }
            }
        }
        writer.finish().await?;
        Ok(reservation)
    }
}
