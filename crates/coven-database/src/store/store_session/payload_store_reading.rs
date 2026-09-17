use super::*;
use std::io::{Read, Write};

impl PayloadStore<'_> {
    pub(crate) fn read(self, hash: ObjectHash) -> Result<Vec<u8>, PayloadStoreError> {
        let stored = self.require_stored(hash)?;
        let mut bytes = Vec::new();
        self.copy_stored(hash, stored, &mut bytes)?;
        Ok(bytes)
    }

    pub(crate) fn read_verified(self, hash: ObjectHash) -> Result<Vec<u8>, PayloadStoreError> {
        let mut bytes = Vec::new();
        self.copy_verified(hash, &mut bytes)?;
        Ok(bytes)
    }

    pub(super) fn require_stored(
        self,
        hash: ObjectHash,
    ) -> Result<StoredPayload, PayloadStoreError> {
        self.stored(hash)?
            .ok_or_else(|| PayloadStoreError::Storage {
                hash,
                error: "no catalog row".to_string(),
            })
    }

    pub(super) fn copy_stored(
        self,
        hash: ObjectHash,
        stored: StoredPayload,
        output: &mut impl Write,
    ) -> Result<(), PayloadStoreError> {
        match stored {
            StoredPayload::Inline {
                compressed,
                payload_size,
            } => copy_decoded_payload(hash, compressed.as_slice(), payload_size, output),
            StoredPayload::File {
                compressed_size,
                payload_size,
            } => {
                let path = self.store_dir.payload_spool_path(hash);
                let file = std::fs::File::open(&path)
                    .map_err(|error| read_error(hash, path.clone(), error))?;
                let size = file
                    .metadata()
                    .map_err(|error| read_error(hash, path, error))?
                    .len();
                if size != compressed_size {
                    return Err(PayloadStoreError::Storage {
                        hash,
                        error: format!(
                            "catalog records {compressed_size} compressed file bytes, but the spool contains {size}"
                        ),
                    });
                }
                copy_decoded_payload(hash, file, payload_size, output)
            }
        }
    }

    pub(super) fn verify_hash(
        self,
        expected: ObjectHash,
        actual: ObjectHash,
        inline: bool,
    ) -> Result<(), PayloadStoreError> {
        if actual == expected {
            return Ok(());
        }
        Err(if inline {
            PayloadStoreError::InlineContentMismatch { expected, actual }
        } else {
            PayloadStoreError::ContentMismatch {
                expected,
                actual,
                path: self.store_dir.payload_spool_path(expected),
            }
        })
    }
}

fn copy_decoded_payload(
    hash: ObjectHash,
    compressed: impl Read,
    payload_size: u64,
    output: &mut impl Write,
) -> Result<(), PayloadStoreError> {
    let mut decoder =
        lz4_flex::frame::FrameDecoder::new(compressed).take(payload_size.saturating_add(1));
    let mut size = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = decoder
            .read(&mut buffer)
            .map_err(|source| PayloadStoreError::CompressionIo { hash, source })?;
        if read == 0 {
            break;
        }
        size += read as u64;
        output
            .write_all(&buffer[..read])
            .map_err(|source| PayloadStoreError::CompressionIo { hash, source })?;
    }
    if size != payload_size {
        return Err(PayloadStoreError::Storage {
            hash,
            error: format!(
                "catalog records {payload_size} payload bytes, but decompression produced {size}"
            ),
        });
    }
    Ok(())
}

pub(super) struct HashedPayloadOutput<'output, W> {
    output: &'output mut W,
    hasher: coven_protocol::blob::ContentHasher,
}

impl<'output, W: Write> HashedPayloadOutput<'output, W> {
    pub(super) fn new(output: &'output mut W) -> Self {
        Self {
            output,
            hasher: coven_protocol::blob::ContentHasher::new(),
        }
    }

    pub(super) fn finish(self) -> ObjectHash {
        self.hasher
            .finish()
            .parse()
            .expect("SHA-256 hex is an ObjectHash")
    }
}

impl<W: Write> Write for HashedPayloadOutput<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.output.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.output.flush()
    }
}
