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
        let payload_size = stored.payload_size;
        self.verify_chunk_set(hash, &stored)?;
        let chunks = PayloadChunkReader::open(self.conn, hash, stored)?;
        copy_decoded_payload(hash, chunks, payload_size, output)
    }

    pub(super) fn verify_hash(
        self,
        expected: ObjectHash,
        actual: ObjectHash,
    ) -> Result<(), PayloadStoreError> {
        if actual == expected {
            return Ok(());
        }
        Err(PayloadStoreError::ContentMismatch { expected, actual })
    }
}

/// One payload's compressed frame, read back in ordinal order one chunk at a
/// time, so decoding a payload larger than memory holds one chunk rather than
/// the frame. The chunk set is checked against its catalog row before the first
/// byte is read, and a missing ordinal ends the stream short of
/// `compressed_size`, which the decoder's length check then refuses.
struct PayloadChunkReader<'store> {
    statement: rusqlite::Statement<'store>,
    hash: ObjectHash,
    chunk_count: u64,
    next_ordinal: u64,
    chunk: Vec<u8>,
    offset: usize,
}

impl<'store> PayloadChunkReader<'store> {
    fn open(
        conn: &'store rusqlite::Connection,
        hash: ObjectHash,
        stored: StoredPayload,
    ) -> Result<Self, PayloadStoreError> {
        let statement = conn
            .prepare(
                "SELECT bytes FROM payload_chunks
                 WHERE payload_hash = ?1 AND ordinal = ?2",
            )
            .map_err(|source| PayloadStoreError::Database { hash, source })?;
        Ok(Self {
            statement,
            hash,
            chunk_count: stored.chunk_count,
            next_ordinal: 0,
            chunk: Vec::new(),
            offset: 0,
        })
    }

    /// Load the next chunk, reporting whether the frame continues.
    fn advance(&mut self) -> Result<bool, PayloadStoreError> {
        if self.next_ordinal == self.chunk_count {
            return Ok(false);
        }
        let hash = self.hash;
        let ordinal = i64::try_from(self.next_ordinal)
            .map_err(|source| PayloadStoreError::SizeConversion { hash, source })?;
        self.chunk = self
            .statement
            .query_row(rusqlite::params![hash.to_string(), ordinal], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .map_err(|source| match source {
                rusqlite::Error::QueryReturnedNoRows => PayloadStoreError::Storage {
                    hash,
                    error: format!(
                        "payload chunk {ordinal} is absent, but its catalog row names {} chunks",
                        self.chunk_count
                    ),
                },
                source => PayloadStoreError::Database { hash, source },
            })?;
        self.offset = 0;
        self.next_ordinal += 1;
        Ok(true)
    }
}

impl Read for PayloadChunkReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        while self.offset == self.chunk.len() {
            if !self.advance().map_err(std::io::Error::other)? {
                return Ok(0);
            }
        }
        let taken = (self.chunk.len() - self.offset).min(out.len());
        out[..taken].copy_from_slice(&self.chunk[self.offset..self.offset + taken]);
        self.offset += taken;
        Ok(taken)
    }
}

/// Decompress `chunks` into `output`, bounded by the payload length the catalog
/// states: a frame that decodes to more or less than its row says is refused
/// rather than accepted.
fn copy_decoded_payload(
    hash: ObjectHash,
    chunks: PayloadChunkReader<'_>,
    payload_size: u64,
    output: &mut impl Write,
) -> Result<(), PayloadStoreError> {
    let mut decoder =
        lz4_flex::frame::FrameDecoder::new(chunks).take(payload_size.saturating_add(1));
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
