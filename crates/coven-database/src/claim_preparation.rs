//! Ciphertext staging outside the application writer.
use crate::{sqlite::DatabaseConnection, ClaimAttempt, DbError};
use std::io::{self, Read};

pub(crate) struct PreparedClaim<R> {
    pub(crate) attempt: ClaimAttempt<R>,
    pub(crate) digest: coven_crypto::ObjectDigest,
    pub(crate) size: u64,
}

/// The input database owns this temporary table and its transaction. A failed
/// preparation rolls it back; a successful journal commit drops it before return.
pub(crate) fn prepare<T>(
    db: &DatabaseConnection,
    mut attempt: ClaimAttempt<impl Read>,
    consume: impl FnOnce(PreparedClaim<PreparedBytes<'_>>) -> Result<T, DbError>,
) -> Result<T, DbError> {
    db.batch("CREATE TEMP TABLE _coven_prepared_claim(chunk INTEGER PRIMARY KEY,bytes BLOB NOT NULL) STRICT")?;
    let mut hash = coven_crypto::ObjectHasher::new();
    let mut size = 0;
    for chunk in 0i64.. {
        let mut bytes = Vec::new();
        attempt
            .bytes
            .by_ref()
            .take(coven_format::chunks::CHUNK_SIZE as u64)
            .read_to_end(&mut bytes)
            .map_err(|error| DbError::SyncStream(Box::new(error)))?;
        if bytes.is_empty() {
            break;
        }
        hash.update(&bytes);
        size += bytes.len() as u64;
        db.internal_execute(
            "INSERT INTO temp._coven_prepared_claim VALUES(?1,?2)",
            (chunk, bytes),
        )?;
    }
    let result = consume(PreparedClaim {
        attempt: ClaimAttempt {
            path: attempt.path,
            bytes: PreparedBytes {
                db,
                chunk: 0,
                current: io::Cursor::new(Vec::new()),
                ended: false,
            },
        },
        digest: hash.finish(),
        size,
    })?;
    db.batch("DROP TABLE temp._coven_prepared_claim")?;
    Ok(result)
}

pub(crate) struct PreparedBytes<'a> {
    db: &'a DatabaseConnection,
    chunk: i64,
    current: io::Cursor<Vec<u8>>,
    ended: bool,
}
impl Read for PreparedBytes<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            let length = self.current.read(output)?;
            if length > 0 || self.ended {
                return Ok(length);
            }
            let bytes: Option<Vec<u8>> = self
                .db
                .query_row(
                    "SELECT (SELECT bytes FROM temp._coven_prepared_claim WHERE chunk=?1)",
                    [self.chunk],
                    |row| row.get(0),
                )
                .map_err(io::Error::other)?;
            match bytes {
                Some(bytes) => {
                    self.current = io::Cursor::new(bytes);
                    self.chunk += 1;
                }
                None => self.ended = true,
            }
        }
    }
}
