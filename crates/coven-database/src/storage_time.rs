//! The SQLite encoding of a provider storage time (§4): Unix seconds then
//! nanoseconds, big-endian, twelve bytes, so byte order is time order. It
//! keeps every digit the provider returned on every platform.

use crate::DbError;
use coven_format::value::StorageTime;

pub(crate) fn encode(time: StorageTime) -> Vec<u8> {
    let mut bytes = time.seconds().to_be_bytes().to_vec();
    bytes.extend(time.nanos().to_be_bytes());
    bytes
}

pub(crate) fn decode(bytes: &[u8]) -> Result<StorageTime, DbError> {
    let bytes: &[u8; 12] = bytes.try_into().map_err(|_| DbError::DamagedDatabase)?;
    StorageTime::new(
        u64::from_be_bytes(bytes[..8].try_into().expect("eight bytes")),
        u32::from_be_bytes(bytes[8..].try_into().expect("four bytes")),
    )
    .map_err(|_| DbError::DamagedDatabase)
}

#[cfg(test)]
#[path = "storage_time_tests.rs"]
mod tests;
