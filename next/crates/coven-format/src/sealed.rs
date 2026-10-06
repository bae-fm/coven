//! Bounds shared by the write and snapshot sealed layouts.

use crate::error::{bound, require, Error, Rule};

use coven_crypto::SEALED_OBJECT_CHUNK_OVERHEAD;

pub(crate) fn prefix(bytes: &[u8], kind: u8) -> Result<(), Error> {
    let prefix = bytes.get(..3).ok_or(Error::Truncated)?;
    require(prefix[0] == kind, "sealed object kind", Rule::Kind)?;
    let version = u16::from_be_bytes([prefix[1], prefix[2]]);
    if version != crate::FORMAT_VERSION {
        return Err(Error::UnsupportedVersion(version));
    }
    Ok(())
}

pub(crate) fn chunk_length(prefix: &[u8], maximum: usize) -> Result<usize, Error> {
    let bytes: [u8; 4] = prefix
        .get(..4)
        .ok_or(Error::Truncated)?
        .try_into()
        .expect("four bytes");
    let length = u32::from_be_bytes(bytes) as usize;
    bound(
        length,
        maximum + SEALED_OBJECT_CHUNK_OVERHEAD,
        "sealed chunk",
    )?;
    require(
        length > SEALED_OBJECT_CHUNK_OVERHEAD,
        "sealed chunk",
        Rule::Chunk,
    )?;
    Ok(length + 4)
}

pub(crate) fn chunk(bytes: &[u8], maximum: usize) -> Result<&[u8], Error> {
    let length = chunk_length(bytes, maximum)?;
    if bytes.len() < length {
        return Err(Error::Truncated);
    }
    if bytes.len() > length {
        return Err(Error::TrailingBytes);
    }
    Ok(&bytes[4..])
}

pub(crate) fn encode_chunk(bytes: &[u8]) -> Result<Vec<u8>, Error> {
    let mut result = Vec::new();
    result
        .try_reserve_exact(4 + bytes.len())
        .map_err(|_| Error::Allocation)?;
    result.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    result.extend_from_slice(bytes);
    Ok(result)
}
