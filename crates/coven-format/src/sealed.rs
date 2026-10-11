//! Bounds shared by every sealed object layout.

use crate::error::{bound, require, Error, Rule};

use coven_crypto::SEALED_OBJECT_CHUNK_OVERHEAD;

pub(crate) fn signature(bytes: &[u8]) -> Result<coven_crypto::Signature, Error> {
    if bytes.len() < 64 {
        return Err(Error::Truncated);
    }
    if bytes.len() > 64 {
        return Err(Error::TrailingBytes);
    }
    Ok(coven_crypto::Signature::from_bytes(
        bytes.try_into().expect("64 bytes"),
    ))
}

pub(crate) fn prefix(bytes: &[u8], kind: u8) -> Result<(), Error> {
    let prefix = bytes.get(..3).ok_or(Error::Truncated)?;
    if prefix[0] != kind {
        return Err(Error::UnknownTag {
            field: "sealed object kind",
            tag: prefix[0],
        });
    }
    let crate::FormatVersion::V1 = crate::FormatVersion::decode(prefix)?;
    Ok(())
}

pub(crate) fn chunk_length(prefix: &[u8], maximum: usize) -> Result<usize, Error> {
    let bytes: [u8; 4] = prefix
        .get(..4)
        .ok_or(Error::Truncated)?
        .try_into()
        .expect("four bytes");
    let length = u32::from_be_bytes(bytes) as usize;
    bound(length, maximum, "sealed chunk")?;
    require(length > 0, "sealed chunk", Rule::Chunk)?;
    Ok(length + 4 + SEALED_OBJECT_CHUNK_OVERHEAD)
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
    let length = bytes
        .len()
        .checked_sub(SEALED_OBJECT_CHUNK_OVERHEAD)
        .ok_or(Error::Truncated)?;
    require(length > 0, "sealed chunk", Rule::Chunk)?;
    bound(length, crate::wire::MAX_OBJECT, "sealed chunk")?;
    let mut result = Vec::new();
    result
        .try_reserve_exact(4 + bytes.len())
        .map_err(|_| Error::Allocation)?;
    result.extend_from_slice(&(length as u32).to_be_bytes());
    result.extend_from_slice(bytes);
    Ok(result)
}

#[cfg(test)]
#[path = "sealed_tests.rs"]
mod tests;
