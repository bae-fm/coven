//! Bounds shared by every sealed object layout.

use crate::error::{bound, exact_length, require, Error, Rule};

use coven_crypto::{SEALED_OBJECT_CHUNK_OVERHEAD, SIGNATURE_LEN};

/// Bytes a stored sealed chunk adds to its plaintext: the 4-byte length
/// prefix, then the nonce and tag.
pub const STORED_CHUNK_OVERHEAD: usize = 4 + SEALED_OBJECT_CHUNK_OVERHEAD;

pub(crate) fn signature(bytes: &[u8]) -> Result<coven_crypto::Signature, Error> {
    exact_length(bytes.len(), SIGNATURE_LEN)?;
    Ok(coven_crypto::Signature::from_bytes(bytes.try_into().expect("a signature's length")))
}

pub(crate) fn prefix(bytes: &[u8], kind: u8) -> Result<(), Error> {
    let prefix = bytes.get(..3).ok_or(Error::Truncated)?;
    if prefix[0] != kind {
        return Err(Error::UnknownTag { field: "sealed object kind", tag: prefix[0] });
    }
    let crate::FormatVersion::V1 = crate::FormatVersion::decode(prefix)?;
    Ok(())
}

pub(crate) fn chunk_length(prefix: &[u8], maximum: usize) -> Result<usize, Error> {
    let bytes: [u8; 4] = prefix.get(..4).ok_or(Error::Truncated)?.try_into().expect("four bytes");
    let length = u32::from_be_bytes(bytes) as usize;
    bound(length, maximum, "sealed chunk")?;
    require(length > 0, "sealed chunk", Rule::Chunk)?;
    Ok(length + STORED_CHUNK_OVERHEAD)
}

pub(crate) fn chunk(bytes: &[u8], maximum: usize) -> Result<&[u8], Error> {
    exact_length(bytes.len(), chunk_length(bytes, maximum)?)?;
    Ok(&bytes[4..])
}

pub(crate) fn encode_chunk(bytes: &[u8]) -> Result<Vec<u8>, Error> {
    let length = bytes.len().checked_sub(SEALED_OBJECT_CHUNK_OVERHEAD).ok_or(Error::Truncated)?;
    require(length > 0, "sealed chunk", Rule::Chunk)?;
    bound(length, crate::wire::MAX_OBJECT, "sealed chunk")?;
    let mut result = Vec::new();
    result.try_reserve_exact(4 + bytes.len())?;
    result.extend_from_slice(&(length as u32).to_be_bytes());
    result.extend_from_slice(bytes);
    Ok(result)
}

#[cfg(test)]
#[path = "sealed_tests.rs"]
mod tests;
