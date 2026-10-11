//! One authenticated D8 decoder for agreement checks and retention.

use crate::{
    write_object::{checked, damaged, invalid},
    Refusal, SyncError,
};
use coven_crypto::{ObjectHasher, StoreKeyring};
use coven_database::StoreLog;
use coven_format::{objects::PostedPositions, sealed_single::SingleChunkObject, Object};
use coven_merge::Audience;
use coven_storage::ObjectPath;
use std::sync::Arc;

pub(crate) fn open(
    bytes: &[u8],
    path: &ObjectPath,
    ring: Option<&StoreKeyring>,
    log: &StoreLog,
) -> Result<PostedPositions, SyncError> {
    let object = checked(path, SingleChunkObject::decode(bytes))?;
    let SingleChunkObject::PostedPositions {
        key,
        chunk,
        ref signature,
    } = object
    else {
        return Err(damaged(
            path,
            invalid("positions path holds another object"),
        ));
    };
    let author = crate::object_author::member(log, path)?;
    let mut hash = ObjectHasher::new();
    hash.update(&bytes[..bytes.len() - 64]);
    author
        .verify_object(path.as_str(), &hash.finish(), signature)
        .map_err(|error| {
            damaged(
                path,
                Refusal::Signature {
                    cause: Some(Arc::new(error)),
                },
            )
        })?;
    let secret = crate::write_seal::derive(
        ring.ok_or(SyncError::KeyUnavailable(key))?,
        &Audience::Store,
        key,
    )?;
    let plain = secret
        .open_object_chunk(path.as_str(), &object.prefix().encode()?, 0, 0, chunk)
        .map_err(|error| {
            damaged(
                path,
                Refusal::Decryption {
                    cause: Some(Arc::new(error)),
                },
            )
        })?;
    let Object::PostedPositions(positions) = checked(path, Object::decode(&plain))? else {
        return Err(damaged(
            path,
            invalid("posted positions contain another frame"),
        ));
    };
    if path.device() != Some(positions.device) {
        return Err(damaged(path, Refusal::WrongIdentity { cause: None }));
    }
    checked(path, positions.validate_reporter(author))?;
    Ok(positions)
}

#[cfg(test)]
#[path = "posted_positions_tests.rs"]
pub(crate) mod tests;
