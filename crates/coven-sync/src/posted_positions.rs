//! One authenticated D8 decoder for agreement checks and retention.

use crate::{
    write_object::{checked, damaged, invalid},
    ObjectCheckFailure, SyncError,
};
use coven_crypto::StoreKeyring;
use coven_format::{objects::PostedPositions, sealed_single::SingleChunkObject, Object};
use coven_merge::Audience;
use coven_storage::ObjectPath;

pub(crate) fn open(
    bytes: &[u8],
    path: &ObjectPath,
    ring: Option<&StoreKeyring>,
) -> Result<PostedPositions, SyncError> {
    let object = checked(path, SingleChunkObject::decode(bytes))?;
    let SingleChunkObject::PostedPositions { key, chunk } = object else {
        return Err(damaged(
            path,
            invalid("positions path holds another object"),
        ));
    };
    let secret = crate::write_seal::derive(
        ring.ok_or(SyncError::KeyUnavailable(key))?,
        &Audience::Store,
        key,
    )?;
    let plain = secret
        .open_object_chunk(path.as_str(), &object.prefix().encode()?, 0, 0, chunk)
        .map_err(|error| damaged(path, ObjectCheckFailure::Decryption(error)))?;
    let Object::PostedPositions(positions) = checked(path, Object::decode(&plain))? else {
        return Err(damaged(
            path,
            invalid("posted positions contain another frame"),
        ));
    };
    if path.device() != Some(positions.device) {
        return Err(damaged(path, invalid("positions path and device differ")));
    }
    Ok(positions)
}
