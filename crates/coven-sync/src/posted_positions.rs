//! One authenticated D8 decoder for agreement checks and retention.

use crate::{PendingReason, Refusal, SyncError};
use coven_crypto::{ObjectHasher, StoreKeyring};
use coven_database::StoreLog;
use coven_format::{objects::PostedPositions, sealed_single::SingleChunkObject, Object};
use coven_merge::Audience;
use coven_storage::ObjectPath;
use std::sync::Arc;

/// Authenticate and decode a peer's posted positions. The inner error is the
/// positions subject's first unmet condition, in processing order: a newer
/// format, its author's registration or store key not yet applied, or a failed
/// check of this replaceable object, which counts as not posted (§19.1). The
/// outer error is a local failure.
pub(crate) fn open(
    bytes: &[u8],
    path: &ObjectPath,
    ring: Option<&StoreKeyring>,
    log: &StoreLog,
) -> Result<Result<PostedPositions, PendingReason>, SyncError> {
    let invalid = |failure| Ok(Err(PendingReason::InvalidPositions(failure)));
    let object = match checked(SingleChunkObject::decode(bytes)) {
        Ok(object) => object,
        Err(reason) => return Ok(Err(reason)),
    };
    // The path names a positions object; another kind has the wrong identity.
    let SingleChunkObject::PostedPositions {
        key,
        chunk,
        ref signature,
    } = object
    else {
        return invalid(Refusal::WrongIdentity { cause: None });
    };
    let author = match crate::object_author::member(log, path.device().expect("positions path")) {
        Ok(author) => author,
        Err(reason) => return Ok(Err(reason)),
    };
    let mut hash = ObjectHasher::new();
    hash.update(&bytes[..bytes.len() - 64]);
    if let Err(error) = author.verify_object(path.as_str(), &hash.finish(), signature) {
        return invalid(Refusal::Signature {
            cause: Some(Arc::new(error)),
        });
    }
    let secret = match ring.map(|ring| crate::write_seal::derive(ring, &Audience::Store, key)) {
        Some(Ok(secret)) => secret,
        None | Some(Err(SyncError::KeyUnavailable(_))) => {
            return Ok(Err(PendingReason::KeyUnavailable {
                audience: Audience::Store,
                key,
            }))
        }
        Some(Err(error)) => return Err(error),
    };
    let plain =
        match secret.open_object_chunk(path.as_str(), &object.prefix().encode()?, 0, 0, chunk) {
            Ok(plain) => plain,
            Err(error) => {
                return invalid(Refusal::Decryption {
                    cause: Some(Arc::new(error)),
                })
            }
        };
    let positions = match checked(Object::decode(&plain)) {
        Ok(Object::PostedPositions(positions)) => positions,
        Ok(_) => return invalid(Refusal::WrongIdentity { cause: None }),
        Err(reason) => return Ok(Err(reason)),
    };
    if path.device() != Some(positions.device) {
        return invalid(Refusal::WrongIdentity { cause: None });
    }
    Ok(checked(positions.validate_reporter(author, path.store())).map(|()| positions))
}

/// A newer format waits for an update; any other format failure refuses
/// this version of the positions.
fn checked<T>(value: Result<T, coven_format::Error>) -> Result<T, PendingReason> {
    value.map_err(|error| match crate::error::required_format(&error) {
        Some(update) => PendingReason::UpdateRequired(update),
        None => PendingReason::InvalidPositions(Refusal::from(error)),
    })
}

#[cfg(test)]
#[path = "posted_positions_tests.rs"]
pub(crate) mod tests;
