//! One authenticated D8 decoder for agreement checks and retention.

use crate::{PendingReason, PendingRecord, PendingSubject, Refusal, SyncError};
use coven_crypto::{ObjectHasher, StoreKeyring};
use coven_database::{Database, LocalStoreLog, StoreLog};
use coven_format::{objects::PostedPositions, sealed_single::SingleChunkObject, Object};
use coven_foundation::id_source::DeviceId;
use coven_merge::Audience;
use coven_storage::{ObjectPath, ObjectPrefix, Storage, StoredObject};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Observed {
    object: StoredObject,
    checked: Checked,
}

#[derive(Clone, Serialize, Deserialize)]
enum Checked {
    Valid(Vec<u8>),
    Invalid {
        version: String,
        reason: PendingReason,
    },
    Waiting(PendingReason),
}

/// One complete positions observation, shared by all consumers in a pass.
#[derive(Clone, Default)]
pub(crate) struct Observation {
    pub(crate) positions: BTreeMap<DeviceId, PostedPositions>,
}

pub(crate) fn decode(bytes: &[u8]) -> Result<PostedPositions, SyncError> {
    match Object::decode(bytes) {
        Ok(Object::PostedPositions(positions)) => Ok(positions),
        _ => Err(coven_database::DbError::DamagedDatabase.into()),
    }
}

fn same_revision(a: &StoredObject, b: &StoredObject) -> bool {
    a.path == b.path && a.size == b.size && a.stored_at == b.stored_at && a.revision == b.revision
}

pub(crate) fn failure(error: &SyncError, storage: &dyn Storage) -> PendingReason {
    match error {
        SyncError::Storage(error) => PendingReason::Storage {
            provider: storage.config().provider(),
            failure: error.failure(),
        },
        SyncError::KeyUnavailable(key) => PendingReason::KeyUnavailable {
            audience: Audience::Store,
            key: *key,
        },
        SyncError::Damaged(damage) => PendingReason::InvalidPositions(damage.failure.clone()),
        SyncError::UpdateRequired(update) => PendingReason::UpdateRequired(*update),
        _ => PendingReason::Failed(error.local_failure()),
    }
}

/// A failed observation keeps all preceding reports and checked peer state.
pub(crate) async fn observe(
    database: &Database,
    reads: &crate::pass_reads::PassReads,
    storage: &dyn Storage,
    local: &LocalStoreLog,
    ring: Option<&StoreKeyring>,
) -> Result<Observation, SyncError> {
    if let Some(observed) = reads.positions() {
        // Local writes and file references can change after the observation.
        // Reuse its authenticated reports against the current local needs.
        database
            .replace_pending_reports(relevant_reports(database, local, &observed).await?)
            .await?;
        return Ok(observed);
    }
    let result = scan(database, storage, local, ring).await;
    match result {
        Ok(observed) => {
            reads.keep_positions(observed.clone());
            Ok(observed)
        }
        Err((device, error)) => {
            database
                .record_pending(PendingRecord {
                    subject: PendingSubject::Positions(device),
                    reason: failure(&error, storage),
                    reported_by: local.device,
                })
                .await?;
            Err(error)
        }
    }
}

async fn scan(
    database: &Database,
    storage: &dyn Storage,
    local: &LocalStoreLog,
    ring: Option<&StoreKeyring>,
) -> Result<Observation, (DeviceId, SyncError)> {
    let local_error = |error: SyncError| (local.device, error);
    let previous = database
        .positions_observation::<Observed>()
        .await
        .map_err(|error| local_error(error.into()))?;
    let objects = storage
        .list(&ObjectPrefix::positions(local.store))
        .await
        .map_err(|error| local_error(error.into()))?;
    let mut next = BTreeMap::new();
    let mut observed = Observation::default();
    let mut conditions = Vec::new();
    // Peer removal resolves its observation. Own conditions belong to the
    // publication too, and clear only with confirmation of that publication.
    let subjects = database
        .pending::<PendingRecord>()
        .await
        .map_err(|error| local_error(error.into()))?
        .into_iter()
        .filter(|r| {
            r.reported_by == local.device
                && matches!(r.subject, PendingSubject::Positions(device) if device != local.device)
        })
        .map(|r| r.subject);
    let mut subjects: Vec<_> = subjects.collect();
    for object in objects {
        let device = object
            .path
            .device()
            .ok_or_else(|| local_error(coven_database::DbError::DamagedDatabase.into()))?;
        let subject = PendingSubject::Positions(device);
        if device != local.device {
            subjects.push(subject.clone());
        }
        let known = previous
            .get(&device)
            .filter(|known| same_revision(&known.object, &object));
        let (object, checked) = match known {
            Some(Observed {
                object,
                checked: Checked::Valid(bytes),
            }) => (object.clone(), Checked::Valid(bytes.clone())),
            Some(Observed {
                object,
                checked: checked @ Checked::Invalid { version, .. },
            }) if version == env!("CARGO_PKG_VERSION") => (object.clone(), checked.clone()),
            _ => check(database, storage, local, ring, &object)
                .await
                .map_err(|error| (device, error))?,
        };
        match &checked {
            Checked::Valid(bytes) => {
                observed
                    .positions
                    .insert(device, decode(bytes).map_err(local_error)?);
            }
            Checked::Invalid { reason, .. } | Checked::Waiting(reason) => {
                if device == local.device {
                    subjects.push(subject.clone());
                }
                conditions.push(PendingRecord {
                    subject,
                    reason: reason.clone(),
                    reported_by: local.device,
                })
            }
        }
        next.insert(device, Observed { object, checked });
    }
    let reports = relevant_reports(database, local, &observed)
        .await
        .map_err(local_error)?;
    database
        .complete_positions_observation(next, reports, subjects, conditions)
        .await
        .map_err(|error| local_error(error.into()))?;
    Ok(observed)
}

async fn check(
    database: &Database,
    storage: &dyn Storage,
    local: &LocalStoreLog,
    ring: Option<&StoreKeyring>,
    listed: &StoredObject,
) -> Result<(StoredObject, Checked), SyncError> {
    // The verdict belongs to the revision actually read, which can be newer
    // than the listing's.
    let (input, bytes) =
        crate::object_input::ObjectInput::whole_replaceable(database, storage, listed).await?;
    let object = input.object().clone();
    // A verdict on these bytes holds for this coven version; a wait for a
    // registration or key is checked again on the next listing.
    let checked = match open(&bytes, &object.path, ring, &local.log)? {
        Ok(positions) => Checked::Valid(Object::PostedPositions(positions).encode()?),
        Err(reason @ (PendingReason::InvalidPositions(_) | PendingReason::UpdateRequired(_))) => {
            Checked::Invalid {
                version: env!("CARGO_PKG_VERSION").into(),
                reason,
            }
        }
        Err(reason) => Checked::Waiting(reason),
    };
    Ok((object, checked))
}

async fn relevant_reports(
    database: &Database,
    local: &LocalStoreLog,
    observed: &Observation,
) -> Result<Vec<PendingRecord>, SyncError> {
    use coven_format::pending as wire;
    let state = database.sync_state(Vec::new()).await?;
    let files = database.retained_files().await?.references;
    let member = &local
        .log
        .replay
        .state
        .devices
        .get(&local.device)
        .ok_or(coven_database::DbError::DamagedDatabase)?
        .member;
    let mut reports = Vec::new();
    for peer in observed
        .positions
        .values()
        .filter(|peer| peer.device != local.device)
    {
        for report in &peer.pending {
            let own_object = match &report.subject {
                wire::PendingSubject::Write(id) => {
                    id.device == local.device && state.positions.covers(*id)
                }
                wire::PendingSubject::Entry(id) => {
                    id.device == local.device && state.store_log.covers(*id)
                }
                wire::PendingSubject::KeyCopy {
                    member: recipient, ..
                } => recipient == member,
                wire::PendingSubject::File { device, file } => {
                    *device == local.device || files.contains(&(*device, *file))
                }
                wire::PendingSubject::Snapshot(id) => id.device == local.device,
                wire::PendingSubject::Positions(device) => *device == local.device,
            };
            let needed = match &report.reason {
                wire::PendingReason::Missing { path }
                | wire::PendingReason::Waits(wire::Prerequisite::Object(path)) => {
                    path.device() == Some(local.device)
                }
                wire::PendingReason::Waits(wire::Prerequisite::DeviceRegistration(device)) => {
                    *device == local.device
                }
                _ => false,
            };
            if own_object || needed {
                reports.push(crate::pending_logs::reports::receive(
                    report.clone(),
                    peer.device,
                ));
            }
        }
    }
    Ok(reports)
}

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
