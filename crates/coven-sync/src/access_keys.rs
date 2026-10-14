//! S3 deletion obligations recorded with each store-log application, and the
//! evidence that clears one (§13, E9).
//!
//! Every S3 key a received entry records, kept or dropped, is the member's
//! current access, pending deletion, or confirmed deleted. Recording a
//! replacement also retires the key it replaced in its author's view, even
//! if the replacement later loses. Only an explicit confirmation or a
//! deletion with admin access followed by a read of that exact key's absence
//! clears an obligation; replay, time and retries never do.

use crate::{effects, operation_data::Data, AccessKeyToDelete, SyncError};
use coven_crypto::MemberId;
use coven_database::{AccessKeyDeletion, ReplayEntry, StoreLogCheck, StoreLogReplay};
use coven_format::{
    store_log::{StoreChange, StoreLogEntry},
    MemberAccess,
};
use coven_storage::StorageError;

/// The access an entry records and its member: creation's first admin, an
/// addition's new member, or a set-access entry's named member.
pub(crate) fn recorded(entry: &StoreLogEntry) -> Option<(&MemberId, &MemberAccess)> {
    match &entry.change {
        StoreChange::CreateStore { access, .. } => Some((&entry.author, access)),
        StoreChange::AddMember { keys, access, .. } => Some((&keys.signing, access)),
        StoreChange::SetAccess { member, access } => Some((member, access)),
        _ => None,
    }
}

/// The deletion obligation for one S3 key; shared accounts have none.
pub(crate) fn deletion(
    access: &MemberAccess,
    member: Option<MemberId>,
) -> Option<Result<AccessKeyDeletion, SyncError>> {
    let MemberAccess::S3AccessKey {
        access_key_id,
        principal,
    } = access
    else {
        return None;
    };
    let key = AccessKeyToDelete {
        access_key_id: access_key_id.clone(),
        principal: principal.clone(),
        member,
    };
    Some(
        Data::DeleteAccessKey(key.clone())
            .new_operation("coven")
            .map(|operation| AccessKeyDeletion { key, operation }),
    )
}

/// The obligations a batch creates, from the complete log and its new replay.
/// `previous` holds the entries already applied, `incoming` the batch. The
/// database skips keys already pending or confirmed, so repeating one is harmless.
pub(crate) fn deletions(
    previous: &[ReplayEntry],
    incoming: &[ReplayEntry],
    replay: &StoreLogReplay,
) -> Result<Vec<AccessKeyDeletion>, SyncError> {
    let all: Vec<ReplayEntry> = previous.iter().chain(incoming).cloned().collect();
    let mut keys: Vec<(MemberAccess, MemberId)> = Vec::new();
    let mut add = |access: &MemberAccess, member: &MemberId| {
        if matches!(access, MemberAccess::S3AccessKey { .. })
            && !keys.iter().any(|(known, _)| known.same_credential(access))
        {
            keys.push((access.clone(), member.clone()));
        }
    };
    // A key stops being intended when its member is no longer active or no
    // longer uses it, whatever the fate of the entry that recorded it.
    for entry in &all {
        if let Some((member, access)) = recorded(&entry.entry) {
            let current = effects::member(&replay.state, member);
            if current.is_none_or(|current| !current.access.same_credential(access)) {
                add(access, member);
            }
        }
    }
    // A replacement retires the key its author saw, even if it later drops.
    for entry in incoming {
        if let StoreChange::SetAccess { member, access } = &entry.entry.change {
            let positions = crate::replay::author_positions(&entry.entry);
            let view = crate::replay::at_entries(&all, &positions).state;
            if effects::check(&view, &entry.entry) == StoreLogCheck::NotAllowed {
                continue;
            }
            if let Some(old) = effects::member(&view, member) {
                if !old.access.same_credential(access) {
                    add(&old.access, member);
                }
            }
        }
    }
    keys.into_iter()
        .filter_map(|(access, member)| deletion(&access, Some(member)))
        .collect()
}

/// Whether a provider read found a key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyPresence {
    /// The provider still lists the key.
    Present,
    /// The provider's read no longer lists the key.
    Absent,
}

/// A provider read made after deleting a key, naming the key it read.
#[derive(Debug)]
pub(crate) struct KeyObservation {
    /// The key id the read concerned.
    pub(crate) access_key_id: String,
    /// What the read found, or why it failed.
    pub(crate) reply: Result<KeyPresence, StorageError>,
}

/// Why a deletion with admin access did not clear its notice (§13).
#[derive(Debug, thiserror::Error)]
pub enum DeleteAccessKeyError {
    /// The provider refused or failed the deletion.
    #[error("deleting the S3 key failed")]
    DeleteFailed(#[source] StorageError),
    /// No read followed the deletion.
    #[error("no provider read confirmed the S3 key's deletion")]
    ObservationMissing,
    /// The read concerned another key.
    #[error("the provider read concerned another S3 key")]
    WrongKey,
    /// The read still found the key.
    #[error("the provider still has the S3 key")]
    StillPresent,
    /// The read failed.
    #[error("reading the S3 key's state failed")]
    ObservationFailed(#[source] StorageError),
}

/// Accept a deletion only when it succeeded and a later read of this exact key
/// found it absent. Admin access is required by the deleting call itself.
pub(crate) fn verify_deletion(
    access_key_id: &str,
    deleted: Result<(), StorageError>,
    observation: Option<KeyObservation>,
) -> Result<(), DeleteAccessKeyError> {
    deleted.map_err(DeleteAccessKeyError::DeleteFailed)?;
    let read = observation.ok_or(DeleteAccessKeyError::ObservationMissing)?;
    if read.access_key_id != access_key_id {
        return Err(DeleteAccessKeyError::WrongKey);
    }
    match read.reply {
        Err(error) => Err(DeleteAccessKeyError::ObservationFailed(error)),
        Ok(KeyPresence::Present) => Err(DeleteAccessKeyError::StillPresent),
        Ok(KeyPresence::Absent) => Ok(()),
    }
}

#[cfg(test)]
#[path = "access_keys_tests.rs"]
mod tests;
