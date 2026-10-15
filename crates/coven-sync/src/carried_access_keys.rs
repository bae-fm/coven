//! S3 keys a discarded database leaves behind (§10, §13, T203).
//!
//! §10's reset replaces a stale or damaged copy with a new device loaded from
//! storage. A key the old copy made under an unfinished replacement or
//! invite, but never recorded in a kept entry, would otherwise stay live at
//! the provider with nothing recording it; a deletion the old copy owed but
//! the store log does not derive would be forgotten. When the old database
//! can be read, its replacement keeps each such key pending
//! `DeleteAccessKey` (KeyDelivery `reset_keeps_accounted`).

use crate::operation_data::{Data, InviteGrant};
use crate::{PendingRecord, SyncError};
use coven_database::{Database, OperationRecord};
use coven_format::MemberAccess;
use coven_foundation::id_source::MemberId;

/// One key the discarded database held.
pub struct CarriedAccessKey {
    access: MemberAccess,
    member: Option<MemberId>,
    /// Already owed: a deletion is never cancelled, even by a later recording.
    owed: bool,
}

/// The keys a discarded database made but may not have recorded, and the
/// deletions it owed, from its operation journal.
pub fn discarded_access_keys(
    records: &[OperationRecord],
) -> Result<Vec<CarriedAccessKey>, SyncError> {
    let made = |key: &coven_storage::S3MemberKey| MemberAccess::S3AccessKey {
        access_key_id: key.credentials.access_key_id.clone(),
        principal: key.principal.clone(),
    };
    let mut keys = Vec::new();
    for record in records {
        match Data::read(record)? {
            Data::ReplaceAccessKey(work) => {
                if let Some(key) = &work.mint.minted {
                    keys.push(CarriedAccessKey {
                        access: made(key),
                        member: Some(work.member),
                        owed: false,
                    });
                }
            }
            Data::Invite(work) => {
                let key = match &work.access {
                    InviteGrant::S3Key(key) => Some(key),
                    InviteGrant::Minting(mint) => mint.minted.as_ref(),
                    InviteGrant::Account { .. } => None,
                };
                if let Some(key) = key {
                    // An ended invite's key is recorded for no member (§12.2).
                    keys.push(CarriedAccessKey {
                        access: made(key),
                        member: None,
                        owed: false,
                    });
                }
            }
            Data::DeleteAccessKey(key) => keys.push(CarriedAccessKey {
                access: MemberAccess::S3AccessKey {
                    access_key_id: key.access_key_id,
                    principal: key.principal,
                },
                member: key.member,
                owed: true,
            }),
            _ => {}
        }
    }
    Ok(keys)
}

/// Record each carried key as pending deletion in the replacement database,
/// which has loaded the store log. A made key the log records as an active
/// member's current access is in use and stays; an owed deletion always
/// stays pending. Keys already pending or confirmed deleted are skipped.
pub async fn carry_access_keys(
    database: &Database,
    keys: Vec<CarriedAccessKey>,
) -> Result<(), SyncError> {
    let local = database.local_store_log().await?;
    let state = &local.log.replay.state;
    let mut deletions = Vec::new();
    for key in keys {
        let in_use = state
            .members
            .values()
            .any(|member| !member.removed && member.access.same_credential(&key.access));
        if key.owed || !in_use {
            deletions.extend(crate::access_keys::deletion(&key.access, key.member).transpose()?);
        }
    }
    database
        .record_access_key_deletions::<PendingRecord>(None, deletions)
        .await?;
    Ok(())
}
