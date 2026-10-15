//! Whether a new installation's member binding admits it to register its
//! device (§12.1, §12.2).

use crate::SyncError;
use coven_database::{EntryOutcome, StoreLog};
use coven_format::store_log::{MemberPublicKeys, StoreChange};
use coven_foundation::id_source::{DeviceId, MemberId};

/// How a new installation reached its member id and pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingSource {
    /// A restore code: its pair must be the id's current binding. An old
    /// code never binds its keys back to the id (§12.1).
    Restore,
    /// An invite: the approving writer's own binding of this id to exactly
    /// this pair must be kept. Another writer's equal-payload winner, an
    /// active target or a usable prerequisite copy does not admit it (§12.2).
    Join {
        /// The inviting device, the only one that approves the request.
        approver: DeviceId,
    },
}

/// `Ok(true)` once the checked store log admits `member` with `keys`, and
/// `Ok(false)` while it still waits for the binding. A dropped approval
/// reports its replay reason; a binding replaced or removed since reports
/// that instead.
pub(crate) fn admitted(
    log: &StoreLog,
    member: &MemberId,
    keys: &MemberPublicKeys,
    admission: BindingSource,
) -> Result<bool, SyncError> {
    let current = crate::effects::member(&log.replay.state, member);
    let bound = || match current {
        Some(held) if held.keys == *keys => Ok(true),
        Some(_) => Err(SyncError::MemberKeysReplaced { member: *member }),
        None => Err(SyncError::Removed),
    };
    let BindingSource::Join { approver } = admission else {
        return match current {
            None => Ok(false),
            Some(_) => bound(),
        };
    };
    let mut dropped = None;
    for applied in &log.entries {
        let entry = &applied.entry;
        let binds = entry.position.device == approver
            && match &entry.change {
                StoreChange::AddMember {
                    member: id,
                    keys: pair,
                    ..
                }
                | StoreChange::ReplaceMemberKeys {
                    member: id,
                    keys: pair,
                } => id == member && pair == keys,
                _ => false,
            };
        if !binds {
            continue;
        }
        match log.replay.entries.get(&entry.position) {
            Some(EntryOutcome::Kept) => return bound(),
            Some(EntryOutcome::Dropped(reason)) => {
                dropped.get_or_insert_with(|| reason.clone());
            }
            None => {}
        }
    }
    match dropped {
        Some(reason) => Err(SyncError::Rejected(reason)),
        None => Ok(false),
    }
}
