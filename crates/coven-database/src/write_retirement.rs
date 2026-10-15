//! Which kept entries retire a received write (§10, D5): removal of its
//! device or member, replacement of its device id, and each unread
//! replacement of its owner's keys installing another signing key. A key
//! replacement retires no device: its member's device logs continue.

use crate::{
    sqlite::DatabaseConnection, store_log_tables::entry_id, write_landing::Retirement, DbError,
};
use coven_crypto::SigningKey;
use coven_format::{store_log::StoreChange, write::WriteHeader};
use coven_foundation::id_source::DeviceId;
use rusqlite::params;

/// The columns `_coven_store_log` keeps for an entry that can retire writes:
/// the device a removal or device replacement retires, the member a removal
/// retires, a member-key replacement's target and new signing key (§10), and
/// the device a registration names, which retires that device if replay
/// drops the registration, or it landed too late, and keeps none (§6, §10).
#[derive(Default)]
pub(crate) struct Retires {
    pub(crate) device: Option<Vec<u8>>,
    pub(crate) member: Option<Vec<u8>>,
    pub(crate) keys_member: Option<Vec<u8>>,
    pub(crate) keys_signing: Option<Vec<u8>>,
    pub(crate) registers: Option<Vec<u8>>,
}

pub(crate) fn retires(entry: &coven_format::store_log::StoreLogEntry) -> Retires {
    let bytes = |device: &coven_foundation::id_source::DeviceId| device.0.to_be_bytes().to_vec();
    match &entry.change {
        StoreChange::AddDevice {
            device, replaces, ..
        } => Retires {
            device: replaces.as_ref().map(bytes),
            registers: Some(bytes(device)),
            ..Retires::default()
        },
        StoreChange::RemoveDevice { device } => Retires {
            device: Some(bytes(device)),
            ..Retires::default()
        },
        StoreChange::RemoveMember { member } => Retires {
            member: Some(member.0.as_bytes().to_vec()),
            ..Retires::default()
        },
        StoreChange::ReplaceMemberKeys { member, keys } => Retires {
            keys_member: Some(member.0.as_bytes().to_vec()),
            keys_signing: Some(keys.signing.as_bytes().to_vec()),
            ..Retires::default()
        },
        _ => Retires::default(),
    }
}

/// Every entry retiring this write, except permanently late removals and key
/// replacements, with whether it is in force and whether it is final (stored
/// strictly before the horizon, §9). A retirement is in force while replay
/// keeps it; a registration of the write's device that replay does not keep,
/// dropped or late, is one while no kept entry registers that device
/// (`Writers.droppedRetirements`). A key replacement retires it only when the write's own
/// `store_log_read` did not cover it and it installs a signing key other than
/// `signer`, the key current for the owner in that recorded view: a read
/// replacement cannot retire a later key current there, and the winner's
/// own key is current (`OldKeyObjects.retires`).
pub(crate) fn candidates(
    db: &DatabaseConnection,
    header: &WriteHeader,
    signer: &SigningKey,
) -> Result<Vec<(Retirement, bool, bool)>, DbError> {
    Ok(of_device(db, header.position.device, signer)?
        .into_iter()
        // A device retirement counts read or not: one the write's own view
        // kept already refused it, and one that view dropped is judged by
        // time alone (`read_retirement_judged_by_time`). A key replacement
        // counts only unread.
        .filter(|(retirement, _, _, key)| !key || !header.store_log_read.covers(retirement.entry))
        .map(|(retirement, kept, fixed, _)| (retirement, kept, fixed))
        .collect())
}

/// Every entry that can retire a write of `device` signed with `signer`,
/// except permanently late removals and key replacements, with whether it is
/// in force, whether it is
/// final, and whether it is a key replacement, read or not.
pub(crate) fn of_device(
    db: &DatabaseConnection,
    device: DeviceId,
    signer: &SigningKey,
) -> Result<Vec<(Retirement, bool, bool, bool)>, DbError> {
    let horizon = crate::store_log_tables::horizon(db)?;
    let rows = db.query(
        "SELECT device,number,stored_at,outcome='kept',0 FROM _coven_store_log
         WHERE retires_device=?1 AND outcome<>'late'
         UNION ALL
         SELECT device,number,stored_at,outcome='kept',0 FROM _coven_store_log
         WHERE retires_member=(SELECT member FROM _coven_devices WHERE device=?1) AND outcome<>'late'
         UNION ALL
         SELECT device,number,stored_at,outcome='kept',1 FROM _coven_store_log
         WHERE keys_member=(SELECT member FROM _coven_devices WHERE device=?1) AND outcome<>'late'
           AND keys_signing<>?2
         UNION ALL
         SELECT device,number,stored_at,
           NOT EXISTS(SELECT 1 FROM _coven_devices WHERE device=?1),0 FROM _coven_store_log
         WHERE registers_device=?1 AND outcome<>'kept'",
        params![device.0.to_be_bytes().as_slice(), signer.as_bytes().as_slice()],
        |r| {
            Ok((
                entry_id(r, 0)?,
                r.get::<_, Vec<u8>>(2)?,
                r.get::<_, bool>(3)?,
                r.get::<_, bool>(4)?,
            ))
        },
    )?;
    rows.into_iter()
        .map(|(entry, stored, kept, key)| {
            let stored_at = crate::storage_time::decode(&stored)?;
            let fixed = horizon.is_some_and(|h| stored_at < h);
            Ok((Retirement { entry, stored_at }, kept, fixed, key))
        })
        .collect()
}

#[cfg(test)]
#[path = "write_retirement_tests.rs"]
mod tests;
