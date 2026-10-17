//! One committed store prefix, including claims whose contents were refused.

use crate::internal_schema::WRITE_DEVICE_SQL;
use crate::{sqlite::DatabaseConnection, write_encoding::counter, DbError};
use coven_crypto::SigningKey;
use coven_format::{admin::StoreChange, sequence::SlotHeader, snapshot::LastClaim};
use coven_foundation::id_source::{DeviceId, MemberId};
use coven_merge::{ClaimOrder, Slot, WriteId};
use rusqlite::params;

pub(crate) fn applied(db: &DatabaseConnection) -> Result<u64, DbError> {
    db.query_row(
        "SELECT applied FROM _coven_sequence WHERE singleton=1",
        [],
        |r| Ok(counter(r.get(0)?)),
    )
}

pub(crate) fn set_applied(db: &DatabaseConnection, slot: u64) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE _coven_sequence SET applied=?1 WHERE singleton=1",
        [slot.to_be_bytes().as_slice()],
    )?;
    Ok(())
}

pub(crate) fn claims(db: &DatabaseConnection) -> Result<Vec<LastClaim>, DbError> {
    db.query(
        "SELECT device,number,digest FROM _coven_last_claims ORDER BY device",
        [],
        |r| {
            Ok(LastClaim {
                device: DeviceId(counter(r.get(0)?)),
                number: counter(r.get(1)?),
                digest: r.get(2)?,
            })
        },
    )
}

/// Coverage orders observations from different audience snapshots and their suffixes.
pub(crate) fn note_record(
    db: &DatabaseConnection,
    claim: &LastClaim,
    through: u64,
) -> Result<(), DbError> {
    db.internal_execute("INSERT INTO _coven_last_claims(device,number,digest,through) VALUES(?1,?2,?3,?4) ON CONFLICT(device) DO UPDATE SET number=excluded.number,digest=excluded.digest,through=excluded.through WHERE excluded.through>=through AND excluded.number>=number",
        params![claim.device.0.to_be_bytes(),claim.number.to_be_bytes(),claim.digest,through.to_be_bytes()])?;
    Ok(())
}

pub(crate) fn note_claim(
    db: &DatabaseConnection,
    header: &SlotHeader,
    slot: Slot,
    digest: [u8; 32],
) -> Result<(), DbError> {
    note_record(
        db,
        &LastClaim {
            device: header.device,
            number: header.claim,
            digest: Some(digest),
        },
        slot.0,
    )
}

pub(crate) fn write_slot(db: &DatabaseConnection, write: WriteId) -> Result<Option<Slot>, DbError> {
    db.query_row(
        &format!(
            "SELECT (SELECT slot FROM _coven_writes WHERE {WRITE_DEVICE_SQL}=?1 AND number=?2)"
        ),
        params![
            write.device.0.to_be_bytes().as_slice(),
            write.number.to_be_bytes().as_slice()
        ],
        |r| Ok(r.get::<_, Option<Vec<u8>>>(0)?.map(|s| Slot(counter(s)))),
    )
}

/// Local originals have no containing slot until their batch is applied.
pub(crate) fn write_order(db: &DatabaseConnection, write: WriteId) -> Result<ClaimOrder, DbError> {
    Ok(match write_slot(db, write)? {
        Some(slot) => ClaimOrder::Published {
            slot,
            number: write.number,
        },
        None => ClaimOrder::Local {
            number: write.number,
        },
    })
}

/// Record the irrevocable cut-off introduced by an applied admin slot.
pub(crate) fn cutoff(
    db: &DatabaseConnection,
    slot: Slot,
    change: &StoreChange,
) -> Result<(), DbError> {
    let (device, member, signing) = match change {
        StoreChange::RemoveDevice { device }
        | StoreChange::AddDevice {
            replaces: Some(device),
            ..
        } => (Some(device.0.to_be_bytes().to_vec()), None, None),
        StoreChange::RemoveMember { member } => (None, Some(member.0.as_bytes().to_vec()), None),
        StoreChange::ReplaceMemberKeys {
            member, expected, ..
        } => (
            None,
            Some(member.0.as_bytes().to_vec()),
            Some(expected.signing.as_bytes().to_vec()),
        ),
        _ => return Ok(()),
    };
    db.internal_execute(
        "INSERT INTO _coven_cutoffs(slot,device,member,signing) VALUES(?1,?2,?3,?4)",
        params![slot.0.to_be_bytes().as_slice(), device, member, signing],
    )?;
    Ok(())
}

/// The earliest prior removal or replacement that refuses this original.
pub(crate) fn refusal(
    db: &DatabaseConnection,
    slot: Slot,
    device: DeviceId,
    member: MemberId,
    signer: &SigningKey,
) -> Result<Option<Slot>, DbError> {
    db.query_row("SELECT min(slot) FROM _coven_cutoffs WHERE slot<?1 AND (device=?2 OR (member=?3 AND (signing IS NULL OR signing=?4)))",
        params![slot.0.to_be_bytes().as_slice(),device.0.to_be_bytes().as_slice(),member.0.as_bytes().as_slice(),signer.as_bytes().as_slice()],
        |r|Ok(r.get::<_,Option<Vec<u8>>>(0)?.map(|s|Slot(counter(s)))))
}

pub(crate) fn refused(db: &DatabaseConnection, write: WriteId) -> Result<bool, DbError> {
    db.query_row(&format!("SELECT EXISTS(SELECT 1 FROM _coven_writes w JOIN _coven_write_refusals r ON r.slot=w.slot WHERE {WRITE_DEVICE_SQL}=?1 AND w.number=?2)"),
        params![write.device.0.to_be_bytes().as_slice(),write.number.to_be_bytes().as_slice()],|r|r.get(0))
}

#[cfg(test)]
#[path = "sequence_tests.rs"]
pub(crate) mod tests;
