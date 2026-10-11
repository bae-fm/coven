//! Local judgments stop downloads; signed peer reports only inform their author.

use crate::{sqlite::DatabaseConnection, DbError};
use crate::{LogObject, LogRefusal};
use coven_format::pending::RefusalCode;
use coven_foundation::id_source::DeviceId;
use std::time::SystemTime;

/// A stopped log observed here, or this device's log reported by a peer (§19.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StuckLog {
    /// The object, its author and the failed check.
    pub record: LogRefusal,
    /// None for a local judgment; otherwise the device whose signed positions report it.
    pub reported_by: Option<DeviceId>,
}

fn kind(object: LogObject) -> u8 {
    u8::from(matches!(object, LogObject::Entry(_)))
}

pub(crate) fn read(db: &DatabaseConnection) -> Result<Vec<StuckLog>, DbError> {
    db.query(
        "SELECT kind,device,number,failure,reporter FROM _coven_stuck_logs ORDER BY kind,device,reporter",
        [],
        |row| {
            Ok((
                row.get::<_, u8>(0)?,
                row.get::<_, [u8; 8]>(1)?,
                row.get::<_, [u8; 8]>(2)?,
                row.get::<_, u8>(3)?,
                row.get::<_, Vec<u8>>(4)?,
            ))
        },
    )?
    .into_iter()
    .map(|(log, device, number, failure, reporter)| {
        let device = DeviceId(u64::from_be_bytes(device));
        let number = u64::from_be_bytes(number);
        if number == 0 { return Err(DbError::DamagedDatabase); }
        let object = match log {
            0 => LogObject::Write(coven_merge::WriteId { device, number }),
            1 => LogObject::Entry(crate::EntryId { device, number }),
            _ => return Err(DbError::DamagedDatabase),
        };
        let record = LogRefusal { object, failure: RefusalCode::try_from(failure).map_err(|_| DbError::DamagedDatabase)? };
        let reported_by = if reporter.is_empty() {
            None
        } else {
            Some(DeviceId(u64::from_be_bytes(
                reporter.try_into().map_err(|_| DbError::DamagedDatabase)?,
            )))
        };
        Ok(StuckLog {
            record,
            reported_by,
        })
    })
    .collect()
}

pub(crate) fn local(db: &DatabaseConnection) -> Result<Vec<LogRefusal>, DbError> {
    Ok(read(db)?
        .into_iter()
        .filter(|log| log.reported_by.is_none())
        .map(|log| log.record)
        .collect())
}

pub(crate) fn record(
    db: &DatabaseConnection,
    record: LogRefusal,
    now: SystemTime,
) -> Result<(), DbError> {
    db.transaction(|db| {
        db.internal_execute(
            "INSERT INTO _coven_stuck_logs(kind,device,reporter,number,failure,judged_at,coven_version) VALUES(?1,?2,x'',?3,?4,?5,?6)
             ON CONFLICT(kind,device,reporter) DO UPDATE SET number=excluded.number,failure=excluded.failure,judged_at=excluded.judged_at,coven_version=excluded.coven_version
             WHERE excluded.number<_coven_stuck_logs.number",
            (kind(record.object), record.object.device().0.to_be_bytes().as_slice(), record.object.number().to_be_bytes().as_slice(), u8::from(record.failure), crate::user_file::encode_time(now), env!("CARGO_PKG_VERSION")),
        )?;
        Ok(())
    })
}

pub(crate) fn version_changed(db: &DatabaseConnection) -> Result<(), DbError> {
    db.transaction(|db| {
        db.internal_execute(
            "DELETE FROM _coven_stuck_logs WHERE coven_version!=?1",
            [env!("CARGO_PKG_VERSION")],
        )?;
        Ok(())
    })
}

pub(crate) fn peer_reports(
    db: &DatabaseConnection,
    own: DeviceId,
    reports: Vec<(DeviceId, LogRefusal)>,
) -> Result<(), DbError> {
    db.transaction(|db| {
        db.internal_execute("DELETE FROM _coven_stuck_logs WHERE length(reporter)=8", [])?;
        for (peer, record) in reports {
            if peer == own || record.object.device() != own {
                return Err(DbError::DamagedDatabase);
            }
            db.internal_execute(
                "INSERT INTO _coven_stuck_logs(kind,device,reporter,number,failure) VALUES(?1,?2,?3,?4,?5)
                 ON CONFLICT(kind,device,reporter) DO UPDATE SET number=excluded.number,failure=excluded.failure
                 WHERE excluded.number<_coven_stuck_logs.number",
                (
                    kind(record.object),
                    own.0.to_be_bytes().as_slice(),
                    peer.0.to_be_bytes().as_slice(),
                    record.object.number().to_be_bytes().as_slice(),
                    u8::from(record.failure),
                ),
            )?;
        }
        Ok(())
    })
}

#[cfg(test)]
#[path = "stuck_tests.rs"]
mod tests;
