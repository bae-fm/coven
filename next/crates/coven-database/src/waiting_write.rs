//! First-observed waiting times survive retries and process restarts.

use crate::{sqlite::DatabaseConnection, DbError, WriteId};
use std::collections::BTreeSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) fn record(
    database: &DatabaseConnection,
    writes: Vec<WriteId>,
    now: SystemTime,
) -> Result<Vec<(WriteId, SystemTime)>, DbError> {
    let now = now
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DbError::ClockOutOfRange)?;
    let bytes: Vec<_> = now
        .as_secs()
        .to_be_bytes()
        .into_iter()
        .chain(now.subsec_nanos().to_be_bytes())
        .collect();
    database.transaction(|database| {
        let selected: BTreeSet<_> = writes.into_iter().collect();
        let old = database.query("SELECT device,number FROM _coven_waiting_writes", [], |r| Ok(WriteId {
            device: coven_foundation::id_source::DeviceId(crate::write_encoding::counter(r.get(0)?)),
            number: crate::write_encoding::counter(r.get(1)?),
        }))?;
        for write in old.into_iter().filter(|w| !selected.contains(w)) {
            database.internal_execute("DELETE FROM _coven_waiting_writes WHERE device=?1 AND number=?2", (write.device.0.to_be_bytes().as_slice(), write.number.to_be_bytes().as_slice()))?;
        }
        selected.into_iter().map(|write| {
            database.internal_execute("INSERT INTO _coven_waiting_writes(device,number,since) VALUES(?1,?2,?3) ON CONFLICT DO NOTHING", (write.device.0.to_be_bytes().as_slice(), write.number.to_be_bytes().as_slice(), &bytes))?;
            let time: Vec<u8> = database.query_row("SELECT since FROM _coven_waiting_writes WHERE device=?1 AND number=?2", (write.device.0.to_be_bytes().as_slice(), write.number.to_be_bytes().as_slice()), |r| r.get(0))?;
            let seconds = u64::from_be_bytes(time[..8].try_into().expect("stored seconds"));
            let nanos = u32::from_be_bytes(time[8..].try_into().expect("stored nanoseconds"));
            if nanos >= 1_000_000_000 { return Err(DbError::DamagedDatabase); }
            let since = UNIX_EPOCH.checked_add(Duration::new(seconds, nanos)).ok_or(DbError::ClockOutOfRange)?;
            Ok((write, since))
        }).collect()
    })
}
