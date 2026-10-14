//! Received immutable input is independent of causally applied positions.

use crate::{sqlite::DatabaseConnection, DbError};
use coven_foundation::id_source::DeviceId;
use std::num::NonZeroU64;

/// Permanent numbered input discovered independently for each writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogKind {
    /// Membership entries, including entries awaiting keys or causes.
    Entry,
    /// Sealed key copies, including copies whose contents were refused.
    KeyCopy,
}

impl LogKind {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Entry => "entry",
            Self::KeyCopy => "key",
        }
    }
}

/// One complete encrypted object and its provider observation. The sync owner
/// encodes the observation; the database retains it without interpreting
/// provider-specific evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogInput {
    /// The writer whose immutable number was read.
    pub writer: DeviceId,
    /// The positive number in that writer's log.
    pub number: NonZeroU64,
    /// The provider metadata of the read, in the sync owner's encoding.
    pub observation: Vec<u8>,
    /// The complete encrypted object exactly as read.
    pub bytes: Vec<u8>,
}

pub(crate) fn inputs(db: &DatabaseConnection, kind: LogKind) -> Result<Vec<LogInput>, DbError> {
    db.query(
        "SELECT writer,number,observation,bytes FROM _coven_log_inputs WHERE kind=?1 ORDER BY writer,number",
        [kind.name()],
        |row| {
            Ok(LogInput {
                writer: DeviceId(u64::from_be_bytes(row.get(0)?)),
                number: NonZeroU64::new(u64::from_be_bytes(row.get(1)?))
                    .ok_or(rusqlite::Error::InvalidQuery)?,
                observation: row.get(2)?,
                bytes: row.get(3)?,
            })
        },
    )
}

pub(crate) fn positions(
    db: &DatabaseConnection,
    kind: LogKind,
) -> Result<Vec<(DeviceId, u64)>, DbError> {
    db.query(
        "SELECT writer,number FROM _coven_log_downloads WHERE kind=?1 ORDER BY writer",
        [kind.name()],
        |row| {
            Ok((
                DeviceId(u64::from_be_bytes(row.get(0)?)),
                u64::from_be_bytes(row.get(1)?),
            ))
        },
    )
}

pub(crate) fn remember_writers(
    db: &DatabaseConnection,
    writers: &[DeviceId],
) -> Result<(), DbError> {
    db.transaction(|db| {
        for writer in writers {
            for kind in [LogKind::Entry, LogKind::KeyCopy] {
                // Zero records knowledge of a writer without claiming any received input.
                db.internal_execute(
                    "INSERT INTO _coven_log_downloads(kind,writer,number) VALUES(?1,?2,x'0000000000000000') ON CONFLICT DO NOTHING",
                    (kind.name(), &writer.0.to_be_bytes()[..]),
                )?;
            }
        }
        Ok(())
    })
}

/// Retain complete inputs and advance each writer's contiguous position across
/// them in one transaction. Out-of-order completions remain for later.
pub(crate) fn keep(
    db: &DatabaseConnection,
    kind: LogKind,
    inputs: &[LogInput],
) -> Result<std::collections::BTreeMap<DeviceId, u64>, DbError> {
    db.transaction(|db| {
        let mut writers = std::collections::BTreeSet::new();
        for input in inputs {
            insert(db, kind, input)?;
            writers.insert(input.writer);
        }
        writers
            .into_iter()
            .map(|writer| Ok((writer, advance(db, kind, writer)?)))
            .collect()
    })
}

fn insert(db: &DatabaseConnection, kind: LogKind, input: &LogInput) -> Result<(), DbError> {
    let writer = input.writer.0.to_be_bytes();
    let number = input.number.get().to_be_bytes();
    let previous = db
        .query(
            "SELECT observation,bytes FROM _coven_log_inputs WHERE kind=?1 AND writer=?2 AND number=?3",
            (kind.name(), &writer[..], &number[..]),
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )?
        .into_iter()
        .next();
    match previous {
        // Discovery never rereads a retained number, so another version here
        // means the retained table no longer matches its own reads.
        Some((observation, bytes)) if observation != input.observation || bytes != input.bytes => {
            Err(DbError::DamagedDatabase)
        }
        Some(_) => Ok(()),
        None => db
            .internal_execute(
                "INSERT INTO _coven_log_inputs(kind,writer,number,observation,bytes) VALUES(?1,?2,?3,?4,?5)",
                (kind.name(), &writer[..], &number[..], &input.observation, &input.bytes),
            )
            .map(|_| ()),
    }
}

fn advance(db: &DatabaseConnection, kind: LogKind, writer: DeviceId) -> Result<u64, DbError> {
    let writer = writer.0.to_be_bytes();
    let previous = db
        .query(
            "SELECT number FROM _coven_log_downloads WHERE kind=?1 AND writer=?2",
            (kind.name(), &writer[..]),
            |row| Ok(u64::from_be_bytes(row.get(0)?)),
        )?
        .into_iter()
        .next();
    // No row means this writer has no contiguous retained input yet.
    let mut position = previous.unwrap_or(0);
    while let Some(next) = position.checked_add(1) {
        let present: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM _coven_log_inputs WHERE kind=?1 AND writer=?2 AND number=?3)",
            (kind.name(), &writer[..], &next.to_be_bytes()[..]),
            |row| row.get(0),
        )?;
        if !present {
            break;
        }
        position = next;
    }
    if previous != Some(position) {
        db.internal_execute(
            "INSERT INTO _coven_log_downloads(kind,writer,number) VALUES(?1,?2,?3) ON CONFLICT(kind,writer) DO UPDATE SET number=excluded.number",
            (kind.name(), &writer[..], &position.to_be_bytes()[..]),
        )?;
    }
    Ok(position)
}

#[cfg(test)]
#[path = "log_input_tests.rs"]
mod tests;
