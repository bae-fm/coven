//! Durable coverage facts used to judge writes after a breaking change or reset.
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, decoded, encoded};
use crate::DbError;
use coven_format::{
    merge_fields,
    snapshot_rows::LostWriteCause,
    value::WritePositions,
    write::{WriteHeader, WritePart},
};
use coven_merge::{Audience, WriteId};
use rusqlite::params;

/// Coverage of applied store-log boundaries, in application order.
pub(crate) enum WriteBoundary {
    /// An applied breaking schema change excludes uncovered older-schema writes.
    SchemaChange {
        /// The version reached by the change.
        version: u32,
        /// Writes included in its snapshot.
        included: WritePositions,
    },
    /// An applied reset excludes a write whose cause its snapshot omitted.
    Reset {
        /// The reset's store-log identity.
        entry: coven_format::value::EntryId,
        /// The audience reset by this snapshot.
        audience: Audience,
        /// Writes included in its snapshot.
        included: WritePositions,
    },
}

impl WriteBoundary {
    pub(crate) fn record(&self, database: &DatabaseConnection) -> Result<bool, DbError> {
        let (cause, audience, included) = match self {
            Self::SchemaChange { version, included } => {
                (LostWriteCause::SchemaChange(*version), None, included)
            }
            Self::Reset {
                entry,
                audience,
                included,
            } => (
                LostWriteCause::Reset(*entry),
                Some(audience_text(audience)),
                included,
            ),
        };
        database.transaction(|database| {
            Ok(database.internal_execute(
                "INSERT INTO coven_applied_boundaries(cause,audience,included) VALUES(?1,?2,?3) ON CONFLICT(cause) DO NOTHING",
                params![encoded(merge_fields::encode_lost_write_cause(&cause))?, audience, encoded(merge_fields::encode_write_positions(included))?],
            )? != 0)
        })
    }

    pub(crate) fn load(database: &DatabaseConnection) -> Result<Vec<Self>, DbError> {
        database.query(
            "SELECT cause,audience,included FROM coven_applied_boundaries ORDER BY id",
            [],
            |row| {
                let cause = decoded(merge_fields::decode_lost_write_cause(
                    &row.get::<_, Vec<u8>>(0)?,
                ))?;
                let audience: Option<String> = row.get(1)?;
                let included = decoded(merge_fields::decode_write_positions(
                    &row.get::<_, Vec<u8>>(2)?,
                ))?;
                match (cause, audience) {
                    (LostWriteCause::SchemaChange(version), None) => {
                        Ok(Self::SchemaChange { version, included })
                    }
                    (LostWriteCause::Reset(entry), Some(text)) => Ok(Self::Reset {
                        entry,
                        audience: crate::write_encoding::audience(&text)?,
                        included,
                    }),
                    _ => Err(rusqlite::Error::InvalidQuery),
                }
            },
        )
    }

    pub(crate) fn excludes(
        &self,
        header: &WriteHeader,
        part: &WritePart,
    ) -> Option<LostWriteCause> {
        match self {
            Self::SchemaChange { version, included } => (header.schema_version < *version
                && !included.covers(header.position))
            .then_some(LostWriteCause::SchemaChange(*version)),
            Self::Reset {
                entry,
                audience,
                included,
            } => {
                let follows = included.0.iter().all(|id| {
                    header.had_read.covers(*id)
                        || (id.device == header.position.device
                            && id.number < header.position.number)
                });
                let missing = header.had_read.0.iter().any(|id| !included.covers(*id))
                    || (header.position.number > 1
                        && !included.covers(WriteId {
                            number: header.position.number - 1,
                            ..header.position
                        }));
                (part.audience == *audience
                    && !included.covers(header.position)
                    && !follows
                    && missing)
                    .then_some(LostWriteCause::Reset(*entry))
            }
        }
    }
}
#[cfg(test)]
#[path = "write_boundary_tests.rs"]
mod tests;
