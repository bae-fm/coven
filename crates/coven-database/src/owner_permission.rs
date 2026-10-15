//! Local owner permission (§8.8): only a row's owner may directly change an
//! `owner_only()` row.
//!
//! Direct targets are what the write's own statements and its shared triggers
//! did, including a trigger fired by a foreign-key action, and what coven did
//! for it without SQL: copying a row into another audience, or dismissing a
//! removed row. A foreign-key action needs no permission of its own: its root
//! is a direct target, checked here, and the capture records an action only
//! when it is exactly the schema's consequence of that root. A refused target
//! fails the whole local transaction, which then leaves no row, loss or upload.

use crate::execution_capture::{CapturedOperations, Origin};
use crate::sqlite::DatabaseConnection;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::write::WriteRecord;
use coven_foundation::id_source::DeviceId;
use coven_merge::RowId;

/// The member a local write is authored by, and the device it runs on.
#[derive(Clone, Copy)]
pub(crate) struct Writer {
    device: DeviceId,
    member: Option<coven_foundation::id_source::MemberId>,
}

impl Writer {
    pub(crate) fn local(db: &DatabaseConnection, device: DeviceId) -> Result<Self, DbError> {
        Ok(Self {
            device,
            member: crate::ownership::member(db, device)?,
        })
    }

    /// Refuse a direct target of an owned row this writer did not create.
    /// A row no write has created yet is new: inserting it makes this writer
    /// its owner. An insert over an existing or removed key is an update here.
    pub(crate) fn may_change(
        &self,
        db: &DatabaseConnection,
        schema: &WriteSchema,
        row: &RowId,
    ) -> Result<(), DbError> {
        if !crate::ownership::owner_table(schema, &row.table) {
            return Ok(());
        }
        let Some(creation) = crate::ownership::creation(db, row)? else {
            return Ok(());
        };
        self.owns(db, row, &creation.creator)
    }

    /// Refuse an action on a row whose original creation is `creator`
    /// unless this writer's member made it.
    fn owns(
        &self,
        db: &DatabaseConnection,
        row: &RowId,
        creator: &coven_format::write_sources::ChangeId,
    ) -> Result<(), DbError> {
        // This device's own creations are its member's, even before the
        // device's registration reaches the store log.
        if creator.write.device == self.device {
            return Ok(());
        }
        // Every received write's device is registered before it applies.
        let owner =
            crate::ownership::member(db, creator.write.device)?.ok_or(DbError::Invariant {
                what: "an applied write's device has a member",
            })?;
        if self.member == Some(owner) {
            return Ok(());
        }
        Err(DbError::NotOwner {
            table: row.table.clone(),
            key: row_key(row)?,
            owner,
        })
    }

    /// Each statement and shared-trigger operation of a local write on an
    /// owned row, before anything commits.
    pub(crate) fn check_operations(
        &self,
        db: &DatabaseConnection,
        schema: &WriteSchema,
        operations: &CapturedOperations,
    ) -> Result<(), DbError> {
        for (row, steps) in &operations.steps {
            let direct = steps.iter().any(|step| {
                !matches!(
                    operations.origins[&(row.clone(), step.sequence)],
                    Origin::ForeignKey { .. }
                )
            });
            if direct {
                self.may_change(db, schema, row)?;
            }
        }
        Ok(())
    }

    /// Each copy of an owned row into another audience in a local write. A
    /// copy keeps its original creation; moving another member's row, or a
    /// descendant carried with it, is a direct change of that row.
    pub(crate) fn check_copies(
        &self,
        db: &DatabaseConnection,
        schema: &WriteSchema,
        record: Option<&WriteRecord>,
    ) -> Result<(), DbError> {
        for change in record
            .into_iter()
            .flat_map(|r| &r.parts)
            .flat_map(|p| &p.rows)
        {
            if let Some(creator) = &change.creator {
                if crate::ownership::owner_table(schema, &change.row.table) {
                    self.owns(db, &change.row, creator)?;
                }
            }
        }
        Ok(())
    }
}

/// A row's primary key as SQL values, in declared key order.
pub(crate) fn row_key(row: &RowId) -> Result<crate::RowKey, DbError> {
    let values = coven_format::key::decode_key(&row.key)?;
    Ok(crate::RowKey(
        values
            .iter()
            .map(crate::write_encoding::sql_value)
            .collect(),
    ))
}

#[cfg(test)]
#[path = "owner_permission_tests.rs"]
pub(crate) mod tests;
