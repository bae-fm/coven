//! Who may change a synced table's rows directly (§9.6): any member, each
//! row's owner, or a member who is an admin in the state just before the
//! write's slot (Lean `Ownership.allowed`).

use crate::sqlite::StoreSql;
use crate::store_call::StoreCall;
use crate::write_schema::WriteSchema;
use crate::{DbError, WritePermission};
use coven_format::loss::AuthorizationFailure;
use coven_foundation::id_source::{DeviceId, MemberId};
use coven_merge::RowId;
use std::collections::BTreeMap;

/// Why a write's author may not change a row directly (Lean `refusal`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Denial {
    /// The row belongs to this other member.
    NotOwner(MemberId),
    /// The row's table is limited to admins, and the author is none.
    NotAdmin,
}

impl Denial {
    /// The cause a receiver records for a change it refuses for this.
    pub(crate) fn failure(self) -> AuthorizationFailure {
        match self {
            Self::NotOwner(owner) => AuthorizationFailure::NotOwner { owner },
            Self::NotAdmin => AuthorizationFailure::NotAdmin,
        }
    }
}

/// The error that refuses a local write whose change of `row` receivers
/// would refuse for `failure`.
pub(crate) fn refusal(row: &RowId, failure: &AuthorizationFailure) -> Result<DbError, DbError> {
    let (table, key) = (row.table.clone(), crate::RowKey::decode(&row.key)?);
    Ok(match failure {
        AuthorizationFailure::NotOwner { owner } => DbError::NotOwner { table, key, owner: *owner },
        AuthorizationFailure::NotAdmin => DbError::NotAdmin { table, key },
        AuthorizationFailure::InvalidForeignKeyEffect => {
            DbError::InvalidForeignKeyEffect { table, key }
        }
    })
}

/// What one write's author may change, for one check of its rows: each
/// row's owner as the selected creations and the counted membership name
/// it, and whether the author's member is an admin in the counted state.
/// A receiver applies slots in order, so the counted state is the one just
/// before the write's slot.
pub(crate) struct Permissions<'a> {
    database: &'a StoreSql,
    schema: &'a WriteSchema,
    version: u32,
    current: u32,
    device: DeviceId,
    member: Option<MemberId>,
    admin: bool,
    known: BTreeMap<RowId, Option<Denial>>,
}

impl<'a> Permissions<'a> {
    /// The permissions of a write by `device` under schema `version`.
    pub(crate) fn new(
        db: &'a StoreSql,
        call: &'a StoreCall<'a>,
        schema: &'a WriteSchema,
        device: DeviceId,
        version: u32,
    ) -> Result<Self, DbError> {
        let member = crate::store_state_tables::device_member(db, device)?;
        let admin = match member {
            Some(member) => crate::store_state_tables::is_admin(db, member)?,
            None => false,
        };
        Ok(Self {
            database: db,
            schema,
            version,
            current: crate::transaction_constants::schema_version(db, call)?,
            device,
            member,
            admin,
            known: BTreeMap::new(),
        })
    }

    /// Why the author may not change this row directly, if it may not
    /// (Lean `!allowed`). A row no write has created yet is new, and its
    /// creation is the author's.
    pub(crate) fn denial(&mut self, row: &RowId) -> Result<Option<Denial>, DbError> {
        if let Some(denial) = self.known.get(row) {
            return Ok(*denial);
        }
        let denial = self.lookup(row)?;
        self.known.insert(row.clone(), denial);
        Ok(denial)
    }

    fn lookup(&self, row: &RowId) -> Result<Option<Denial>, DbError> {
        let db = self.database;
        let Some(table) = crate::source_names::table(db, self.version, &row.table, self.current)?
        else {
            return Ok(None);
        };
        match self.schema.permission(&table) {
            WritePermission::Anyone => Ok(None),
            WritePermission::Admin => Ok((!self.admin).then_some(Denial::NotAdmin)),
            WritePermission::Owner => {
                let current = RowId { table, ..row.clone() };
                let Some(creation) = crate::ownership::creation(db, &current)? else {
                    return Ok(None);
                };
                // A device's own creations are its member's, even before its
                // registration acquires a slot.
                if creation.creator.write.device == self.device {
                    return Ok(None);
                }
                // Every applied write's device is registered before it applies.
                let owner =
                    crate::store_state_tables::device_member(db, creation.creator.write.device)?
                        .ok_or(DbError::Invariant {
                            what: "an applied write's device has a member",
                        })?;
                Ok((self.member != Some(owner)).then_some(Denial::NotOwner(owner)))
            }
        }
    }
}

#[cfg(test)]
#[path = "permission_tests.rs"]
mod tests;
