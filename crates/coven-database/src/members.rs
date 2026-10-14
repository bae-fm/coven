//! Members, devices and circles as the committed replay has them (E9, E12).
//! Each call and its live subscription run the same query.

use crate::{sqlite::DatabaseConnection, store_log::DeviceState, write_encoding::counter, DbError};
use coven_crypto::{MemberId, SealingPublicKey};
use coven_format::store_log::{MemberPublicKeys, MemberRole};
use coven_foundation::id_source::{CircleId, DeviceId};

/// A live member and every device they registered (E9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberInfo {
    /// Signing identity.
    pub id: MemberId,
    /// The current public signing and sealing pair.
    pub keys: MemberPublicKeys,
    /// Current store role.
    pub role: MemberRole,
    /// Every registered device, including removed and replaced ids.
    pub devices: Vec<DeviceInfo>,
    /// Whether this is the local member.
    pub is_self: bool,
}

/// One of a member's device ids as replay has it (E9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeviceInfo {
    /// The device id.
    pub id: DeviceId,
    /// Its name from its registration.
    pub name: String,
    /// Active, removed, or replaced by a kept registration (§10).
    pub state: DeviceState,
}

/// A circle the local member belongs to (E12).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Circle {
    /// Circle identity.
    pub id: CircleId,
    /// Current name.
    pub name: String,
}

/// An active store member in a circle (E12).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CircleMemberInfo {
    /// Signing identity.
    pub member: MemberId,
    /// Whether this is the local member.
    pub is_self: bool,
}

use crate::store_log_tables::{circle, member};

/// The member using this device, once its registration has replayed.
fn me(db: &DatabaseConnection, device: DeviceId) -> Result<Option<MemberId>, DbError> {
    Ok(db
        .query(
            "SELECT member FROM _coven_devices WHERE device=?1",
            [device.0.to_be_bytes().as_slice()],
            |r| member(r, 0),
        )?
        .pop())
}

pub(crate) fn members(
    db: &DatabaseConnection,
    device: DeviceId,
) -> Result<Vec<MemberInfo>, DbError> {
    let me = me(db, device)?;
    let mut members = db.query(
        "SELECT member,role,sealing FROM _coven_members WHERE removed=0",
        [],
        |r| {
            Ok((
                member(r, 0)?,
                match r.get::<_, String>(1)?.as_str() {
                    "admin" => MemberRole::Admin,
                    "member" => MemberRole::Member,
                    _ => return Err(rusqlite::Error::InvalidQuery),
                },
                SealingPublicKey::from_bytes(r.get(2)?),
            ))
        },
    )?;
    members.sort_by(|a, b| a.0.cmp(&b.0));
    members
        .into_iter()
        .map(|(id, role, sealing)| {
            let mut devices = db.query(
                "SELECT device,name,state FROM _coven_devices WHERE member=?1",
                [id.to_bytes().as_slice()],
                |r| {
                    Ok(DeviceInfo {
                        id: DeviceId(counter(r.get(0)?)),
                        name: r.get(1)?,
                        state: crate::store_log_tables::device_state(r.get(2)?)?,
                    })
                },
            )?;
            devices.sort_by_key(|device| device.id);
            Ok(MemberInfo {
                is_self: me.as_ref() == Some(&id),
                keys: MemberPublicKeys {
                    signing: id.clone(),
                    sealing,
                },
                id,
                role,
                devices,
            })
        })
        .collect()
}

pub(crate) fn circles(db: &DatabaseConnection, device: DeviceId) -> Result<Vec<Circle>, DbError> {
    let Some(me) = me(db, device)? else {
        return Ok(Vec::new());
    };
    let mut circles = db.query(
        "SELECT c.circle,c.name FROM _coven_circle_members m JOIN _coven_circles c ON c.circle=m.circle
         WHERE m.member=?1 AND NOT EXISTS(SELECT 1 FROM _coven_circle_deletions d WHERE d.circle=c.circle)",
        [me.to_bytes().as_slice()],
        |r| {
            Ok(Circle {
                id: circle(r.get(0)?)?,
                name: r.get(1)?,
            })
        },
    )?;
    circles.sort_by_key(|circle| circle.id);
    Ok(circles)
}

/// A circle's members who remain in the store; none for an absent or deleted circle.
pub(crate) fn circle_members(
    db: &DatabaseConnection,
    device: DeviceId,
    circle: CircleId,
) -> Result<Vec<CircleMemberInfo>, DbError> {
    let me = me(db, device)?;
    let mut members = db.query(
        "SELECT m.member FROM _coven_circle_members m JOIN _coven_members s ON s.member=m.member
         WHERE m.circle=?1 AND s.removed=0
           AND NOT EXISTS(SELECT 1 FROM _coven_circle_deletions d WHERE d.circle=m.circle)",
        [circle.to_string()],
        |r| member(r, 0),
    )?;
    members.sort();
    Ok(members
        .into_iter()
        .map(|member| CircleMemberInfo {
            is_self: me.as_ref() == Some(&member),
            member,
        })
        .collect())
}

#[cfg(test)]
#[path = "members_tests.rs"]
mod tests;
