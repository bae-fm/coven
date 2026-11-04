//! Members, devices and circles as the committed prefix has them (E9, E12).
//! Each call and its live subscription run the same query.

use crate::{sqlite::DatabaseConnection, store_state::DeviceState, DbError};
use coven_format::admin::{MemberPublicKeys, MemberRole};
use coven_foundation::id_source::{CircleId, DeviceId, MemberId};

/// A live member and every device they registered (E9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberInfo {
    /// The stable member id, independent of its keys (§10).
    pub id: MemberId,
    /// The current public signing and sealing pair.
    pub keys: MemberPublicKeys,
    /// Current store role.
    pub role: MemberRole,
    /// How the member reaches storage: their provider account, or the id of
    /// the S3 key they use, never its secret (§14.3).
    pub access: coven_format::MemberAccess,
    /// Every registered device, including removed and replaced ids.
    pub devices: Vec<DeviceInfo>,
    /// Whether this is the local member.
    pub is_self: bool,
}

/// One of a member's device ids in the committed prefix (E9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeviceInfo {
    /// The device id.
    pub id: DeviceId,
    /// Its name from its registration.
    pub name: String,
    /// Active, removed, or replaced by a kept registration (§14.1).
    pub state: DeviceState,
    /// Whether this is the local installation, regardless of registration state.
    pub is_self: bool,
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
    /// The stable member id, independent of its keys (§10).
    pub member: MemberId,
    /// Whether this is the local member.
    pub is_self: bool,
}

use crate::store_state_tables::{circle, device_member, member};

pub(crate) fn members(
    db: &DatabaseConnection,
    device: DeviceId,
) -> Result<Vec<MemberInfo>, DbError> {
    let me = device_member(db, device)?;
    let members = db.query(
        "SELECT member,signing,sealing,role,removed,access FROM _coven_members WHERE removed=0
         ORDER BY member",
        [],
        crate::store_state_tables::stored_member,
    )?;
    members
        .into_iter()
        .map(|(id, crate::StoreMember { keys, access, role, .. })| {
            let devices = db.query(
                "SELECT device,name,state FROM _coven_devices WHERE member=?1 ORDER BY device",
                [id.0.as_bytes().as_slice()],
                |r| {
                    let id = r.get::<_, DeviceId>(0)?;
                    Ok(DeviceInfo {
                        id,
                        is_self: id == device,
                        name: r.get(1)?,
                        state: crate::store_state_tables::device_state(r, 2)?,
                    })
                },
            )?;
            Ok(MemberInfo { is_self: me.as_ref() == Some(&id), keys, id, role, access, devices })
        })
        .collect()
}

pub(crate) fn circles(db: &DatabaseConnection, device: DeviceId) -> Result<Vec<Circle>, DbError> {
    let Some(me) = device_member(db, device)? else {
        return Ok(Vec::new());
    };
    // Ordered by the membership's circle, which the member's index holds in
    // order; `c.circle` is the same value but would be sorted after the join.
    let circles = db.query(
        "SELECT c.circle,c.name FROM _coven_circle_members m JOIN _coven_circles c ON c.circle=m.circle
         WHERE m.member=?1 AND NOT EXISTS(SELECT 1 FROM _coven_circle_deletions d WHERE d.circle=c.circle)
         ORDER BY m.circle",
        [me.0.as_bytes().as_slice()],
        |r| {
            Ok(Circle {
                id: circle(r.get(0)?)?,
                name: r.get(1)?,
            })
        },
    )?;
    Ok(circles)
}

/// A circle's members who remain in the store; none for an absent or deleted circle.
pub(crate) fn circle_members(
    db: &DatabaseConnection,
    device: DeviceId,
    circle: CircleId,
) -> Result<Vec<CircleMemberInfo>, DbError> {
    let me = device_member(db, device)?;
    let members = db.query(
        "SELECT m.member FROM _coven_circle_members m JOIN _coven_members s ON s.member=m.member
         WHERE m.circle=?1 AND s.removed=0
           AND NOT EXISTS(SELECT 1 FROM _coven_circle_deletions d WHERE d.circle=m.circle)
         ORDER BY m.member",
        [circle.to_string()],
        |r| member(r, 0),
    )?;
    Ok(members
        .into_iter()
        .map(|member| CircleMemberInfo { is_self: me.as_ref() == Some(&member), member })
        .collect())
}

#[cfg(test)]
#[path = "members_tests.rs"]
mod tests;
