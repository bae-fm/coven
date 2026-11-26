//! Members, devices and circles as the committed prefix has them (E9, E12).
//! Each call and its live subscription run the same query.

use crate::sqlite::StoreSql;
use crate::{store_state::DeviceState, DbError};
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
    /// the S3 key they use, never its secret (§14.3). `None` for the store's
    /// creator before Set up storage records theirs (§10.3).
    pub access: Option<coven_format::MemberAccess>,
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

use crate::store_state_tables::{circle, member};

/// Every active member with each device they registered, and the member the
/// local device registered under, as one statement: a member without a device
/// has one row whose device columns are NULL.
const MEMBERS: &str = "SELECT m.member,m.signing,m.sealing,m.role,m.removed,m.access,m.joined_circle,m.joined_person,m.keys_by,
        d.device,d.name,d.state,
        (SELECT x.member FROM _coven_devices x WHERE x.device=?1)
     FROM _coven_members m LEFT JOIN _coven_devices d ON d.member=m.member
     WHERE m.removed=0
     ORDER BY m.member,d.device";

pub(crate) fn members(db: &StoreSql, device: DeviceId) -> Result<Vec<MemberInfo>, DbError> {
    let rows = db.host_live_read(MEMBERS, [device], |r| {
        let (id, member) = crate::store_state_tables::stored_member(r)?;
        let listed = match r.get::<_, Option<DeviceId>>(9)? {
            Some(id) => Some(DeviceInfo {
                id,
                is_self: id == device,
                name: r.get(10)?,
                state: crate::write_encoding::stored(r, 11, sqlive::rusqlite::types::Type::Text)?,
            }),
            None => None,
        };
        let me = match r.get_ref(12)? {
            sqlive::rusqlite::types::ValueRef::Null => None,
            _ => Some(crate::store_state_tables::member(r, 12)?),
        };
        Ok((id, member, listed, me))
    })?;
    let mut members: Vec<MemberInfo> = Vec::new();
    for (id, crate::StoreMember { keys, access, role, .. }, listed, me) in rows {
        if members.last().is_none_or(|last| last.id != id) {
            members.push(MemberInfo {
                is_self: me.as_ref() == Some(&id),
                keys,
                id,
                role,
                access,
                devices: Vec::new(),
            });
        }
        if let (Some(device), Some(last)) = (listed, members.last_mut()) {
            last.devices.push(device);
        }
    }
    Ok(members)
}

/// The circles of the member the local device registered under, as one
/// statement: none while the device has no registration. The first arm reads
/// the device's member, so a registration that doesn't decode fails the read
/// whether or not the member has circles.
pub(crate) fn circles(db: &StoreSql, device: DeviceId) -> Result<Vec<Circle>, DbError> {
    let rows = db.host_live_read(
        "SELECT x.member,NULL,NULL FROM _coven_devices x WHERE x.device=?1
         UNION ALL
         SELECT NULL,c.circle,c.name FROM _coven_devices x
         JOIN _coven_circle_members m ON m.member=x.member
         JOIN _coven_circles c ON c.circle=m.circle
         WHERE x.device=?1 AND NOT EXISTS(SELECT 1 FROM _coven_circle_deletions d WHERE d.circle=c.circle)",
        [device],
        |r| {
            Ok(match r.get_ref(0)? {
                sqlive::rusqlite::types::ValueRef::Null => {
                    Some(Circle { id: circle(r, 1)?, name: r.get(2)? })
                }
                _ => {
                    member(r, 0)?;
                    None
                }
            })
        },
    )?;
    let mut circles: Vec<Circle> = rows.into_iter().flatten().collect();
    circles.sort_by_key(|circle| circle.id);
    Ok(circles)
}

/// A circle's members who remain in the store; none for an absent or deleted
/// circle. One statement, which also reads the member the local device
/// registered under.
pub(crate) fn circle_members(
    db: &StoreSql,
    device: DeviceId,
    circle: CircleId,
) -> Result<Vec<CircleMemberInfo>, DbError> {
    let members = db.host_live_read(
        "SELECT m.member,(SELECT x.member FROM _coven_devices x WHERE x.device=?1)
         FROM _coven_circle_members m JOIN _coven_members s ON s.member=m.member
         WHERE m.circle=?2 AND s.removed=0
           AND NOT EXISTS(SELECT 1 FROM _coven_circle_deletions d WHERE d.circle=m.circle)
         ORDER BY m.member",
        (device, circle),
        |r| {
            let me = match r.get_ref(1)? {
                sqlive::rusqlite::types::ValueRef::Null => None,
                _ => Some(member(r, 1)?),
            };
            Ok((member(r, 0)?, me))
        },
    )?;
    Ok(members
        .into_iter()
        .map(|(member, me)| CircleMemberInfo { is_self: me.as_ref() == Some(&member), member })
        .collect())
}

#[cfg(test)]
#[path = "members_tests.rs"]
mod tests;
