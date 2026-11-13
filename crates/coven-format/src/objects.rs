//! Signed join request frames (D8).

use crate::admin::MemberPublicKeys;
use crate::codes::InviteAdmits;
use crate::error::Error;
use crate::value::name;
use crate::wire::{wire_struct, Decoder, Encoder, Wire};
use coven_foundation::id_source::{DeviceId, InviteId, MemberId};

/// A new person's request, encrypted with the invite-derived key and signed (§13.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinRequest {
    /// The invite this request answers.
    pub invite: InviteId,
    /// The member id fixed by the invite: fresh for an addition, or the
    /// existing member's for a re-binding (D8, D13).
    pub member: MemberId,
    /// The requested public pair; it verifies this request's signature.
    pub keys: MemberPublicKeys,
    /// The joining installation's own id, fixed when the join begins: the
    /// approval registers it (D6).
    pub device: DeviceId,
    /// The new device's name shown to the approving admin.
    pub device_name: String,
    /// Who asks: a new person with the name they typed, or a member
    /// recovering.
    pub joiner: Joiner,
}
wire_struct!(JoinRequest, invite, member, keys, device, device_name => crate::wire::get_name, joiner);
impl JoinRequest {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        name(&self.device_name)?;
        self.joiner.validate()
    }
}

/// Who a join request is for (§13.2). The invite's kind fixes which a request
/// may carry, and a pairing takes `OwnDevice` alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Joiner {
    /// A new person, with the name they typed: the admin sees it with the
    /// request, and the app makes their profile with it.
    NewPerson {
        /// The name, which follows the `name` rule (D2).
        name: String,
    },
    /// An existing member, whose profile already has a name: the request
    /// carries none.
    MemberKeys,
    /// The member's own new device, asking one of their devices to pair it:
    /// the request carries no name.
    OwnDevice,
}
impl Joiner {
    /// What the invite this request answers admits; none for a member's own
    /// device, which no invite has.
    pub fn admits(&self) -> Option<InviteAdmits> {
        match self {
            Self::NewPerson { .. } => Some(InviteAdmits::NewPerson),
            Self::MemberKeys => Some(InviteAdmits::MemberKeys),
            Self::OwnDevice => None,
        }
    }
    fn validate(&self) -> Result<(), Error> {
        match self {
            Self::NewPerson { name: person } => name(person),
            Self::MemberKeys | Self::OwnDevice => Ok(()),
        }
    }
}
impl Wire for Joiner {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::MemberKeys => 0u8.put(out),
            Self::NewPerson { name: person } => {
                1u8.put(out)?;
                person.put(out)
            }
            Self::OwnDevice => 2u8.put(out),
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::MemberKeys),
            1 => Ok(Self::NewPerson { name: crate::wire::get_name(input)? }),
            2 => Ok(Self::OwnDevice),
            tag => Err(Error::UnknownTag { field: "joiner", tag }),
        }
    }
}

#[cfg(test)]
#[path = "objects_tests.rs"]
mod tests;
