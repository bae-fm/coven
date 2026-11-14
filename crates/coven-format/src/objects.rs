//! Signed join request frames (D8).

use crate::admin::{MemberPublicKeys, NewCircle};
use crate::codes::InviteAdmits;
use crate::error::{require, Error, Rule};
use crate::sealed_key::CopyBox;
use crate::value::name;
use crate::wire::{wire_struct, Decoder, Encoder, Wire};
use coven_foundation::id_source::{DeviceId, InviteId, MemberId};
use coven_merge::Audience;

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

/// The circle a joining person's request makes, for the admission to make in
/// its own slot (§13.2, D8): its id, name, base version and key commitment,
/// and the key's only copy, which the joining phone sealed to its own new
/// sealing key. The key is kept nowhere else, so the approving admin never
/// holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoiningCircle {
    /// The circle and its key's commitment, which the admission repeats.
    pub circle: NewCircle,
    /// The key's copy for the joining member, sealed with the own-copy
    /// context (D11); the admission's clear prefix carries it verbatim.
    pub copy: CopyBox,
}
wire_struct!(JoiningCircle, circle, copy);
impl JoiningCircle {
    fn validate(&self) -> Result<(), Error> {
        self.circle.validate()?;
        // The copy is the key the circle commits to, for the circle it makes.
        require(
            self.copy.routing.audience == Audience::Circle(self.circle.circle)
                && self.copy.routing.key == self.circle.key,
            "joining circle copy",
            Rule::Audience,
        )
    }
}

/// Who a join request is for (§13.2). The invite's kind fixes which a request
/// may carry, and a pairing takes `OwnDevice` alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Joiner {
    /// A new person, with the name they typed: the admin sees it with the
    /// request, and the admission records it for the app's joining write.
    NewPerson {
        /// The name, which follows the `name` rule (D2).
        name: String,
        /// The circle the admission makes for them.
        circle: JoiningCircle,
    },
    /// An existing member, whose profile already has a name: the request
    /// carries none, and a circle to replace the private one they can no
    /// longer read.
    MemberKeys {
        /// The circle the recovery makes for them.
        circle: JoiningCircle,
    },
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
            Self::MemberKeys { .. } => Some(InviteAdmits::MemberKeys),
            Self::OwnDevice => None,
        }
    }
    /// The name the request carries, which a new person typed; none for a
    /// member's keys and for a member's own device.
    pub fn person(&self) -> Option<&str> {
        match self {
            Self::NewPerson { name, .. } => Some(name),
            Self::MemberKeys { .. } | Self::OwnDevice => None,
        }
    }
    /// The circle the request makes; none for a member's own device.
    pub fn circle(&self) -> Option<&JoiningCircle> {
        match self {
            Self::NewPerson { circle, .. } | Self::MemberKeys { circle } => Some(circle),
            Self::OwnDevice => None,
        }
    }
    fn validate(&self) -> Result<(), Error> {
        match self {
            Self::NewPerson { name: person, circle } => {
                name(person)?;
                circle.validate()
            }
            Self::MemberKeys { circle } => circle.validate(),
            Self::OwnDevice => Ok(()),
        }
    }
}
impl Wire for Joiner {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::MemberKeys { circle } => {
                0u8.put(out)?;
                circle.put(out)
            }
            Self::NewPerson { name: person, circle } => {
                1u8.put(out)?;
                person.put(out)?;
                circle.put(out)
            }
            Self::OwnDevice => 2u8.put(out),
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::MemberKeys { circle: JoiningCircle::get(input)? }),
            1 => Ok(Self::NewPerson {
                name: crate::wire::get_name(input)?,
                circle: JoiningCircle::get(input)?,
            }),
            2 => Ok(Self::OwnDevice),
            tag => Err(Error::UnknownTag { field: "joiner", tag }),
        }
    }
}

#[cfg(test)]
#[path = "objects_tests.rs"]
mod tests;
