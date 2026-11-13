//! Who an invite's join request names, as the admin's app shows it (api.md E10).

use coven_format::codes::InviteAdmits;

/// Who asks to join through an invite (§13.2): one of two, as the invite
/// admits. A member's own new device asks through an add-device pairing
/// instead, which the format's request can say and an invite never takes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Joiner {
    /// A new person, with the name they typed: the admin sees it with the
    /// request, and the app makes their profile with it.
    NewPerson {
        /// The name, which follows the `name` rule (format.md D2).
        name: String,
    },
    /// An existing member getting new keys, whose profile already has a name.
    MemberKeys,
}

impl Joiner {
    /// What the invite this joiner answers admits.
    pub fn admits(&self) -> InviteAdmits {
        match self {
            Self::NewPerson { .. } => InviteAdmits::NewPerson,
            Self::MemberKeys => InviteAdmits::MemberKeys,
        }
    }
}

impl From<Joiner> for coven_format::objects::Joiner {
    fn from(joiner: Joiner) -> Self {
        match joiner {
            Joiner::NewPerson { name } => Self::NewPerson { name },
            Joiner::MemberKeys => Self::MemberKeys,
        }
    }
}

/// A request for the member's own device is no invite's, and the app has no
/// name for it.
impl TryFrom<coven_format::objects::Joiner> for Joiner {
    type Error = crate::SyncError;

    fn try_from(joiner: coven_format::objects::Joiner) -> Result<Self, Self::Error> {
        match joiner {
            coven_format::objects::Joiner::NewPerson { name } => Ok(Self::NewPerson { name }),
            coven_format::objects::Joiner::MemberKeys => Ok(Self::MemberKeys),
            coven_format::objects::Joiner::OwnDevice => {
                Err(coven_format::Error::UnknownTag { field: "invite joiner", tag: 2 }.into())
            }
        }
    }
}

#[cfg(test)]
#[path = "joiner_tests.rs"]
mod tests;
