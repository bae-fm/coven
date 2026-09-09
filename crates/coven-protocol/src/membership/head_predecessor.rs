use super::{AuthorHead, MembershipHeadActivation, MembershipHeadRef};
use crate::objects::ExactObjectRef;
use crate::store_commit::StoreProtocolError;
use serde::{Deserialize, Serialize};

/// The exact predecessor head and, for a Store control, its finalized result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MembershipHeadPredecessor {
    Direct {
        head: MembershipHeadRef,
    },
    Accepted {
        head: MembershipHeadRef,
        acceptance: ExactObjectRef,
    },
}

impl MembershipHeadPredecessor {
    pub fn head(&self) -> &MembershipHeadRef {
        match self {
            Self::Direct { head } | Self::Accepted { head, .. } => head,
        }
    }

    pub fn acceptance(&self) -> Option<&ExactObjectRef> {
        match self {
            Self::Direct { .. } => None,
            Self::Accepted { acceptance, .. } => Some(acceptance),
        }
    }

    pub fn verify_head(&self, head: &AuthorHead) -> Result<(), StoreProtocolError> {
        self.head().object.verify(&head.to_bytes())?;
        if self.head().coord != head.entry_coord() || self.head().head_hash != head.head_hash() {
            return Err(StoreProtocolError::Malformed(
                "membership predecessor differs from its exact head".into(),
            ));
        }
        match (self, &head.activation) {
            (Self::Direct { .. }, MembershipHeadActivation::Direct) => Ok(()),
            (
                Self::Accepted { acceptance, .. },
                MembershipHeadActivation::StoreCommit {
                    acceptance_slot, ..
                },
            ) if acceptance.slot() == acceptance_slot => Ok(()),
            _ => Err(StoreProtocolError::Malformed(
                "membership predecessor omits or changes its exact acceptance result".into(),
            )),
        }
    }
}
