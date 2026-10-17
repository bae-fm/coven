//! Signed join request frames (D8).

use crate::admin::MemberPublicKeys;
use crate::error::Error;
use crate::value::name;
use crate::wire::wire_struct;
use coven_foundation::id_source::{InviteId, MemberId};

/// A new person's request, encrypted with the invite-derived key and signed (§12.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinRequest {
    /// The invite this request answers.
    pub invite: InviteId,
    /// The member id fixed by the invite: fresh for an addition, or the
    /// existing member's for a re-binding (D8, D13).
    pub member: MemberId,
    /// The requested public pair; it verifies this request's signature.
    pub keys: MemberPublicKeys,
    /// The new device's name shown to the approving admin.
    pub device_name: String,
}
wire_struct!(JoinRequest, invite, member, keys, device_name => crate::wire::get_name);
impl JoinRequest {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        name(&self.device_name)
    }
}
