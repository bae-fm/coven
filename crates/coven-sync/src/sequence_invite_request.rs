//! Opening a join request: an invite's, or a pairing's.

use super::*;

/// Whom a request must answer for, by the person it names (§13.2).
#[derive(Clone, Copy)]
pub(crate) enum Expected {
    /// An invite takes the person it admits.
    Invite(coven_format::codes::InviteAdmits),
    /// A pairing takes only the member's own new device.
    OwnDevice,
}

impl Expected {
    fn accepts(self, joiner: &coven_format::objects::Joiner) -> bool {
        match self {
            Self::Invite(admits) => joiner.admits() == Some(admits),
            Self::OwnDevice => matches!(joiner, coven_format::objects::Joiner::OwnDevice),
        }
    }
}

/// The request an invite or a pairing opens: what it was made with and
/// whom it is for.
pub(crate) struct RequestChecks<'a> {
    /// The request's one path, under the device that made the code.
    pub path: &'a ObjectPath,
    /// The code's one-time secret.
    pub secret: &'a InviteSecret,
    /// The invite's or pairing's id, which the request must repeat.
    pub id: InviteId,
    /// The member id the code fixes, which the request must repeat.
    pub member: MemberId,
    /// Whom the request must name.
    pub expected: Expected,
}

impl RequestChecks<'_> {
    /// Open and check a request: it opens with the secret, its own new
    /// signing key verifies it, it repeats the code's id and member id, it is
    /// for the person the code admits, and `state`, the history this
    /// pass caught up with, has bound neither of its keys and registers no
    /// device with its device id (§13.2). A request that fails a check is
    /// `Refused`; one in a newer format than this device reads waits for its
    /// update, as an error.
    pub(crate) fn open(
        &self,
        state: &coven_database::StoreState,
        bytes: &[u8],
    ) -> Result<Opened, SyncError> {
        let opened =
            match crate::joining_identity::opened_join_request(self.secret, self.path, bytes) {
                Ok(opened) => opened,
                Err(refusal @ (SyncError::Crypto(_) | SyncError::Format(_))) => {
                    return Ok(Opened::Refused(refusal))
                }
                Err(error) => return Err(error),
            };
        let Some((request, plain)) = opened else {
            return Ok(Opened::Refused(crate::write_object::wrong_identity(self.path)));
        };
        // A request for a new person carries the name they typed, and one for
        // a recovery none; an invite takes only the person it admits (§13.2).
        if request.invite != self.id
            || request.member != self.member
            || !self.expected.accepts(&request.joiner)
        {
            return Ok(Opened::Refused(crate::write_object::wrong_identity(self.path)));
        }
        if !crate::effects::fresh(state, &request.keys) {
            return Ok(Opened::Refused(SyncError::Rejected(OperationRejection::KeysAlreadyUsed)));
        }
        if state.devices.contains_key(&request.device) {
            return Ok(Opened::Refused(SyncError::Rejected(OperationRejection::TargetGone(
                crate::EntryTarget::Device(request.device),
            ))));
        }
        Ok(Opened::Checked(plain))
    }
}

impl SequenceSync {
    /// Open and check an invite's request (`RequestChecks::open`).
    pub(super) fn open_request(
        &self,
        work: &InviteWork,
        state: &coven_database::StoreState,
        bytes: &[u8],
    ) -> Result<Opened, SyncError> {
        let path = ObjectPath::join_request(self.store(), work.device, work.id);
        let secret = work.secret()?;
        RequestChecks {
            path: &path,
            secret: &secret,
            id: work.id,
            member: work.member,
            expected: Expected::Invite(work.admits()),
        }
        .open(state, bytes)
    }
}

/// What opening a request found (`RequestChecks::open`).
pub(crate) enum Opened {
    /// The request passed every check: its exact plaintext.
    Checked(Vec<u8>),
    /// It failed this check. The request is created once at the code's one
    /// path and never changes, so no later attempt would pass it (§13.2).
    Refused(SyncError),
}

#[cfg(test)]
#[path = "sequence_invite_request_tests.rs"]
mod tests;
