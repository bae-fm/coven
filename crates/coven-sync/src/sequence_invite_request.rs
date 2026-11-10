//! Opening an invite's join request.

use super::*;

impl SequenceSync {
    /// Open and check a request: it opens with the invite's secret, its own
    /// new signing key verifies it, it repeats the invite's id and member id,
    /// it is for the invite's kind of person, and `state`, the history this
    /// pass caught up with, has bound neither of its keys and registers no
    /// device with its device id (§13.2). A
    /// request that fails a check is `Refused`; one in a newer format than
    /// this device reads waits for its update, as an error.
    pub(super) fn open_request(
        &self,
        work: &InviteWork,
        state: &coven_database::StoreState,
        bytes: &[u8],
    ) -> Result<Opened, SyncError> {
        let path = ObjectPath::join_request(self.store(), work.device, work.id);
        let secret = work.secret()?;
        let opened = match crate::joining_identity::opened_join_request(&secret, &path, bytes) {
            Ok(opened) => opened,
            Err(refusal @ (SyncError::Crypto(_) | SyncError::Format(_))) => {
                return Ok(Opened::Refused(refusal))
            }
            Err(error) => return Err(error),
        };
        let Some((request, plain)) = opened else {
            return Ok(Opened::Refused(crate::write_object::wrong_identity(&path)));
        };
        // A request for a new person carries the name they typed, and one for
        // a recovery none; an invite takes only its own kind (§13.2).
        if request.invite != work.id
            || request.member != work.member
            || request.joiner.kind() != work.kind()
        {
            return Ok(Opened::Refused(crate::write_object::wrong_identity(&path)));
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

/// What opening an invite's request found (`open_request`).
pub(crate) enum Opened {
    /// The request passed every check: its exact plaintext.
    Checked(Vec<u8>),
    /// It failed this check. The request is created once at the invite's one
    /// path and never changes, so no later attempt would pass it (§13.2).
    Refused(SyncError),
}

#[cfg(test)]
#[path = "sequence_invite_request_tests.rs"]
mod tests;
