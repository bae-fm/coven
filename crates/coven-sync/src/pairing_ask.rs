//! The new device's side of a pairing's request: the pair it asks with, the
//! request sealed with the code's secret, and the checks of the request's
//! path (§13.2). Nothing here is written to disk or the keychain: the pair
//! lives as long as the call that made it.

use crate::request_at::{Placed, RequestAt};
use crate::SyncError;
use coven_crypto::{ConfirmationNumber, MemberKeys};
use coven_format::{
    codes::PairingCode,
    handover::Handover,
    invite_end::InviteEnd,
    objects::{JoinRequest, Joiner},
    sealed_single::SingleChunkObject,
};
use coven_foundation::id_source::{DeviceId, MemberId};
use coven_storage::{ObjectPath, Storage, StorageFailure};
use std::time::SystemTime;

/// What a check of the request's path finds.
#[derive(Debug, PartialEq, Eq)]
pub enum PairingCheck {
    /// A request is at the path, this device's or another, or this device's
    /// has just been sent again.
    Waiting,
    /// The showing device's approval put its handover in the request's place.
    Handover(Handover),
    /// The marker or the showing device's gone marker is there, or nothing is
    /// and the pairing has expired: the pairing has ended.
    Ended,
}

/// A request for a pairing and the pair it asks with.
pub struct PairingAsk {
    keys: MemberKeys,
    member: MemberId,
    path: ObjectPath,
    request: Vec<u8>,
    showing: DeviceId,
    expires_at: u64,
    confirmation: ConfirmationNumber,
}

impl PairingAsk {
    /// A request for the pairing `code` shows, from the installation
    /// `device`, with a pair made here for the handover to be sealed to.
    pub fn new(code: &PairingCode, device: DeviceId, device_name: &str) -> Result<Self, SyncError> {
        let keys = MemberKeys::generate()?;
        let path = code.request_path();
        let public = keys.public_keys();
        let request = crate::sealed_join_request(
            &code.secret,
            &path,
            &JoinRequest {
                invite: code.pairing,
                member: code.member,
                keys: public,
                device,
                device_name: device_name.into(),
                joiner: Joiner::OwnDevice,
            },
            &keys,
        )?;
        Ok(Self {
            confirmation: ConfirmationNumber::of(code.pairing, &public),
            keys,
            member: code.member,
            path,
            request,
            showing: code.showing,
            expires_at: code.expires_at,
        })
    }

    /// The number the person compares on the showing device before approving.
    pub fn confirmation(&self) -> ConfirmationNumber {
        self.confirmation
    }

    /// Whether the pairing still lets a first request be sent at `now`.
    pub fn open_at(&self, now: SystemTime) -> bool {
        now.duration_since(SystemTime::UNIX_EPOCH)
            .map_or(true, |since| since.as_secs() < self.expires_at)
    }

    fn at(&self) -> RequestAt<'_> {
        RequestAt { path: &self.path, request: &self.request, showing: self.showing }
    }

    /// Send the request. `false`, sending nothing, when the pairing has
    /// ended already: the showing device is gone for good, or its marker holds
    /// the path. A path taken by anything else fails with `InvitationChanged`.
    pub async fn publish(&self, storage: &dyn Storage) -> Result<bool, SyncError> {
        let at = self.at();
        if at.showing_gone(storage).await? {
            return Ok(false);
        }
        match at.place(storage).await? {
            Placed::Ours => Ok(true),
            Placed::Other(found) if InviteEnd::decode(&found).is_ok() => Ok(false),
            Placed::Other(_) => Err(SyncError::InvitationChanged),
        }
    }

    /// Look at the request's path at `now`. The showing device's gone marker
    /// ends the pairing whatever the path holds. Otherwise the handover
    /// wins, a request waits and the marker ends it; and nothing, because the
    /// showing device is between its delete and its create or this device's
    /// own create never arrived, is answered by sending the same request
    /// again while the pairing is open, and ends the pairing once it is not.
    pub async fn check(
        &self,
        storage: &dyn Storage,
        now: SystemTime,
    ) -> Result<PairingCheck, SyncError> {
        let at = self.at();
        if at.showing_gone(storage).await? {
            // A request that reached storage after an admin's device listed
            // the showing device's requests is deleted (`RequestAt::pending`).
            at.pending(storage).await?;
            return Ok(PairingCheck::Ended);
        }
        let found = match storage.read(&self.path).await {
            Ok(found) => found,
            Err(error) if error.failure() == StorageFailure::NotFound => {
                if !self.open_at(now) {
                    return Ok(PairingCheck::Ended);
                }
                match at.place(storage).await? {
                    Placed::Ours => return Ok(PairingCheck::Waiting),
                    Placed::Other(found) => found,
                }
            }
            Err(error) => return Err(error.into()),
        };
        if let Ok(handover) = Handover::decode(&found) {
            return Ok(PairingCheck::Handover(handover));
        }
        if InviteEnd::decode(&found).is_ok() {
            return Ok(PairingCheck::Ended);
        }
        // This device's request, or another sealed for the same path: a
        // request the showing device has not answered.
        SingleChunkObject::decode(&found)?;
        Ok(PairingCheck::Waiting)
    }

    /// Open the handover with the pair this device asked with. It must carry
    /// the pairing's member; whether its pair is that member's current one is
    /// for loading to check (§13.2).
    pub fn open(&self, handover: &Handover) -> Result<(MemberId, MemberKeys), SyncError> {
        let (member, keys) = handover.open(&self.path, &self.keys)?;
        if member != self.member {
            return Err(SyncError::InvitationChanged);
        }
        Ok((member, keys))
    }
}
