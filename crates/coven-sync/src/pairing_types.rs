//! What the screen that adds a device sees of a pairing (§13.2, E10).

use coven_crypto::ConfirmationNumber;
use coven_foundation::id_source::{DeviceId, InviteId};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use uuid::Uuid;

/// A pairing's UUID, which names its request's path in an invite's place
/// (§13.2, format.md D10).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PairingId(
    /// The pairing's UUID, supplied by the id source.
    pub Uuid,
);

impl From<InviteId> for PairingId {
    fn from(id: InviteId) -> Self {
        Self(id.0)
    }
}

impl From<PairingId> for InviteId {
    fn from(id: PairingId) -> Self {
        Self(id.0)
    }
}

/// A pairing `start_pairing` made: what the screen that adds a device shows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pairing {
    /// Names the pairing in every later call.
    pub id: PairingId,
    /// The code to show as a QR code, behind a tap. On S3 it carries the key
    /// this device connects with, so the new device asks for none: whoever
    /// sees it reaches the storage as this member until the key is replaced
    /// (§14.3).
    pub qr_code: String,
    /// The code to copy or share as text. It carries no key; on S3 the new
    /// device asks the person for theirs. On other providers it is `qr_code`.
    pub shared_code: String,
    /// 30 minutes after it was made, by this device's clock.
    pub expires_at: SystemTime,
}

/// Where a pairing is (§13.2). The app offers approving and declining only
/// for a request that is `Waiting`, and ends the pairing when its screen
/// closes, whatever its status.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PairingStatus {
    /// Waiting for a request, then with the checked request, for the person
    /// to compare its number and decide.
    Waiting {
        /// The checked request, once one has come.
        request: Option<PairingRequest>,
    },
    /// The person approved `request`: its handover is on its way to the
    /// request's place, or there, waiting for the new device to register.
    Approved {
        /// The approved request.
        request: PairingRequest,
    },
    /// The pairing has ended, its marker in place or on its way; the app
    /// shows how until it ends the pairing and forgets it.
    Ended(PairingEnd),
}

/// How a pairing ended (§13.2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PairingEnd {
    /// This device applied the new device's add-device entry.
    Added {
        /// The device the request named.
        device: DeviceId,
        /// The name the new device gave itself.
        name: String,
    },
    /// The person declined the request.
    Declined,
    /// The request failed a check, such as answering for another member or
    /// carrying keys history knows.
    Refused,
    /// 30 minutes passed by this device's clock first.
    Expired,
}

/// A checked pairing request, as `PairingStatus` shows it (§13.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PairingRequest {
    /// The pairing it answers.
    pub pairing: PairingId,
    /// The name the new device gave itself, for the person to check.
    pub device_name: String,
    /// Computed from the pairing id and the request's keys; the new device
    /// shows the same number (format.md D11).
    pub confirmation: ConfirmationNumber,
    /// The checked request's exact bytes, which approving compares with the
    /// pairing's own.
    pub(crate) record: Vec<u8>,
}
