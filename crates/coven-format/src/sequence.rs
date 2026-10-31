//! Plaintext slot headers and snapshot announcements (D9).

use crate::admin::{SnapshotId, StoreChange};
use crate::error::{require, Error, Rule};
use crate::path::ObjectPath;
use crate::value::{positive, Slot};
use crate::wire::{decode_frame, wire_struct, Decoder, Encoder, Wire};
use coven_foundation::id_source::{DeviceId, MemberId};

/// Complete limit for an admin, key-copy or announcement slot (D2).
pub const MAX_BOUNDED_SLOT: usize = 4 * 1024 * 1024;

/// The author and claim identity sealed under the store key in every slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotHeader {
    /// The claiming device.
    pub device: DeviceId,
    /// Positive number from this device's counter across every slot kind.
    pub claim: u64,
    /// The member whose credential signs the slot.
    pub author: MemberId,
    /// The immediately preceding slot, zero for creation.
    pub after: u64,
}
wire_struct!(SlotHeader, device, claim, author, after);
impl SlotHeader {
    /// Encode the kind-14 frame, checking its path binding.
    pub fn encode(&self, path: &ObjectPath) -> Result<Vec<u8>, Error> {
        self.validate_at(path)?;
        crate::encode_frame(14, self)
    }

    /// Decode exactly the kind-14 frame and check its path binding.
    pub fn decode(bytes: &[u8], path: &ObjectPath) -> Result<Self, Error> {
        let (kind, mut input) = decode_frame(bytes)?;
        require(kind == 14, "slot header", Rule::Kind)?;
        let header = Self::get(&mut input)?;
        input.finish()?;
        header.validate_at(path)?;
        Ok(header)
    }

    /// A header's predecessor must be exactly one less than its path's slot.
    pub fn validate_at(&self, path: &ObjectPath) -> Result<(), Error> {
        positive(self.claim)?;
        require(
            path.slot_number().is_some_and(|slot| slot.0 - 1 == self.after),
            "slot predecessor",
            Rule::Coverage,
        )
    }

    /// Check an opened inner write against its containing slot. The consumer
    /// resolves its historical signer and its source writes separately.
    pub fn check_write(&self, write: &crate::write::WriteHeader) -> Result<(), Error> {
        write.validate()?;
        require(write.position.device == self.device, "slot write device", Rule::WriteDevice)?;
        require(write.read_through <= self.after, "slot write read-through", Rule::Coverage)
    }
}

/// The state a snapshot supplies; preparations require an activating admin slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Purpose {
    /// State at the existing schema and reset.
    Ordinary,
    /// Prepared state at a positive new app schema version.
    Raise(u32),
    /// Prepared replacement state for a reset.
    Reset,
}
impl Purpose {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if let Self::Raise(version) = self {
            require(*version > 0, "snapshot raise version", Rule::RaiseVersion)?;
        }
        Ok(())
    }
}
impl Wire for Purpose {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.validate()?;
        match self {
            Self::Ordinary => 0u8.put(out),
            Self::Raise(version) => {
                1u8.put(out)?;
                version.put(out)
            }
            Self::Reset => 2u8.put(out),
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        let purpose = match u8::get(input)? {
            0 => Self::Ordinary,
            1 => Self::Raise(u32::get(input)?),
            2 => Self::Reset,
            tag => return Err(Error::UnknownTag { field: "snapshot purpose", tag }),
        };
        purpose.validate()?;
        Ok(purpose)
    }
}

/// A claimed announcement of an immutable snapshot body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotAnnouncement {
    /// Audience, writer and object number forming the snapshot path.
    pub snapshot: SnapshotId,
    /// Last included slot; strictly below the announcing slot.
    pub covers: u64,
    /// Ordinary state or an admin operation's preparation.
    pub purpose: Purpose,
    /// SHA-256 of the complete sealed snapshot object.
    pub digest: [u8; 32],
}
wire_struct!(SnapshotAnnouncement, snapshot, covers, purpose, digest);
impl SnapshotAnnouncement {
    /// Encode the announcement at its claimed slot.
    pub fn encode(&self, slot: Slot) -> Result<Vec<u8>, Error> {
        self.validate_at(slot)?;
        crate::encode_frame(15, self)
    }
    /// Decode exactly one announcement and check coverage precedes its slot.
    pub fn decode(bytes: &[u8], slot: Slot) -> Result<Self, Error> {
        let (kind, mut input) = decode_frame(bytes)?;
        require(kind == 15, "snapshot announcement", Rule::Kind)?;
        let announcement = Self::get(&mut input)?;
        input.finish()?;
        announcement.validate_at(slot)?;
        Ok(announcement)
    }
    fn validate_at(&self, slot: Slot) -> Result<(), Error> {
        positive(self.snapshot.number)?;
        require(self.covers < slot.0, "announcement coverage", Rule::Coverage)?;
        self.purpose.validate()
    }
}

/// The second frame of a bounded slot. Writes use a streamed body instead.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SlotFrame {
    /// The admin operation.
    Admin(StoreChange),
    /// Acknowledges the key box in the clear prefix.
    KeyCopy,
    /// Announces a snapshot body uploaded before the claim.
    Announcement(SnapshotAnnouncement),
}
impl SlotFrame {
    /// Encode both plaintext frames, checking path-dependent facts.
    pub fn encode(&self, header: &SlotHeader, path: &ObjectPath) -> Result<Vec<u8>, Error> {
        let mut bytes = header.encode(path)?;
        let slot = path.slot_number().expect("header checked slot path");
        let frame = match self {
            Self::Admin(change) => {
                check_admin(change, header, path)?;
                change.encode()?
            }
            Self::KeyCopy => crate::encode_frame_with(16, |_| Ok(()))?,
            Self::Announcement(announcement) => announcement.encode(slot)?,
        };
        bytes.extend_from_slice(&frame);
        Ok(bytes)
    }

    /// Decode both frames. The clear prefix's kind must also match this result.
    pub fn decode(bytes: &[u8], path: &ObjectPath) -> Result<(SlotHeader, Self), Error> {
        let length = crate::frame_length(bytes)?;
        let header = SlotHeader::decode(bytes.get(..length).ok_or(Error::Truncated)?, path)?;
        let frame = bytes.get(length..).ok_or(Error::Truncated)?;
        let (kind, input) = decode_frame(frame)?;
        let body = match kind {
            4 => {
                let change = StoreChange::decode(frame)?;
                check_admin(&change, &header, path)?;
                Self::Admin(change)
            }
            15 => Self::Announcement(SnapshotAnnouncement::decode(
                frame,
                path.slot_number().expect("checked path"),
            )?),
            16 => {
                input.finish()?;
                Self::KeyCopy
            }
            tag => return Err(Error::UnknownTag { field: "slot frame", tag }),
        };
        Ok((header, body))
    }
}

fn check_admin(change: &StoreChange, header: &SlotHeader, path: &ObjectPath) -> Result<(), Error> {
    match change {
        StoreChange::CreateStore { store, .. } => {
            require(header.after == 0 && *store == path.store(), "creation slot", Rule::Store)
        }
        StoreChange::AddDevice { replaces, .. } => {
            require(*replaces != Some(header.device), "replacement device", Rule::DeviceReplacement)
        }
        StoreChange::RaiseSchema { announcement, .. } | StoreChange::Reset { announcement } => {
            require(announcement.0 <= header.after, "activation announcement", Rule::Coverage)
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
#[path = "sequence_tests.rs"]
mod tests;
