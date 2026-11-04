//! Kind numbers: the first byte of every stored object and frame (format.md,
//! "Kind numbers"). A kind is written by the module that encodes the object
//! and read back by the module that decodes it, both through these names.

/// Write header frame.
pub(crate) const WRITE_HEADER: u8 = 1;
/// Row change frame.
pub(crate) const ROW_CHANGE: u8 = 2;
/// Dismissal frame.
pub(crate) const DISMISSAL: u8 = 3;
/// Admin entry frame.
pub(crate) const ADMIN: u8 = 4;
/// Snapshot header frame.
pub(crate) const SNAPSHOT_HEADER: u8 = 5;
/// Snapshot record frame; the floor carries its announcement copies in it too.
pub(crate) const SNAPSHOT_RECORD: u8 = 6;
/// Snapshot end frame; it also ends the floor's body.
pub(crate) const SNAPSHOT_END: u8 = 7;
/// Join request frame.
pub(crate) const JOIN_REQUEST_FRAME: u8 = 9;
/// Restore code frame.
pub(crate) const RESTORE_CODE: u8 = 10;
/// Invite code frame.
pub(crate) const INVITE_CODE: u8 = 11;
/// Floor header frame.
pub(crate) const FLOOR_HEADER: u8 = 12;
/// Execution record frame.
pub(crate) const ROW_EXECUTION: u8 = 13;
/// Slot header frame.
pub(crate) const SLOT_HEADER: u8 = 14;
/// Snapshot announcement frame.
pub(crate) const SNAPSHOT_ANNOUNCEMENT: u8 = 15;
/// Key-copy acknowledgement frame.
pub(crate) const KEY_COPY_ACK: u8 = 16;
/// Saved restore code frame.
pub(crate) const SAVED_RESTORE_CODE: u8 = 17;
/// Sealed inner write.
pub(crate) const INNER_WRITE: u8 = 32;
/// Sealed sequence slot.
pub(crate) const SEQUENCE_SLOT: u8 = 33;
/// Sealed snapshot.
pub(crate) const SNAPSHOT: u8 = 34;
/// Sealed join request.
pub(crate) const JOIN_REQUEST: u8 = 36;
/// An ended invite's marker.
pub(crate) const ENDED_INVITE: u8 = 37;
/// A file.
pub(crate) const FILE: u8 = 38;
/// Sealed floor.
pub(crate) const FLOOR: u8 = 39;
/// A gone device's marker.
pub(crate) const GONE_DEVICE: u8 = 40;

/// Whether `kind` starts a plaintext frame, as opposed to a stored object.
pub(crate) const fn is_frame(kind: u8) -> bool {
    matches!(kind, WRITE_HEADER..=SNAPSHOT_END | JOIN_REQUEST_FRAME..=SAVED_RESTORE_CODE)
}
