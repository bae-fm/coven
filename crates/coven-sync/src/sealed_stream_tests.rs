use super::*;

/// A sealed stream's size is a `u64` that must still hold its final
/// signature: a piece that leaves no room for it names the size it
/// overflows (E5).
#[test]
fn a_piece_that_leaves_no_room_for_the_signature_overflows_the_size() {
    let room = u64::MAX - SIGNATURE_LEN as u64;
    assert_eq!(grown(room - 10, 10).unwrap(), room);
    let overflowed = grown(room - 10, 11);
    assert!(
        matches!(
            &overflowed,
            Err(SyncError::Format(coven_format::Error::Invalid {
                field: "sealed stream size",
                ..
            }))
        ),
        "{overflowed:?}"
    );
}
