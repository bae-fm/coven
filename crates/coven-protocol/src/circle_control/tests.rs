use super::*;
use crate::circle_roster;

#[test]
fn semantic_paths_bind_writer_and_author_stream_id() {
    let grant = MembershipGrantId(ObjectHash::digest(b"path grant"));
    let first = circle_roster::CircleRosterCoord {
        author_pubkey: "owner".to_string(),
        device_id: "device".to_string(),
        stream_id: AuthorStreamId::from_bytes([1; 32]),
        author_owner_grant: grant.clone(),
        seq: 1,
        entry_hash: ObjectHash::digest(b"entry"),
    };
    let mut substituted = first.clone();
    substituted.stream_id = AuthorStreamId::from_bytes([2; 32]);
    let circle_id = CircleId::from_bytes([7; 16]);
    let first_path = circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
        circle_id,
        coord: &first,
    });

    assert!(first_path.contains(&first.stream_id.to_string()));
    assert!(verify_circle_semantic_prefix(
        &first_path,
        CircleSemanticSlot::RosterEntry {
            circle_id,
            coord: &substituted,
        },
    )
    .is_err());
}
