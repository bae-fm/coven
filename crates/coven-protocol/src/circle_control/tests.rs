use super::*;
use crate::circle_roster;

#[test]
fn semantic_paths_bind_the_authoring_device_and_owner_grant() {
    let grant = MembershipGrantId(ObjectHash::digest(b"path grant"));
    let first = circle_roster::CircleRosterCoord {
        author_pubkey: "owner".to_string(),
        device_id: "device".to_string(),
        author_owner_grant: grant.clone(),
        seq: 1,
        entry_hash: ObjectHash::digest(b"entry"),
    };
    let mut substituted = first.clone();
    substituted.device_id = "other-device".to_string();
    let mut regranted = first.clone();
    regranted.author_owner_grant = MembershipGrantId(ObjectHash::digest(b"other path grant"));
    let circle_id = CircleId::from_bytes([7; 16]);
    let first_path = circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
        circle_id,
        coord: &first,
    });

    assert!(first_path.contains(&first.device_id));
    assert!(first_path.contains(&grant.to_string()));
    for other in [&substituted, &regranted] {
        assert!(verify_circle_semantic_prefix(
            &first_path,
            CircleSemanticSlot::RosterEntry {
                circle_id,
                coord: other,
            },
        )
        .is_err());
    }
}
