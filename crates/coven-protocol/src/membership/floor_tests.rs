use super::*;

fn reference(stream: u8, sequence: u64) -> MembershipHeadRef {
    let bytes = format!("head/{stream}/{sequence}").into_bytes();
    let hash = ObjectHash::digest(&bytes);
    MembershipHeadRef {
        coord: MembershipCoord {
            author_pubkey: "author".into(),
            author_owner_grant: MembershipGrantId(ObjectHash::digest(b"owner grant")),
            stream_id: AuthorStreamId::from_bytes([stream; 32]),
            seq: sequence,
            entry_hash: hash,
        },
        head_hash: hash,
        object: ExactObjectRef::new(
            crate::objects::ObjectSlot::logical(format!("head/{stream}/{sequence}.json"))
                .expect("head slot"),
            bytes.len() as u64,
            hash,
        ),
    }
}

#[test]
fn merging_heads_preserves_each_streams_exact_tip() {
    let first = reference(1, 1);
    let latest = reference(1, 3);
    let other = reference(2, 2);
    assert_eq!(
        MembershipFloor::from_heads([other.clone(), latest.clone(), first, latest.clone(),])
            .expect("merge accepted references")
            .0,
        vec![latest, other],
    );
}

#[test]
fn later_head_cannot_hide_conflicting_exact_predecessor_references() {
    let original = reference(1, 1);
    let mut conflicting = original.clone();
    conflicting.head_hash = ObjectHash::digest(b"another signed head");
    let latest = reference(1, 3);
    for heads in [
        vec![original.clone(), latest.clone(), conflicting.clone()],
        vec![latest, conflicting, original],
    ] {
        assert_eq!(
            MembershipFloor::from_heads(heads),
            Err(MembershipFloorError::ConflictingHeads),
        );
    }
}
