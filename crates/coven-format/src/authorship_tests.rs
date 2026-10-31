use super::*;
use crate::test_utils::{column, row, store_key, transient_execution, write};
use crate::write::Execution;
use coven_merge::{Audience, Change};
use std::collections::BTreeMap;

fn hasher() -> coven_crypto::AuthorshipHasher {
    store_key().derive().authorship_hasher()
}

/// Leaves standing for positions, so a tree's shape is visible in its root.
fn numbered(count: u64) -> Vec<Leaf> {
    (0..count).map(|generation| Leaf::Deletion { row: row(), generation }).collect()
}

/// RFC 6962's tree hash, written independently of the streaming builder.
fn reference_root(hasher: &impl PartHasher, leaves: &[Leaf]) -> [u8; 32] {
    match leaves.len() {
        0 => coven_crypto::authorship_empty(),
        1 => hasher.leaf(&leaves[0].encode().unwrap()),
        n => {
            let mut left = 1;
            while left * 2 < n {
                left *= 2;
            }
            coven_crypto::authorship_node(
                &reference_root(hasher, &leaves[..left]),
                &reference_root(hasher, &leaves[left..]),
            )
        }
    }
}

#[test]
fn streamed_roots_split_like_rfc_6962_at_every_size() {
    let hasher = hasher();
    for count in 0..40 {
        let leaves = numbered(count);
        assert_eq!(
            root(&hasher, &leaves).unwrap(),
            reference_root(&hasher, &leaves),
            "{count} leaves"
        );
    }
}

#[test]
fn each_change_kind_has_its_own_leaves_and_every_operation_one_more() {
    let change = write().parts.remove(0).rows.remove(0);
    let leaves = change_leaves(&change);
    // An update: one cell per column, then its one operation.
    assert!(matches!(
        leaves.as_slice(),
        [Leaf::Cell { kind: CellKind::Update, column, .. }, Leaf::Step { .. }] if column == "x"
    ));
    let mut insert = change.clone();
    insert.change = Change {
        generation: 0,
        operation: Operation::Insert(BTreeMap::from([
            ("x".into(), column(Value::Integer(7))),
            ("y".into(), column(Value::Integer(8))),
        ])),
    };
    insert.old.clear();
    insert.sources = Default::default();
    insert.execution = Execution::default();
    let leaves = change_leaves(&insert);
    assert_eq!(leaves.len(), 3, "a creation marker and one leaf per cell");
    assert!(matches!(&leaves[0], Leaf::Creation { generation: 0, creator: None, .. }));
    let mut delete = change;
    delete.change.operation = Operation::Delete;
    delete.execution = Execution::default();
    assert_eq!(change_leaves(&delete), [Leaf::Deletion { row: row(), generation: 1 }]);
    // A row with no net change still commits to each operation.
    assert_eq!(execution_leaves(&transient_execution(Audience::Store)).len(), 2);
    // The part's leaves follow its stream's record order.
    let part = &write().parts[0];
    let rows: Vec<_> = part_leaves(part).iter().map(|l| l.row().key.clone()).collect();
    assert!(rows.windows(2).all(|pair| pair[0] <= pair[1]));
    assert_eq!(rows.len(), 4);
}

#[test]
fn leaf_bytes_are_pinned_per_kind_and_round_trip() {
    let row = RowId { table: "t".into(), key: vec![0x20, 0x6b, 0, 0], audience: Audience::Store };
    let creation = Leaf::Creation { row: row.clone(), generation: 0, creator: None };
    assert_eq!(
        hex::encode(creation.encode().unwrap()),
        concat!("00", "0000000174", "00000004206b0000", "00", "0000000000000000", "00")
    );
    let cell = Leaf::Cell {
        row: row.clone(),
        generation: 2,
        kind: CellKind::Update,
        column: "x".into(),
        value: column(Value::Integer(9)),
    };
    assert_eq!(
        hex::encode(cell.encode().unwrap()),
        concat!(
            "01",
            "0000000174",
            "00000004206b0000",
            "00",
            "0000000000000002",
            "01",
            "0000000178",
            "010000000000000009",
            "00000000"
        )
    );
    for leaf in [
        creation,
        cell,
        Leaf::Deletion { row: row.clone(), generation: 3 },
        execution_leaves(&transient_execution(Audience::Store)).remove(0),
    ] {
        assert_eq!(Leaf::decode(&leaf.encode().unwrap()).unwrap(), leaf);
    }
    assert!(matches!(
        Leaf::decode(&[4]),
        Err(Error::UnknownTag { field: "authorship leaf", tag: 4 })
    ));
}

#[test]
fn an_operation_tag_or_a_copied_creator_changes_the_root() {
    let hasher = hasher();
    let cell = |kind| Leaf::Cell {
        row: row(),
        generation: 1,
        kind,
        column: "x".into(),
        value: column(Value::Integer(7)),
    };
    assert_ne!(
        root(&hasher, &[cell(CellKind::Insert)]).unwrap(),
        root(&hasher, &[cell(CellKind::Update)]).unwrap()
    );
    let creation = |creator| Leaf::Creation { row: row(), generation: 0, creator };
    let copied = crate::write_sources::ChangeId {
        write: coven_merge::WriteId { device: coven_foundation::id_source::DeviceId(2), number: 1 },
        row: row(),
    };
    assert_ne!(
        root(&hasher, &[creation(None)]).unwrap(),
        root(&hasher, &[creation(Some(copied))]).unwrap()
    );
}

/// The number of maximal subtrees with no kept leaf: the fewest hashes any
/// proof in this tree format can use.
fn absent_subtrees(kept: &[bool]) -> usize {
    if !kept.iter().any(|k| *k) {
        return 1;
    }
    if kept.len() == 1 {
        return 0;
    }
    let mut left = 1;
    while left * 2 < kept.len() {
        left *= 2;
    }
    absent_subtrees(&kept[..left]) + absent_subtrees(&kept[left..])
}

#[test]
fn multiproofs_open_exactly_the_kept_leaves_with_the_fewest_hashes() {
    let hasher = hasher();
    for count in 1..18u64 {
        let leaves = numbered(count);
        for mask in [0u64, 1, 0b101, 0b1001_0110, u64::MAX, 1 << (count - 1)] {
            let keep = |leaf: &Leaf| matches!(leaf, Leaf::Deletion { generation, .. } if mask >> generation & 1 == 1);
            let proof = Multiproof::prove(&hasher, &leaves, keep).unwrap();
            assert_eq!(proof.root(&hasher).unwrap(), root(&hasher, &leaves).unwrap());
            let opened: Vec<_> = proof.revealed.iter().map(|(i, _)| *i).collect();
            let kept: Vec<_> = (0..count).filter(|i| mask >> i & 1 == 1).collect();
            assert_eq!(opened, kept);
            assert_eq!(
                proof.opened().unwrap(),
                kept.iter().map(|i| leaves[*i as usize].clone()).collect::<Vec<_>>()
            );
            let flags: Vec<_> = (0..count).map(|i| mask >> i & 1 == 1).collect();
            assert_eq!(proof.hidden.len(), absent_subtrees(&flags));
        }
    }
}

#[test]
fn a_surviving_cell_needs_no_overwritten_sibling_value() {
    // CellProof's example: creation (7, 8) with only the first value still
    // needed opens the marker and 7, and hides 8 behind one hash.
    let hasher = hasher();
    let leaves = vec![
        Leaf::Creation { row: row(), generation: 0, creator: None },
        Leaf::Cell {
            row: row(),
            generation: 0,
            kind: CellKind::Insert,
            column: "a".into(),
            value: column(Value::Integer(7)),
        },
        Leaf::Cell {
            row: row(),
            generation: 0,
            kind: CellKind::Insert,
            column: "b".into(),
            value: column(Value::Integer(8)),
        },
    ];
    let proof = Multiproof::prove(&hasher, &leaves, |leaf| {
        matches!(leaf, Leaf::Creation { .. })
            || matches!(leaf, Leaf::Cell { column, .. } if column == "a")
    })
    .unwrap();
    assert_eq!(proof.opened().unwrap(), leaves[..2]);
    assert_eq!(proof.hidden.len(), 1);
    assert_eq!(proof.root(&hasher).unwrap(), root(&hasher, &leaves).unwrap());
    // Ownership needs only the creation marker once both values changed: in
    // this split, the marker's sibling and the right subtree are two hashes.
    let marker =
        Multiproof::prove(&hasher, &leaves, |leaf| matches!(leaf, Leaf::Creation { .. })).unwrap();
    assert_eq!(marker.opened().unwrap(), leaves[..1]);
    assert_eq!(marker.hidden.len(), absent_subtrees(&[true, false, false]));
    assert_eq!(marker.hidden.len(), 2);
}

#[test]
fn malformed_multiproofs_are_refused() {
    let hasher = hasher();
    let leaves = numbered(5);
    let proof = Multiproof::prove(&hasher, &leaves, |leaf| {
        matches!(leaf, Leaf::Deletion { generation: 1 | 3, .. })
    })
    .unwrap();
    let mut extra = proof.clone();
    extra.hidden.push([0; 32]);
    let mut missing = proof.clone();
    missing.hidden.pop();
    let mut unordered = proof.clone();
    unordered.revealed.reverse();
    let mut outside = proof.clone();
    outside.revealed[1].0 = 5;
    let mut garbage = proof.clone();
    garbage.revealed[0].1 = vec![9];
    let mut shorter = proof.clone();
    shorter.leaves = 4;
    for bad in [extra, missing, unordered, outside, garbage] {
        assert!(bad.root(&hasher).is_err());
    }
    // A different tree size reconstructs a different root, not the signed one.
    assert_ne!(shorter.root(&hasher).ok(), Some(root(&hasher, &leaves).unwrap()));
    // An opened leaf cannot be replaced while keeping the root.
    let mut forged = proof.clone();
    forged.revealed[0].1 = numbered(4)[3].encode().unwrap();
    assert_ne!(forged.root(&hasher).unwrap(), root(&hasher, &leaves).unwrap());
    let mut out = crate::wire::Encoder::new();
    proof.put(&mut out).unwrap();
    let mut input = crate::wire::Decoder::new(&out.bytes).unwrap();
    assert_eq!(Multiproof::get(&mut input).unwrap(), proof);
    input.finish().unwrap();
}

#[test]
fn an_empty_part_has_the_empty_root() {
    let hasher = hasher();
    assert_eq!(root(&hasher, &[]).unwrap(), coven_crypto::authorship_empty());
    let proof = Multiproof::prove(&hasher, &[], |_| true).unwrap();
    assert_eq!(proof.root(&hasher).unwrap(), coven_crypto::authorship_empty());
}

#[test]
fn retained_leaf_hashes_reopen_a_part_without_its_key() {
    let hasher = hasher();
    let leaves = numbered(11);
    let hashes: Vec<_> = leaves.iter().map(|leaf| hasher.leaf(&leaf.encode().unwrap())).collect();
    assert_eq!(root_of_hashes(&hashes), root(&hasher, &leaves).unwrap());
    let keep = |leaf: &Leaf| matches!(leaf, Leaf::Deletion { generation: 2 | 7, .. });
    let opened = leaves.iter().map(|leaf| keep(leaf).then(|| leaf.encode().unwrap())).collect();
    assert_eq!(
        Multiproof::from_hashes(&hashes, opened),
        Multiproof::prove(&hasher, &leaves, keep).unwrap()
    );
}

#[test]
fn retained_openings_match_fresh_proofs_for_every_subtree_shape() {
    let hasher = hasher();
    for count in 0..40 {
        let leaves = numbered(count);
        for opened in [0u64, 1, 0b101, 0b1001_0110, u64::MAX] {
            let proof = Multiproof::prove(&hasher, &leaves, |leaf| {
                matches!(leaf, Leaf::Deletion { generation, .. } if opened >> generation & 1 == 1)
            }).unwrap();
            let (root, hashes) = proof.root_with_leaf_hashes(&hasher).unwrap();
            for kept in [0u64, 1, 0b10, 0b101, 0b1001_0110, u64::MAX] {
                let keep: Vec<_> = proof.revealed.iter().map(|(i, _)| kept >> i & 1 == 1).collect();
                let retained = proof.retain_opened(&hashes, &keep);
                let expected = Multiproof::prove(&hasher, &leaves, |leaf| {
                    matches!(leaf, Leaf::Deletion { generation, .. } if (opened & kept) >> generation & 1 == 1)
                }).unwrap();
                assert_eq!(
                    retained, expected,
                    "{count} leaves, {opened:b} opened, {kept:b} retained"
                );
                assert_eq!(retained.root(&hasher).unwrap(), root);
            }
        }
    }
}
