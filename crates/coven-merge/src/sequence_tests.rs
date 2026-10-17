use super::*;
use crate::{tests::*, *};
use coven_foundation::id_source::DeviceId;

#[test]
fn authored_prefix_and_own_predecessors_match_causality() {
    // Causality.own_predecessor_is_read and intervening_write_not_read.
    let reader = WriteId {
        device: DeviceId(1),
        number: 4,
    };
    for read_through in 0..5 {
        for slot in 1..7 {
            for device in 1..=2 {
                for number in 1..6 {
                    let earlier = WriteId {
                        device: DeviceId(device),
                        number,
                    };
                    let past = SlotPast::new(reader, read_through, |_| Some(Slot(slot)), None);
                    assert_eq!(
                        past.contains(&earlier),
                        slot <= read_through || (device == 1 && number < 4)
                    );
                }
            }
        }
    }
    assert!(had_read(
        reader,
        0,
        WriteId {
            number: 3,
            ..reader
        },
        None
    ));
    assert!(!had_read(reader, 0, id(2), None));
    assert!(!had_read(reader, 0, id(2), Some(Slot(0))));
}

#[test]
fn unique_values_use_slot_then_write_then_primary_key() {
    let mut view = MemoryView::default();
    for n in 1..=3 {
        view.present(row(n), 1, stamp(100 - n));
    }
    let claimed = |slot, number| ClaimOrder::Published {
        slot: Slot(slot),
        number,
    };
    view.claim(&row(1), ["title"], "Plan", claimed(2, 1));
    view.claim(&row(2), ["title"], "Plan", claimed(1, 2));
    view.claim(&row(3), ["title"], "Plan", claimed(1, 1));
    let result = removals(&view).unwrap();
    assert!(!result.removed.contains_key(&row(3)));
    assert!(result.removed.contains_key(&row(1)));
    assert!(result.removed.contains_key(&row(2)));
    view.claim(&row(2), ["title"], "Plan", claimed(1, 1));
    assert!(!removals(&view).unwrap().removed.contains_key(&row(2)));
    view.claim(&row(1), ["title"], "Plan", ClaimOrder::Local { number: 1 });
    assert!(removals(&view).unwrap().removed.contains_key(&row(1)));
}

#[test]
fn older_live_loser_returns_before_a_new_claim() {
    // LiveClaims.older_loser_returns: claims 1 and 2, delete 1, then claim 3.
    let mut view = MemoryView::default();
    for n in 1..=3 {
        view.present(row(n), 1, stamp(4 - n));
        view.claim(
            &row(n),
            ["title"],
            "Plan",
            ClaimOrder::Published {
                slot: Slot(n),
                number: 1,
            },
        );
    }
    view.data
        .insert(row(1), RemovalRow::Absent { generation: 2 });
    let result = removals(&view).unwrap();
    assert!(!result.removed.contains_key(&row(2)));
    assert!(result.removed.contains_key(&row(3)));
}
