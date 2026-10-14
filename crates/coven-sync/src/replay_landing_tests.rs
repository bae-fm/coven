use crate::{
    effects::tests::{rotation, snapshot},
    replay::tests::{add, device, role, History},
    tests::{
        compare_arrivals, input, lean_results, orders, projected, publication_times,
        received_prefixes,
    },
};
use coven_database::{DropReason, EntryOutcome};
use coven_format::store_log::{MemberRole, StoreChange};
use coven_merge::Audience;

#[test]
#[ignore = "requires COVEN_STORELOG_LEAN, supplied by scripts/check.sh"]
fn lean_differential_storage_admission() {
    // Earlier author timestamp, later storage arrival. The unread witness is
    // outside [0, 1], so admission must inspect the complete storage history.
    let mut h = History::new();
    h.push(
        0,
        1,
        &[0],
        StoreChange::Reset {
            snapshot: snapshot(10, Audience::Store),
        },
    );
    h.push(
        0,
        2,
        &[0],
        StoreChange::Reset {
            snapshot: snapshot(20, Audience::Store),
        },
    );
    let subsets = vec![
        vec![],
        vec![0],
        vec![0, 1],
        vec![0, 2],
        vec![0, 1, 2],
        vec![2, 0, 1],
    ];
    for (landing, too_late) in [(1, false), (31, false), (32, true)] {
        let stored = [0, landing, 1];
        let publications = publication_times(&h, &stored);
        let results = lean_results(&[input(&h, &stored, &subsets)]);
        for (subset, actual) in subsets.iter().zip(&results[0]) {
            let entries: Vec<_> = subset.iter().map(|&i| h.entries[i].clone()).collect();
            let rust = crate::replay(&entries, &publications);
            assert_eq!(*actual, projected(&h, &rust));
            if subset.contains(&1) {
                assert_eq!(
                    rust.entries[&h.entries[1].position],
                    if too_late {
                        EntryOutcome::Dropped(DropReason::LandedTooLate)
                    } else {
                        EntryOutcome::Kept
                    }
                );
            }
        }
        let orders = orders(&h);
        let query = input(&h, &stored, &received_prefixes(&orders));
        compare_arrivals(
            &h,
            &orders,
            &lean_results(std::slice::from_ref(&query))[0],
            &query,
        );
    }

    // An unauthorized, dropped witness still excludes the delayed addition.
    // Its grantee cannot gain authority from that addition in an author view.
    let mut authority = History::new();
    authority.push(0, 1, &[0], add(1, MemberRole::Member));
    authority.push(4, 2, &[0], rotation(Audience::Store, 21));
    authority.push(1, 3, &[0, 1, 2], device(3));
    let stored = [0, 32, 1, 32];
    let arrival_orders = orders(&authority);
    let query = input(&authority, &stored, &received_prefixes(&arrival_orders));
    compare_arrivals(
        &authority,
        &arrival_orders,
        &lean_results(std::slice::from_ref(&query))[0],
        &query,
    );
    let result = crate::replay(&authority.entries, &publication_times(&authority, &stored));
    assert_eq!(
        result.entries[&authority.entries[1].position],
        EntryOutcome::Dropped(DropReason::LandedTooLate)
    );
    for i in [2, 3] {
        assert_eq!(
            result.entries[&authority.entries[i].position],
            EntryOutcome::Dropped(DropReason::NotAllowed)
        );
    }

    // Equal storage times stay equal; the day-40 reset is permanently late,
    // independently of the concurrent reset that also defeats it.
    let mut tied = History::new();
    for i in 1..=3 {
        tied.push(
            0,
            i,
            &[0],
            StoreChange::Reset {
                snapshot: snapshot(i, Audience::Store),
            },
        );
    }
    for landing in [1, 30, 31, 32, 40, 100] {
        let stored = [0, 1, 1, landing];
        let orders = orders(&tied);
        let query = input(&tied, &stored, &received_prefixes(&orders));
        compare_arrivals(
            &tied,
            &orders,
            &lean_results(std::slice::from_ref(&query))[0],
            &query,
        );
    }

    // A late device removal does not erase the owner seen by a new attempt.
    let mut devices = History::new();
    devices.all(0, 0, device(1));
    devices.push(
        0,
        0,
        &[0, 1],
        StoreChange::RemoveDevice {
            device: coven_foundation::id_source::DeviceId(1),
        },
    );
    devices.push(0, 2, &[0, 1], device(2));
    devices.all(
        0,
        0,
        StoreChange::RemoveDevice {
            device: coven_foundation::id_source::DeviceId(1),
        },
    );
    let stored = [0, 0, 32, 1, 32];
    let arrivals = orders(&devices);
    let query = input(&devices, &stored, &received_prefixes(&arrivals));
    compare_arrivals(
        &devices,
        &arrivals,
        &lean_results(std::slice::from_ref(&query))[0],
        &query,
    );
    let result = crate::replay(&devices.entries, &publication_times(&devices, &stored));
    assert_eq!(
        result.entries[&devices.entries[2].position],
        EntryOutcome::Dropped(DropReason::LandedTooLate)
    );
    assert_eq!(
        result.entries[&devices.entries[4].position],
        EntryOutcome::Kept
    );

    // Own earlier entries are read implicitly even when replay drops them.
    let mut own = History::new();
    own.push(0, 0, &[], role(4, MemberRole::Admin));
    own.push(0, 0, &[], device(9));
    assert!(own.entries[2].had_read.0.is_empty());
    let stored = [0, 1, 100];
    let arrivals = orders(&own);
    let query = input(&own, &stored, &received_prefixes(&arrivals));
    compare_arrivals(
        &own,
        &arrivals,
        &lean_results(std::slice::from_ref(&query))[0],
        &query,
    );
    let result = crate::replay(&own.entries, &publication_times(&own, &stored));
    assert!(matches!(
        result.entries[&own.entries[1].position],
        EntryOutcome::Dropped(DropReason::TargetGone(_))
    ));
    assert_eq!(result.entries[&own.entries[2].position], EntryOutcome::Kept);
}

/// Lateness compares provider times at their full precision on every
/// platform: one nanosecond past thirty days is late, exactly thirty days is
/// not. A 100 ns Windows `SystemTime` could not tell these apart (§4, §9).
#[test]
fn one_nanosecond_past_thirty_days_lands_too_late() {
    use coven_format::value::StorageTime;
    use std::time::Duration;
    let mut h = History::new();
    h.push(0, 1, &[0], device(1));
    h.push(0, 2, &[0], device(2));
    let unread = StorageTime::new(1_760_400_000, 123_456_789).unwrap();
    let thirty_days = Duration::from_secs(30 * 86_400);
    for (after, late) in [
        (thirty_days, false),
        (thirty_days + Duration::from_nanos(1), true),
    ] {
        let publications = [
            (h.entries[0].position, StorageTime::UNIX_EPOCH),
            (h.entries[1].position, unread),
            (h.entries[2].position, unread + after),
        ]
        .into();
        assert_eq!(
            super::landed_too_late(&h.entries[2], &publications),
            late,
            "{after:?}"
        );
    }
}
