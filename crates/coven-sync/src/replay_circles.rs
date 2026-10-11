//! Finish circle deletion and select the responsible kept entry (§9, §14.7).

use std::collections::BTreeMap;

use coven_database::{CircleRemoval, StoreLogCheck, StoreLogState};
use coven_format::store_log::{StoreChange, StoreLogEntry};

/// Select causes from kept entries, including removals already satisfied during
/// the scan. Emptiness is judged only after all compatible changes have applied.
pub(super) fn finish(
    state: &mut StoreLogState,
    entries: &[&StoreLogEntry],
    views: &[StoreLogCheck],
    kept: &[usize],
) {
    let mut deletions = BTreeMap::new();
    let mut removals = BTreeMap::new();
    // Kept indices have timestamp order, so replacement selects the latest cause.
    for &index in kept {
        let entry = entries[index];
        match &entry.change {
            StoreChange::DeleteCircle { circle } => {
                deletions.insert(*circle, entry.position);
            }
            _ => {
                if let StoreLogCheck::MemberRemoval(circles) = &views[index] {
                    for (circle, removal) in circles {
                        removals.insert(*circle, entry.position);
                        if *removal == CircleRemoval::Circle {
                            deletions.insert(*circle, entry.position);
                        }
                    }
                }
            }
        }
    }
    for (id, circle) in &state.circles {
        if !state.circle_deletions.contains_key(id) {
            if circle.members.is_empty() {
                deletions.insert(
                    *id,
                    *removals.get(id).expect("empty circle has a kept removal"),
                );
            } else {
                deletions.remove(id);
            }
        }
    }
    state.circle_deletions = deletions;
}

#[cfg(test)]
#[path = "replay_circles_tests.rs"]
pub(crate) mod tests;
