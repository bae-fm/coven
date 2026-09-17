use super::current_state::*;
use crate::circle::{CircleRole, CircleState};
use crate::circle_activation_test_fixtures::{test_circle_activation, test_circle_owner_keypair};
use std::collections::BTreeSet;

fn owner_pubkey() -> String {
    coven_keys::keys::public_key_hex(&test_circle_owner_keypair())
}

fn active_state() -> CircleCurrentState {
    test_circle_activation("derived-state", true).current
}

fn accessible(state: &CircleCurrentState) -> Box<CircleAccessibleState> {
    match state {
        CircleCurrentState::Active(accessible) => accessible.clone(),
        other => panic!("expected an active current state, got {other:?}"),
    }
}

#[test]
fn active_maps_by_rotation_over_the_membership() {
    let owner_pubkey = owner_pubkey();
    let state = active_state();

    // Owner still a Store member: Active.
    let members = BTreeSet::from([owner_pubkey.clone()]);
    assert_eq!(state.derived_state(&members), CircleState::Active);

    // Owner no longer a Store member: RotationRequired naming it.
    let empty = BTreeSet::new();
    assert_eq!(
        state.derived_state(&empty),
        CircleState::RotationRequired {
            removed_members: vec![owner_pubkey.clone()],
        }
    );

    // Display resolves the name and the local role.
    let (name, role) = state.display(&owner_pubkey);
    assert!(name.is_some());
    assert_eq!(role, Some(CircleRole::Owner));
}

#[test]
fn closing_maps_to_closing_regardless_of_rotation() {
    let owner_pubkey = owner_pubkey();
    let active = active_state();
    let closing = CircleCurrentState::Closing(accessible(&active));
    // A closing Circle whose roster still names the (removed) member stays
    // Closing rather than reporting RotationRequired.
    assert_eq!(
        closing.derived_state(&BTreeSet::new()),
        CircleState::Closing
    );
    assert_eq!(
        closing.derived_state(&BTreeSet::from([owner_pubkey])),
        CircleState::Closing
    );
}

#[test]
fn inactive_maps_to_inactive_with_no_name_or_role() {
    let state = test_circle_activation("derived-state-inactive", false).current;
    assert_eq!(state.derived_state(&BTreeSet::new()), CircleState::Inactive);
    let (name, role) = state.display("anyone");
    assert_eq!(name, None);
    assert_eq!(role, None);
}

#[test]
fn deleted_maps_to_deleted() {
    let active = active_state();
    let deleted = CircleCurrentState::Deleted(Box::new(accessible(&active).current.clone()));
    assert_eq!(
        deleted.derived_state(&BTreeSet::new()),
        CircleState::Deleted
    );
    assert_eq!(deleted.display("anyone"), (None, None));
}

#[test]
fn control_conflict_maps_to_its_retained_branches() {
    let active = active_state();
    let current = accessible(&active).current.clone();
    let expected = vec![current.coordinate().clone()];
    let conflict = CircleCurrentState::ControlConflict {
        branches: vec![current],
    };
    assert_eq!(
        conflict.derived_state(&BTreeSet::new()),
        CircleState::ControlConflict { branches: expected }
    );
    assert_eq!(conflict.display("anyone"), (None, None));
}

/// A conflicted control projects no authoring state: the reduction keeps the
/// branch controls and drops each branch's access material, so every exit —
/// resolution and deletion alike — loads the branch it authors from out of that
/// branch's retained accepted activation. What the conflicted state does name
/// is which branch a deletion takes, so every device takes the same one.
#[test]
fn a_conflicted_control_names_its_deletion_branch_and_no_authoring_state() {
    let active = active_state();
    let current = accessible(&active).current.clone();
    let expected = current.coordinate().clone();
    let conflict = CircleCurrentState::ControlConflict {
        branches: vec![current],
    };

    assert!(conflict.authoring_state().is_none());
    assert!(conflict.closing_authoring_state().is_none());
    assert!(
        conflict.deletable_authoring_state().is_none(),
        "a conflicted branch's access material is not in the projected state"
    );
    assert_eq!(
        conflict.deletion_branch(),
        Some(&expected),
        "the deletion authors from the first branch in canonical order"
    );
    assert!(
        active.deletion_branch().is_none(),
        "a resolved state names no deletion branch"
    );
}
