use super::current_state::*;
use crate::protocol::circle::{CircleRole, CircleState};
use std::collections::BTreeSet;

fn accessible(state: &CircleCurrentState) -> Box<CircleAccessibleState> {
    match state {
        CircleCurrentState::Active(accessible) => accessible.clone(),
        other => panic!("expected an active current state, got {other:?}"),
    }
}

#[tokio::test]
async fn active_maps_by_rotation_over_the_membership() {
    let owner_pubkey = crate::keys::public_key_hex(&crate::database::test_circle_owner_keypair());
    let state = crate::database::StoreDatabase::new(&crate::sync::test_helpers::open_test_db())
        .install_test_active_circle_state("derived-state".to_string())
        .await
        .expect("install and read the active current state");

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

#[tokio::test]
async fn closing_maps_to_closing_regardless_of_rotation() {
    let owner_pubkey = crate::keys::public_key_hex(&crate::database::test_circle_owner_keypair());
    let active = crate::database::StoreDatabase::new(&crate::sync::test_helpers::open_test_db())
        .install_test_active_circle_state("derived-state".to_string())
        .await
        .expect("install and read the active current state");
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

#[tokio::test]
async fn inactive_maps_to_inactive_with_no_name_or_role() {
    let state = crate::database::StoreDatabase::new(&crate::sync::test_helpers::open_test_db())
        .install_test_inactive_circle_state("derived-state-inactive".to_string())
        .await
        .expect("install and read the inactive current state");
    assert_eq!(state.derived_state(&BTreeSet::new()), CircleState::Inactive);
    let (name, role) = state.display("anyone");
    assert_eq!(name, None);
    assert_eq!(role, None);
}

#[tokio::test]
async fn deleted_maps_to_deleted() {
    let active = crate::database::StoreDatabase::new(&crate::sync::test_helpers::open_test_db())
        .install_test_active_circle_state("derived-state".to_string())
        .await
        .expect("install and read the active current state");
    let deleted = CircleCurrentState::Deleted(Box::new(accessible(&active).current.clone()));
    assert_eq!(
        deleted.derived_state(&BTreeSet::new()),
        CircleState::Deleted
    );
    assert_eq!(deleted.display("anyone"), (None, None));
}

#[tokio::test]
async fn control_conflict_maps_to_its_retained_branches() {
    let active = crate::database::StoreDatabase::new(&crate::sync::test_helpers::open_test_db())
        .install_test_active_circle_state("derived-state".to_string())
        .await
        .expect("install and read the active current state");
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
