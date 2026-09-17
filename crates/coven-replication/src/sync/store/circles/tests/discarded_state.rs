//! A device authoring from Circle state it has already moved past.
//!
//! One author-stream position belongs to one `(identity, device, Owner grant)`,
//! so the only way two different entries reach one position is that device
//! authoring twice from the same state — which its own durable operation
//! journal exists to prevent. These tests hold the position closed anyway, on
//! the device that would author it and on every peer that would admit it.

use super::conflict_fixture::ConflictFixture;
use super::*;

/// The device's own verifier refuses the second entry, before it uploads
/// anything.
///
/// Its accepted Circle history already holds an entry at that position, and a
/// control may not introduce a different one there. This is the Circle
/// counterpart of the Store-sequence refusals acceptance already makes.
#[tokio::test]
async fn a_second_entry_at_one_position_is_refused_by_its_own_device() {
    let fixture = ConflictFixture::build("one-position-own-device").await;
    let attempt = fixture.attempt_one_position_fork().await;

    assert!(
        matches!(&attempt.refusal, CircleOperationError::InvalidState(reason)
            if reason == "Circle control reuses an already accepted Circle entry position"),
        "{:?}",
        attempt.refusal
    );
    assert!(
        !fixture.published_exact_object(&attempt.journal.operation().commit_ref().object),
        "the refused commit never reaches the cloud"
    );
    assert!(
        fixture.retained_conflict_device1().await.is_none(),
        "no fork enters accepted history"
    );
}

/// And a peer holds it too. Device 2 never ran device 1's local check, so this
/// is the refusal that matters: the fork cannot enter accepted history through
/// any device, whatever the authoring one did.
#[tokio::test]
async fn a_second_entry_at_one_position_is_held_by_a_peer() {
    let fixture = ConflictFixture::build("one-position-peer").await;
    let attempt = fixture.attempt_one_position_fork().await;

    // Device 2 must already hold the entry the commit would duplicate.
    fixture.pull_device2().await;
    let refusal = fixture
        .peer_verifies(&attempt.journal)
        .await
        .expect_err("a peer refuses a commit that refills an accepted position");

    assert!(
        matches!(&refusal, CircleOperationError::InvalidState(reason)
            if reason == "Circle control reuses an already accepted Circle entry position"),
        "{refusal:?}"
    );
}
