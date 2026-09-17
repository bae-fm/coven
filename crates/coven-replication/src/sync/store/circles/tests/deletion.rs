//! Deleting a conflicted Circle. A deletion covers every branch, so it is the
//! exit a conflict always offers — and the one conflict it cannot collapse is
//! the one no control can: two entries at one author-stream position.

use super::conflict_fixture::{routing, ConflictFixture};
use super::*;
use coven_protocol::circle::CircleInfo;

#[tokio::test]
async fn deleting_a_conflicted_circle_covers_every_branch() {
    let fixture = ConflictFixture::build("delete-conflicted").await;
    let _ = fixture.fork().await;

    // Every command that extends the Circle's history still refuses: there is
    // no single current control to author a successor from.
    let refused = fixture
        .store1()
        .await
        .circles()
        .rename_circle("0000000001300-0000-device1", fixture.circle_id(), "Gamma")
        .await
        .expect_err("renaming a conflicted Circle is refused");
    assert!(
        matches!(&refused, CircleOperationError::Conflicted { circle_id }
            if *circle_id == fixture.circle_id()),
        "{refused:?}"
    );

    // Deletion is the exception: it authors from the canonical branch and
    // covers every other one, so the conflict collapses to one terminal state.
    fixture
        .store1()
        .await
        .circles()
        .delete_circle(fixture.circle_id(), Some(&routing()))
        .await
        .expect("delete the conflicted Circle");
    let device1 = fixture.circles_device1().await;
    assert!(
        matches!(device1.as_slice(), [CircleInfo::Deleted { id }] if *id == fixture.circle_id()),
        "the deletion collapses the conflict on the deleting device: {device1:?}"
    );
    assert!(
        fixture.retained_conflict_device1().await.is_none(),
        "no branch is left retained once the deletion covers them all"
    );

    // Device 2 retained only its own branch. It verifies the deletion — every
    // covered branch resolves to an accepted activation whose entries the
    // deletion carries — and reduces to the same terminal state.
    fixture.pull_device2().await;
    let device2 = fixture.circles_device2().await;
    assert!(
        matches!(device2.as_slice(), [CircleInfo::Deleted { id }] if *id == fixture.circle_id()),
        "the deletion collapses the conflict on the other device too: {device2:?}"
    );
}

#[tokio::test]
async fn deleting_a_resolved_circle_is_terminal() {
    let fixture = ConflictFixture::build("delete-resolved").await;
    let (chosen, _losing) = fixture.fork().await;

    fixture
        .store1()
        .await
        .circles()
        .resolve_circle_control(fixture.circle_id(), chosen.clone())
        .await
        .expect("resolve the control conflict");

    // Once resolved, deletion proceeds and the Circle surfaces as deleted.
    fixture
        .store1()
        .await
        .circles()
        .delete_circle(fixture.circle_id(), Some(&routing()))
        .await
        .expect("delete the resolved Circle");
    let device1 = fixture.circles_device1().await;
    assert!(
        matches!(device1.as_slice(), [CircleInfo::Deleted { id }] if *id == fixture.circle_id()),
        "the resolving device reports the Circle as deleted: {device1:?}"
    );

    // A second deletion is refused: the Circle is already terminal.
    let already = fixture
        .store1()
        .await
        .circles()
        .delete_circle(fixture.circle_id(), Some(&routing()))
        .await
        .expect_err("deleting an already-deleted Circle is refused");
    assert!(
        matches!(&already, CircleOperationError::Deleted { circle_id }
            if *circle_id == fixture.circle_id()),
        "{already:?}"
    );
}
