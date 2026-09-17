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

/// Two entries at one author-stream position wedge a Circle permanently, and
/// the terminal deletion is not an exit from it.
///
/// An entry position belongs to one `(author_pubkey, device_id, owner_grant)`,
/// so two entries at one position are authored by one device, and the two
/// controls that introduce them sit at one position of *that device's control
/// stream* too. `covered_controls` is a frontier — `CircleControl::verify`
/// requires it to be strictly ascending by author stream, one control per
/// stream — so no successor can name both, and `previous_control_hash` names
/// exactly one. The deletion covers the branch it authors from, and the other
/// branch stays: the Circle reduces to a conflict between the deletion and the
/// branch it could not cover.
///
/// This is the one state nothing in the protocol repairs. Reaching it takes an
/// Owner device that authors a successor from a Circle state it has already
/// moved past — which its own durable operation journal exists to prevent.
#[tokio::test]
async fn a_one_position_conflict_refuses_resolution_and_outlives_deletion() {
    let fixture = ConflictFixture::build("wedge-delete").await;
    let branches = fixture.fork_one_device_at_one_position().await;

    // Surfaced before the deletion, whichever branch the Owner picks: a
    // resolution covers both branches, so it inherits both entries, and one
    // frontier position per author stream cannot reach both. Preparation
    // refuses it at this device, before it publishes anything, and the
    // conflict stands exactly as it was.
    for branch in &branches {
        let error = fixture
            .store1()
            .await
            .circles()
            .resolve_circle_control(fixture.circle_id(), branch.clone())
            .await
            .expect_err("a one-position conflict has no resolution");
        let rendered = format!("{error}");
        assert!(
            rendered.contains("inherits two metadata entries at one author stream position"),
            "{rendered}"
        );
        assert_eq!(
            fixture.retained_conflict_device1().await,
            Some(branches.clone()),
            "the refused resolution leaves every branch standing"
        );
    }
    let rename = fixture
        .store1()
        .await
        .circles()
        .rename_circle("0000000001400-0000-device1", fixture.circle_id(), "Gamma")
        .await
        .expect_err("renaming a conflicted Circle is refused");
    assert!(
        matches!(&rename, CircleOperationError::Conflicted { circle_id }
            if *circle_id == fixture.circle_id()),
        "{rename:?}"
    );

    // The deletion is accepted and activates — but it covers one control per
    // author stream, and both branches occupy one position of one stream. It
    // names the branch it authors from; the other survives it.
    fixture
        .store1()
        .await
        .circles()
        .delete_circle(fixture.circle_id(), Some(&routing()))
        .await
        .expect("author a terminal deletion over the wedged Circle");
    let after = fixture
        .retained_conflict_device1()
        .await
        .expect("the deletion leaves the Circle conflicted");
    assert_eq!(
        after.len(),
        2,
        "the deletion replaces one branch: {after:?}"
    );
    assert!(
        after.contains(&branches[1]),
        "the branch the deletion could not cover survives it: {after:?}"
    );
    assert!(
        !after.contains(&branches[0]),
        "the branch the deletion authored from is covered: {after:?}"
    );
    let device1 = fixture.circles_device1().await;
    assert!(
        matches!(device1.as_slice(), [CircleInfo::Conflicted { .. }]),
        "a one-position conflict outlives its own deletion: {device1:?}"
    );

    // Every device reaches the same held state, in the same canonical order:
    // device 2 installs the same history and reduces to the same two branches.
    fixture.pull_device2().await;
    assert_eq!(
        fixture.retained_conflict_device2().await,
        Some(after),
        "the conflict is identical on every device"
    );

    // And it is held to this Circle. The Store keeps taking work: another
    // Circle is created and read back beside the wedged one.
    fixture
        .store1()
        .await
        .circles()
        .create_circle("0000000009000-0000-device1", "Allotment")
        .await
        .expect("the Store still accepts unrelated work");
    let circles = fixture.circles_device1().await;
    assert_eq!(circles.len(), 2, "{circles:?}");
    assert!(
        circles
            .iter()
            .any(|circle| matches!(circle, CircleInfo::Active { name, .. } if name == "Allotment")),
        "{circles:?}"
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
