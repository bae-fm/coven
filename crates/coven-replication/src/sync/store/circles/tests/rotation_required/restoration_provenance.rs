use super::restoration::{ActiveMemberCircleSnapshot, CircleFixtureMode, RestoreTarget};

/// A restored recipient resolves an inherited entry's introduction through its
/// own retained accepted activations.
///
/// A cold restore installs a snapshot image and stands its replay baseline on
/// it, so the predecessor history a live device would walk is not what answers
/// here. The retention rule keeps the commit every inherited origin names, and
/// that commit is what the restored device reads back — with the entry object
/// and the introduction unchanged, which is what preserves the entry's original
/// author and device across the restore.
#[tokio::test]
async fn a_restored_recipient_resolves_an_inherited_entry_introduction() {
    let fixture =
        ActiveMemberCircleSnapshot::build("snapshot-restore-provenance", CircleFixtureMode::Live)
            .await;
    // The Circle's current control admitted the member, so it introduced that
    // member's roster entry and inherited an earlier one from an earlier commit.
    let (coord, object, introduction) = fixture.inherited_roster_entry().await;

    let target = RestoreTarget::new();
    let restored = fixture
        .restore_as_member(&target, "provenance-restore-device")
        .await;

    let accepted = restored
        .retained_circle_activation_for_test(fixture.circle_id(), introduction)
        .await
        .expect("read the restored device's retained activations")
        .expect("the restore keeps the commit the inherited entry names");
    let introduced = accepted
        .reference
        .objects()
        .roster_entries
        .get(&coord)
        .expect("the named activation introduced this entry");
    assert_eq!(
        introduced.object, object,
        "the restored introduction holds the same entry object"
    );
    assert!(
        introduced.origin.is_introduced(),
        "the named activation is where the entry entered accepted history"
    );
}
