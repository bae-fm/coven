/// Outside a store, `coven_sync_state` shows the state sync computes from
/// the facts there: a new store's sync status says no storage is set up, so
/// the store hasn't synced.
#[test]
fn the_sync_state_reads_outside_a_store() {
    let connection = coven_database::test_utils::Connection::open_in_memory().unwrap();
    super::make_readable_objects(&connection, crate::DeviceId(7)).unwrap();
    let (state, pending): (String, Option<i64>) = connection
        .query_row("SELECT state,pending FROM coven_sync_state", [], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    assert_eq!((state.as_str(), pending), ("not_synced", None));
}

/// A connection outside any store, with coven's readable objects made on it.
fn outside_store() -> coven_database::test_utils::Connection {
    let connection = coven_database::test_utils::Connection::open_in_memory().unwrap();
    super::make_readable_objects(&connection, crate::DeviceId(7)).unwrap();
    connection
}

/// A connection outside any store whose storage is connected, so that the
/// sync state follows its last pass and pending records.
fn connected_store() -> coven_database::test_utils::Connection {
    let connection = outside_store();
    let location = crate::StorageConfig::S3 {
        bucket: "library".into(),
        region: "us-east-1".into(),
        endpoint: None,
        profile: None,
    };
    super::record_connected_storage(&connection, location).unwrap();
    connection
}

/// An invite whose id and member are built from `n`, at `stage`.
fn invite(n: u8, stage: crate::RecordedInviteStage) -> crate::RecordedInvite {
    crate::RecordedInvite {
        id: crate::InviteId(uuid::Uuid::from_bytes([n; 16])),
        member: crate::MemberId(uuid::Uuid::from_bytes([n + 100; 16])),
        stage,
    }
}

/// The text `coven_invites.request_member` shows for the member of the
/// invite built from `n`.
fn member_text(n: u8) -> String {
    uuid::Uuid::from_bytes([n + 100; 16]).to_string()
}

/// Each invite a test records outside a store shows in `coven_invites` at
/// its stage, with the member and the name of the request it holds: a new
/// person's request shows the name they typed, and a recovering member's
/// shows none.
#[test]
fn recorded_invites_show_their_stage_and_request() {
    use crate::{RecordedInviteStage as Stage, RecordedJoiner as Joiner};
    let connection = outside_store();
    let binding = super::record_change(
        &connection,
        crate::StoreChange::RenameStore { name: "Household".into() },
    )
    .unwrap();
    let granting = super::record_invite(&connection, invite(1, Stage::Granting)).unwrap();
    let waiting =
        super::record_invite(&connection, invite(2, Stage::Waiting { joiner: None })).unwrap();
    let requested = super::record_invite(
        &connection,
        invite(3, Stage::Waiting { joiner: Some(Joiner::NewPerson("Dan".into())) }),
    )
    .unwrap();
    let approving = super::record_invite(
        &connection,
        invite(4, Stage::Approving { joiner: Joiner::Recovering, binding }),
    )
    .unwrap();

    let shown: Vec<(i64, String, Option<String>, Option<String>)> = connection
        .prepare(
            "SELECT operation,stage,request_member,joiner_name FROM coven_invites
             ORDER BY operation",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        shown,
        [
            (granting.0, "granting".to_owned(), None, None),
            (waiting.0, "waiting".to_owned(), None, None),
            (requested.0, "waiting".to_owned(), Some(member_text(3)), Some("Dan".to_owned())),
            (approving.0, "approving".to_owned(), Some(member_text(4)), None),
        ]
    );
}

/// `coven_operations` lists, outside a store, an invite a sharing refusal
/// ended, with the refused address, and each operation the app's write
/// journaled, running or finished with its outcome. The refused invite
/// leaves `coven_invites`.
#[test]
fn recorded_operations_list_their_outcomes() {
    let connection = outside_store();
    let refused =
        super::record_invite(&connection, invite(1, crate::RecordedInviteStage::Granting)).unwrap();
    super::refuse_invite(
        &connection,
        refused,
        "dan@example.com",
        crate::SharingRefusal::TooManyMembers,
    )
    .unwrap();
    let rename = || crate::StoreChange::RenameStore { name: "Household".into() };
    let renamed = super::record_change(&connection, rename()).unwrap();
    super::finish_operation(&connection, renamed, crate::ChangeOutcome::Satisfied).unwrap();
    let running = super::record_change(&connection, rename()).unwrap();

    let listed: Vec<(i64, String, Option<String>, Option<String>)> = connection
        .prepare(
            "SELECT operation,kind,outcome_kind,account FROM coven_operations ORDER BY operation",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        listed,
        [
            (
                refused.0,
                "Invite".to_owned(),
                Some("sharing_refused".to_owned()),
                Some("dan@example.com".to_owned())
            ),
            (renamed.0, "RenameStore".to_owned(), Some("satisfied".to_owned()), None),
            (running.0, "RenameStore".to_owned(), None, None),
        ]
    );
    let invites: i64 =
        connection.query_row("SELECT count(*) FROM coven_invites", [], |r| r.get(0)).unwrap();
    assert_eq!(invites, 0);
}

/// A test ends an operation only as coven could: an outcome outside the
/// outcome type of the operation's kind fails, and the operation runs on.
#[test]
fn a_recorded_operation_refuses_an_outcome_its_kind_never_ends_with() {
    let connection = outside_store();
    let rename = crate::StoreChange::RenameStore { name: "Household".into() };
    let renamed = super::record_change(&connection, rename).unwrap();
    let cancelled =
        super::finish_operation(&connection, renamed, crate::StorageSetupOutcome::Cancelled);
    assert!(
        matches!(cancelled, Err(crate::CovenError::Database(crate::DbError::Invariant { .. }))),
        "{cancelled:?}"
    );
    let outcome: Option<Vec<u8>> = connection
        .query_row("SELECT outcome FROM coven_operations WHERE operation=?1", [renamed.0], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(outcome, None);
}

/// A pending record a test records outside a store reaches `coven_pending`
/// as coven's triggers keep it in a store, with its operation and kinds,
/// and an app's trigger on `coven_pending` that reads `coven_user_files`
/// fires on it.
#[test]
fn a_recorded_pending_record_fires_an_app_trigger_outside_a_store() {
    let connection = outside_store();
    connection
        .execute_batch(
            "CREATE TABLE heard(operation INTEGER, subject_kind TEXT, reason_kind TEXT,
                 originals INTEGER);
             CREATE TRIGGER heard_record AFTER INSERT ON coven_pending BEGIN
                 INSERT INTO heard SELECT NEW.operation,NEW.subject_kind,NEW.reason_kind,
                     count(*) FROM coven_user_files;
             END;",
        )
        .unwrap();
    let stage = || crate::RecordedInviteStage::Granting;
    let operations: Vec<crate::OperationId> =
        (1..=3).map(|n| super::record_invite(&connection, invite(n, stage())).unwrap()).collect();
    let missing = crate::ObjectPath::join_request(
        crate::StoreId(uuid::Uuid::from_bytes([9; 16])),
        crate::DeviceId(7),
        crate::InviteId(uuid::Uuid::from_bytes([2; 16])),
    );
    for (operation, reason) in operations.iter().zip([
        crate::PendingReason::NoStorage,
        crate::PendingReason::Missing { path: missing },
        crate::PendingReason::Paused,
    ]) {
        let record = crate::PendingRecord {
            subject: crate::PendingSubject::Operation {
                id: *operation,
                kind: crate::OperationKind::Invite,
            },
            reason,
            cause: None,
        };
        super::record_pending(&connection, &record).unwrap();
    }

    let heard: Vec<(i64, String, String, i64)> = connection
        .prepare("SELECT * FROM heard ORDER BY rowid")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let kinds = ["no_storage", "missing", "paused"];
    let expected: Vec<(i64, String, String, i64)> = operations
        .iter()
        .zip(kinds)
        .map(|(operation, kind)| (operation.0, "operation".to_owned(), kind.to_owned(), 0))
        .collect();
    assert_eq!(heard, expected);
}

/// The state `coven_sync_state` shows outside a store, and the kinds of the
/// pending records beside it.
fn shown(connection: &coven_database::test_utils::Connection) -> (String, Vec<String>) {
    let state =
        connection.query_row("SELECT state FROM coven_sync_state", [], |r| r.get(0)).unwrap();
    let kinds = connection
        .prepare("SELECT reason_kind FROM coven_pending ORDER BY position")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    (state, kinds)
}

/// A last pass and a pending record that a test saves outside a store give
/// the sync state coven computes from them: a failed operation, a removed
/// device and replaced member keys.
#[test]
fn a_recorded_last_pass_and_pending_record_give_each_pending_state() {
    // An operation whose last attempt failed.
    let connection = connected_store();
    let operation =
        super::record_invite(&connection, invite(1, crate::RecordedInviteStage::Granting)).unwrap();
    let record = crate::PendingRecord::new(
        crate::PendingSubject::Operation { id: operation, kind: crate::OperationKind::Invite },
        crate::PendingReason::NoStorage,
    );
    super::record_pending(&connection, &record).unwrap();
    let pass = crate::LastPass::failed(crate::SyncFailure::Operation(Box::new(record)));
    super::record_last_pass(&connection, &pass).unwrap();
    assert_eq!(shown(&connection), ("failed".to_owned(), vec!["no_storage".to_owned()]));

    // A device the store no longer holds.
    let connection = connected_store();
    let removed = crate::PendingRecord::new(
        crate::PendingSubject::Connection,
        crate::PendingReason::Removed(crate::Removal::Device),
    );
    super::record_pending(&connection, &removed).unwrap();
    let pass = crate::LastPass::failed(crate::SyncFailure::Removed(crate::Removal::Device));
    super::record_last_pass(&connection, &pass).unwrap();
    assert_eq!(shown(&connection), ("removed".to_owned(), vec!["removed".to_owned()]));

    // A member whose keys another installation replaced.
    let connection = connected_store();
    let member = crate::MemberId(uuid::Uuid::from_bytes([5; 16]));
    let replaced = crate::PendingRecord::new(
        crate::PendingSubject::Slot(crate::Slot::new(3).unwrap()),
        crate::PendingReason::MemberKeysReplaced {
            member,
            by: Some(crate::ReplacingDevice {
                device: crate::NamedDevice {
                    id: coven_foundation::id_source::DeviceId(9),
                    name: "Ana's phone".into(),
                },
                member,
            }),
        },
    );
    super::record_pending(&connection, &replaced).unwrap();
    assert_eq!(
        shown(&connection),
        ("needs_reset".to_owned(), vec!["member_keys_replaced".to_owned()])
    );
}

/// The text a build script reads is the database crate's, which makes the
/// views the sync state reads.
#[test]
fn the_sql_text_names_the_views_an_app_reads() {
    let sql = super::readable_objects_sql();
    assert!(sql.contains("coven_sync_state"), "{sql}");
}
