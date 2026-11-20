use crate::author_judgement::tests::{deliver, member, value, Pair, ANA, BEN};
use crate::tests::{database_error, TestStore};
use crate::write::tests::{count, records, sql};
use crate::{DbError, RowAuthorizationFailure, RowIdentity, SyncedTable};
use coven_format::admin::MemberRole;

/// An admin-only `settings` table whose rows may point at a `groups` row any
/// member changes; a group's deletion cascades to its settings.
const SCHEMA: &str = "CREATE TABLE groups(id TEXT NOT NULL PRIMARY KEY);
CREATE TABLE settings(id TEXT NOT NULL PRIMARY KEY, grp TEXT REFERENCES groups(id) ON DELETE CASCADE, value TEXT);";

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("groups", RowIdentity::SharedKey),
        SyncedTable::new("settings", RowIdentity::SharedKey).admin_only(),
    ]
}

/// Give member `seed` `role` in `db`'s counted state, as an applied role
/// change would.
fn set_role(db: &crate::StoreDatabase, seed: u8, role: MemberRole) {
    db.inspect_writer(|c, _| {
        c.transaction(|c| {
            let mut state = crate::store_state_tables::read(c)?;
            state.members.get_mut(&member(seed)).expect("a registered member").role = role;
            crate::store_state_tables::replace(c, &state)
        })
        .unwrap();
    });
}

/// Ana's and Ben's devices, both counting Ana an admin and Ben a member.
async fn pair() -> Pair {
    let pair = Pair::with(tables(), SCHEMA).await;
    for db in [&pair.ana, &pair.ben] {
        set_role(db, ANA, MemberRole::Admin);
    }
    pair
}

fn not_admin(error: DbError, table: &str) {
    match error {
        DbError::NotAdmin { table: refused, .. } => assert_eq!(refused, table),
        error => panic!("expected NotAdmin on {table}, got {error:?}"),
    }
}

/// A member who isn't an admin can't insert, update or delete a row of an
/// admin-only table, whoever created it. The whole transaction rolls back.
/// An admin changes any of its rows, another admin's included (§9.6).
#[tokio::test]
async fn only_an_admin_changes_an_admin_only_table() {
    let pair = pair().await;
    let queued = records(&pair.ben).len();
    not_admin(
        sql(
            &pair.ben,
            "INSERT INTO groups VALUES('g'); INSERT INTO settings VALUES('rating',NULL,'stars')",
        )
        .await
        .unwrap_err(),
        "settings",
    );
    assert_eq!((count(&pair.ben, "groups"), records(&pair.ben).len()), (0, queued));
    sql(&pair.ana, "INSERT INTO settings VALUES('rating',NULL,'stars')").await.unwrap();
    deliver(&pair.ana, &pair.ben).await;
    assert_eq!(value(&pair.ben, "SELECT value FROM settings"), Some("stars".into()));
    for attempt in ["UPDATE settings SET value='hearts'", "DELETE FROM settings"] {
        not_admin(sql(&pair.ben, attempt).await.unwrap_err(), "settings");
    }
    assert_eq!(value(&pair.ben, "SELECT value FROM settings"), Some("stars".into()));
    // Once both devices count Ben an admin, he changes Ana's row.
    for db in [&pair.ana, &pair.ben] {
        set_role(db, BEN, MemberRole::Admin);
    }
    sql(&pair.ben, "UPDATE settings SET value='hearts'").await.unwrap();
    deliver(&pair.ben, &pair.ana).await;
    assert_eq!(value(&pair.ana, "SELECT value FROM settings"), Some("hearts".into()));
    assert!(pair.ana.lost_values().await.unwrap().is_empty());
    pair.close().await;
}

/// A receiver judges a write by the role its author has in the receiver's
/// counted state: a write Ana made as an admin is refused where she is no
/// longer one, as a lost value naming the reason, and her other changes in
/// the same write still apply.
#[tokio::test]
async fn a_receiver_refuses_a_change_by_an_author_it_counts_no_admin() {
    let pair = pair().await;
    sql(
        &pair.ana,
        "INSERT INTO groups VALUES('g'); INSERT INTO settings VALUES('rating',NULL,'stars')",
    )
    .await
    .unwrap();
    set_role(&pair.ben, ANA, MemberRole::Member);
    deliver(&pair.ana, &pair.ben).await;
    assert_eq!((count(&pair.ben, "groups"), count(&pair.ben, "settings")), (1, 0));
    let lost = pair.ben.lost_values().await.unwrap();
    assert_eq!(lost.len(), 1, "{lost:?}");
    assert_eq!(lost[0].table, "settings");
    assert!(matches!(
        lost[0].replaced_by,
        crate::Replacement::NotAuthorized { reason: RowAuthorizationFailure::NotAdmin, .. }
    ));
    pair.close().await;
}

/// A foreign-key action of a member's authorized change needs no permission
/// of its own: Ben deleting his group deletes Ana's setting that points at
/// it, on his device and on hers (§9.6).
#[tokio::test]
async fn a_members_cascade_reaches_an_admin_only_row() {
    let pair = pair().await;
    sql(&pair.ben, "INSERT INTO groups VALUES('g')").await.unwrap();
    deliver(&pair.ben, &pair.ana).await;
    sql(&pair.ana, "INSERT INTO settings VALUES('rating','g','stars')").await.unwrap();
    deliver(&pair.ana, &pair.ben).await;
    sql(&pair.ben, "DELETE FROM groups WHERE id='g'").await.unwrap();
    assert_eq!(count(&pair.ben, "settings"), 0);
    deliver(&pair.ben, &pair.ana).await;
    assert_eq!(count(&pair.ana, "settings"), 0);
    assert!(pair.ana.lost_values().await.unwrap().is_empty());
    pair.close().await;
}

/// Changing a table between owner-only, admin-only and neither is a
/// breaking schema change: the database refuses to open with another
/// write permission than the one it registered (§18.1).
#[tokio::test]
async fn a_write_permission_change_refuses_to_open() {
    let store = TestStore::new();
    store.schema(tables(), SCHEMA).await.unwrap().close().await.unwrap();
    let owner = vec![
        SyncedTable::new("groups", RowIdentity::SharedKey),
        SyncedTable::new("settings", RowIdentity::SharedKey).owner_only(),
    ];
    let anyone = vec![
        SyncedTable::new("groups", RowIdentity::SharedKey),
        SyncedTable::new("settings", RowIdentity::SharedKey),
    ];
    for declared in [owner, anyone] {
        let error = store.schema(declared, SCHEMA).await.err().expect("a changed permission");
        assert!(
            matches!(
                database_error(error),
                DbError::Schema(crate::SchemaError::WritePermissionChanged { ref table }) if table == "settings"
            ),
            "a changed write permission"
        );
    }
    store.schema(tables(), SCHEMA).await.unwrap().close().await.unwrap();
}

/// A demotion that lands before an admin's queued write refuses that write
/// on the admin's own device, as every receiver will, as a lost value; the
/// write still waits to go out.
#[tokio::test]
async fn a_demotion_refuses_the_demoted_admins_queued_write() {
    let store = TestStore::new();
    let db = store.schema(tables(), SCHEMA).await.unwrap();
    TestStore::register(&db, store.device(), ANA);
    set_role(&db, ANA, MemberRole::Admin);
    sql(&db, "INSERT INTO settings VALUES('rating',NULL,'stars')").await.unwrap();
    let queued = records(&db).len();
    let mut state = db.store_state().await.unwrap();
    state.members.get_mut(&member(ANA)).unwrap().role = MemberRole::Member;
    let demote = coven_format::admin::StoreChange::ChangeRole {
        member: member(ANA),
        role: MemberRole::Member,
    };
    db.admin_fixture(demote, state, None).await.unwrap();
    assert_eq!(count(&db, "settings"), 0);
    let lost = db.lost_values().await.unwrap();
    assert_eq!(lost.len(), 1, "{lost:?}");
    assert!(matches!(
        lost[0].replaced_by,
        crate::Replacement::NotAuthorized { reason: RowAuthorizationFailure::NotAdmin, .. }
    ));
    assert_eq!(records(&db).len(), queued);
    db.close().await.unwrap();
}
