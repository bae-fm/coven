use crate::file_write::tests::{complete_uploads, namespaces, stage, tables};
use crate::tests::{database_error, TestStore};
use crate::{DbError, Migration, Provenance, SchemaError};
use coven_foundation::id_source::FileId;

const FILES: &str = "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT,title TEXT)";

/// A local table of each change to a file's record, kept by the app's
/// triggers on `coven_files`: whether the change made, switched or deleted
/// the record, and whether its file was stored after it.
const FOLLOWED: &str = "CREATE TABLE file_changes(id INTEGER PRIMARY KEY,
         kind TEXT NOT NULL, file TEXT NOT NULL, stored INTEGER NOT NULL);
     CREATE TRIGGER file_made AFTER INSERT ON coven_files BEGIN
         INSERT INTO file_changes(kind,file,stored) VALUES('made',NEW.id,NEW.hash IS NOT NULL);
     END;
     CREATE TRIGGER file_switched AFTER UPDATE OF coordinates ON coven_files BEGIN
         INSERT INTO file_changes(kind,file,stored) VALUES('switched',NEW.id,NEW.hash IS NOT NULL);
     END;
     CREATE TRIGGER file_deleted AFTER DELETE ON coven_files BEGIN
         INSERT INTO file_changes(kind,file,stored) VALUES('deleted',OLD.id,OLD.hash IS NOT NULL);
     END;";

/// The files table and the triggers following `coven_files`, in the one
/// migration making the store's schema.
fn followed_from_the_start() -> Vec<Migration> {
    vec![Migration::addition_with(1, "files", |context| {
        context.execute_batch(FILES)?;
        context.execute_batch(FOLLOWED)?;
        Ok(())
    })]
}

async fn open(store: &TestStore, migrations: Vec<Migration>) -> crate::Database {
    store
        .builder(tables(), migrations)
        .namespaces(namespaces(Provenance::AppProvided))
        .open()
        .await
        .unwrap()
}

/// What the app's triggers recorded, in order.
type Changes = Vec<(String, FileId, bool)>;

fn changes(sql: crate::SqlReadContext<'_>) -> crate::CovenResult<Changes> {
    Ok(sql.query(
        "SELECT kind,file,stored FROM file_changes ORDER BY id",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?)
}

/// A local trigger on `coven_files` follows each record this device makes,
/// stores and deletes, in the write that does it, and a live query over
/// what it keeps reruns with each (§9.4, §17.2).
#[tokio::test]
async fn a_local_trigger_follows_each_record_this_device_changes() {
    let store = TestStore::new();
    let db = open(&store, followed_from_the_start()).await;
    let mut followed = db.subscribe(changes);
    assert!(followed.next().await.unwrap().unwrap().is_empty());
    let first = stage(&db, b"take".to_vec(), true).await.unwrap();
    let made = ("made".to_owned(), first, false);
    assert_eq!(
        followed.next().await.unwrap().unwrap(),
        std::slice::from_ref(&made)
    );
    // The upload's completion switches the record to its stored bytes.
    complete_uploads(&db).await;
    let stored = ("switched".to_owned(), first, true);
    assert_eq!(
        followed.next().await.unwrap().unwrap(),
        [made.clone(), stored.clone()]
    );
    // A newer file for row 7 leaves the first with no reference: the same
    // write makes the new record and deletes the old one.
    let second = stage(&db, b"retake".to_vec(), false).await.unwrap();
    assert_eq!(
        followed.next().await.unwrap().unwrap(),
        [
            made,
            stored,
            ("made".to_owned(), second, false),
            ("deleted".to_owned(), first, true),
        ]
    );
    db.close().await.unwrap();
}

/// Ana's device makes, stores and replaces row 7's file; Ben's applies each
/// of her writes as it arrives. His trigger sees each change to the records
/// in the transaction applying it, the upload's completion included, which
/// changes no app row there: it is how his app learns that her file is
/// stored (§9.4, §17.8).
#[tokio::test]
async fn a_local_trigger_follows_each_record_another_device_changes() {
    use coven_foundation::id_source::SequentialIds;
    let ids = SequentialIds::new();
    let (ana_store, ben_store) = (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
    let ana = open(&ana_store, followed_from_the_start()).await;
    let ben = open(&ben_store, followed_from_the_start()).await;
    let mut followed = ben.subscribe(changes);
    assert!(followed.next().await.unwrap().unwrap().is_empty());
    // Each of Ana's writes reaches its slot, and then Ben's device.
    let deliver = |delivered: usize| {
        let (ana, ben) = (ana.clone(), ben.clone());
        async move {
            let records = crate::write::tests::records(&ana);
            assert_eq!(records.len(), delivered + 1, "one more write of Ana's");
            let record = records[delivered].clone();
            ana.receive(record.clone().into()).await.unwrap();
            ben.receive(record.into()).await.unwrap();
        }
    };
    let first = stage(&ana, b"take".to_vec(), true).await.unwrap();
    deliver(0).await;
    let made = ("made".to_owned(), first, false);
    assert_eq!(
        followed.next().await.unwrap().unwrap(),
        std::slice::from_ref(&made)
    );
    complete_uploads(&ana).await;
    deliver(1).await;
    let stored = ("switched".to_owned(), first, true);
    assert_eq!(
        followed.next().await.unwrap().unwrap(),
        [made.clone(), stored.clone()]
    );
    let second = stage(&ana, b"retake".to_vec(), false).await.unwrap();
    deliver(2).await;
    let mut applied = followed.next().await.unwrap().unwrap();
    // One write makes the new record and deletes the old: Ben's device
    // applies both changes, in an order of its own.
    applied[2..].sort();
    let mut replaced = vec![
        ("deleted".to_owned(), first, true),
        ("made".to_owned(), second, false),
    ];
    replaced.sort();
    assert_eq!(applied, [vec![made, stored], replaced].concat());
    ana.close().await.unwrap();
    ben.close().await.unwrap();
}

/// A trigger on `coven_files` is the app's local trigger, as one on
/// `coven_pins` is: it runs after the change and writes only local tables,
/// and it reads only what app SQL may, since it also runs as coven applies
/// another device's write. Coven refuses any other as the migration
/// declaring it ends, and the app still can't write the table (§9.4).
#[tokio::test]
async fn coven_refuses_a_files_trigger_that_could_break_its_rules() {
    for (trigger, expected) in [
        (
            "CREATE TRIGGER skip BEFORE INSERT ON coven_files BEGIN SELECT RAISE(IGNORE); END",
            DbError::Schema(SchemaError::TriggerTiming {
                trigger: "skip".into(),
                table: "coven_files".into(),
            }),
        ),
        (
            "CREATE TRIGGER title AFTER UPDATE OF coordinates ON coven_files BEGIN
                 UPDATE files SET title='stored' WHERE file=NEW.id;
             END",
            DbError::TriggerTarget {
                trigger: "title".into(),
                table: "files".into(),
            },
        ),
        (
            "CREATE TRIGGER unpin AFTER DELETE ON coven_files BEGIN
                 DELETE FROM coven_pins WHERE file=OLD.id;
             END",
            DbError::InternalTable {
                table: "coven_pins".into(),
            },
        ),
        (
            "CREATE TABLE seen(rows INTEGER);
             CREATE TRIGGER count AFTER INSERT ON coven_files BEGIN
                 INSERT INTO seen SELECT count(*) FROM _coven_rows;
             END",
            DbError::InternalTable {
                table: "_coven_rows".into(),
            },
        ),
    ] {
        let store = TestStore::new();
        let error = store
            .builder(
                tables(),
                vec![
                    Migration::addition(1, "files", FILES),
                    Migration::addition(2, "follow", trigger),
                ],
            )
            .open()
            .await
            .err()
            .expect("the trigger is refused");
        let error = database_error(error);
        assert_eq!(error.to_string(), expected.to_string(), "{trigger}");
    }
    let store = TestStore::new();
    let db = open(&store, followed_from_the_start()).await;
    for statement in [
        "INSERT INTO coven_files(id,audience,size,coordinates,namespace) VALUES('f','',0,'','files')",
        "UPDATE coven_files SET coordinates=coordinates",
        "DELETE FROM coven_files",
    ] {
        let error = crate::write::tests::sql(&db, statement).await.unwrap_err();
        assert!(
            matches!(&error, DbError::InternalTable { table } if table == "coven_files"),
            "{statement}: {error:?}"
        );
    }
    db.close().await.unwrap();
}

/// The app's trigger on `coven_files` syncs nothing and changes no record,
/// so a migration making or dropping one changes only this device, as one
/// changing a local table does: declared an addition it runs, and declared
/// breaking it is refused (§18.1).
#[tokio::test]
async fn a_migration_following_coven_files_changes_only_this_device() {
    let store = TestStore::new();
    let first = vec![Migration::addition(1, "files", FILES)];
    open(&store, first.clone()).await.close().await.unwrap();
    let mut followed = first.clone();
    followed.push(Migration::addition(2, "follow", FOLLOWED));
    let db = open(&store, followed.clone()).await;
    stage(&db, b"take".to_vec(), true).await.unwrap();
    let recorded = db.read(changes).await.unwrap();
    assert_eq!(recorded.len(), 1);
    db.close().await.unwrap();
    let mut dropped = followed;
    dropped.push(Migration::addition(
        3,
        "unfollow",
        "DROP TRIGGER file_made; DROP TRIGGER file_switched; DROP TRIGGER file_deleted",
    ));
    let db = open(&store, dropped).await;
    complete_uploads(&db).await;
    assert_eq!(db.read(changes).await.unwrap(), recorded);
    db.close().await.unwrap();

    let store = TestStore::new();
    open(&store, first.clone()).await.close().await.unwrap();
    let mut breaking = first;
    breaking.push(Migration::breaking(
        2,
        "follow",
        FOLLOWED,
        crate::OldWrites::Drop,
    ));
    let error = store
        .builder(tables(), breaking)
        .namespaces(namespaces(Provenance::AppProvided))
        .open()
        .await
        .err()
        .expect("a migration following coven_files is no breaking change");
    assert!(
        matches!(
            database_error(error),
            DbError::MigrationKindMismatch { version: 2 }
        ),
        "a migration following coven_files declared breaking"
    );
}
