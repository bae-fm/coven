use crate::file_write::tests::{namespaces, tables, SCHEMA};
use crate::tests::TestStore;
use crate::{
    Database, DbError, FileDatabase, FileWork, Migration, MoveId, PendingValue, Provenance,
};
use coven_foundation::id_source::{DeviceId, FileId, ObjectId};
use serde::{Deserialize, Serialize};

/// A pending record about an upload's stored bytes or about a move, with
/// the cause written beside it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Record {
    about: About,
    #[serde(skip)]
    cause: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
enum About {
    Upload(DeviceId, ObjectId),
    Move(MoveId),
}

impl PendingValue for Record {
    fn writes_refused(_: crate::Slot, _: crate::Slot, _: crate::WriteCutoff) -> Self {
        unreachable!("no slot in this test")
    }
    fn reload(_: coven_merge::Audience) -> Self {
        unreachable!("no audience in this test")
    }
    fn provider_access(_: crate::OperationId, _: String, _: bool) -> Self {
        unreachable!("no operation in this test")
    }
    type Subject = About;
    fn subject(&self) -> &About {
        &self.about
    }
    fn operation(&self) -> Option<crate::OperationId> {
        None
    }
    fn object(&self) -> Option<(FileWork, DeviceId, ObjectId)> {
        match self.about {
            About::Upload(device, object) => Some((FileWork::Upload, device, object)),
            About::Move(_) => None,
        }
    }
    fn move_of(&self) -> Option<MoveId> {
        match self.about {
            About::Move(id) => Some(id),
            About::Upload(..) => None,
        }
    }
    fn pin_of(&self) -> Option<FileId> {
        None
    }
    fn slot(&self) -> Option<crate::Slot> {
        None
    }
    fn ends_when_covered(&self) -> bool {
        false
    }
    const BUILD: u128 = 1;
    fn can_retry(&self, _: u128, _: u32) -> bool {
        false
    }
    fn cause(&self) -> Option<&str> {
        self.cause.as_deref()
    }
    fn with_cause(self, cause: Option<String>) -> Self {
        Self { cause, ..self }
    }
}

/// The app's local log of what coven does to `coven_pending`, kept by its
/// triggers on the table.
const FOLLOWING: &str = "CREATE TABLE heard(n INTEGER PRIMARY KEY, change TEXT NOT NULL,
         position INTEGER NOT NULL, file TEXT, cause TEXT, move INTEGER);
     CREATE TRIGGER record_arose AFTER INSERT ON coven_pending BEGIN
         INSERT INTO heard(change,position,file,cause,move)
         VALUES('arose',NEW.position,NEW.file,NEW.cause,NEW.move);
     END;
     CREATE TRIGGER record_changed AFTER UPDATE ON coven_pending BEGIN
         INSERT INTO heard(change,position,file,cause,move)
         VALUES('changed',NEW.position,NEW.file,NEW.cause,NEW.move);
     END;
     CREATE TRIGGER record_ended AFTER DELETE ON coven_pending BEGIN
         INSERT INTO heard(change,position,file,cause,move)
         VALUES('ended',OLD.position,OLD.file,OLD.cause,OLD.move);
     END;";

/// Stage `bytes` for the cloud and point row `id` of `files` at them.
async fn stage(db: &Database, id: &'static str, bytes: &'static [u8]) -> FileId {
    db.staged_write::<_, _, _, _, DbError>(
        move |batch| Ok(batch.put_file("files", bytes.to_vec(), crate::Place::Cloud)),
        move |sql, file| {
            sql.execute("INSERT INTO files(id,file) VALUES(?1,?2)", (id, file))?;
            Ok(file)
        },
    )
    .await
    .unwrap()
}

fn save(db: &Database, record: Record) {
    db.inspect_writer(|sql, _| crate::pending::save(sql, &record).unwrap());
}

fn clear(db: &Database, about: About) {
    db.inspect_writer(|sql, _| crate::pending::clear(sql, &about).unwrap());
}

/// One change the app's triggers heard: what happened, and the row's
/// position, file, cause and move.
type Heard = (String, i64, Option<FileId>, Option<String>, Option<MoveId>);

/// What the app's triggers heard since the last call, in order; the rows
/// `coven_pending` shows now must be what its source says.
fn heard(db: &Database) -> Vec<Heard> {
    db.inspect_writer(|sql, _| {
        let heard = sql
            .query("SELECT change,position,file,cause,move FROM heard ORDER BY n", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
            })
            .unwrap();
        sql.batch("DELETE FROM heard").unwrap();
        let differ: i64 = sql
            .query_row(
                "SELECT (SELECT count(*) FROM (SELECT * FROM coven_pending
                            EXCEPT SELECT * FROM _coven_pending_source))
                      + (SELECT count(*) FROM (SELECT * FROM _coven_pending_source
                            EXCEPT SELECT * FROM coven_pending))",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(differ, 0, "coven_pending shows what its source says");
        heard
    })
}

fn step(
    change: &str,
    position: i64,
    file: Option<FileId>,
    cause: Option<&str>,
    of: Option<MoveId>,
) -> Heard {
    (change.to_owned(), position, file, cause.map(str::to_owned), of)
}

/// A local trigger on `coven_pending` runs when coven saves a record, when
/// the record's columns change, when the record comes to name a file or
/// stops naming one, and when coven clears the record, each in the statement
/// that changes the record or its files. A change that leaves a row as it
/// was, such as an upload switching its file to the bytes it stored, fires
/// no trigger (§4, §9.4).
#[tokio::test]
async fn a_local_trigger_follows_each_record_coven_writes_changes_and_ends() {
    let store = TestStore::new();
    let db = store
        .builder(
            tables(),
            vec![
                Migration::addition(1, "files", SCHEMA),
                Migration::addition(2, "following", FOLLOWING),
            ],
        )
        .namespaces(namespaces(Provenance::AppProvided))
        .open()
        .await
        .unwrap();
    let owner = FileDatabase::new(db.clone());
    let a = stage(&db, "a", b"take").await;
    let upload = owner.uploads().await.unwrap().remove(0);
    let bytes = About::Upload(upload.location.uploader(), upload.location.object());
    save(&db, Record { about: bytes.clone(), cause: Some("disk full".into()) });
    assert_eq!(heard(&db), [step("arose", 1, Some(a), Some("disk full"), None)]);
    save(&db, Record { about: bytes.clone(), cause: Some("still full".into()) });
    assert_eq!(heard(&db), [step("changed", 1, Some(a), Some("still full"), None)]);
    // Saving the same record with the same cause changes nothing.
    save(&db, Record { about: bytes.clone(), cause: Some("still full".into()) });
    assert_eq!(heard(&db), []);
    save(&db, Record { about: About::Move(upload.of), cause: None });
    assert_eq!(heard(&db), [step("arose", 2, None, None, Some(upload.of))]);
    // The upload's file becomes the stored file over the same bytes, and
    // the switch ends the move and its record: the bytes' record still
    // names the file, and its row stays as it was.
    owner.complete_upload(upload.id, ()).await.unwrap();
    assert_eq!(heard(&db), [step("ended", 2, None, None, Some(upload.of))]);
    // A record with no file left shows one row with none.
    db.write(|sql| {
        sql.execute("DELETE FROM files WHERE id='a'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(
        heard(&db),
        [
            step("ended", 1, Some(a), Some("still full"), None),
            step("arose", 1, None, Some("still full"), None)
        ]
    );
    clear(&db, bytes);
    assert_eq!(heard(&db), [step("ended", 1, None, Some("still full"), None)]);
    db.close().await.unwrap();
}

/// A trigger on `coven_pending` is the app's local trigger, as one on
/// `coven_moves` is: it runs after the change and writes only local tables.
/// Coven refuses any other as the migration declaring it ends, and the app
/// still can't write the table (§9.4).
#[tokio::test]
async fn coven_refuses_a_pending_trigger_that_could_break_its_rules() {
    for (trigger, expected) in [
        (
            "CREATE TRIGGER skip BEFORE INSERT ON coven_pending BEGIN SELECT RAISE(IGNORE); END",
            DbError::Schema(crate::SchemaError::TriggerTiming {
                trigger: "skip".into(),
                table: "coven_pending".into(),
            }),
        ),
        (
            "CREATE TRIGGER title AFTER INSERT ON coven_pending BEGIN UPDATE files SET title='stuck'; END",
            DbError::TriggerTarget { trigger: "title".into(), table: "files".into() },
        ),
    ] {
        let store = TestStore::new();
        let error = store
            .builder(
                tables(),
                vec![Migration::addition(1, "files", SCHEMA), Migration::addition(2, "follow", trigger)],
            )
            .open()
            .await
            .err()
            .expect("coven refuses the trigger");
        let error = crate::tests::database_error(error);
        assert_eq!(error.to_string(), expected.to_string(), "{trigger}");
    }
    let store = TestStore::new();
    let db = crate::file_write::tests::open(&store, Provenance::AppProvided).await;
    for statement in
        ["INSERT INTO coven_pending(record,position) VALUES(x'',1)", "DELETE FROM coven_pending"]
    {
        let error = crate::write::tests::sql(&db, statement).await.unwrap_err();
        assert!(
            matches!(&error, DbError::InternalTable { table } if table == "coven_pending"),
            "{statement}: {error:?}"
        );
    }
    db.close().await.unwrap();
}
