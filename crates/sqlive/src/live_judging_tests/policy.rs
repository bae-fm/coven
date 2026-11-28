//! A lookup can name a table the host's policy keeps from app SQL, which a
//! statement reads through a view the policy lets it read (SPEC.md section 7).

use std::sync::Arc;

use rusqlite::hooks::AuthAction;

use crate::tests::HeardBugs;
use crate::{
    Database, EngineFailure, Handle, HostSql, LiveQuery, Options, Policy, PolicyAction,
    ReconfigurableLiveQuery, Refresh,
};

/// The table the policy refused app SQL.
#[derive(Debug, thiserror::Error)]
#[error("app SQL reads {0} only through `shown`")]
struct Refused(String);

#[derive(Clone, Debug, thiserror::Error)]
#[error("the handle failed")]
struct Failed;

impl From<EngineFailure<Failed, Refused>> for Failed {
    fn from(_failure: EngineFailure<Failed, Refused>) -> Self {
        Self
    }
}

#[derive(Debug)]
struct Error(crate::Error<Failed, Refused>);

impl<T: Into<crate::Error<Failed, Refused>>> From<T> for Error {
    fn from(error: T) -> Self {
        Self(error.into())
    }
}

impl panic_as_error::CarriesBug for Error {
    fn bug(&self) -> Option<&panic_as_error::Bug> {
        match &self.0 {
            crate::Error::Bug(bug) | crate::Error::HandleFailed(bug) => Some(bug),
            _ => None,
        }
    }
}

/// App SQL reads `marks` and `bodies` only through the view `shown`.
struct ThroughTheView;

impl Policy for ThroughTheView {
    type Refusal = Refused;
    type Failure = Failed;
    type Scope = ();

    fn judge(&self, action: &PolicyAction<'_>) -> Result<(), Refused> {
        match action.action {
            AuthAction::Read { table_name: table @ ("marks" | "bodies"), .. }
                if action.owner != crate::Owner::Host && action.accessor != Some("shown") =>
            {
                Err(Refused(table.into()))
            }
            _ => Ok(()),
        }
    }

    fn check_sql(&self, _tokens: &[crate::sql::Token]) -> Result<(), Refused> {
        Ok(())
    }

    fn begin_app_call(&self) {}

    fn host_object(&self, _name: &str) -> bool {
        false
    }

    fn statement_key(&self) -> u64 {
        0
    }

    fn enter(&self, _scope: &()) {}

    fn leave(&self, _scope: &()) {}

    fn refresh(&self, _sql: &HostSql<Self>, _extent: Refresh) -> rusqlite::Result<()> {
        Ok(())
    }
}

const SCHEMA: &str = "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);
    CREATE TABLE marks(track TEXT NOT NULL PRIMARY KEY, body INTEGER NOT NULL);
    CREATE INDEX marks_body ON marks(body);
    CREATE TABLE bodies(id INTEGER PRIMARY KEY, text TEXT NOT NULL);
    CREATE VIEW shown AS SELECT m.track, b.text AS body
        FROM marks AS m JOIN bodies AS b ON b.id = m.body;
    INSERT INTO tracks VALUES ('a1','first'),('b1','second');
    INSERT INTO bodies VALUES (1,'loud')";

/// The titles of the tracks with a loud body, through the view.
const LOUD: &str = "SELECT t.title FROM shown AS s JOIN tracks AS t ON t.id = s.track
    WHERE s.body = 'loud'";

/// A change to one of the view's tables asks about the rows of the other in a
/// lookup that names it alone. The write commits, and the query runs again
/// only for a track with a loud body.
#[tokio::test]
async fn a_lookup_reads_a_table_app_sql_reads_only_through_a_view() {
    let directory = tempfile::tempdir().unwrap();
    let bugs = Arc::<HeardBugs>::default();
    let handle = Handle::<Failed>::recording_all(Arc::clone(&bugs) as panic_as_error::BugSinkRef);
    let (database, ()) = Database::open_with(
        &directory.path().join("policy.sqlite"),
        handle.clone(),
        Options {
            settings: crate::SqliteSettings::default(),
            readers: std::num::NonZeroUsize::MIN,
            prepare_on_reader: false,
            policy: &|| ThroughTheView,
            setup: &|_, _| Ok(()),
            capture: Some(Default::default()),
            checkpoint_pages: None,
            statistics: false,
        },
        |writer| writer.batch(SCHEMA),
    )
    .expect("the database opens");
    let database = Arc::new(database);
    let reading = Arc::clone(&database);
    let mut query = LiveQuery::new(ReconfigurableLiveQuery::new(
        (),
        Arc::new(move |(), completion| {
            let mut titles = Vec::new();
            let loud = crate::LiveStatement::app(LOUD, crate::Values::Positional(Vec::new()));
            reading.reader().host(|reader| {
                reader.live_read(
                    &mut |reads, lookups| completion.finish_judging(reads, lookups),
                    &[loud],
                    |_, row| {
                        titles.push(row.get::<_, String>(0)?);
                        Ok(())
                    },
                )
            })?;
            Ok::<_, Error>(titles)
        }),
        |error: crate::Error<Failed, Refused>| Error(error),
        &handle,
        bugs as panic_as_error::BugSinkRef,
    ));
    let write = |sql: &str| {
        database.host(|writer| {
            writer.write_with(
                |writer, _| {
                    let _write = writer.write_scope();
                    Ok::<_, crate::Error<Failed, Refused>>(writer.app_batch(sql)?)
                },
                |_, _, ()| Ok(()),
            )
        })
    };
    let host = |sql: &str| {
        database
            .host(|writer| {
                writer.transaction(|writer| -> Result<(), crate::Error<Failed, Refused>> {
                    writer.batch(sql)
                })
            })
            .expect("the host's write commits");
    };
    assert!(query.next_inline().await.unwrap().unwrap().is_empty());
    // Whether a loud body has the new mark's number: a lookup of `bodies`.
    host("INSERT INTO marks VALUES ('b1',2)");
    assert!(!query.is_marked_for_rerun());
    host("INSERT INTO marks VALUES ('a1',1)");
    assert_eq!(query.next_inline().await.unwrap().unwrap(), ["first"]);
    write("UPDATE tracks SET title = 'again' WHERE id = 'b1'").expect("the write commits");
    assert!(!query.is_marked_for_rerun());
    write("UPDATE tracks SET title = 'changed' WHERE id = 'a1'").expect("the write commits");
    assert_eq!(query.next_inline().await.unwrap().unwrap(), ["changed"]);
}
