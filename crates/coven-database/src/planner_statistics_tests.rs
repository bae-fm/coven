use crate::tests::TestStore;
use coven_foundation::clock::HANG_GUARD;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

impl super::UpkeepThread<super::PlannerStatistics> {
    /// Each run from now on, told once it committed.
    pub(crate) fn report_runs(&self) -> UnboundedReceiver<()> {
        let (runs, reported) = unbounded_channel();
        *self.upkeep().runs.lock().unwrap() = Some(runs);
        reported
    }

    /// Visit the thread's connection, after any check or run in progress.
    pub(crate) fn with_connection(
        &self,
        visit: impl FnOnce(&crate::sqlite::DatabaseConnection) -> Result<(), crate::DbError>,
    ) -> Result<(), crate::DbError> {
        visit(&self.upkeep().connection.lock().expect("statistics lock poisoned"))
    }
}

/// Scan candidates under a watched folder: every row of a scan holds the
/// same root, so only statistics tell SQLite that a file root finds a few
/// rows and the root all of them.
fn scan_candidates(store: &TestStore) -> crate::DatabaseBuilder {
    store.builder(
        Vec::new(),
        vec![crate::Migration::addition(
            1,
            "candidates",
            "CREATE TABLE candidates(root TEXT NOT NULL, path TEXT NOT NULL,
                 file_root TEXT NOT NULL, kind TEXT NOT NULL, PRIMARY KEY(root, path));
             CREATE INDEX candidates_file_root ON candidates(file_root)",
        )],
    )
}

/// How each reader plans the lookup of a file root's candidates at its
/// next read.
fn readers_candidate_plans(db: &crate::Database) -> Vec<String> {
    let plans = std::sync::Mutex::new(Vec::new());
    db.inspect_readers(|reader| {
        let plan = reader
            .read_snapshot(|sql| {
                Ok::<_, crate::DbError>(sql.query(
                    "EXPLAIN QUERY PLAN SELECT path, kind FROM candidates \
                     WHERE root=?1 AND file_root=?2 ORDER BY path",
                    ["root", "folder 7"],
                    |r| r.get::<_, String>(3),
                )?)
            })
            .unwrap();
        plans.lock().unwrap().push(plan.join("; "));
    });
    plans.into_inner().unwrap()
}

/// A library's first scan fills its candidates from empty in one session.
/// Coven gathers their statistics while the store stays open, so every
/// reader, read-only as each is, looks a file root's candidates up by
/// their index instead of reading the whole root in key order (§4).
#[tokio::test]
async fn a_table_that_fills_in_one_session_gets_statistics_before_close() {
    let store = TestStore::new();
    let db = scan_candidates(&store).open().await.unwrap();
    let mut runs = db.report_statistics_runs();
    let blind = "SEARCH candidates USING INDEX sqlite_autoindex_candidates_1 (root=?)";
    assert!(readers_candidate_plans(&db).iter().all(|plan| plan == blind));
    for batch in 0..20 {
        let insert = format!(
            "WITH RECURSIVE n(i) AS (VALUES({}) UNION ALL SELECT i+1 FROM n WHERE i<{})
             INSERT INTO candidates SELECT 'root', printf('path %05d', i),
                 printf('folder %d', i / 3), 'valid' FROM n",
            batch * 500,
            batch * 500 + 499
        );
        db.write(move |sql| {
            sql.execute_batch(&insert)?;
            Ok(())
        })
        .await
        .unwrap();
    }
    tokio::time::timeout(HANG_GUARD, runs.recv())
        .await
        .expect("the candidates' statistics are gathered")
        .expect("the store stays open");
    let counted: Option<String> = db.inspect_writer(|writer| {
        writer
            .query("SELECT stat FROM sqlite_stat1 WHERE idx='candidates_file_root'", [], |r| {
                r.get(0)
            })
            .unwrap()
            .pop()
    });
    assert!(counted.is_some(), "no statistics for the candidates");
    let indexed = "SEARCH candidates USING INDEX candidates_file_root (file_root=?)";
    for plan in readers_candidate_plans(&db) {
        assert!(plan.starts_with(indexed), "{plan}");
    }
    db.close().await.unwrap();
}
