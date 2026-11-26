use crate::tests::{TestDatabase, TestError, TestOptions, TestRead, HANG_GUARD};

/// Scan candidates under a watched folder: every row of a scan holds the
/// same root, so only statistics tell SQLite that a file root finds a few
/// rows and the root all of them.
fn scan_candidates() -> TestDatabase {
    TestDatabase::open_with(
        "CREATE TABLE candidates(root TEXT NOT NULL, path TEXT NOT NULL,
             file_root TEXT NOT NULL, kind TEXT NOT NULL, PRIMARY KEY(root, path));
         CREATE INDEX candidates_file_root ON candidates(file_root)",
        TestOptions { statistics: true, ..TestOptions::default() },
    )
}

/// How each reader plans the lookup of a file root's candidates at its
/// next read.
fn readers_candidate_plans(db: &TestDatabase) -> Vec<String> {
    let mut plans = Vec::new();
    db.with_database(|database| {
        database.each_reader(|reader| {
            let plan = reader
                .read_snapshot(|reader| {
                    Ok::<_, TestError>(TestRead(reader).query(
                        "EXPLAIN QUERY PLAN SELECT path, kind FROM candidates \
                         WHERE root=?1 AND file_root=?2 ORDER BY path",
                        ["root", "folder 7"],
                        |r| r.get::<_, String>(3),
                    )?)
                })
                .unwrap();
            plans.push(plan.join("; "));
        })
    });
    plans
}

/// A table that fills from empty in one session gets its statistics while
/// the database stays open, so every reader, read-only as each is, looks a
/// file root's candidates up by their index instead of reading the whole
/// root in key order (SPEC.md section 10).
#[tokio::test]
async fn a_table_that_fills_in_one_session_gets_statistics_before_close() {
    let db = scan_candidates();
    let mut runs = db.with_database(|database| database.report_statistics_runs());
    let blind = "SEARCH candidates USING INDEX sqlite_autoindex_candidates_1 (root=?)";
    assert!(readers_candidate_plans(&db).iter().all(|plan| plan == blind));
    for batch in 0..20 {
        db.write(&format!(
            "WITH RECURSIVE n(i) AS (VALUES({}) UNION ALL SELECT i+1 FROM n WHERE i<{})
             INSERT INTO candidates SELECT 'root', printf('path %05d', i),
                 printf('folder %d', i / 3), 'valid' FROM n",
            batch * 500,
            batch * 500 + 499
        ))
        .unwrap();
    }
    tokio::time::timeout(HANG_GUARD, runs.recv())
        .await
        .expect("the thread gathers the candidates' statistics")
        .expect("the database stays open");
    let counted: Option<String> = db.host(|writer| {
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
    db.close();
}
