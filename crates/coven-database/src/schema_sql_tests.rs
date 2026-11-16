use super::*;
use crate::sql::tokens;

#[test]
fn shared_guard_is_a_required_conjunct_not_text_anywhere() {
    for predicate in [
        "NOT coven_applying()",
        "not /* comment */ coven_applying ( )",
        "((NOT (coven_applying())))",
        "NOT coven_applying() AND (new.n = 1 OR new.n = 2)",
        "new.n = 1 AND NOT coven_applying()",
        "NOT \"CoVeN_Applying\"()",
        "NOT [coven_applying]()",
        "NOT `coven_applying`()",
        "new.n BETWEEN 1 AND 2 AND NOT coven_applying()",
        "CASE WHEN new.n THEN 1 ELSE 0 END AND NOT coven_applying()",
    ] {
        assert!(
            guarded_trigger(&format!(
                "CREATE TRIGGER t AFTER UPDATE ON x WHEN {predicate} BEGIN SELECT 1; END"
            )),
            "{predicate}"
        );
    }
    for sql in [
        "CREATE TRIGGER t AFTER UPDATE ON x /* WHEN NOT coven_applying() */ BEGIN SELECT 1; END",
        "CREATE TRIGGER t AFTER UPDATE ON x BEGIN SELECT 'WHEN NOT coven_applying()'; END",
        "CREATE TRIGGER t AFTER UPDATE ON x WHEN NOT coven_applying() OR 1 BEGIN SELECT 1; END",
        "CREATE TRIGGER t AFTER UPDATE ON x WHEN 'NOT coven_applying()' BEGIN SELECT 1; END",
        "CREATE TRIGGER t AFTER UPDATE ON x WHEN NOT coven_applying(1) BEGIN SELECT 1; END",
        "CREATE TRIGGER t AFTER UPDATE ON x WHEN NOT coven_applying() = 0 BEGIN SELECT 1; END",
        "CREATE TRIGGER t AFTER UPDATE ON x WHEN 0 BETWEEN 0 AND NOT coven_applying() BEGIN SELECT 1; END",
        "CREATE TRIGGER t AFTER UPDATE ON x WHEN CASE WHEN new.n THEN 1 ELSE 1 AND NOT coven_applying() END BEGIN SELECT 1; END",
    ] { assert!(!guarded_trigger(sql), "{sql}"); }
}

#[test]
fn table_comparison_splits_only_top_level_clauses() {
    let sql = "CREATE TABLE t (id TEXT PRIMARY KEY, value TEXT DEFAULT ('a,b'), CHECK (coalesce(value, 'x') != '')) WITHOUT ROWID;";
    let (parts, suffix) = table_parts(sql).unwrap();
    let expected: Vec<_> =
        ["id TEXT PRIMARY KEY", "value TEXT DEFAULT ('a,b')", "CHECK (coalesce(value, 'x') != '')"]
            .into_iter()
            .map(|part| tokens(part).unwrap())
            .collect();
    assert_eq!(parts, expected);
    assert_eq!(suffix, tokens("WITHOUT ROWID;").unwrap());
    assert!(table_parts("CREATE TABLE t AS SELECT 1").is_none());
}

#[test]
fn malformed_trigger_syntax_is_rejected() {
    assert!(!guarded_trigger("CREATE TRIGGER t WHEN NOT coven_applying() BEGIN"));
}
