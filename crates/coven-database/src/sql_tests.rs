use super::*;

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
fn quoted_identifiers_and_strings_are_distinct_from_keywords() {
    assert_eq!(
        tokens("/* a */ \"WHEN\" 'it''s' [BEGIN] -- b\n`OR`").unwrap(),
        vec![
            Token::Quoted("WHEN".into()),
            Token::String("it's".into()),
            Token::Quoted("BEGIN".into()),
            Token::Quoted("OR".into())
        ]
    );
    assert_eq!(identifier("a\"b"), "\"a\"\"b\"");
}

#[test]
fn migration_tokens_preserve_literal_case_and_operator_boundaries() {
    assert_eq!(
        tokens("1.2e-3 != X'AB' || 0xAB").unwrap(),
        vec![
            Token::Word("1".into()),
            Token::Symbol('.'),
            Token::Word("2e".into()),
            Token::Symbol('-'),
            Token::Word("3".into()),
            Token::Symbol('!'),
            Token::Symbol('='),
            Token::Word("x".into()),
            Token::String("AB".into()),
            Token::Symbol('|'),
            Token::Symbol('|'),
            Token::Word("0xab".into()),
        ],
    );
    assert_eq!(tokens("X'AB'").unwrap(), tokens("x 'AB'").unwrap());
    assert_ne!(tokens("X'AB'").unwrap(), tokens("X'ab'").unwrap());
    assert_ne!(tokens("\"Name\"").unwrap(), tokens("\"name\"").unwrap());
    assert_eq!(name("\"a\"\"b\""), "a\"b");
    assert_eq!(name("`a``b`"), "a`b");
}

#[test]
fn table_comparison_splits_only_top_level_clauses() {
    let sql = "CREATE TABLE t (id TEXT PRIMARY KEY, value TEXT DEFAULT ('a,b'), CHECK (coalesce(value, 'x') != '')) WITHOUT ROWID;";
    let (parts, suffix) = table_parts(sql).unwrap();
    let expected: Vec<_> = [
        "id TEXT PRIMARY KEY",
        "value TEXT DEFAULT ('a,b')",
        "CHECK (coalesce(value, 'x') != '')",
    ]
    .into_iter()
    .map(|part| tokens(part).unwrap())
    .collect();
    assert_eq!(parts, expected);
    assert_eq!(suffix, tokens("WITHOUT ROWID;").unwrap());
    assert!(table_parts("CREATE TABLE t AS SELECT 1").is_none());
}

#[test]
fn source_ranges_keep_constraint_spelling() {
    let mut source =
        SchemaSource::new("CREATE TABLE t (id TEXT, CHECK /* a */ (id <> 'A,B')) STRICT");
    let body = source.body();
    let parts = body.parts();
    assert_eq!(parts[0].text(), "id TEXT");
    assert_eq!(parts[1].text(), " CHECK /* a */ (id <> 'A,B')");
    assert_eq!(source.tokens().collect::<Vec<_>>(), ["STRICT"]);
}

#[test]
fn malformed_tokens_and_trigger_syntax_are_rejected() {
    assert!(tokens("SELECT 'unterminated").is_err());
    assert!(!guarded_trigger(
        "CREATE TRIGGER t WHEN NOT coven_applying() BEGIN"
    ));
}

#[test]
fn token_source_accepts_sqlites_comments_and_parameter_names() {
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in [
        "SELECT 1 /* trailing comment",
        "SELECT $value::suffix(extra)",
        "SELECT $::value(a!b)",
    ] {
        sqlite.prepare(sql).unwrap();
        assert!(tokens(sql).is_ok(), "{sql}");
    }
}
