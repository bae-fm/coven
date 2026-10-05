use super::*;

#[test]
fn shared_guard_is_a_required_conjunct_not_text_anywhere() {
    for predicate in [
        "NOT coven_applying()",
        "not /* comment */ coven_applying ( )",
        "((NOT (coven_applying())))",
        "NOT coven_applying() AND (new.n = 1 OR new.n = 2)",
        "new.n = 1 AND NOT coven_applying()",
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
        tokens("/* a */ \"WHEN\" 'it''s' [BEGIN] -- b\n`OR`"),
        vec![
            Token::Quoted("WHEN".into()),
            Token::String("it's".into()),
            Token::Quoted("BEGIN".into()),
            Token::Quoted("OR".into())
        ]
    );
    assert_eq!(identifier("a\"b"), "\"a\"\"b\"");
}
