use super::*;
use crate::token_scan::scan;

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
fn scan_gives_each_token_its_kind_and_range() {
    let sql = "CREATE /* a */ TABLE t (x)";
    let ranges: Vec<_> = scan(sql).unwrap().into_iter().map(|(_, range)| &sql[range]).collect();
    assert_eq!(ranges, ["CREATE", "TABLE", "t", "(", "x", ")"]);
}

#[test]
fn malformed_tokens_are_rejected() {
    assert!(tokens("SELECT 'unterminated").is_err());
}

#[test]
fn token_source_accepts_sqlites_comments_and_parameter_names() {
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in
        ["SELECT 1 /* trailing comment", "SELECT $value::suffix(extra)", "SELECT $::value(a!b)"]
    {
        sqlite.prepare(sql).unwrap();
        assert!(tokens(sql).is_ok(), "{sql}");
    }
}
