use super::{apply, tokens, Kind};

fn kinds(sql: &str) -> Vec<(Kind, &str)> {
    tokens(sql).into_iter().map(|token| (token.kind, &sql[token.range])).collect()
}

#[test]
fn words_names_literals_and_symbols_are_tokens() {
    assert_eq!(
        kinds("SELECT \"a b\", 'it''s' FROM t.x WHERE n >= 10.5"),
        [
            (Kind::Word, "SELECT"),
            (Kind::Quoted, "\"a b\""),
            (Kind::Symbol, ","),
            (Kind::Text, "'it''s'"),
            (Kind::Word, "FROM"),
            (Kind::Word, "t"),
            (Kind::Symbol, "."),
            (Kind::Word, "x"),
            (Kind::Word, "WHERE"),
            (Kind::Word, "n"),
            (Kind::Symbol, ">"),
            (Kind::Symbol, "="),
            (Kind::Number, "10.5"),
        ]
    );
}

#[test]
fn comments_and_the_text_of_strings_hide_what_they_hold() {
    let sql = "SELECT 1 -- :a @b\n, /* :c */ ':d @e', [@f g] FROM t";
    let parameters: Vec<_> = kinds(sql)
        .into_iter()
        .filter(|(kind, _)| matches!(kind, Kind::Parameter | Kind::Marker))
        .collect();
    assert_eq!(parameters, []);
}

#[test]
fn parameters_and_markers_keep_their_names() {
    assert_eq!(
        kinds("a = :issue AND b = @issues::Issue(parent: i) AND c = @parent.id"),
        [
            (Kind::Word, "a"),
            (Kind::Symbol, "="),
            (Kind::Parameter, ":issue"),
            (Kind::Word, "AND"),
            (Kind::Word, "b"),
            (Kind::Symbol, "="),
            (Kind::Marker, "@issues::Issue"),
            (Kind::Symbol, "("),
            (Kind::Word, "parent"),
            (Kind::Symbol, ":"),
            (Kind::Word, "i"),
            (Kind::Symbol, ")"),
            (Kind::Word, "AND"),
            (Kind::Word, "c"),
            (Kind::Symbol, "="),
            (Kind::Marker, "@parent"),
            (Kind::Symbol, "."),
            (Kind::Word, "id"),
        ]
    );
}

#[test]
fn a_quoted_name_unquotes_to_lowercase() {
    let sql = "\"Odd \"\"Name\"\" \" [Other] `Back`";
    let names: Vec<_> = tokens(sql).iter().map(|token| token.name(sql).unwrap()).collect();
    assert_eq!(names, ["odd \"name\" ", "other", "back"]);
}

#[test]
fn edits_replace_ranges_and_keep_the_rest() {
    let sql = "SELECT :a, :b";
    let parameters: Vec<_> =
        tokens(sql).into_iter().filter(|t| t.kind == Kind::Parameter).collect();
    let edits: Vec<_> =
        parameters.iter().map(|token| (token.range.clone(), ":x".to_owned())).collect();
    assert_eq!(apply(sql, &edits), "SELECT :x, :x");
}
