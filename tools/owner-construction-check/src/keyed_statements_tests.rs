use super::*;
use crate::crate_code::checked;
use crate::syntax::RustFile;
use owner_construction_policy::Policy;

fn policy() -> Policy {
    Policy::fixture(
        r#"
        rules = ["read-shapes"]
        test_support_features = ["test-utils"]
        [read_shapes]
        statement_rows = []
        column_values = []
        read_types = ["SqlReadContext"]
        plumbing = []
        rule = "the read rule"
        "#,
    )
}

const HELPERS: &str = "
    struct Detail { album_id: String }
    fn load_detail(sql: &SqlReadContext<'_>, id: u8) -> Result<Detail, Error> { todo!() }
    fn load_album(sql: &SqlReadContext<'_>, id: &str) -> Result<u8, Error> { todo!() }
    fn shown(detail: &Detail) {}
";

/// Returns the line and message of each keyed statement in a read function
/// with this body.
fn keyed(body: &str) -> Vec<(usize, String)> {
    let source =
        format!("{HELPERS}\nfn read(sql: &SqlReadContext<'_>, request: Request) {{\n{body}\n}}");
    let files = [RustFile::fixture("crates/app/src/a.rs", &source)];
    let policy = policy();
    let files = checked(&files, &policy);
    let read = ReadCode::collect(&files, &policy, policy.read_shapes.as_ref().expect("rows"));
    keyed_statements(&read)
        .into_iter()
        .map(|hit| {
            assert_eq!(hit.shape.id(), "keyed-statement");
            assert_eq!(hit.item, "read");
            (hit.line - 7, hit.what)
        })
        .collect()
}

#[test]
fn a_statement_keyed_by_what_an_earlier_one_returned_is_found() {
    assert_eq!(
        keyed(
            "let detail = load_detail(sql, 1)?;
             let album = load_album(sql, &detail.album_id)?;"
        ),
        [(2, "runs `load_album`, which runs SQL, keyed by what `detail` holds".to_string())]
    );
}

#[test]
fn a_key_carried_through_a_binding_a_sql_call_or_a_nested_statement_is_found() {
    let found = keyed(
        "let detail = load_detail(sql, 1)?;
         let id = detail.album_id.clone();
         load_album(sql, &id)?;
         let name: String = sql.query_row(\"SELECT name FROM a\", [], |r| r.get(0))?;
         sql.query_row(\"SELECT id FROM b WHERE name = ?\", [&name], |r| r.get(0))?;
         load_album(sql, &load_detail(sql, 2)?.album_id)?;
         if let Ok(found) = load_detail(sql, 3) { load_album(sql, &found.album_id)?; }
         match detail.album_id.as_str() { \"\" => {}, album => { load_album(sql, album)?; } }",
    );
    let found = found.iter().map(|(line, what)| (*line, what.as_str())).collect::<Vec<_>>();
    assert_eq!(
        found,
        [
            (3, "runs `load_album`, which runs SQL, keyed by what `id` holds"),
            (5, "runs `query_row`, which runs SQL, keyed by what `name` holds"),
            (6, "runs `load_album`, which runs SQL, keyed by what `load_detail` returns"),
            (7, "runs `load_album`, which runs SQL, keyed by what `found` holds"),
            (8, "runs `load_album`, which runs SQL, keyed by what `album` holds"),
        ]
    );
}

#[test]
fn independent_statements_a_prepared_statement_a_row_mapper_a_named_argument_and_a_statement_per_row_are_not_keyed(
) {
    assert_eq!(
        keyed(
            "let detail = load_detail(sql, 1)?;
             let album = load_album(sql, &request.id)?;
             shown(&detail);
             let mut statement = sql.prepare(\"SELECT 1\")?;
             statement.query_row([request.id], |r| r.get(0))?;
             for id in ids { load_album(sql, &detail.album_id)?; }
             let all = ids.iter().map(|id| load_album(sql, &detail.album_id));
             match request.id { Some(id) => { load_album(sql, &id)?; } None => {} }
             sql.query_row(\"SELECT 2\", [], |r| shown(&detail))?;
             sql.query_row(&format!(\"SELECT {detail}\", detail = request.columns), [], f)?;"
        ),
        []
    );
}

#[test]
fn a_statement_keyed_by_what_a_read_function_value_returned_is_found() {
    let source = format!(
        "{HELPERS}
        type LanguageRead = fn(&SqlReadContext<'_>) -> Result<u8, Error>;
        type Shown = fn(&Detail) -> u8;
        fn list(sql: &SqlReadContext<'_>, language: LanguageRead, shown: Shown, detail: Detail) {{
            let language = language(sql)?;
            load_album(sql, &language)?;
            let count = shown(&detail);
            load_album(sql, &count)?;
        }}"
    );
    let files = [RustFile::fixture("crates/app/src/a.rs", &source)];
    let policy = policy();
    let files = checked(&files, &policy);
    let read = ReadCode::collect(&files, &policy, policy.read_shapes.as_ref().expect("rows"));
    let found =
        keyed_statements(&read).into_iter().map(|hit| (hit.item, hit.what)).collect::<Vec<_>>();
    assert_eq!(
        found,
        [(
            "list".to_string(),
            "runs `load_album`, which runs SQL, keyed by what `language` holds".to_string()
        )]
    );
}
