use crate::compiler::test_tracker::{compile_lib, messages};

fn types_of(lib: &str, name: &str) -> Vec<String> {
    let output = compile_lib(lib).unwrap();
    let (_, code) = output.files.iter().find(|(file, _)| file == &format!("{name}.rs")).unwrap();
    let file = syn::parse_file(code).unwrap();
    let syn::Item::Struct(item) = &file.items[0] else { panic!("a struct comes first") };
    item.fields
        .iter()
        .map(|field| {
            let ty = &field.ty;
            format!("{}: {}", field.ident.as_ref().unwrap(), quote::quote!(#ty))
        })
        .collect()
}

#[test]
fn a_column_on_the_right_of_a_left_join_can_be_null() {
    let lib = r##"
sqlive_compose::fragment!(Assigned, r#"
    SELECT i.id, a.name, a.photo
    FROM issues AS i LEFT JOIN people AS a ON a.id = i.assignee_id
"#);
"##;
    assert_eq!(
        types_of(lib, "Assigned"),
        ["id: String", "name: Option < String >", "photo: Option < Vec < u8 > >"]
    );
}

#[test]
fn a_bare_column_is_placed_by_its_table() {
    let lib = r##"
sqlive_compose::fragment!(Assigned, r#"
    SELECT title, name
    FROM issues LEFT JOIN people ON people.id = issues.assignee_id
"#);
"##;
    assert_eq!(types_of(lib, "Assigned"), ["title: String", "name: Option < String >"]);
}

#[test]
fn a_star_takes_the_columns_the_other_items_leave() {
    let lib = r##"
sqlive_compose::fragment!(Starred, r#"
    SELECT j.*, i.title FROM projects AS j JOIN issues AS i ON i.project_id = j.id
"#);
"##;
    assert_eq!(types_of(lib, "Starred"), ["id: String", "name: String", "title: String"]);
    let errors = messages(compile_lib(
        r##"sqlive_compose::fragment!(Starred, r#"SELECT j.*, i.* FROM projects AS j JOIN issues AS i ON i.project_id = j.id"#);"##,
    ));
    assert_eq!(errors, ["src/lib.rs:1:1: Starred selects more than one `*`, so name the columns"]);
}

#[test]
fn a_scalar_subquery_can_be_null_and_a_declaration_may_say_so() {
    let lib = r##"
sqlive_compose::fragment!(Latest { latest: Option<i64> }, r#"
    SELECT i.id, (SELECT max(c.written_at) FROM comments AS c WHERE c.issue_id = i.id) AS latest
    FROM issues AS i
"#);
"##;
    assert_eq!(types_of(lib, "Latest"), ["id: String", "latest: Option < i64 >"]);
}

#[test]
fn two_columns_with_one_name_fail() {
    let output =
        compile_lib(r##"sqlive_compose::fragment!(Same { a: i64 }, r#"SELECT 1 AS a, 2 AS a"#);"##);
    assert_eq!(messages(output), ["src/lib.rs:1:1: Same returns the column a twice"]);
}

#[test]
fn a_bare_column_that_two_items_could_give_needs_a_declaration() {
    let lib = |declaration: &str| {
        format!(
            r##"sqlive_compose::fragment!(Twice {declaration}, r#"
    SELECT name FROM people AS a LEFT JOIN people AS b ON b.id = a.id
"#);"##
        )
    };
    assert_eq!(
        messages(compile_lib(&lib(""))),
        ["src/lib.rs:1:1: Twice names the column name with no table, and more than one FROM item could give it, so the header must declare name"]
    );
    assert_eq!(types_of(&lib("{ name: String }"), "Twice"), ["name: String"]);
}
