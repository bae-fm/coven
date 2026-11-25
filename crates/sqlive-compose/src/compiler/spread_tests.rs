use super::{empty_relation, expand, spread_paths, with_ctes, without_parent, Registry};
use crate::compiler::header::{Parameter, Relation, RustType};
use crate::compiler::model::Record;

fn record(path: &str, sql: &str) -> Record {
    Record {
        path: path.into(),
        parameters: vec![],
        parent: None,
        relations: vec![],
        sql: sql.into(),
        width: 1,
        presence: Some(0),
        one_row: false,
    }
}

fn registry() -> Registry {
    let mut registry = Registry::new("tracker");
    registry.add(record("people::Person", "SELECT p.id AS person_id FROM people AS p"));
    registry.add(Record {
        parent: Some("issues".into()),
        sql: "SELECT count(*) AS unread FROM comments AS c WHERE c.issue_id = @parent.id AND c.author_id = :viewer".into(),
        parameters: vec![Parameter { name: "viewer".into(), ty: RustType("PersonId".into()) }],
        one_row: true,
        ..record("comments::UnreadCount", "")
    });
    registry.add(Record {
        relations: vec![Relation {
            name: "chosen".into(),
            columns: vec![("issue_id".into(), "TEXT".into())],
        }],
        sql: "SELECT chosen.issue_id FROM people__X__chosen AS chosen".into(),
        ..record("labels::LabelUse", "")
    });
    registry
}

#[test]
fn a_table_spread_becomes_a_subquery_and_keeps_its_alias() {
    let expansion = expand(
        &registry(),
        "Page",
        false,
        "SELECT a.* FROM @people::Person AS a JOIN @people::Person b ON 1",
    )
    .unwrap();
    assert_eq!(
        expansion.sql,
        "SELECT a.* FROM (SELECT p.id AS person_id FROM people AS p) AS a JOIN (SELECT p.id AS person_id FROM people AS p) b ON 1"
    );
    let aliases: Vec<_> = expansion.placed.iter().map(|placed| placed.alias.clone()).collect();
    assert_eq!(aliases, [Some("a".into()), Some("b".into())]);
}

#[test]
fn a_value_spread_binds_its_parameters_and_its_parent_row() {
    let expansion = expand(
        &registry(),
        "Page",
        false,
        "SELECT i.title, @comments::UnreadCount(parent: i, viewer: :me) AS unread FROM issues AS i",
    )
    .unwrap();
    assert_eq!(
        expansion.sql,
        "SELECT i.title, (SELECT count(*) AS unread FROM comments AS c WHERE c.issue_id = i.id AND c.author_id = :me) AS unread FROM issues AS i"
    );
    assert_eq!(expansion.placed[0].parent.as_deref(), Some("i"));
}

#[test]
fn a_relation_becomes_a_cte_of_the_subquery() {
    let expansion = expand(
        &registry(),
        "Page",
        false,
        "SELECT * FROM @labels::LabelUse(chosen: (SELECT id FROM issues)) AS used",
    )
    .unwrap();
    assert_eq!(
        expansion.sql,
        "SELECT * FROM (WITH labels__LabelUse__chosen (issue_id) AS (SELECT id FROM issues) SELECT chosen.issue_id FROM people__X__chosen AS chosen) AS used"
    );
}

#[test]
fn a_relation_may_be_another_spread() {
    let expansion = expand(
        &registry(),
        "Page",
        false,
        "SELECT * FROM @labels::LabelUse(chosen: @people::Person) AS used",
    )
    .unwrap();
    assert!(
        expansion
            .sql
            .contains("AS (SELECT p.id AS person_id FROM people AS p) SELECT chosen.issue_id"),
        "{}",
        expansion.sql
    );
}

#[test]
fn a_spread_that_binds_wrongly_fails() {
    let fail = |sql: &str| expand(&registry(), "Page", false, sql).err().unwrap().to_string();
    assert_eq!(
        fail("SELECT @comments::UnreadCount(parent: i) FROM issues AS i"),
        "the spread of comments::UnreadCount binds no value to its parameter viewer"
    );
    assert_eq!(
        fail("SELECT @comments::UnreadCount(viewer: :v) FROM issues AS i"),
        "the spread of comments::UnreadCount binds no parent row, and the fragment reads one"
    );
    assert_eq!(
        fail("SELECT * FROM @people::Person(parent: i) AS p"),
        "the spread of people::Person binds a parent row, and the fragment reads none"
    );
    assert_eq!(
        fail("SELECT @comments::UnreadCount(parent: i, viewer: :v, extra: :x) FROM issues AS i"),
        "the spread of comments::UnreadCount binds extra, which the fragment does not declare"
    );
    assert_eq!(
        fail("SELECT @comments::UnreadCount(parent: i, parent: i, viewer: :v) FROM issues AS i"),
        "the spread of comments::UnreadCount binds parent twice"
    );
    assert_eq!(
        fail("SELECT * FROM @labels::LabelUse AS u"),
        "the spread of labels::LabelUse binds no rows to its relation chosen"
    );
    assert_eq!(
        fail("SELECT * FROM @nobody::Nothing AS n"),
        "Page spreads nobody::Nothing, which is no fragment of this crate or of a dependency"
    );
    assert_eq!(
        fail("SELECT @x"),
        "Page writes @x: `@` marks a spread or a parent column, and it is no parameter"
    );
    assert_eq!(
        fail("SELECT @parent.id"),
        "Page writes @parent, and only a fragment with a parent row can"
    );
}

#[test]
fn crate_paths_name_the_crates_own_fragments() {
    let mut registry = Registry::new("my-app");
    registry.add(record("my_app::menu::Entry", "SELECT 1 AS x"));
    assert_eq!(registry.key("crate::menu::Entry"), "my_app::menu::Entry");
    assert_eq!(registry.code_path("my_app::menu::Entry"), "crate::menu::Entry");
    assert_eq!(registry.code_path("people::Person"), "people::Person");
    let expansion =
        expand(&registry, "Page", false, "SELECT e.* FROM @crate::menu::Entry AS e").unwrap();
    assert_eq!(expansion.sql, "SELECT e.* FROM (SELECT 1 AS x) AS e");
    assert_eq!(
        spread_paths("SELECT @crate::a::B, @parent.x, @c::D(p: :q)"),
        ["crate::a::B", "c::D"]
    );
}

#[test]
fn ctes_merge_in_front_of_a_fragments_own() {
    assert_eq!(with_ctes(&["a AS (SELECT 1)".into()], "SELECT 2"), "WITH a AS (SELECT 1) SELECT 2");
    assert_eq!(
        with_ctes(&["a AS (SELECT 1)".into()], "WITH b AS (SELECT 2) SELECT * FROM b"),
        "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM b"
    );
    assert_eq!(
        with_ctes(&["a AS (SELECT 1)".into()], "WITH RECURSIVE b AS (SELECT 2) SELECT * FROM b"),
        "WITH RECURSIVE a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM b"
    );
    assert_eq!(with_ctes(&[], " SELECT 2 "), "SELECT 2");
}

#[test]
fn a_parent_column_prepares_as_a_read_of_the_table_and_a_relation_as_no_rows() {
    assert_eq!(
        without_parent("SELECT 1 WHERE x = @parent.id AND y = @parent.\"odd col\"", "issues"),
        "SELECT 1 WHERE x = (SELECT id FROM issues WHERE 0) AND y = (SELECT \"odd col\" FROM issues WHERE 0)"
    );
    let relation = Relation {
        name: "chosen".into(),
        columns: vec![("a".into(), "TEXT".into()), ("b".into(), "INTEGER".into())],
    };
    assert_eq!(
        empty_relation("p__F", &relation),
        "p__F__chosen (a, b) AS (SELECT CAST(NULL AS TEXT), CAST(NULL AS INTEGER) WHERE 0)"
    );
}
