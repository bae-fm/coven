use super::{outline, ItemKind};
use crate::compiler::lex::tokens;

fn core(sql: &str) -> super::Core {
    outline(sql, &tokens(sql))
}

#[test]
fn result_columns_are_classified() {
    let sql = "SELECT i.id AS issue_id, title, p.*, *, (SELECT 1) AS one, upper(name) AS shout FROM issues AS i";
    let kinds: Vec<_> = core(sql).items.into_iter().map(|item| (item.kind, item.alias)).collect();
    assert_eq!(
        kinds,
        [
            (
                ItemKind::Column { qualifier: Some("i".into()), name: "id".into() },
                Some("issue_id".into())
            ),
            (ItemKind::Column { qualifier: None, name: "title".into() }, None),
            (ItemKind::QualifiedStar("p".into()), None),
            (ItemKind::Star, None),
            (ItemKind::Subquery, Some("one".into())),
            (ItemKind::Expression, Some("shout".into())),
        ]
    );
}

#[test]
fn from_items_know_their_alias_and_table() {
    let sql = "WITH c AS (SELECT 1 AS x) SELECT 1 FROM issues i LEFT JOIN (SELECT 1) AS sub ON 1 JOIN people AS p USING (id), c";
    let from: Vec<_> = core(sql)
        .from
        .iter()
        .map(|item| (item.qualifier.clone(), item.table().map(str::to_owned)))
        .collect();
    assert_eq!(
        from,
        [
            (Some("i".into()), Some("issues".into())),
            (Some("sub".into()), None),
            (Some("p".into()), Some("people".into())),
            (Some("c".into()), Some("c".into())),
        ]
    );
}

#[test]
fn aggregates_count_outside_subqueries_and_windows() {
    assert!(core("SELECT count(*) AS n FROM t").aggregates);
    assert!(core("SELECT json_group_array(x) AS n FROM t").aggregates);
    assert!(core("SELECT max(a) FROM t").aggregates);
    assert!(!core("SELECT max(a, b) FROM t").aggregates);
    assert!(!core("SELECT (SELECT count(*) FROM u) AS n FROM t").aggregates);
    assert!(!core("SELECT count(*) OVER () AS n FROM t").aggregates);
}

#[test]
fn group_by_having_and_compounds_are_seen() {
    assert!(core("SELECT count(*) FROM t GROUP BY a").groups);
    assert!(core("SELECT count(*) FROM t HAVING count(*) > 0").groups);
    assert!(!core("SELECT count(*) FROM t WHERE a IN (SELECT a FROM u GROUP BY a)").groups);
    assert!(core("SELECT 1 UNION ALL SELECT 2").compound);
}
