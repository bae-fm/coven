use super::{outline, ItemKind, Join};
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
fn from_items_know_their_alias_table_and_join() {
    let sql = "WITH c AS (SELECT 1 AS x) SELECT 1 FROM issues i LEFT JOIN (SELECT 1) AS sub ON 1 JOIN people AS p USING (id), c";
    let from: Vec<_> = core(sql)
        .from
        .iter()
        .map(|item| (item.qualifier.clone(), item.table().map(str::to_owned), item.join))
        .collect();
    assert_eq!(
        from,
        [
            (Some("i".into()), Some("issues".into()), Join::Inner),
            (Some("sub".into()), None, Join::Left),
            (Some("p".into()), Some("people".into()), Join::Inner),
            (Some("c".into()), Some("c".into()), Join::Inner),
        ]
    );
}

#[test]
fn right_and_full_joins_are_kept() {
    let from = core("SELECT 1 FROM a RIGHT JOIN b ON 1 FULL OUTER JOIN c ON 1").from;
    assert_eq!(
        from.iter().map(|item| item.join).collect::<Vec<_>>(),
        [Join::Inner, Join::Right, Join::Full]
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
