use super::rename;

#[test]
fn a_cte_and_its_reads_are_renamed_and_keep_their_qualifier() {
    let sql = "WITH named AS (SELECT i.id, i.title FROM issues AS i) SELECT named.id AS issue_id, named.title FROM named";
    assert_eq!(
        rename(sql, "issues__IssueTitle", &[]),
        "WITH issues__IssueTitle__named AS (SELECT i.id, i.title FROM issues AS i) SELECT named.id AS issue_id, named.title FROM issues__IssueTitle__named AS named"
    );
}

#[test]
fn an_aliased_read_keeps_its_alias() {
    let sql = "WITH named AS (SELECT 1 AS x) SELECT n.x FROM named n JOIN named AS m ON m.x = n.x";
    assert_eq!(
        rename(sql, "p__F", &[]),
        "WITH p__F__named AS (SELECT 1 AS x) SELECT n.x FROM p__F__named n JOIN p__F__named AS m ON m.x = n.x"
    );
}

#[test]
fn a_relation_is_renamed_where_the_fragment_reads_it() {
    let sql = "SELECT l.id FROM chosen JOIN issue_labels AS il ON il.issue_id = chosen.issue_id WHERE il.issue_id IN (SELECT issue_id FROM chosen)";
    assert_eq!(
        rename(sql, "labels__LabelUse", &["chosen".to_owned()]),
        "SELECT l.id FROM labels__LabelUse__chosen AS chosen JOIN issue_labels AS il ON il.issue_id = chosen.issue_id WHERE il.issue_id IN (SELECT issue_id FROM labels__LabelUse__chosen AS chosen)"
    );
}

#[test]
fn a_cte_named_like_a_table_reads_the_table_in_its_own_body() {
    let sql = "WITH labels AS (SELECT * FROM labels WHERE hidden = 0) SELECT * FROM labels";
    assert_eq!(
        rename(sql, "p__F", &[]),
        "WITH p__F__labels AS (SELECT * FROM labels WHERE hidden = 0) SELECT * FROM p__F__labels AS labels"
    );
    let recursive = "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3) SELECT n FROM c";
    assert_eq!(
        rename(recursive, "p__F", &[]),
        "WITH RECURSIVE p__F__c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM p__F__c AS c WHERE n < 3) SELECT n FROM p__F__c AS c"
    );
}

#[test]
fn text_that_declares_nothing_is_unchanged() {
    let sql = "SELECT a.x FROM t AS a, schema.named AS b";
    assert_eq!(rename(sql, "p__F", &[]), sql);
}
