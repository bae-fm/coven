use crate::write::tests::sql;
use crate::{tests::TestStore, RowIdentity, SyncedTable};
use coven_merge::UniqueConstraint;

#[tokio::test]
async fn parsed_constraints_keep_names_expressions_collations_and_generated_values() {
    let fixture = TestStore::new();
    let db = fixture
        .schema(
            vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
            r#"
        CREATE TABLE notes(
            id TEXT NOT NULL PRIMARY KEY,
            title TEXT COLLATE NOCASE CONSTRAINT title_key UNIQUE,
            body TEXT CONSTRAINT "body""valid" CHECK( length(body) > 0 ) CHECK(body NOT NULL),
            active INT,
            folded TEXT GENERATED ALWAYS AS (lower(title)) STORED,
            tagged TEXT AS ('CHECK, UNIQUE(' || title || ')') VIRTUAL COLLATE RTRIM,
            CONSTRAINT other_key UNIQUE(body COLLATE RTRIM DESC, active ASC),
            CHECK( active IN (0, /* comma , */ 1) ),
            CONSTRAINT folded_ok CHECK(folded = lower(title))
        );
        CREATE UNIQUE INDEX expr_key ON notes(lower( title ) COLLATE NOCASE DESC) WHERE active == 1;
        CREATE UNIQUE INDEX partial_key ON notes(title) WHERE active IS NOT NULL;
    "#,
        )
        .await
        .unwrap();
    db.inspect_writer_schema(|_, schema| {
        let rules = &schema.rules["notes"];
        assert_eq!(
            rules.checks,
            vec![
                ("body\"valid".into(), " length(body) > 0 ".into()),
                ("body NOT NULL".into(), "body NOT NULL".into()),
                (
                    " active IN (0, /* comma , */ 1) ".into(),
                    " active IN (0, /* comma , */ 1) ".into()
                ),
                ("folded_ok".into(), "folded = lower(title)".into()),
            ]
        );
        let unique: std::collections::BTreeSet<_> =
            rules.unique.iter().map(|u| u.identity.clone()).collect();
        assert_eq!(
            unique,
            [
                UniqueConstraint::from(["title"]),
                UniqueConstraint::from(["body COLLATE RTRIM", "active"]),
                UniqueConstraint {
                    terms: vec!["lower( title ) COLLATE NOCASE".into()],
                    partial: Some("active == 1".into())
                },
                UniqueConstraint {
                    terms: vec!["title".into()],
                    partial: Some("active IS NOT NULL".into())
                },
            ]
            .into()
        );
    });
    sql(
        &db,
        "INSERT INTO notes(id,title,body,active) VALUES('1','Title','body',1)",
    )
    .await
    .unwrap();
    db.inspect_writer_schema(|writer, schema| {
        let app = crate::write_rows::AppView::after(writer, schema);
        let values = app
            .row(&(
                "notes".into(),
                coven_format::key::encode_key(&[coven_format::value::Value::Text("1".into())])
                    .unwrap(),
            ))
            .unwrap()
            .unwrap()
            .values;
        let evaluated =
            crate::removal_sql::evaluate_values(writer, schema.table("notes"), &values).unwrap();
        assert_eq!(
            evaluated["folded"],
            coven_format::value::Value::Text("title".into())
        );
        assert_eq!(
            evaluated["tagged"],
            coven_format::value::Value::Text("CHECK, UNIQUE(Title)".into())
        );
    });
    db.close().await.unwrap();
}

#[tokio::test]
async fn adjacent_table_constraints_and_sqlite_expression_spellings_keep_their_source() {
    let fixture = TestStore::new();
    let db = fixture
        .schema(
            vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
            r#"CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT,n INT,
        CHECK(n IS DISTINCT FROM NULL) CONSTRAINT "has title" CHECK(title NOT NULL)
        UNIQUE(title COLLATE RTRIM) CHECK(n != 0))"#,
        )
        .await
        .unwrap();
    db.inspect_writer_schema(|_, schema| {
        assert_eq!(
            schema.rules["notes"].checks,
            vec![
                (
                    "n IS DISTINCT FROM NULL".into(),
                    "n IS DISTINCT FROM NULL".into()
                ),
                ("has title".into(), "title NOT NULL".into()),
                ("n != 0".into(), "n != 0".into()),
            ]
        );
        assert_eq!(
            schema.rules["notes"].unique[0].identity,
            UniqueConstraint::from(["title COLLATE RTRIM"])
        );
    });
    db.close().await.unwrap();
}

#[tokio::test]
async fn distinct_unique_terms_survive_sqlite_sharing_their_physical_index() {
    let fixture = TestStore::new();
    let db=fixture.schema(vec![SyncedTable::new("notes",RowIdentity::SharedKey)],"CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE,UNIQUE(title COLLATE BINARY))").await.unwrap();
    db.inspect_writer_schema(|_, schema| {
        assert_eq!(
            schema
                .table("notes")
                .indices
                .iter()
                .filter(|i| !i.primary)
                .count(),
            1
        );
        let unique: std::collections::BTreeSet<_> = schema.rules["notes"]
            .unique
            .iter()
            .map(|u| u.identity.clone())
            .collect();
        assert_eq!(
            unique,
            [
                UniqueConstraint::from(["title"]),
                UniqueConstraint::from(["title COLLATE BINARY"])
            ]
            .into()
        );
    });
    db.close().await.unwrap();
}

#[tokio::test]
async fn index_boolean_literals_are_not_column_names() {
    let fixture = TestStore::new();
    let db=fixture.schema(vec![SyncedTable::new("notes",RowIdentity::SharedKey)],"CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT); CREATE UNIQUE INDEX constant ON notes(TRUE)").await.unwrap();
    db.inspect_writer_schema(|_, schema| {
        assert_eq!(
            schema.rules["notes"].unique[0].identity,
            UniqueConstraint::from(["TRUE"])
        );
        assert!(schema.rules["notes"].unique[0].dependencies.is_empty());
    });
    db.close().await.unwrap();
}

#[tokio::test]
async fn parenthesized_unique_columns_keep_their_expression_text() {
    let fixture = TestStore::new();
    let db=fixture.schema(vec![SyncedTable::new("notes",RowIdentity::SharedKey)],"CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT,UNIQUE((title) COLLATE BINARY))").await.unwrap();
    db.inspect_writer_schema(|_, schema| {
        assert_eq!(
            schema.rules["notes"].unique[0].identity,
            UniqueConstraint::from(["(title) COLLATE BINARY"])
        );
    });
    db.close().await.unwrap();
}
