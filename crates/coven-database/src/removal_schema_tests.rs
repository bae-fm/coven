use crate::write::tests::sql;
use crate::{tests::TestStore, RowIdentity, SyncedTable};
use coven_merge::UniqueConstraint;

#[tokio::test]
async fn unnamed_check_identity_keeps_the_full_expression() {
    for table_constraint in [false, true] {
        for expression in [
            " n > 0 ",
            "\t\n n > 0 \r\u{000c}",
            " /* first */ n > 0 /* last */ ",
            " \"n\" > 0 ",
            " [n] > 0 ",
            " `n` > 0 ",
            " '0''quoted' != '0''quoted' ",
        ] {
            let fixture = TestStore::new();
            let schema = format!(
                "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,n INT{} CHECK({expression}))",
                if table_constraint { "," } else { "" }
            );
            let db = fixture
                .builder(
                    vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
                    vec![crate::Migration::run(1, "check", move |sql| {
                        sql.execute_batch(&schema)?;
                        Ok(())
                    })],
                )
                .open()
                .await
                .unwrap();
            let error = sql(&db, "INSERT INTO notes VALUES('n',0)")
                .await
                .unwrap_err();
            let crate::DbError::Sqlite(rusqlite::Error::SqliteFailure(_, Some(message))) = error
            else {
                panic!("expected SQLite CHECK failure: {error:?}")
            };
            let reported = message.strip_prefix("CHECK constraint failed: ").unwrap();
            db.inspect_writer_schema(|_, schema| {
                let (identity, evaluation) = &schema.rules["notes"].checks[0];
                assert_eq!(identity, expression.trim_ascii());
                assert!(!reported.is_empty());
                assert_eq!(evaluation, expression);
            });
            db.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn parsed_constraints_keep_names_expressions_collations_and_generated_values() {
    let fixture = TestStore::new();
    let db = fixture
        .schema(
            vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
            r#"
        CREATE TABLE notes(
            id TEXT NOT NULL PRIMARY KEY,
            title TEXT CONSTRAINT title_key UNIQUE,
            body TEXT CONSTRAINT "body""valid" CHECK( length(body) > 0 ) CHECK(body NOT NULL),
            active INT,
            folded TEXT GENERATED ALWAYS AS (lower(title)) STORED,
            tagged TEXT AS ('CHECK, UNIQUE(' || title || ')') VIRTUAL COLLATE RTRIM,
            CONSTRAINT other_key UNIQUE(body COLLATE BINARY DESC, active ASC),
            CHECK( active IN (0, /* comma , */ 1) ),
            CONSTRAINT folded_ok CHECK(folded = lower(title))
        );
        CREATE UNIQUE INDEX expr_key ON notes(lower( title ) COLLATE BINARY DESC) WHERE active == 1;
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
                    "active IN (0, /* comma , */ 1)".into(),
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
                UniqueConstraint::from(["body COLLATE BINARY", "active"]),
                UniqueConstraint {
                    terms: vec!["lower( title ) COLLATE BINARY".into()],
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
        UNIQUE(title COLLATE BINARY) CHECK(n != 0))"#,
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
            UniqueConstraint::from(["title COLLATE BINARY"])
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

#[tokio::test]
async fn unnamed_checks_on_one_generated_column_keep_each_loss_identity() {
    use crate::write::tests::{count, records};
    use crate::{RemovalRule, Replacement};
    for (second, changed, expected) in [
        ("\"n\"<10", 0, vec!["\"n\">0"]),
        ("\"n\"<10", 6, vec!["\"n\"<10"]),
        ("\"n\">1", 0, vec!["\"n\">0", "\"n\">1"]),
    ] {
        let ids = coven_foundation::id_source::SequentialIds::new();
        let sa = TestStore::with_ids(&ids);
        let sb = TestStore::with_ids(&ids);
        let tables = || vec![SyncedTable::new("notes", RowIdentity::SharedKey)];
        let schema = format!("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,x INT,y INT,n INT AS(x+y),CHECK( \"n\">0 ),CHECK({second}))");
        let migrations = || {
            let schema = schema.clone();
            vec![crate::Migration::run(1, "checks", move |sql| {
                sql.execute_batch(&schema)?;
                Ok(())
            })]
        };
        let a = sa.builder(tables(), migrations()).open().await.unwrap();
        let b = sb.builder(tables(), migrations()).open().await.unwrap();
        sql(&a, "INSERT INTO notes(id,x,y) VALUES('row',3,3)")
            .await
            .unwrap();
        let initial = records(&a).remove(0);
        b.receive(initial.clone().into()).await.unwrap();
        for (db, statement) in [(&a, "UPDATE notes SET x=?1"), (&b, "UPDATE notes SET y=?1")] {
            db.write(move |sql| {
                sql.execute(statement, [changed])?;
                Ok(())
            })
            .await
            .unwrap();
        }
        let writes = [
            initial,
            records(&a).pop().unwrap(),
            records(&b).pop().unwrap(),
        ];
        // SQLite's message cannot distinguish these constraints, including two
        // checks that fail together. Local writes retain SQLite's own error.
        let error = a
            .write(move |sql| {
                sql.execute(
                    "INSERT INTO notes(id,x,y) VALUES('invalid',?1,?1)",
                    [changed],
                )?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(
            matches!(error, crate::DbError::Sqlite(rusqlite::Error::SqliteFailure(_, Some(message))) if message == "CHECK constraint failed: n")
        );
        sql(&b, "UPDATE notes SET y=3").await.unwrap();
        let repaired = records(&b).pop().unwrap();
        let expected = Replacement::Rules(
            expected
                .into_iter()
                .map(|constraint| RemovalRule::Check {
                    constraint: constraint.into(),
                })
                .collect(),
        );
        let mut snapshots = Vec::new();
        for order in [[0, 1, 2], [0, 2, 1]] {
            let fixture = TestStore::new();
            let db = fixture
                .builder(tables(), migrations())
                .open()
                .await
                .unwrap();
            for i in order {
                db.receive(writes[i].clone().into()).await.unwrap();
            }
            assert_eq!(count(&db, "notes"), 0);
            let losses = db.lost_values().await.unwrap();
            assert_eq!(
                losses
                    .iter()
                    .filter(|l| matches!(l.replaced_by, Replacement::Rules(_)))
                    .map(|l| &l.replaced_by)
                    .collect::<Vec<_>>(),
                [&expected]
            );
            snapshots.push(crate::tests::data(&db, coven_merge::Audience::Store));
            let target_store = TestStore::new();
            let target = target_store
                .builder(tables(), migrations())
                .open()
                .await
                .unwrap();
            crate::snapshot_write::tests::assert_loaded_losses(&db, &target).await;
            target.close().await.unwrap();
            db.close().await.unwrap();
            let db = fixture
                .builder(tables(), migrations())
                .open()
                .await
                .unwrap();
            assert_eq!(db.lost_values().await.unwrap(), losses);
            db.receive(repaired.clone().into()).await.unwrap();
            assert_eq!(count(&db, "notes"), 1);
            assert!(!db
                .lost_values()
                .await
                .unwrap()
                .iter()
                .any(|l| matches!(l.replaced_by, Replacement::Rules(_))));
            db.close().await.unwrap();
        }
        assert_eq!(snapshots[0], snapshots[1]);
        a.close().await.unwrap();
        b.close().await.unwrap();
    }
}

#[tokio::test]
async fn unnamed_check_sqlite_differential() {
    use coven_format::value::Value;
    use std::collections::BTreeSet;
    let expressions = [
        "\"n\">0",
        "\"n\"<10",
        "\"n\">1",
        "[n] IS NOT NULL",
        "`n` <> 'literal  with  spaces'",
    ];
    let checks = expressions
        .iter()
        .map(|e| format!("CHECK({e})"))
        .collect::<Vec<_>>()
        .join(",");
    let mut schema =
        format!("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,n NUMERIC,{checks});");
    for (i, expression) in expressions.iter().enumerate() {
        schema.push_str(&format!(
            "CREATE TABLE oracle{i}(n NUMERIC CHECK({expression}));"
        ));
    }
    let fixture = TestStore::new();
    let db = fixture
        .builder(
            vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
            vec![crate::Migration::run(1, "checks", move |sql| {
                sql.execute_batch(&schema)?;
                Ok(())
            })],
        )
        .open()
        .await
        .unwrap();
    sql(&db, "INSERT INTO notes VALUES('row',5)").await.unwrap();
    let record = crate::write::tests::records(&db).remove(0);
    db.inspect_writer_schema(|writer, schema| {
        let app = crate::write_rows::AppView::after(writer, schema);
        let store = crate::merge_store::MergeStore::new(writer, &app);
        let state = store.row(&record.parts[0].rows[0].row).unwrap().state;
        let mut values: crate::write_rows::AppValues = state
            .cells()
            .iter()
            .map(|(c, v)| (c.clone(), v.value.value.clone()))
            .collect();
        for value in [
            Value::Null,
            Value::Integer(-1),
            Value::Integer(0),
            Value::Integer(1),
            Value::Integer(5),
            Value::Integer(10),
            Value::Real(0.5_f64.to_bits()),
            Value::Text("0".into()),
            Value::Text("11".into()),
            Value::Text("literal  with  spaces".into()),
            Value::Blob(vec![0]),
        ] {
            values.insert("n".into(), value.clone());
            let actual = crate::removal_sql::constraints(
                writer,
                schema.table("notes"),
                &schema.rules["notes"],
                &state,
                &values,
                |_| {
                    Ok(coven_merge::ClaimOrder::Local {
                        number: record.header.position.number,
                    })
                },
            )
            .unwrap()
            .failed_checks;
            let mut expected = BTreeSet::new();
            for (i, expression) in expressions.iter().enumerate() {
                match writer.internal_execute(
                    &format!("INSERT INTO oracle{i} VALUES(?1)"),
                    [crate::write_encoding::sql_value(&value)],
                ) {
                    Ok(_) => {
                        writer
                            .internal_execute(&format!("DELETE FROM oracle{i}"), [])
                            .unwrap();
                    }
                    Err(crate::DbError::Sqlite(rusqlite::Error::SqliteFailure(e, _)))
                        if e.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_CHECK =>
                    {
                        expected.insert((*expression).to_owned());
                    }
                    result => panic!("unexpected SQLite enforcement result: {result:?}"),
                }
            }
            assert_eq!(actual, expected, "{value:?}");
        }
    });
    db.close().await.unwrap();
}
