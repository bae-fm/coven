use crate::tests::TestStore;
use crate::write::tests::{count, records, sql};
use crate::{Lost, RemovalRule, Replacement, RowIdentity, SyncedTable};
use coven_foundation::id_source::SequentialIds;

#[tokio::test]
async fn stale_references_are_judged_as_one_null_substitution() {
    for (check, shown) in [
        ("(a IS NULL) = (b IS NULL)", true),
        ("a IS NOT NULL OR b IS NOT NULL", false),
    ] {
        let ids = SequentialIds::new();
        let tables = || {
            ["p", "q", "children"]
                .into_iter()
                .map(|name| SyncedTable::new(name, RowIdentity::SharedKey))
                .collect()
        };
        let schema = format!(
            "CREATE TABLE p(id TEXT NOT NULL PRIMARY KEY);
            CREATE TABLE q(id TEXT NOT NULL PRIMARY KEY);
            CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,
                a TEXT REFERENCES p(id) ON DELETE SET NULL,
                b TEXT REFERENCES q(id) ON DELETE SET NULL,
                CONSTRAINT parents_together CHECK({check}))"
        );
        let open = |store: &TestStore| {
            store.builder(
                tables(),
                vec![crate::Migration::addition_with(1, "parents", {
                    let schema = schema.clone();
                    move |sql| {
                        sql.execute_batch(&schema)?;
                        Ok(())
                    }
                })],
            )
        };
        let sa = TestStore::with_ids(&ids);
        let sb = TestStore::with_ids(&ids);
        let a = open(&sa).open().await.unwrap();
        let b = open(&sb).open().await.unwrap();
        sql(&a, "INSERT INTO p VALUES('p'); INSERT INTO q VALUES('q')")
            .await
            .unwrap();
        let initial = records(&a).remove(0);
        b.receive(initial.clone().into()).await.unwrap();
        sql(&b, "INSERT INTO children VALUES('c','p','q')")
            .await
            .unwrap();
        sql(&a, "DELETE FROM p; DELETE FROM q").await.unwrap();
        let writes = [records(&a).remove(1), records(&b).remove(0)];
        for order in [[0, 1], [1, 0]] {
            let store = TestStore::with_ids(&ids);
            let db = open(&store).open().await.unwrap();
            db.receive(initial.clone().into()).await.unwrap();
            for index in order {
                db.receive(writes[index].clone().into()).await.unwrap();
            }
            assert_eq!(
                count(&db, "children"),
                i64::from(shown),
                "{check}: {order:?}"
            );
            let losses = db.lost_values().await.unwrap();
            if shown {
                assert!(losses.is_empty());
                let values: (Option<String>, Option<String>) = db
                    .read(|sql| {
                        Ok(sql.query_row("SELECT a,b FROM children", [], |r| {
                            Ok((r.get(0)?, r.get(1)?))
                        })?)
                    })
                    .await
                    .unwrap();
                assert_eq!(values, (None, None));
            } else {
                assert_eq!(losses.len(), 1);
                assert_eq!(
                    losses[0].replaced_by,
                    Replacement::Rules(vec![
                        RemovalRule::ForeignKey {
                            columns: vec!["a".into()],
                            parent: "p".into(),
                            parent_columns: vec!["id".into()]
                        },
                        RemovalRule::ForeignKey {
                            columns: vec!["b".into()],
                            parent: "q".into(),
                            parent_columns: vec!["id".into()]
                        },
                    ])
                );
                let Lost::Row(cells) = &losses[0].lost else {
                    panic!("removed row")
                };
                for (column, value) in [("a", "p"), ("b", "q")] {
                    assert_eq!(
                        cells.iter().find(|c| c.column == column).unwrap().value,
                        rusqlite::types::Value::Text(value.into())
                    );
                }
            }
            db.close().await.unwrap();
        }
        a.close().await.unwrap();
        b.close().await.unwrap();
    }
}
