use super::*;
use crate::crate_code::checked;
use crate::policy::Policy;
use crate::syntax::RustFile;

fn policy() -> Policy {
    Policy::fixture(
        r#"
        rules = ["read-shapes"]
        [database]
        capability = "SQLite"
        test_support_features = ["test-utils"]
        [read_shapes]
        read_types = ["SqlReadContext"]
        plumbing = []
        rule = "the read rule"
        "#,
    )
}

/// Each hit of `source`'s read code, as its shape's id.
fn hits_in(source: &str) -> Vec<Hit> {
    let files = [RustFile::fixture("crates/app/src/a.rs", source)];
    let files = checked(&files);
    let policy = policy();
    let read = ReadCode::collect(&files, &policy, policy.read_shapes.as_ref().expect("rows"));
    row_work(&read)
}

/// The shapes found in a read function with this body.
fn shapes(body: &str) -> Vec<&'static str> {
    let source = format!("fn read(sql: &SqlReadContext<'_>) {{ {body} }}");
    let mut found = hits_in(&source).into_iter().map(|hit| hit.shape.id()).collect::<Vec<_>>();
    found.sort_unstable();
    found
}

#[test]
fn every_sorting_form_is_found_and_its_neighbors_are_not() {
    assert_eq!(
        shapes(
            "rows.sort(); rows.sort_unstable(); rows.sort_by(|a, b| a.cmp(b));
             rows.sort_by_key(|r| r.id); rows.sort_by_cached_key(|r| r.id);
             rows.sort_unstable_by(|a, b| a.cmp(b)); rows.sort_unstable_by_key(|r| r.id);
             let a = rows.iter().sorted(); let b = rows.iter().sorted_by(|a, b| a.cmp(b));
             let c = rows.iter().sorted_by_key(|r| r.id);"
        ),
        ["sort"; 10]
    );
    assert_eq!(
        shapes("let a = x.max(y); let b = Ord::max(x, y); let c = x.min(y); db.sort(order);"),
        Vec::<&str>::new()
    );
}

#[test]
fn a_sort_outside_read_code_is_not_found() {
    let hits = hits_in(
        "fn write(tx: &WriteTransaction) { rows.sort(); }
         fn helper(rows: &mut Vec<u8>) { rows.sort(); }",
    );
    assert!(hits.is_empty());
}

#[test]
fn every_filtering_form_is_found_and_its_neighbors_are_not() {
    assert_eq!(
        shapes(
            "rows.retain(|r| r.ok); rows.retain_mut(|r| r.ok);
             let a = rows.iter().filter(|r| r.ok); let b = rows.iter().filter_map(|r| r.ok);
             let c = rows.iter().partition(|r| r.ok);
             let d = rows.iter().skip_while(|r| r.ok); let e = rows.iter().take_while(|r| r.ok);
             rows.dedup(); rows.dedup_by(|a, b| a == b); rows.dedup_by_key(|r| r.id);
             let f = rows.iter().unique(); let g = rows.iter().filter(Self::is_ok);"
        ),
        ["filter"; 12]
    );
    assert_eq!(
        shapes("let a = rows.iter().filter(keep); rows.retain(flag); let b = rows.iter().map(|r| r.id);"),
        Vec::<&str>::new()
    );
}

#[test]
fn every_grouping_form_is_found_and_its_neighbors_are_not() {
    assert_eq!(
        shapes(
            "let a = HashMap::new(); let b = BTreeMap::with_capacity(4);
             let c = HashSet::default(); let d = BTreeSet::from([1]);
             let e = IndexMap::from_iter(rows);
             let f = rows.iter().collect::<HashMap<_, _>>();
             let g: BTreeSet<u8> = Default::default();
             m.entry(key); rows.iter().chunk_by(|r| r.id); rows.iter().group_by(|r| r.id);
             rows.iter().into_group_map(); rows.iter().counts();"
        ),
        ["group"; 12]
    );
    assert_eq!(
        shapes("let a = Vec::new(); let b: Vec<u8> = Vec::new(); let c = rows.iter().collect::<Vec<_>>();"),
        Vec::<&str>::new()
    );
}

#[test]
fn one_map_made_and_typed_on_one_line_is_one_finding() {
    assert_eq!(shapes("let a: HashMap<u8, u8> = HashMap::new();"), ["group"]);
}

#[test]
fn a_read_function_returning_a_map_is_found() {
    let hits =
        hits_in("fn read(sql: &SqlReadContext<'_>) -> Result<HashMap<u8, u8>, Error> { todo!() }");
    assert_eq!(hits.iter().map(|hit| hit.shape.id()).collect::<Vec<_>>(), ["group"]);
    let hits = hits_in("fn read(sql: &SqlReadContext<'_>) -> BTreeMap<u8, u8> { todo!() }");
    assert_eq!(hits.iter().map(|hit| hit.shape.id()).collect::<Vec<_>>(), ["group"]);
    assert_eq!(hits[0].item, "read");
}

#[test]
fn every_aggregating_form_is_found_and_its_neighbors_are_not() {
    assert_eq!(
        shapes(
            "let a = rows.iter().count(); let b = rows.iter().sum::<u32>();
             let c = rows.iter().product::<u32>(); let d = rows.iter().max(); let e = rows.iter().min();
             let f = rows.iter().max_by(|a, b| a.cmp(b)); let g = rows.iter().max_by_key(|r| r.id);
             let h = rows.iter().min_by(|a, b| a.cmp(b)); let i = rows.iter().min_by_key(|r| r.id);
             let j = rows.iter().fold(0, |a, b| a + b); let k = rows.iter().reduce(|a, b| a);
             let mut total = 0; for row in rows { total += row.n; total -= 1; total *= 2; }
             while more { count += 1; }"
        ),
        ["aggregate"; 15]
    );
    assert_eq!(
        shapes(
            "let a = x.max(y); let b = rows.len(); total += 1; for row in rows { self.seen += 1; row.n += 1; }
             let c = x.min(y);"
        ),
        Vec::<&str>::new()
    );
}

#[test]
fn every_picking_form_is_found_and_its_neighbors_are_not() {
    assert_eq!(
        shapes(
            "let a = rows.iter().any(|r| r.ok); let b = rows.iter().all(|r| r.ok);
             let c = rows.iter().find(|r| r.ok); let d = rows.iter().find_map(|r| r.ok);
             let e = rows.iter().position(|r| r.ok); let f = rows.iter().rposition(|r| r.ok);
             let g = rows.iter().any(Self::is_ok);"
        ),
        ["pick"; 7]
    );
    assert_eq!(
        shapes("let a = line.find(':'); let b = line.find(\"x\"); let c = line.find(delimiter);"),
        Vec::<&str>::new()
    );
}

#[test]
fn a_statement_run_for_each_row_is_found() {
    assert_eq!(
        shapes("for id in ids { sql.query_row(\"SELECT 1\", [], |r| r.get(0)); }"),
        ["query-per-row"]
    );
    assert_eq!(
        shapes("while more { sql.prepare(\"SELECT 1\"); sql.execute(\"DELETE\", []); }"),
        ["query-per-row"; 2]
    );
    assert_eq!(
        shapes("let all = ids.iter().map(|id| sql.query_row(\"SELECT 1\", [], |r| r.get(0)));"),
        ["query-per-row"]
    );
    assert_eq!(
        shapes(
            "sql.query_map(\"SELECT id FROM a\", [], |row| { sql.query_row(\"SELECT 1\", [], |r| r.get(0)) });"
        ),
        ["query-per-row"]
    );
    assert_eq!(shapes("sql.query_row(\"SELECT 1\", [], |r| r.get(0));"), Vec::<&str>::new());
}

#[test]
fn a_call_to_read_code_handed_a_read_value_runs_a_statement() {
    let hits = hits_in(
        "fn read(sql: &SqlReadContext<'_>) { for id in ids { other(sql, id); plain(id); } }
         fn other(sql: &SqlReadContext<'_>, id: u8) {}
         fn plain(id: u8) {}",
    );
    assert_eq!(hits.iter().map(|hit| hit.shape.id()).collect::<Vec<_>>(), ["query-per-row"]);
}

#[test]
fn row_work_is_found_in_a_closure_passed_to_a_read_callee_and_named_by_its_item() {
    let hits = hits_in(
        "struct Screen { store: Store }
         impl Screen { fn open(&self) { self.store.subscribe(|sql| { rows.sort(); }); } }
         struct Store;
         impl Store { fn subscribe<F>(&self, read: F) where F: Fn(SqlReadContext<'_>) {} }",
    );
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].item, "Screen::open");
    assert_eq!(hits[0].shape.id(), "sort");
    assert_eq!(hits[0].path, "crates/app/src/a.rs");
}

#[test]
fn test_code_and_test_support_items_have_no_findings() {
    let hits = hits_in(
        r#"
        #[cfg(test)]
        mod tests { fn read(sql: &SqlReadContext<'_>) { rows.sort(); } }
        #[cfg(feature = "test-utils")]
        fn gated(sql: &SqlReadContext<'_>) { rows.sort(); }
        "#,
    );
    assert!(hits.is_empty());
}

#[test]
fn a_hit_names_its_line_and_what_it_does() {
    let hits = hits_in("fn read(sql: &SqlReadContext<'_>) {\n    rows.sort_by_key(|r| r.id);\n}");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].line, 2);
    assert_eq!(hits[0].what, "sorts with `sort_by_key`");
}
