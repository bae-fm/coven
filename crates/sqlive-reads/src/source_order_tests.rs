use crate::read_capture::WholeViews;
use crate::{MainSchema, ScopePlan};
use rusqlite::Connection;

const SCHEMA: &str = "CREATE TABLE a(id INTEGER PRIMARY KEY, x INTEGER, y INTEGER);
CREATE TABLE b(id INTEGER PRIMARY KEY, x INTEGER, y INTEGER);
CREATE TABLE c(id INTEGER PRIMARY KEY, x INTEGER, y INTEGER);
CREATE TABLE d(id INTEGER PRIMARY KEY, x INTEGER, y INTEGER);";

/// The qualifiers of `sql`'s sources in the order its plan places them.
fn order(sql: &str) -> Vec<String> {
    let connection = Connection::open_in_memory().unwrap();
    connection.execute_batch(SCHEMA).unwrap();
    let statement = connection.prepare(sql).unwrap();
    let schema = MainSchema::read(&connection).unwrap();
    let plan =
        ScopePlan::build(&connection, &schema, sql, &statement, &WholeViews::new([])).unwrap();
    let sources = plan.sources.unwrap();
    sources.order.iter().map(|&source| sources.sources[source].qualifier.clone()).collect()
}

#[test]
fn an_item_that_waited_goes_as_soon_as_it_stops() {
    // Once c is placed, a takes values from a placed source and b doesn't,
    // but both waited and b comes first in the FROM.
    assert_eq!(
        order("SELECT * FROM b JOIN a ON b.x = a.x WHERE a.y = (SELECT c.y FROM c WHERE c.id = 1)"),
        ["c", "b", "a"]
    );
}

#[test]
fn an_item_waits_for_the_partner_of_an_or() {
    assert_eq!(
        order("SELECT * FROM a, b WHERE a.id = 1 OR a.y = (SELECT c.y FROM c WHERE c.id = 1)"),
        ["b", "c", "a"]
    );
}

#[test]
fn a_core_goes_before_the_cores_inside_it() {
    assert_eq!(
        order("SELECT * FROM a, b WHERE a.id = 1 AND EXISTS (SELECT 1 FROM c WHERE c.x = a.x)"),
        ["a", "b", "c"]
    );
}

#[test]
fn an_item_that_takes_values_from_a_placed_source_goes_before_one_with_a_term() {
    assert_eq!(
        order("SELECT * FROM a, b, c WHERE a.id = 1 AND b.y = 5 AND c.x = a.x"),
        ["a", "c", "b"]
    );
}

#[test]
fn an_item_with_a_term_goes_before_a_table_function() {
    assert_eq!(order("SELECT * FROM json_each('[1]') AS j, a WHERE a.id = 1"), ["a", "j"]);
}

#[test]
fn an_item_that_takes_a_table_functions_values_goes_after_it() {
    assert_eq!(
        order(
            "SELECT * FROM a, json_each('[1]') AS j, b WHERE a.id = 1 AND a.x = j.value AND b.id = 2"
        ),
        ["b", "j", "a"]
    );
}

#[test]
fn a_table_function_goes_before_an_item_nothing_narrows() {
    assert_eq!(order("SELECT * FROM a, json_each('[1]') AS j"), ["j", "a"]);
}

#[test]
fn items_nothing_tells_apart_go_in_from_order() {
    assert_eq!(order("SELECT * FROM b, a, c"), ["b", "a", "c"]);
}

#[test]
fn the_first_item_goes_when_every_item_waits() {
    assert_eq!(
        order("SELECT * FROM a WHERE a.y = (SELECT c.y FROM c WHERE c.x = a.x)"),
        ["a", "c"]
    );
}

#[test]
fn an_edge_passed_into_a_subquery_orders_its_items() {
    assert_eq!(
        order("SELECT * FROM a JOIN (SELECT c.x FROM b, c, d) AS s ON s.x = a.x WHERE a.id = 1"),
        ["a", "s", "c", "b", "d"]
    );
}

#[test]
fn an_item_with_every_partner_waiting_waits_in_turn() {
    // b takes values only from a, which waits for c, and d only from b.
    assert_eq!(
        order(
            "SELECT * FROM b JOIN a ON b.x = a.x
             WHERE EXISTS (SELECT 1 FROM d WHERE d.y = b.y)
             AND a.y = (SELECT c.y FROM c WHERE c.id = 1)"
        ),
        ["c", "b", "a", "d"]
    );
}

#[test]
fn a_subquery_table_with_no_condition_goes_before_the_item_that_waits_for_it() {
    assert_eq!(order("SELECT * FROM a WHERE a.x IN (SELECT c.y FROM c LIMIT 1)"), ["c", "a"]);
}
