//! These tests check that sqlive reruns no live query after a write outside the
//! query's scope, and reruns it after a write inside the scope
//! (SPEC.md section 5). Each module covers one part of the scope judging, with
//! live queries and writes over plain tables.

#[path = "live_judging_tests/burst.rs"]
mod burst;
#[path = "live_judging_tests/candidates.rs"]
mod candidates;
#[path = "live_judging_tests/carried_terms.rs"]
mod carried_terms;
#[path = "live_judging_tests/cell_cards.rs"]
mod cell_cards;
#[path = "live_judging_tests/derived.rs"]
mod derived;
#[path = "live_judging_tests/fragment_relation.rs"]
mod fragment_relation;
#[path = "live_judging_tests/fragment_window.rs"]
mod fragment_window;
#[path = "live_judging_tests/join_shapes.rs"]
mod join_shapes;
#[path = "live_judging_tests/join_through_found.rs"]
mod join_through_found;
#[path = "live_judging_tests/limit.rs"]
mod limit;
#[path = "live_judging_tests/lookup.rs"]
mod lookup;
#[path = "live_judging_tests/narrowing_model.rs"]
mod narrowing_model;
#[path = "live_judging_tests/net_change.rs"]
mod net_change;
#[path = "live_judging_tests/plan.rs"]
mod plan;
#[path = "live_judging_tests/policy.rs"]
mod policy;
#[path = "live_judging_tests/row.rs"]
mod row;
#[path = "live_judging_tests/row_scope.rs"]
mod row_scope;
#[path = "live_judging_tests/subquery.rs"]
mod subquery;
#[path = "live_judging_tests/subquery_window.rs"]
mod subquery_window;
#[path = "live_judging_tests/term.rs"]
mod term;
#[path = "live_judging_tests/unique_order.rs"]
mod unique_order;
#[path = "live_judging_tests/virtual_table.rs"]
mod virtual_table;
