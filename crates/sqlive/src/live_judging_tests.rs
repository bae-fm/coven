//! These tests check that sqlive reruns no live query after a write outside
//! the query's scope, and reruns it after a write inside the scope (§4).
//! Each module covers one part of the scope judging, with live queries and
//! writes over plain tables.

#[path = "live_judging_tests/derived.rs"]
mod derived;
#[path = "live_judging_tests/limit.rs"]
mod limit;
#[path = "live_judging_tests/lookup.rs"]
mod lookup;
#[path = "live_judging_tests/narrowing_model.rs"]
mod narrowing_model;
#[path = "live_judging_tests/plan.rs"]
mod plan;
#[path = "live_judging_tests/row.rs"]
mod row;
#[path = "live_judging_tests/row_scope.rs"]
mod row_scope;
#[path = "live_judging_tests/subquery.rs"]
mod subquery;
#[path = "live_judging_tests/term.rs"]
mod term;
