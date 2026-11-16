//! Live queries over the scopes sqlive judges: a write outside a query's
//! scope wakes nothing, and one inside it does (§4). Each module covers one
//! part of the scope judging, with the store's live queries and writes.

#[path = "live_judging_tests/derived.rs"]
mod derived;
#[path = "live_judging_tests/lookup.rs"]
mod lookup;
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
