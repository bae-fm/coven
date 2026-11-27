//! The equalities of `carry` with the tables they carry terms between behind
//! FROM subqueries, as `or_derived::wrapped` puts them there.

use super::Query;
use crate::random_scopes::Rng;

pub(super) fn statement(rng: &mut Rng) -> Query {
    let query = super::carry::statement(rng);
    super::or_derived::wrapped(rng, query)
}
