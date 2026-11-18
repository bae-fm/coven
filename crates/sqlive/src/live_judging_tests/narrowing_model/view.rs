//! Rule 5: the app's views read as their queries' cores: a plain view, a
//! join, a compound, a grouped view, one with a LEFT JOIN, and one with a
//! WHERE of its own; and a view read inside a subquery, which a lookup still
//! reads alone.

use super::{views, Query, Source};
use crate::random_scopes::Rng;

pub(super) fn statement(rng: &mut Rng) -> Query {
    let views = views();
    let (name, _, columns) = &views[rng.below(views.len())];
    super::push_down::around(rng, Source::View(name), columns)
}
