//! One table named in several places, each reading its own columns of its own
//! rows: a statement of one rule with a statement of another in an EXISTS of
//! its first SELECT's WHERE. Both read the same few tables, and the second's
//! LIMIT, aggregate or window reads every column of its own places alone.

use super::{Cond, Query};
use crate::random_scopes::Rng;

fn part(rng: &mut Rng) -> Query {
    match rng.below(7) {
        0 => super::term::statement(rng),
        1 => super::edge::statement(rng),
        2 => super::limit::statement(rng),
        3 => super::aggregate::statement(rng),
        4 => super::distinct::statement(rng),
        5 => super::parenthesized::statement(rng),
        _ => super::unread_column::statement(rng),
    }
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let mut query = part(rng);
    let inner = part(rng);
    query.arms[0].conditions.push(Cond::Exists(Box::new(inner), rng.chance(15)));
    query
}
