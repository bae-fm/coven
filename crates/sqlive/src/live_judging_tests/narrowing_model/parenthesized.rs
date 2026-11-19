//! A parenthesized join: `m LEFT JOIN (p LEFT JOIN a ON ...) ON ...`, its
//! tables read as items of the core joined one after another, with a third
//! table in the join sometimes, conditions inside its ONs and on its tables
//! in the WHERE, which make the LEFT JOIN an inner join's; an inner join of
//! such a join, whose ON can name any of its tables; and one first in the
//! FROM.

use super::{
    cmp, col, eq, item, out, pick, select, table, Core, Item, Join, Out, Query, Source, TABLES,
};
use crate::random_scopes::Rng;

/// The parenthesized join's items: `p`, then `a` by a LEFT JOIN on a column
/// of `p`, and `z` by a LEFT JOIN on a column of `p` or `a` sometimes, each ON
/// with a condition of the table's own sometimes.
fn group(rng: &mut Rng) -> Vec<Item> {
    let mut on = vec![eq(col("a", pick(rng, &["id", "a"])), col("p", pick(rng, &["c", "a"])))];
    if rng.chance(30) {
        on.push(super::term::condition(rng, "a", 1));
    }
    let mut items = vec![
        table("p", pick(rng, &TABLES)),
        item("a", Source::Table(pick(rng, &TABLES)), Join::Left, on),
    ];
    if rng.chance(35) {
        let before = pick(rng, &["p", "a"]);
        items.push(item(
            "z",
            Source::Table(pick(rng, &TABLES)),
            Join::Left,
            vec![cmp(pick(rng, &["=", "<"]), col("z", "id"), col(before, "c"))],
        ));
    }
    items
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let items = group(rng);
    let third = items.len() == 3;
    let mut shown: Vec<Out> = vec![out(col("m", "id")), out(col("p", "b")), out(col("a", "b"))];
    if third {
        shown.push(out(col("z", "b")));
    }
    let mut conditions = vec![super::term::condition(rng, "m", 1)];
    if rng.chance(30) {
        let alias = pick(rng, &["p", "a"]);
        conditions.push(super::term::condition(rng, alias, 1));
    }
    if rng.chance(20) {
        // The parenthesized join first in the FROM, joined to `m` after it.
        let mut from = vec![Item {
            alias: "g".to_owned(),
            source: Source::Group(items),
            join: Join::Comma,
            on: Vec::new(),
        }];
        from.push(item(
            "m",
            Source::Table(pick(rng, &TABLES)),
            pick_join(rng),
            vec![eq(col("m", "c"), col("p", "id"))],
        ));
        return select(Core {
            from,
            conditions,
            select: shown,
            group: Vec::new(),
            distinct: false,
        });
    }
    let join = pick_join(rng);
    // A LEFT JOIN's ON names the join's first table alone of its tables; an
    // inner join's can name any.
    let partner = if join == Join::Left { "p" } else { pick(rng, &["p", "a"]) };
    let mut on = vec![eq(col(partner, pick(rng, &["id", "a"])), col("m", "c"))];
    if rng.chance(30) {
        on.push(super::term::condition(rng, "p", 1));
    }
    let from = vec![
        table("m", pick(rng, &TABLES)),
        Item { alias: "g".to_owned(), source: Source::Group(items), join, on },
    ];
    select(Core { from, conditions, select: shown, group: Vec::new(), distinct: false })
}

/// A LEFT JOIN or an inner join.
fn pick_join(rng: &mut Rng) -> Join {
    if rng.chance(65) {
        Join::Left
    } else {
        Join::Inner
    }
}
