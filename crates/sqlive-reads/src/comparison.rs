//! What SQLite takes from each side of a comparison, as far as sqlive follows
//! it: the affinity it converts values by and the collation it compares by.

use crate::scope_term::Affinity;

pub(crate) const BINARY: &str = "BINARY";

/// What a comparison takes from one of its sides, as far as sqlive follows
/// it.
#[derive(Clone, PartialEq)]
pub(crate) enum Carried<T> {
    /// The side gives none, as a literal gives no collation.
    Nothing,
    Of(T),
    /// sqlive doesn't follow what the side gives.
    Unfollowed,
}

/// The affinity a side of a comparison has SQLite convert values by, and the
/// collation a column on it gives the comparison.
#[derive(Clone)]
pub(crate) struct Traits {
    pub(crate) affinity: Carried<Affinity>,
    pub(crate) collation: Carried<String>,
}

impl Traits {
    /// A value's, as a literal's or a function's.
    pub(crate) const VALUE: Self = Self { affinity: Carried::Nothing, collation: Carried::Nothing };
    pub(crate) const UNFOLLOWED: Self =
        Self { affinity: Carried::Unfollowed, collation: Carried::Unfollowed };
}

/// Whether a comparison or a sort by `collation` calls no two values equal
/// that a key's `key` collation tells apart.
pub(crate) fn at_least_as_fine(collation: &str, key: &str) -> bool {
    collation.eq_ignore_ascii_case(BINARY) || collation.eq_ignore_ascii_case(key)
}

/// Whether comparing a column of affinity `column` with a value of affinity
/// `value` converts none of the column's stored values, so no two of them a
/// unique key tells apart can both equal the value: SQLite converts a TEXT or
/// untyped column's values to compare them with a numeric column's, and an
/// untyped column's to compare them with a TEXT column's. A value that is a
/// column, as `named` says, must be an untyped one to leave an untyped
/// column's values: sqlive doesn't follow the affinity of a column a FROM
/// subquery computes.
pub(crate) fn keeps_values(column: Affinity, value: &Carried<Affinity>, named: bool) -> bool {
    match column {
        Affinity::Numeric => true,
        Affinity::Text => !matches!(value, Carried::Of(Affinity::Numeric) | Carried::Unfollowed),
        Affinity::Blob => {
            matches!(value, Carried::Of(Affinity::Blob))
                || !named && matches!(value, Carried::Nothing)
        }
    }
}

/// The collation SQLite compares `left = right` by: the left side's when a
/// column on it gives one, else the right side's, else BINARY. `None` when
/// sqlive doesn't follow it.
pub(crate) fn comparison_collation(left: &Traits, right: &Traits) -> Option<String> {
    match (&left.collation, &right.collation) {
        (Carried::Of(collation), _) => Some(collation.clone()),
        (Carried::Nothing, Carried::Of(collation)) => Some(collation.clone()),
        (Carried::Nothing, Carried::Nothing) => Some(BINARY.to_owned()),
        (Carried::Unfollowed, _) | (Carried::Nothing, Carried::Unfollowed) => None,
    }
}
