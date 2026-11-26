//! The columns a `*` lists of a core's FROM items, whatever a reader of the
//! SELECT knows of each column.

/// A FROM item as a `*` reads it.
pub(crate) trait StarItem {
    /// What the reader knows of one of its columns.
    type Column;

    /// Its columns, in order, or `None` when the reader doesn't know them.
    fn listed(&self) -> Option<&[Self::Column]>;

    /// The columns, lowercase, that USING or NATURAL compares with an item to
    /// its left.
    fn using(&self) -> &[String];

    /// The name of `column`, lowercase, when the reader knows it.
    fn name(column: &Self::Column) -> Option<&str>;
}

/// The columns a `*` gives of `items`, a core's FROM items in order, each
/// with its item's position among them: every column of each item but those
/// that USING compares with an item to its left, which that item lists. `None`
/// when an item's columns are unknown, or when USING compares one of an item
/// that has a column of an unknown name, which may be a column it compares.
pub(crate) fn star<'a, I: StarItem + 'a>(
    items: impl IntoIterator<Item = &'a I>,
) -> Option<Vec<(usize, &'a I::Column)>> {
    let mut found = Vec::new();
    for (position, item) in items.into_iter().enumerate() {
        let columns = item.listed()?;
        let using = item.using();
        if !using.is_empty() && columns.iter().any(|column| I::name(column).is_none()) {
            return None;
        }
        let kept = columns.iter().filter(|column| {
            I::name(column).is_none_or(|name| !using.iter().any(|compared| compared == name))
        });
        found.extend(kept.map(|column| (position, column)));
    }
    Some(found)
}
