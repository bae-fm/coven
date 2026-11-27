//! Where a statement's SQL names each table or view it reads, which a read
//! report gives for each place (SPEC.md section 12). The parser keeps no
//! offsets, so this finds them with the parser itself: it parses the SQL with
//! one token that could name the object replaced by a name no SQL writes, and
//! prints both syntax trees, which print token for token alike. Where the
//! replaced name prints, the statement's own tree prints the name the planner
//! read the FROM item by.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::fmt;

use sqlite3_parser::ast::fmt::{ToTokens, TokenStream};
use sqlite3_parser::ast::{Cmd, Select, Stmt};
use sqlite3_parser::dialect::TokenType;
use sqlite3_parser::lexer::sql::{Parser, Tokenizer};
use sqlite3_parser::lexer::Scanner;
use sqlite3_parser::{Bump, FallibleIterator};

use crate::read_report::ReadPlace;
use crate::scope_plan::Planner;

/// Whether a plan finds where the statement's SQL names each table it reads,
/// which only a read report shows.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Places {
    Find,
    Skip,
}

impl Planner<'_, '_> {
    /// Give each item whose table or view `select` names where `sql`, whose
    /// syntax tree it is, writes that name.
    pub(crate) fn place_items(&mut self, sql: &str, select: &Select<'_>) {
        let named = self.items.iter().filter_map(|item| item.named).collect();
        let positions = name_positions(sql, select, &named);
        for item in &mut self.items {
            if let Some(at) = item.named.and_then(|named| positions.get(&named)) {
                item.source.place = Some(ReadPlace::Statement(*at));
            }
        }
    }
}

/// A place in a statement's SQL.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct SqlPosition {
    /// The bytes before it.
    pub offset: usize,
    /// Its line, from 1.
    pub line: usize,
    /// Its column, from 1, in characters.
    pub column: usize,
}

impl SqlPosition {
    /// The place `offset` bytes into `sql`, which a character starts at.
    pub fn at(sql: &str, offset: usize) -> Self {
        let before = &sql[..offset];
        let line_start = before.rfind('\n').map_or(0, |newline| newline + 1);
        Self {
            offset,
            line: before.matches('\n').count() + 1,
            column: before[line_start..].chars().count() + 1,
        }
    }
}

impl fmt::Display for SqlPosition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { line, column, .. } = self;
        write!(formatter, "at line {line}, column {column}")
    }
}

/// The name that stands in for one token of the SQL.
const MARK: &str = "sqlive__marked_place";

/// Where `sql` writes each name of `select` that `wanted` holds, by the
/// address of the name's text, which the planner keeps of each FROM item.
/// `select` is `sql`'s syntax tree, or that of `sql` with its parameters
/// numbered, which prints the same names in the same order. A name that
/// `select` doesn't print, as one a view's SQL writes, has no place.
fn name_positions(
    sql: &str,
    select: &Select<'_>,
    wanted: &BTreeSet<usize>,
) -> BTreeMap<usize, SqlPosition> {
    let printed = printed(select);
    let names: BTreeSet<String> = printed
        .iter()
        .filter(|(address, _)| wanted.contains(address))
        .map(|(_, text)| crate::sql::name(text).to_ascii_lowercase())
        .collect();
    let mut found = BTreeMap::new();
    let mut scanner = Scanner::new(Tokenizer::new());
    // The planner finds places only in SQL the parser read to its end, so
    // each of its tokens reads again.
    while let (_, Some((token, _)), _) =
        scanner.scan(sql.as_bytes()).expect("SQL the parser read tokenizes")
    {
        let text = std::str::from_utf8(token).expect("a token of a string is a string");
        if text.is_empty() || !names.contains(&crate::sql::name(text).to_ascii_lowercase()) {
            continue;
        }
        // The token is a slice of the SQL.
        let start = token.as_ptr() as usize - sql.as_ptr() as usize;
        let marked = format!("{}{MARK}{}", &sql[..start], &sql[start + token.len()..]);
        let bump = Bump::new();
        let mut parser = Parser::new(&bump, marked.as_bytes());
        // A token that is a keyword where it stands, which leaves no
        // statement once a name replaces it, names no FROM item.
        let Ok(Some(Cmd::Stmt(Stmt::Select(other)))) = parser.next() else { continue };
        let other = self::printed(other);
        if other.len() != printed.len() {
            continue;
        }
        for ((address, _), (_, text)) in printed.iter().zip(&other) {
            if text == MARK && wanted.contains(address) {
                found.insert(*address, SqlPosition::at(sql, start));
            }
        }
    }
    found
}

/// The text of each token `select` prints that has one, with the address of
/// that text: a name's is the one its syntax node holds.
fn printed(select: &Select<'_>) -> Vec<(usize, String)> {
    struct Printed(Vec<(usize, String)>);
    impl TokenStream for Printed {
        type Error = Infallible;

        fn append(&mut self, _: TokenType, value: Option<&str>) -> Result<(), Infallible> {
            if let Some(value) = value {
                self.0.push((value.as_ptr() as usize, value.to_owned()));
            }
            Ok(())
        }
    }
    let mut stream = Printed(Vec::new());
    match select.to_tokens(&mut stream) {
        Ok(()) => stream.0,
        Err(never) => match never {},
    }
}
