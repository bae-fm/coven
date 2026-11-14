//! Forced reruns (the read rule): a live query reruns because what it read
//! changed, so nothing counts up to make it run again.
//!
//! - `counter`: a column some statement sets to itself plus a positive integer
//!   (`UPDATE t SET c = c + 1`, an upsert's `DO UPDATE SET c = c + 1`) and
//!   that no statement anywhere sets to anything else. Count tables that
//!   decrement, upserts that copy `excluded.c` and columns set to bound values
//!   are none. The SQL read is that of every `.sql` file and every production
//!   string literal, as the database rule reads it.
//! - `request-bump`: an integer field of a request type (the first type
//!   argument of a `request_query_types` type, written anywhere in the
//!   workspace, the element types of collections and `Option` included)
//!   increased by one: `x.f += 1`, `x.f = x.f + 1`, `f: y.f + 1` in a literal
//!   of that type, and `.f.wrapping_add(1)` with its `saturating_` and
//!   `checked_` forms. A field is matched by its name alone only where no
//!   other struct declares that name.

use std::collections::{BTreeMap, BTreeSet};

use syn::visit::{self, Visit};

use crate::crate_code::CrateCode;
use crate::database_boundary::production_sql;
use crate::macros::parse_macro_body;
use crate::policy::{ReadShapes, Shape};
use crate::read_code::ReadCode;
use crate::read_index::names;
use crate::row_work::Hit;
use crate::sql_statements::statements;
use crate::syntax::RustFile;

/// What the SQL does to one column.
#[derive(Default)]
struct ColumnSets {
    /// Where it is increased, the first by path and line.
    first_increment: Option<(String, usize)>,
    /// Whether any statement sets it to anything else.
    set_otherwise: bool,
}

fn column_sets(code: &CrateCode) -> BTreeMap<String, ColumnSets> {
    let mut columns: BTreeMap<String, ColumnSets> = BTreeMap::new();
    for text in production_sql(code) {
        for statement in statements(&text.text) {
            for assignment in statement.assignments {
                let sets = columns
                    .entry(format!("{}.{}", assignment.table, assignment.column))
                    .or_default();
                if assignment.increment {
                    let place = (text.path.clone(), text.line_at(assignment.at));
                    if sets.first_increment.as_ref().is_none_or(|first| place < *first) {
                        sets.first_increment = Some(place);
                    }
                } else {
                    sets.set_otherwise = true;
                }
            }
        }
    }
    columns
}

/// Each column only ever counted up, at its first increment.
pub(crate) fn counters(code: &CrateCode) -> Vec<Hit> {
    column_sets(code)
        .into_iter()
        .filter(|(_, sets)| !sets.set_otherwise)
        .filter_map(|(column, sets)| {
            let (path, line) = sets.first_increment?;
            Some(Hit {
                path,
                line,
                what: format!("counts up `{column}`, which no statement sets to anything else"),
                item: column,
                shape: Shape::Counter,
            })
        })
        .collect()
}

/// Each place a statement increases a column, by file and `table.column`,
/// whether the column is a counter or not.
pub(crate) fn increment_sites(code: &CrateCode) -> BTreeSet<(String, String)> {
    let mut sites = BTreeSet::new();
    for text in production_sql(code) {
        for statement in statements(&text.text) {
            for assignment in statement.assignments.into_iter().filter(|set| set.increment) {
                sites.insert((
                    text.path.clone(),
                    format!("{}.{}", assignment.table, assignment.column),
                ));
            }
        }
    }
    sites
}

const INTEGERS: [&str; 12] =
    ["u8", "u16", "u32", "u64", "u128", "usize", "i8", "i16", "i32", "i64", "i128", "isize"];

/// The structs' fields: which are integers, and which structs declare each
/// name.
#[derive(Default)]
struct Fields {
    integers: BTreeMap<String, BTreeSet<String>>,
    declaring: BTreeMap<String, BTreeSet<String>>,
    structs: BTreeSet<String>,
    /// The type names written as the first argument of a request query.
    requested: BTreeSet<String>,
}

struct FieldCollector<'a> {
    shapes: &'a ReadShapes,
    fields: Fields,
}

impl<'ast> Visit<'ast> for FieldCollector<'_> {
    fn visit_item_struct(&mut self, node: &'ast syn::ItemStruct) {
        let name = node.ident.to_string();
        self.fields.structs.insert(name.clone());
        for field in &node.fields {
            let Some(field_name) = field.ident.as_ref().map(ToString::to_string) else { continue };
            self.fields.declaring.entry(field_name.clone()).or_default().insert(name.clone());
            let integer = matches!(&field.ty, syn::Type::Path(path)
                if path.path.get_ident().is_some_and(|ident| INTEGERS.contains(&ident.to_string().as_str())));
            if integer {
                self.fields.integers.entry(name.clone()).or_default().insert(field_name);
            }
        }
        visit::visit_item_struct(self, node);
    }

    fn visit_type_path(&mut self, node: &'ast syn::TypePath) {
        if let Some(segment) = node.path.segments.last() {
            if self.shapes.request_query_types.contains(&segment.ident.to_string()) {
                if let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments {
                    let first = arguments.args.iter().find_map(|argument| match argument {
                        syn::GenericArgument::Type(ty) => Some(ty),
                        _ => None,
                    });
                    self.fields.requested.extend(first.into_iter().flat_map(|ty| names(ty, false)));
                }
            }
        }
        visit::visit_type_path(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

impl Fields {
    /// Whether `field` is an integer of a request type.
    fn is_request_integer(&self, request: &str, field: &str) -> bool {
        self.requested.contains(request)
            && self.integers.get(request).is_some_and(|fields| fields.contains(field))
    }

    /// Whether `field`, found on a value of unknown type, is the integer field
    /// of a request: no other struct declares the name.
    fn is_named_request_integer(&self, field: &str) -> bool {
        match self.declaring.get(field) {
            Some(owners) if owners.len() == 1 => {
                owners.iter().any(|owner| self.is_request_integer(owner, field))
            }
            _ => false,
        }
    }
}

/// Each integer field of a request type increased by one.
pub(crate) fn request_bumps(
    read: &ReadCode<'_>,
    files: &[RustFile],
    shapes: &ReadShapes,
) -> Vec<Hit> {
    let mut collector = FieldCollector { shapes, fields: Fields::default() };
    for file in files {
        collector.visit_file(&file.syntax);
    }
    let fields = collector.fields;
    if fields.requested.is_empty() {
        return Vec::new();
    }
    let mut hits = Vec::new();
    for callable in &read.index().callables {
        let (false, Some(body)) = (callable.plumbing, callable.body) else { continue };
        let mut bumps = Bumps { fields: &fields, found: Vec::new() };
        bumps.visit_block(body);
        hits.extend(bumps.found.into_iter().map(|(line, field)| Hit {
            path: callable.file.relative_path.clone(),
            line,
            item: callable.item(),
            shape: Shape::RequestBump,
            what: format!("adds one to the request's `{field}` to run the query again"),
        }));
    }
    hits
}

struct Bumps<'a> {
    fields: &'a Fields,
    found: Vec<(usize, String)>,
}

fn is_one(expr: &syn::Expr) -> bool {
    match expr {
        syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Int(number), .. }) => {
            number.base10_parse::<u128>().is_ok_and(|value| value == 1)
        }
        syn::Expr::Paren(inner) => is_one(&inner.expr),
        _ => false,
    }
}

/// The field a member expression names: `x.f`.
fn field_of(expr: &syn::Expr) -> Option<String> {
    match expr {
        syn::Expr::Field(field) => match &field.member {
            syn::Member::Named(name) => Some(name.to_string()),
            syn::Member::Unnamed(_) => None,
        },
        syn::Expr::Paren(inner) => field_of(&inner.expr),
        _ => None,
    }
}

/// `x.f + 1` or `1 + x.f`: the field.
fn field_plus_one(expr: &syn::Expr) -> Option<String> {
    let syn::Expr::Binary(binary) = expr else { return None };
    if !matches!(binary.op, syn::BinOp::Add(_)) {
        return None;
    }
    match (field_of(&binary.left), field_of(&binary.right)) {
        (Some(field), None) if is_one(&binary.right) => Some(field),
        (None, Some(field)) if is_one(&binary.left) => Some(field),
        _ => None,
    }
}

impl Bumps<'_> {
    fn bump(&mut self, line: usize, field: String) {
        self.found.push((line, field));
    }
}

impl<'ast> Visit<'ast> for Bumps<'_> {
    fn visit_item(&mut self, _: &'ast syn::Item) {}

    fn visit_expr_binary(&mut self, node: &'ast syn::ExprBinary) {
        if matches!(node.op, syn::BinOp::AddAssign(_)) && is_one(&node.right) {
            if let Some(field) = field_of(&node.left) {
                if self.fields.is_named_request_integer(&field) {
                    self.bump(syn::spanned::Spanned::span(node).start().line, field);
                }
            }
        }
        visit::visit_expr_binary(self, node);
    }

    fn visit_expr_assign(&mut self, node: &'ast syn::ExprAssign) {
        if let (Some(target), Some(source)) = (field_of(&node.left), field_plus_one(&node.right)) {
            if target == source && self.fields.is_named_request_integer(&target) {
                self.bump(syn::spanned::Spanned::span(node).start().line, target);
            }
        }
        visit::visit_expr_assign(self, node);
    }

    fn visit_expr_struct(&mut self, node: &'ast syn::ExprStruct) {
        if let Some(request) = node.path.segments.last().map(|segment| segment.ident.to_string()) {
            for value in &node.fields {
                let syn::Member::Named(name) = &value.member else { continue };
                let name = name.to_string();
                if field_plus_one(&value.expr).as_deref() == Some(name.as_str())
                    && self.fields.is_request_integer(&request, &name)
                {
                    self.bump(syn::spanned::Spanned::span(value).start().line, name);
                }
            }
        }
        visit::visit_expr_struct(self, node);
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        let method = node.method.to_string();
        if matches!(method.as_str(), "wrapping_add" | "saturating_add" | "checked_add")
            && node.args.len() == 1
            && node.args.first().is_some_and(is_one)
        {
            if let Some(field) = field_of(&node.receiver) {
                if self.fields.is_named_request_integer(&field) {
                    self.bump(node.method.span().start().line, field);
                }
            }
        }
        visit::visit_expr_method_call(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

#[cfg(test)]
#[path = "counters_tests.rs"]
mod tests;
