//! What read code is, found by structure rather than by a list of names.
//!
//! The policy's `read_types` name what a read receives and the receivers whose
//! callbacks are reads. Read code is:
//!
//! - every function or closure with a parameter of a read type; a callback
//!   parameter that is only given one (`impl Fn(&SqlReadContext)`) is no such
//!   parameter, since its taker receives nothing; a live query
//!   (`live_query_types`) counts only in a function of a type that holds one;
//! - every closure or function path passed where the callee's parameter names
//!   a read type, directly, through `impl Trait`, or through a generic's bound
//!   in the signature, the `impl` block or a `where` clause;
//! - every closure or function path passed to a method whose receiver is a
//!   read type;
//! - every workspace function the read code calls, transitively, when the call
//!   resolves: a path (`NameResolver`), `Type::f`, `Self::f`, and a method call
//!   whose receiver's type is known: `self`, a field of a known type (through
//!   shared pointers), a typed parameter or `let`, a `let` of a struct literal
//!   or a `Type::ctor(..)`, and the return type of a resolved call, with `?`,
//!   `Result` and `Option` peeled. A receiver of unknown type resolves by the
//!   method's name when one method of the workspace with a closure parameter
//!   has it and the call passes a closure or function path there, and passes a
//!   closure to a read callee only when every method of that name with a
//!   callback at that place takes a read closure.
//!
//! The read plumbing (`ReadShapes::plumbing`), test code and items gated on
//! test support alone are no read code. A closure counts with its enclosing
//! item, and a closure inside read code is part of it.

use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::Span;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::macros::parse_macro_body;
use crate::name_resolution::{qualified_path, NameResolver};
use crate::policy::{Policy, ReadShapes};
use crate::read_index::{names, peel, Callable, Index};
use crate::syntax::RustFile;

/// Where a call is in its file: the line and column of its method's name, or
/// of the whole call for a path call.
pub(crate) type CallSite = (usize, usize);

pub(crate) fn call_site(span: Span) -> CallSite {
    let start = span.start();
    (start.line, start.column)
}

/// Why a unit is read code.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum Why {
    /// It takes a value of a read type.
    Root,
    /// It is passed to a callee whose parameter names a read type.
    PassedToReadCallee,
    /// It is passed to a method of a read type.
    PassedToReadReceiver,
    /// Read code calls it.
    Reached,
}

/// What a unit's code is.
pub(crate) enum Body<'a> {
    Function { block: &'a syn::Block, signature: &'a syn::Signature },
    Closure(syn::ExprClosure),
}

/// A function or closure that is read code.
pub(crate) struct Unit<'a> {
    pub(crate) file: &'a RustFile,
    /// How findings name it: `name` or `Type::name`, the enclosing item for a
    /// closure.
    pub(crate) item: String,
    /// The type of the `impl` block the function or closure is in.
    pub(crate) owner: Option<String>,
    pub(crate) body: Body<'a>,
    #[cfg(test)]
    pub(crate) why: Why,
}

impl Unit<'_> {
    /// The line the function's name, or the closure, is on.
    pub(crate) fn body_line(&self) -> usize {
        match &self.body {
            Body::Function { signature, .. } => signature.ident.span().start().line,
            Body::Closure(closure) => closure.span().start().line,
        }
    }
}

/// A closure inside read code, with the bindings of the function around it
/// that it can capture and their types, when known.
pub(crate) struct ReadClosure<'a> {
    pub(crate) file: &'a RustFile,
    /// The item the closure is in.
    pub(crate) item: String,
    pub(crate) owner: Option<String>,
    pub(crate) expr: syn::ExprClosure,
    pub(crate) captures: BTreeMap<String, Option<String>>,
    /// Whether the closure is itself passed to a read place from a function
    /// that is no read code: it outlives that call, so what it captures from
    /// it is kept between reads. A closure inside read code runs within one
    /// call, and the locals it captures are that call's.
    pub(crate) outlives_its_function: bool,
}

pub(crate) struct ReadCode<'a> {
    units: Vec<Unit<'a>>,
    closures: Vec<ReadClosure<'a>>,
    index: Index<'a>,
    /// The calls, by file and place, to a function taking a read type: they
    /// hand over a read value, so they run SQL.
    handoffs: BTreeSet<(String, CallSite)>,
    /// The workspace functions each call resolves to, by file and place.
    callees: BTreeMap<(String, CallSite), Vec<usize>>,
}

impl<'a> ReadCode<'a> {
    pub(crate) fn collect(files: &'a [RustFile], policy: &Policy, shapes: &ReadShapes) -> Self {
        let resolver = NameResolver::collect(files);
        let index = Index::collect(files, &resolver, policy, shapes);
        let mut walked = Walked::default();
        for (id, callable) in index.callables.iter().enumerate() {
            if let (false, Some(body)) = (callable.plumbing, callable.body) {
                let mut walker = Walker {
                    context: Context { index: &index, resolver: &resolver, shapes },
                    callable: id,
                    scopes: vec![BTreeMap::new()],
                    open: Vec::new(),
                    marks: BTreeMap::new(),
                    out: &mut walked,
                };
                walker.run(callable, body);
            }
        }
        let holders = live_query_holders(&index, shapes);
        let takes_read = index
            .callables
            .iter()
            .enumerate()
            .filter(|(_, callable)| takes_read_value(callable, shapes, &holders))
            .map(|(id, _)| id)
            .collect::<BTreeSet<_>>();
        let handoffs = walked
            .calls
            .iter()
            .filter(|call| call.targets.iter().any(|target| takes_read.contains(target)))
            .map(|call| (index.callables[call.callable].file.relative_path.clone(), call.site))
            .collect();
        let callees = walked
            .calls
            .iter()
            .filter(|call| !call.targets.is_empty())
            .map(|call| {
                let path = index.callables[call.callable].file.relative_path.clone();
                ((path, call.site), call.targets.clone())
            })
            .collect();
        let (units, closures) = units(&index, &walked, &takes_read);
        Self { units, closures, index, handoffs, callees }
    }

    /// Every closure in read code: a unit, or inside one.
    pub(crate) fn closures(&self) -> &[ReadClosure<'a>] {
        &self.closures
    }

    /// The workspace's functions, methods and fields the search read.
    pub(crate) fn index(&self) -> &Index<'a> {
        &self.index
    }

    pub(crate) fn units(&self) -> &[Unit<'a>] {
        &self.units
    }

    /// The functions of the index (`index().callables`, by position) the call
    /// at `site` in `path` resolves to.
    pub(crate) fn callees(&self, path: &str, site: CallSite) -> &[usize] {
        self.callees.get(&(path.to_string(), site)).map_or(&[], Vec::as_slice)
    }

    /// Whether the call at `site` in `path` hands a read value to its callee.
    pub(crate) fn hands_read_value(&self, path: &str, site: CallSite) -> bool {
        self.handoffs.contains(&(path.to_string(), site))
    }
}

/// The units of read code: the roots, what is passed to a read place, and
/// what they call, transitively.
fn units<'a>(
    index: &Index<'a>,
    walked: &Walked,
    takes_read: &BTreeSet<usize>,
) -> (Vec<Unit<'a>>, Vec<ReadClosure<'a>>) {
    let readable = |id: usize| {
        let callable = &index.callables[id];
        !callable.plumbing && callable.body.is_some()
    };
    let mut callables: BTreeMap<usize, Why> = BTreeMap::new();
    let mut closures: BTreeMap<usize, Why> = BTreeMap::new();
    let mut queue = Vec::new();
    for &id in takes_read {
        if readable(id) {
            callables.insert(id, Why::Root);
            queue.push(Work::Callable(id));
        }
    }
    for (&id, &why) in &walked.passed_paths {
        if readable(id) && !callables.contains_key(&id) {
            callables.insert(id, why);
            queue.push(Work::Callable(id));
        }
    }
    for (position, closure) in walked.closures.iter().enumerate() {
        let why = if closure.typed_read { Some(Why::Root) } else { closure.passed };
        if let Some(why) = why {
            closures.insert(position, why);
            queue.push(Work::Closure(position));
        }
    }
    let mut calls_of = vec![Vec::new(); index.callables.len()];
    for (position, call) in walked.calls.iter().enumerate() {
        calls_of[call.callable].push(position);
    }
    while let Some(work) = queue.pop() {
        let (callable, closure) = match work {
            Work::Callable(id) => (id, None),
            Work::Closure(position) => (walked.closures[position].callable, Some(position)),
        };
        for &position in &calls_of[callable] {
            let call = &walked.calls[position];
            if closure.is_some_and(|closure| !walked.inside(call.closure, closure)) {
                continue;
            }
            for &target in &call.targets {
                if readable(target) && !callables.contains_key(&target) {
                    callables.insert(target, Why::Reached);
                    queue.push(Work::Callable(target));
                }
            }
        }
    }
    let mut units = Vec::new();
    for (&id, &why) in &callables {
        let callable = &index.callables[id];
        let Some(block) = callable.body else { continue };
        units.push(unit(callable, Body::Function { block, signature: callable.signature }, why));
    }
    for (&position, &why) in &closures {
        let closure = &walked.closures[position];
        let covered = callables.contains_key(&closure.callable)
            || ancestors(&walked.closures, closure.parent)
                .any(|above| closures.contains_key(&above));
        if covered {
            continue;
        }
        let callable = &index.callables[closure.callable];
        units.push(unit(callable, Body::Closure(closure.expr.clone()), why));
    }
    let read_closures = walked
        .closures
        .iter()
        .enumerate()
        .filter(|(position, closure)| {
            closures.contains_key(position)
                || callables.contains_key(&closure.callable)
                || ancestors(&walked.closures, closure.parent)
                    .any(|above| closures.contains_key(&above))
        })
        .map(|(position, closure)| {
            let callable = &index.callables[closure.callable];
            ReadClosure {
                file: callable.file,
                item: callable.item(),
                owner: callable.owner.clone(),
                expr: closure.expr.clone(),
                captures: closure.captures.clone(),
                outlives_its_function: closures.contains_key(&position)
                    && !callables.contains_key(&closure.callable)
                    && !ancestors(&walked.closures, closure.parent)
                        .any(|above| closures.contains_key(&above)),
            }
        })
        .collect();
    (units, read_closures)
}

/// A unit; why it is read code is kept for the tests, which tell the causes
/// apart.
fn unit<'a>(callable: &Callable<'a>, body: Body<'a>, _why: Why) -> Unit<'a> {
    Unit {
        file: callable.file,
        item: callable.item(),
        owner: callable.owner.clone(),
        body,
        #[cfg(test)]
        why: _why,
    }
}

/// The closures enclosing one, innermost first, starting at `from`.
fn ancestors(closures: &[ClosureInfo], from: Option<usize>) -> impl Iterator<Item = usize> + '_ {
    std::iter::successors(from, |&position| closures[position].parent)
}

enum Work {
    Callable(usize),
    Closure(usize),
}

/// Whether a function takes a value of a read type (a callback that is given
/// one takes none). A live query is handed over to be read only by the type
/// that holds one: a function elsewhere that takes a live query to wrap or
/// pass on (a service's `until_closed(&self, query)`) reads nothing.
fn takes_read_value(callable: &Callable, shapes: &ReadShapes, holders: &BTreeSet<String>) -> bool {
    callable.signature.inputs.iter().any(|input| match input {
        syn::FnArg::Typed(typed) => {
            let taken = names(&typed.ty, false);
            let mut read = taken.intersection(&shapes.read_types).peekable();
            read.peek().is_some()
                && (read.any(|name| !shapes.live_query_types.contains(name))
                    || callable.owner.as_ref().is_some_and(|owner| holders.contains(owner)))
        }
        syn::FnArg::Receiver(_) => false,
    })
}

/// The types that hold a live query, directly or through the fields of
/// the workspace's types they hold.
fn live_query_holders(index: &Index<'_>, shapes: &ReadShapes) -> BTreeSet<String> {
    let mut holders = BTreeSet::new();
    loop {
        let before = holders.len();
        for ((owner, _), field) in &index.fields {
            if shapes.live_query_types.contains(field) || holders.contains(field) {
                holders.insert(owner.clone());
            }
        }
        if holders.len() == before {
            return holders;
        }
    }
}

/// A closure and where it is.
struct ClosureInfo {
    expr: syn::ExprClosure,
    /// The function or method it is in.
    callable: usize,
    /// The closure it is directly inside.
    parent: Option<usize>,
    /// A parameter of a read type, written on the closure.
    typed_read: bool,
    /// Why it is read code, when it is passed to a read place.
    passed: Option<Why>,
    /// The bindings in scope where it is written, and their types.
    captures: BTreeMap<String, Option<String>>,
}

/// A call in a function's body.
struct CallInfo {
    callable: usize,
    /// The closure it is directly inside.
    closure: Option<usize>,
    site: CallSite,
    targets: Vec<usize>,
}

#[derive(Default)]
struct Walked {
    closures: Vec<ClosureInfo>,
    calls: Vec<CallInfo>,
    /// The functions passed by path to a read place.
    passed_paths: BTreeMap<usize, Why>,
}

impl Walked {
    /// Whether a call directly inside `closure` is inside `outer`.
    fn inside(&self, closure: Option<usize>, outer: usize) -> bool {
        ancestors(&self.closures, closure).any(|enclosing| enclosing == outer)
    }
}

struct Context<'i, 'a> {
    index: &'i Index<'a>,
    resolver: &'i NameResolver,
    shapes: &'i ReadShapes,
}

/// An argument that is code a callee may run: a closure, or the functions a
/// path names.
enum Passed {
    Closure(usize),
    Functions(Vec<usize>),
}

/// How a call names its callee, for finding the parameter an argument is.
enum Call<'c> {
    /// `path(args)`.
    Path,
    /// `receiver.method(args)`; the receiver's type when known, and whether
    /// it is a read type; else the methods of that name.
    Method { known: bool, receiver_read: bool, candidates: &'c [usize] },
}

struct Walker<'i, 'a> {
    context: Context<'i, 'a>,
    callable: usize,
    /// The names in scope and the type of each, when known.
    scopes: Vec<BTreeMap<String, Option<String>>>,
    open: Vec<usize>,
    /// The closures, by address, passed to a read place in the call being
    /// visited.
    marks: BTreeMap<usize, Why>,
    out: &'i mut Walked,
}

const TYPE_PRESERVING: [&str; 8] =
    ["clone", "unwrap", "expect", "as_ref", "as_mut", "as_deref", "to_owned", "cloned"];

impl Walker<'_, '_> {
    fn callable(&self) -> &Callable<'_> {
        &self.context.index.callables[self.callable]
    }

    fn owner(&self) -> Option<String> {
        self.callable().owner.clone()
    }

    fn run(&mut self, callable: &Callable<'_>, body: &syn::Block) {
        for input in &callable.signature.inputs {
            if let syn::FnArg::Typed(typed) = input {
                let ty = peel(&typed.ty, callable.owner.as_deref(), &self.context.index.pointers);
                self.bind(&typed.pat, ty);
            }
        }
        self.visit_block(body);
    }

    fn lookup(&self, name: &str) -> Option<&Option<String>> {
        self.scopes.iter().rev().find_map(|scope| scope.get(name))
    }

    fn bind(&mut self, pattern: &syn::Pat, ty: Option<String>) {
        let scope = self.scopes.last_mut().expect("a scope");
        match pattern {
            syn::Pat::Ident(ident) => {
                scope.insert(ident.ident.to_string(), ty);
            }
            other => {
                let mut found = PatternNames(Vec::new());
                found.visit_pat(other);
                for name in found.0 {
                    scope.insert(name, None);
                }
            }
        }
    }

    /// The type an expression is, by name, when it is known.
    fn type_of(&self, expr: &syn::Expr) -> Option<String> {
        let index = self.context.index;
        match expr {
            syn::Expr::Paren(inner) => self.type_of(&inner.expr),
            syn::Expr::Group(inner) => self.type_of(&inner.expr),
            syn::Expr::Reference(inner) => self.type_of(&inner.expr),
            syn::Expr::Try(inner) => self.type_of(&inner.expr),
            syn::Expr::Unary(inner) if matches!(inner.op, syn::UnOp::Deref(_)) => {
                self.type_of(&inner.expr)
            }
            syn::Expr::Path(path) if path.qself.is_none() => {
                let name = path.path.get_ident()?.to_string();
                if name == "self" {
                    return self.owner();
                }
                self.lookup(&name).cloned().flatten()
            }
            syn::Expr::Field(field) => {
                let syn::Member::Named(name) = &field.member else { return None };
                let base = self.type_of(&field.base)?;
                index.fields.get(&(base, name.to_string())).cloned()
            }
            syn::Expr::MethodCall(call) => {
                let name = call.method.to_string();
                match self.type_of(&call.receiver) {
                    Some(receiver) => match index.methods.get(&(receiver.clone(), name.clone())) {
                        Some(targets) => self.returned(targets),
                        None => TYPE_PRESERVING.contains(&name.as_str()).then_some(receiver),
                    },
                    None => self.returned(&self.unique_named(&name, &call.args)),
                }
            }
            syn::Expr::Call(call) => {
                let syn::Expr::Path(path) = call.func.as_ref() else { return None };
                if path.qself.is_some() {
                    return None;
                }
                if let Some(returned) = self.returned(&self.path_targets(&path.path)) {
                    return Some(returned);
                }
                let segments = &path.path.segments;
                let owner = segments.iter().rev().nth(1)?.ident.to_string();
                let owner = if owner == "Self" { self.owner()? } else { owner };
                index.declared.contains(&owner).then_some(owner)
            }
            syn::Expr::Struct(literal) => {
                let name = literal.path.segments.last()?.ident.to_string();
                if name == "Self" {
                    self.owner()
                } else {
                    Some(name)
                }
            }
            _ => None,
        }
    }

    /// The type the first of these returns, by name.
    fn returned(&self, targets: &[usize]) -> Option<String> {
        let callable = &self.context.index.callables[*targets.first()?];
        let syn::ReturnType::Type(_, ty) = &callable.signature.output else { return None };
        peel(ty, callable.owner.as_deref(), &self.context.index.pointers)
    }

    /// The one method of this name in the workspace that takes a callback
    /// where the call passes a closure or a function path, when it is one.
    /// Methods that take no callback are never found by name: `.find(..)`,
    /// `.cancel()` and `.words()` are as likely std's.
    fn unique_named(
        &self,
        name: &str,
        arguments: &Punctuated<syn::Expr, syn::Token![,]>,
    ) -> Vec<usize> {
        let index = self.context.index;
        let passes_callback = |callable: &Callable<'_>| {
            arguments.iter().enumerate().any(|(place, argument)| {
                callable.takes_callback(place, false) && self.passed(argument).is_some()
            })
        };
        let named = index.by_name.get(name).map_or(&[][..], Vec::as_slice);
        let mut taking = named.iter().copied().filter(|&id| passes_callback(&index.callables[id]));
        match (taking.next(), taking.next()) {
            (Some(only), None) => vec![only],
            _ => Vec::new(),
        }
    }

    /// The functions and methods a path names: `Type::f` and `Self::f` by
    /// their type, anything else as the resolver resolves it.
    fn path_targets(&self, path: &syn::Path) -> Vec<usize> {
        let index = self.context.index;
        let segments = &path.segments;
        let Some(last) = segments.last() else { return Vec::new() };
        let name = last.ident.to_string();
        if segments.len() >= 2 {
            let owner = segments[segments.len() - 2].ident.to_string();
            if owner == "Self" || owner.starts_with(char::is_uppercase) {
                let Some(owner) = (if owner == "Self" { self.owner() } else { Some(owner) }) else {
                    return Vec::new();
                };
                return index.methods.get(&(owner, name)).cloned().unwrap_or_default();
            }
        } else if self.lookup(&name).is_some() {
            return Vec::new();
        }
        self.context
            .resolver
            .resolve(&self.callable().file.relative_path, last.ident.span(), &qualified_path(path))
            .ok()
            .and_then(|resolved| index.free.get(&resolved).copied())
            .into_iter()
            .collect()
    }

    fn passed(&self, argument: &syn::Expr) -> Option<Passed> {
        match argument {
            syn::Expr::Closure(closure) => {
                Some(Passed::Closure(std::ptr::from_ref(closure) as usize))
            }
            syn::Expr::Reference(inner) => self.passed(&inner.expr),
            syn::Expr::Paren(inner) => self.passed(&inner.expr),
            syn::Expr::Group(inner) => self.passed(&inner.expr),
            syn::Expr::Path(path) if path.qself.is_none() => {
                let targets = self.path_targets(&path.path);
                (!targets.is_empty()).then_some(Passed::Functions(targets))
            }
            _ => None,
        }
    }

    /// Whether the argument at `index` goes where a read closure does.
    fn read_place(&self, call: &Call<'_>, targets: &[usize], index: usize) -> Option<Why> {
        let index_of = &self.context.index.callables;
        let shapes = self.context.shapes;
        match call {
            Call::Method { receiver_read: true, .. } => Some(Why::PassedToReadReceiver),
            Call::Method { known: false, candidates, .. } => {
                let callbacks = candidates
                    .iter()
                    .map(|&id| &index_of[id])
                    .filter(|callable| callable.takes_callback(index, false))
                    .collect::<Vec<_>>();
                (!callbacks.is_empty()
                    && callbacks
                        .iter()
                        .all(|callable| callable.takes_read_callback(index, false, shapes)))
                .then_some(Why::PassedToReadCallee)
            }
            Call::Method { .. } => targets
                .iter()
                .any(|&id| index_of[id].takes_read_callback(index, false, shapes))
                .then_some(Why::PassedToReadCallee),
            Call::Path => targets
                .iter()
                .any(|&id| index_of[id].takes_read_callback(index, true, shapes))
                .then_some(Why::PassedToReadCallee),
        }
    }

    /// Records a call and marks what it passes to a read place.
    fn record(
        &mut self,
        site: CallSite,
        targets: Vec<usize>,
        call: &Call<'_>,
        arguments: &Punctuated<syn::Expr, syn::Token![,]>,
    ) {
        for (index, argument) in arguments.iter().enumerate() {
            let (Some(passed), Some(why)) =
                (self.passed(argument), self.read_place(call, &targets, index))
            else {
                continue;
            };
            match passed {
                Passed::Closure(address) => {
                    self.marks.insert(address, why);
                }
                Passed::Functions(functions) => {
                    for function in functions {
                        self.out.passed_paths.entry(function).or_insert(why);
                    }
                }
            }
        }
        self.out.calls.push(CallInfo {
            callable: self.callable,
            closure: self.open.last().copied(),
            site,
            targets,
        });
    }

    fn forget(&mut self, arguments: &Punctuated<syn::Expr, syn::Token![,]>) {
        for argument in arguments {
            if let syn::Expr::Closure(closure) = argument {
                self.marks.remove(&(std::ptr::from_ref(closure) as usize));
            }
        }
    }
}

struct PatternNames(Vec<String>);

impl<'ast> Visit<'ast> for PatternNames {
    fn visit_pat_ident(&mut self, node: &'ast syn::PatIdent) {
        self.0.push(node.ident.to_string());
        visit::visit_pat_ident(self, node);
    }
}

impl<'ast> Visit<'ast> for Walker<'_, '_> {
    // Nested items are callables of their own.
    fn visit_item(&mut self, _: &'ast syn::Item) {}

    fn visit_block(&mut self, node: &'ast syn::Block) {
        self.scopes.push(BTreeMap::new());
        visit::visit_block(self, node);
        self.scopes.pop();
    }

    fn visit_local(&mut self, node: &'ast syn::Local) {
        if let Some(init) = &node.init {
            self.visit_expr(&init.expr);
            if let Some((_, diverge)) = &init.diverge {
                self.visit_expr(diverge);
            }
        }
        let (pattern, declared) = match &node.pat {
            syn::Pat::Type(typed) => (
                typed.pat.as_ref(),
                peel(&typed.ty, self.callable().owner.as_deref(), &self.context.index.pointers),
            ),
            pattern => (pattern, None),
        };
        let ty = declared.or_else(|| node.init.as_ref().and_then(|init| self.type_of(&init.expr)));
        self.bind(pattern, ty);
    }

    fn visit_arm(&mut self, node: &'ast syn::Arm) {
        self.scopes.push(BTreeMap::new());
        self.bind(&node.pat, None);
        self.visit_pat(&node.pat);
        self.visit_expr(&node.body);
        self.scopes.pop();
    }

    fn visit_expr_let(&mut self, node: &'ast syn::ExprLet) {
        self.visit_expr(&node.expr);
        self.bind(&node.pat, None);
    }

    fn visit_expr_for_loop(&mut self, node: &'ast syn::ExprForLoop) {
        self.visit_expr(&node.expr);
        self.scopes.push(BTreeMap::new());
        self.bind(&node.pat, None);
        self.visit_block(&node.body);
        self.scopes.pop();
    }

    fn visit_expr_closure(&mut self, node: &'ast syn::ExprClosure) {
        let read_types = &self.context.shapes.read_types;
        let typed_read = node.inputs.iter().any(|input| {
            matches!(input, syn::Pat::Type(typed) if !names(&typed.ty, false).is_disjoint(read_types))
        });
        let address = std::ptr::from_ref(node) as usize;
        let position = self.out.closures.len();
        self.out.closures.push(ClosureInfo {
            expr: node.clone(),
            callable: self.callable,
            parent: self.open.last().copied(),
            typed_read,
            passed: self.marks.remove(&address),
            captures: self
                .scopes
                .iter()
                .flat_map(|scope| scope.iter().map(|(name, ty)| (name.clone(), ty.clone())))
                .collect(),
        });
        self.scopes.push(BTreeMap::new());
        for input in &node.inputs {
            match input {
                syn::Pat::Type(typed) => {
                    let ty = peel(
                        &typed.ty,
                        self.callable().owner.as_deref(),
                        &self.context.index.pointers,
                    );
                    self.bind(&typed.pat, ty);
                }
                other => self.bind(other, None),
            }
        }
        self.open.push(position);
        self.visit_expr(&node.body);
        self.open.pop();
        self.scopes.pop();
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        let targets = match node.func.as_ref() {
            syn::Expr::Path(path) if path.qself.is_none() => self.path_targets(&path.path),
            _ => Vec::new(),
        };
        self.record(call_site(node.span()), targets, &Call::Path, &node.args);
        visit::visit_expr_call(self, node);
        self.forget(&node.args);
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        let name = node.method.to_string();
        let receiver = self.type_of(&node.receiver);
        let index = self.context.index;
        let (targets, candidates) = match &receiver {
            Some(receiver) => (
                index.methods.get(&(receiver.clone(), name.clone())).cloned().unwrap_or_default(),
                Vec::new(),
            ),
            None => (
                self.unique_named(&name, &node.args),
                index.by_name.get(&name).cloned().unwrap_or_default(),
            ),
        };
        let call = Call::Method {
            known: receiver.is_some(),
            receiver_read: receiver
                .as_ref()
                .is_some_and(|receiver| self.context.shapes.read_types.contains(receiver)),
            candidates: &candidates,
        };
        self.record(call_site(node.method.span()), targets, &call, &node.args);
        visit::visit_expr_method_call(self, node);
        self.forget(&node.args);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

#[cfg(test)]
#[path = "read_code_tests.rs"]
mod tests;
