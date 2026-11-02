//! A `macro_rules!` definition run as rustc runs it: the arms are tried in
//! order, the first whose matcher matches an invocation's tokens is
//! transcribed with what its fragments bound, `$(…)` repetitions included,
//! nested or not, with or without a separator.
//!
//! A fragment is parsed the way rustc parses it, with `syn` for the
//! fragments that are Rust syntax (`expr`, `ty`, `path`, …) and by token
//! for `ident`, `lifetime`, `literal` and `tt`. An `expr` fragment is
//! transcribed inside an invisible group, so `$a * 2` keeps `$a` whole as
//! rustc does. Hygiene is not modelled: a name in a macro body resolves where
//! the macro is invoked, which is what it does for every item a body declares.
//!
//! The tokens a body contributes take the invocation's span, so whatever the
//! expansion constructs is found on the invocation's line, inside the callable
//! the invocation is in.

use proc_macro2::{Delimiter, Group, Ident, Punct, Spacing, Span, TokenStream, TokenTree};
use std::collections::BTreeMap;
use syn::parse::{ParseStream, Parser};

/// A parsed definition: its arms in order.
pub(crate) struct MacroRules {
    arms: Vec<Arm>,
}

struct Arm {
    matcher: Vec<Matcher>,
    transcriber: Vec<Transcriber>,
}

/// Why a definition cannot be read or an invocation cannot be expanded.
#[derive(Debug, thiserror::Error)]
pub(crate) enum MacroError {
    #[error("its definition is not a list of `(matcher) => {{ body }}` arms")]
    Definition,
    #[error("its matcher uses fragment specifier `{0}`, which the checker does not know")]
    Fragment(String),
    #[error("its matcher has a repetition without `*`, `+` or `?`")]
    Repetition,
    #[error("no arm matches the invocation")]
    NoArm,
    #[error("its body repeats `$(…)` with no variable bound by a repetition")]
    UnboundRepetition,
    #[error("its body repeats variables bound a different number of times")]
    UnevenRepetition,
    #[error("its body uses `${0}` outside the repetition that binds it")]
    StillRepeating(String),
}

#[derive(Clone, Debug)]
enum Token {
    Ident(String),
    Punct(char),
    Literal(String),
}

enum Matcher {
    Token(Token),
    Group(Delimiter, Vec<Matcher>),
    Fragment(String, Fragment),
    Repetition(Repetition<Matcher>),
}

enum Transcriber {
    Token(TokenTree),
    Group(Delimiter, Vec<Transcriber>),
    Variable(Ident),
    Crate,
    Repetition(Repetition<Transcriber>),
}

struct Repetition<T> {
    body: Vec<T>,
    separator: Vec<Token>,
    kleene: Kleene,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Kleene {
    ZeroOrMore,
    OneOrMore,
    ZeroOrOne,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
enum Fragment {
    Ident,
    Lifetime,
    Tt,
    Literal,
    Ty,
    Path,
    Expr,
    Pat,
    PatParam,
    Stmt,
    Block,
    Item,
    Meta,
    Vis,
}

impl Fragment {
    fn named(name: &str) -> Option<Self> {
        Some(match name {
            "ident" => Fragment::Ident,
            "lifetime" => Fragment::Lifetime,
            "tt" => Fragment::Tt,
            "literal" => Fragment::Literal,
            "ty" => Fragment::Ty,
            "path" => Fragment::Path,
            "expr" | "expr_2021" => Fragment::Expr,
            "pat" => Fragment::Pat,
            "pat_param" => Fragment::PatParam,
            "stmt" => Fragment::Stmt,
            "block" => Fragment::Block,
            "item" => Fragment::Item,
            "meta" => Fragment::Meta,
            "vis" => Fragment::Vis,
            _ => return None,
        })
    }
}

/// What a matcher bound: one fragment's tokens, or one binding per
/// iteration of the repetition the variable is in.
#[derive(Clone)]
enum Binding {
    One(TokenStream, Fragment),
    Many(Vec<Binding>),
}

type Bindings = BTreeMap<String, Binding>;

/// Where an expansion lands: what `$crate` stands for there, and the span a
/// body's tokens take.
pub(crate) struct Expansion {
    pub(crate) crate_path: TokenStream,
    pub(crate) span: Span,
}

impl MacroRules {
    /// The arms of `macro_rules! name { … }`, from the definition's tokens.
    pub(crate) fn parse(definition: TokenStream) -> Result<Self, MacroError> {
        let trees = definition.into_iter().collect::<Vec<_>>();
        let mut arms = Vec::new();
        let mut index = 0;
        while index < trees.len() {
            let (
                Some(TokenTree::Group(matcher)),
                Some(TokenTree::Punct(equals)),
                Some(TokenTree::Punct(arrow)),
                Some(TokenTree::Group(body)),
            ) = (
                trees.get(index),
                trees.get(index + 1),
                trees.get(index + 2),
                trees.get(index + 3),
            )
            else {
                return Err(MacroError::Definition);
            };
            if equals.as_char() != '=' || arrow.as_char() != '>' {
                return Err(MacroError::Definition);
            }
            arms.push(Arm {
                matcher: parse_matcher(&trees_of(matcher))?,
                transcriber: parse_transcriber(&trees_of(body)),
            });
            index += 4;
            if is_punct(trees.get(index), ';') {
                index += 1;
            }
        }
        Ok(Self { arms })
    }

    /// The tokens the first matching arm writes for these invocation tokens.
    pub(crate) fn expand(
        &self,
        invocation: TokenStream,
        expansion: &Expansion,
    ) -> Result<TokenStream, MacroError> {
        let input = invocation.into_iter().collect::<Vec<_>>();
        for arm in &self.arms {
            let matched =
                match_sequence(&arm.matcher, &input, 0, Bindings::new(), &mut |end, bindings| {
                    (end == input.len()).then_some(bindings)
                });
            if let Some(bindings) = matched {
                let mut output = TokenStream::new();
                transcribe(&arm.transcriber, &bindings, expansion, &mut output)?;
                return Ok(output);
            }
        }
        Err(MacroError::NoArm)
    }
}

fn trees_of(group: &Group) -> Vec<TokenTree> {
    group.stream().into_iter().collect()
}

fn token(tree: &TokenTree) -> Option<Token> {
    match tree {
        TokenTree::Ident(ident) => Some(Token::Ident(ident.to_string())),
        TokenTree::Punct(punct) => Some(Token::Punct(punct.as_char())),
        TokenTree::Literal(literal) => Some(Token::Literal(literal.to_string())),
        TokenTree::Group(_) => None,
    }
}

fn is_punct(tree: Option<&TokenTree>, character: char) -> bool {
    matches!(tree, Some(TokenTree::Punct(punct)) if punct.as_char() == character)
}

fn kleene(tree: Option<&TokenTree>) -> Option<Kleene> {
    match tree {
        Some(TokenTree::Punct(punct)) => match punct.as_char() {
            '*' => Some(Kleene::ZeroOrMore),
            '+' => Some(Kleene::OneOrMore),
            '?' => Some(Kleene::ZeroOrOne),
            _ => None,
        },
        _ => None,
    }
}

/// After a repetition's group: an optional separator, one token as rustc
/// lexes it (`=>` is one), then `*`, `+` or `?`. A Kleene operator right
/// after the group is the operator, not a separator. Returns the separator,
/// the operator and how many trees they took.
fn separator_and_kleene(trees: &[TokenTree]) -> Option<(Vec<Token>, Kleene, usize)> {
    if let Some(kleene) = kleene(trees.first()) {
        return Some((Vec::new(), kleene, 1));
    }
    let length = token_tree_length(trees)?;
    let separator = trees[..length].iter().map(token).collect::<Option<Vec<_>>>()?;
    let kleene = kleene(trees.get(length))?;
    Some((separator, kleene, length + 1))
}

fn parse_matcher(trees: &[TokenTree]) -> Result<Vec<Matcher>, MacroError> {
    let mut matchers = Vec::new();
    let mut index = 0;
    while index < trees.len() {
        match &trees[index] {
            TokenTree::Punct(dollar) if dollar.as_char() == '$' => match trees.get(index + 1) {
                Some(TokenTree::Ident(name)) if is_punct(trees.get(index + 2), ':') => {
                    let Some(TokenTree::Ident(kind)) = trees.get(index + 3) else {
                        return Err(MacroError::Definition);
                    };
                    let fragment = Fragment::named(&kind.to_string())
                        .ok_or_else(|| MacroError::Fragment(kind.to_string()))?;
                    matchers.push(Matcher::Fragment(name.to_string(), fragment));
                    index += 4;
                }
                Some(TokenTree::Group(group)) if group.delimiter() == Delimiter::Parenthesis => {
                    let (separator, kleene, length) =
                        separator_and_kleene(&trees[index + 2..]).ok_or(MacroError::Repetition)?;
                    let body = parse_matcher(&trees_of(group))?;
                    matchers.push(Matcher::Repetition(Repetition { body, separator, kleene }));
                    index += 2 + length;
                }
                _ => {
                    matchers.push(Matcher::Token(Token::Punct('$')));
                    index += 1;
                }
            },
            TokenTree::Group(group) => {
                matchers.push(Matcher::Group(group.delimiter(), parse_matcher(&trees_of(group))?));
                index += 1;
            }
            tree => {
                matchers.push(Matcher::Token(token(tree).expect("a group is matched above")));
                index += 1;
            }
        }
    }
    Ok(matchers)
}

/// A body's tokens. A `$name` no arm binds is written as it stands, as rustc
/// does for a nested definition's own variables.
fn parse_transcriber(trees: &[TokenTree]) -> Vec<Transcriber> {
    let mut transcribers = Vec::new();
    let mut index = 0;
    while index < trees.len() {
        let tree = &trees[index];
        index += 1;
        match tree {
            TokenTree::Punct(dollar) if dollar.as_char() == '$' => match trees.get(index) {
                Some(TokenTree::Ident(name)) if name == "crate" => {
                    transcribers.push(Transcriber::Crate);
                    index += 1;
                }
                Some(TokenTree::Ident(name)) => {
                    transcribers.push(Transcriber::Variable(name.clone()));
                    index += 1;
                }
                Some(TokenTree::Group(group)) if group.delimiter() == Delimiter::Parenthesis => {
                    match separator_and_kleene(&trees[index + 1..]) {
                        Some((separator, kleene, length)) => {
                            let body = parse_transcriber(&trees_of(group));
                            transcribers.push(Transcriber::Repetition(Repetition {
                                body,
                                separator,
                                kleene,
                            }));
                            index += 1 + length;
                        }
                        None => transcribers.push(Transcriber::Token(tree.clone())),
                    }
                }
                _ => transcribers.push(Transcriber::Token(tree.clone())),
            },
            TokenTree::Group(group) => transcribers
                .push(Transcriber::Group(group.delimiter(), parse_transcriber(&trees_of(group)))),
            tree => transcribers.push(Transcriber::Token(tree.clone())),
        }
    }
    transcribers
}

/// Matches `matchers` against `input` from `position`, calling `next` with
/// where the match ended and what it bound; the first `Some` it returns is
/// the result. Backtracks through repetitions; a fragment parses as rustc
/// parses it, once, with no alternative.
fn match_sequence(
    matchers: &[Matcher],
    input: &[TokenTree],
    position: usize,
    bindings: Bindings,
    next: &mut dyn FnMut(usize, Bindings) -> Option<Bindings>,
) -> Option<Bindings> {
    let Some((first, rest)) = matchers.split_first() else {
        return next(position, bindings);
    };
    match first {
        Matcher::Token(expected) => {
            if !token_matches(expected, input.get(position)?) {
                return None;
            }
            match_sequence(rest, input, position + 1, bindings, next)
        }
        Matcher::Group(delimiter, inner) => {
            let Some(TokenTree::Group(group)) = input.get(position) else { return None };
            if group.delimiter() != *delimiter {
                return None;
            }
            let inner_input = trees_of(group);
            let bindings = match_sequence(inner, &inner_input, 0, bindings, &mut |end, bound| {
                (end == inner_input.len()).then_some(bound)
            })?;
            match_sequence(rest, input, position + 1, bindings, next)
        }
        Matcher::Fragment(name, fragment) => {
            let (length, tokens) = parse_fragment(*fragment, input.get(position..)?)?;
            let mut bindings = bindings;
            bindings.insert(name.clone(), Binding::One(tokens, *fragment));
            match_sequence(rest, input, position + length, bindings, next)
        }
        Matcher::Repetition(repetition) => {
            match_repetition(repetition, rest, input, position, bindings, Vec::new(), next)
        }
    }
}

/// One more iteration of `repetition` if one matches and the rest still
/// does after it, else the repetition ends here.
fn match_repetition(
    repetition: &Repetition<Matcher>,
    rest: &[Matcher],
    input: &[TokenTree],
    position: usize,
    bindings: Bindings,
    iterations: Vec<Bindings>,
    next: &mut dyn FnMut(usize, Bindings) -> Option<Bindings>,
) -> Option<Bindings> {
    let count = iterations.len();
    if !(repetition.kleene == Kleene::ZeroOrOne && count == 1) {
        let start = if count > 0 {
            match_separator(&repetition.separator, input, position)
        } else {
            Some(position)
        };
        if let Some(start) = start {
            let longer = match_sequence(
                &repetition.body,
                input,
                start,
                Bindings::new(),
                &mut |end, bound| {
                    if end == position {
                        return None;
                    }
                    let mut iterations = iterations.clone();
                    iterations.push(bound);
                    match_repetition(
                        repetition,
                        rest,
                        input,
                        end,
                        bindings.clone(),
                        iterations,
                        next,
                    )
                },
            );
            if longer.is_some() {
                return longer;
            }
        }
    }
    if repetition.kleene == Kleene::OneOrMore && count == 0 {
        return None;
    }
    let mut bindings = bindings;
    for name in matcher_variables(&repetition.body) {
        let each = iterations
            .iter()
            .map(|iteration| iteration.get(&name).cloned().unwrap_or(Binding::Many(Vec::new())))
            .collect();
        bindings.insert(name, Binding::Many(each));
    }
    match_sequence(rest, input, position, bindings, next)
}

fn match_separator(separator: &[Token], input: &[TokenTree], position: usize) -> Option<usize> {
    let mut position = position;
    for expected in separator {
        if !token_matches(expected, input.get(position)?) {
            return None;
        }
        position += 1;
    }
    Some(position)
}

fn token_matches(expected: &Token, tree: &TokenTree) -> bool {
    match (expected, tree) {
        (Token::Ident(name), TokenTree::Ident(ident)) => ident == name,
        (Token::Punct(character), TokenTree::Punct(punct)) => punct.as_char() == *character,
        (Token::Literal(text), TokenTree::Literal(literal)) => literal.to_string() == *text,
        _ => false,
    }
}

fn matcher_variables(matchers: &[Matcher]) -> Vec<String> {
    let mut names = Vec::new();
    for matcher in matchers {
        match matcher {
            Matcher::Fragment(name, _) => names.push(name.clone()),
            Matcher::Group(_, inner) => names.extend(matcher_variables(inner)),
            Matcher::Repetition(repetition) => names.extend(matcher_variables(&repetition.body)),
            Matcher::Token(_) => {}
        }
    }
    names
}

/// Rust's multi-character operators, longest first: one token each to
/// rustc, several joint punctuation characters to proc-macro2.
const OPERATORS: &[&str] = &[
    "<<=", ">>=", "...", "..=", "::", "->", "=>", "==", "!=", "<=", ">=", "&&", "||", "+=", "-=",
    "*=", "/=", "%=", "^=", "&=", "|=", "<<", ">>", "..",
];

/// How many trees the token at the start of `trees` takes: a lifetime, a
/// multi-character operator, or one tree.
fn token_tree_length(trees: &[TokenTree]) -> Option<usize> {
    let TokenTree::Punct(first) = trees.first()? else {
        return Some(1);
    };
    if first.as_char() == '\'' && matches!(trees.get(1), Some(TokenTree::Ident(_))) {
        return Some(2);
    }
    let mut joined = String::new();
    for tree in trees {
        let TokenTree::Punct(punct) = tree else { break };
        joined.push(punct.as_char());
        if punct.spacing() == Spacing::Alone {
            break;
        }
    }
    Some(
        OPERATORS
            .iter()
            .find(|operator| joined.starts_with(**operator))
            .map_or(1, |operator| operator.len()),
    )
}

/// The trees a fragment takes from the start of `input`, and their tokens.
fn parse_fragment(fragment: Fragment, input: &[TokenTree]) -> Option<(usize, TokenStream)> {
    let length = match fragment {
        Fragment::Ident => match input.first()? {
            TokenTree::Ident(ident) if ident != "_" => 1,
            _ => return None,
        },
        Fragment::Lifetime => {
            if !is_punct(input.first(), '\'') || !matches!(input.get(1), Some(TokenTree::Ident(_)))
            {
                return None;
            }
            2
        }
        Fragment::Tt => token_tree_length(input)?,
        Fragment::Literal => match (input.first()?, input.get(1)) {
            (TokenTree::Literal(_), _) => 1,
            (TokenTree::Ident(ident), _) if ident == "true" || ident == "false" => 1,
            (TokenTree::Punct(minus), Some(TokenTree::Literal(_))) if minus.as_char() == '-' => 2,
            _ => return None,
        },
        _ => parsed_length(fragment, input)?,
    };
    Some((length, input[..length].iter().cloned().collect()))
}

/// How many trees `syn` takes parsing the fragment at the start of `input`.
fn parsed_length(fragment: Fragment, input: &[TokenTree]) -> Option<usize> {
    let stream = input.iter().cloned().collect::<TokenStream>();
    let parser = |stream: ParseStream<'_>| -> syn::Result<usize> {
        match fragment {
            Fragment::Ty => drop(stream.parse::<syn::Type>()?),
            Fragment::Path => drop(stream.parse::<syn::Path>()?),
            Fragment::Expr => drop(stream.parse::<syn::Expr>()?),
            Fragment::Pat => drop(syn::Pat::parse_multi_with_leading_vert(stream)?),
            Fragment::PatParam => drop(syn::Pat::parse_single(stream)?),
            Fragment::Stmt => drop(stream.parse::<syn::Stmt>()?),
            Fragment::Block => drop(stream.parse::<syn::Block>()?),
            Fragment::Item => drop(stream.parse::<syn::Item>()?),
            Fragment::Meta => drop(stream.parse::<syn::Meta>()?),
            Fragment::Vis => drop(stream.parse::<syn::Visibility>()?),
            Fragment::Ident | Fragment::Lifetime | Fragment::Tt | Fragment::Literal => {
                unreachable!("read by token in parse_fragment")
            }
        }
        Ok(stream.parse::<TokenStream>()?.into_iter().count())
    };
    let remaining = parser.parse2(stream).ok()?;
    Some(input.len() - remaining)
}

fn transcribe(
    transcribers: &[Transcriber],
    bindings: &Bindings,
    expansion: &Expansion,
    output: &mut TokenStream,
) -> Result<(), MacroError> {
    for (index, transcriber) in transcribers.iter().enumerate() {
        match transcriber {
            Transcriber::Token(tree) => {
                let mut tree = tree.clone();
                // A body's punctuation joins only what is written after it
                // in the body (`::`, the quote of `'_`), never what a
                // variable puts there.
                if let TokenTree::Punct(punct) = &tree {
                    let joins = matches!(transcribers.get(index + 1), Some(Transcriber::Token(_)));
                    if punct.spacing() == Spacing::Joint && !joins {
                        tree = TokenTree::Punct(Punct::new(punct.as_char(), Spacing::Alone));
                    }
                }
                tree.set_span(expansion.span);
                output.extend([tree]);
            }
            Transcriber::Group(delimiter, inner) => {
                let mut stream = TokenStream::new();
                transcribe(inner, bindings, expansion, &mut stream)?;
                let mut group = Group::new(*delimiter, stream);
                group.set_span(expansion.span);
                output.extend([TokenTree::Group(group)]);
            }
            Transcriber::Variable(name) => match bindings.get(&name.to_string()) {
                Some(Binding::One(tokens, Fragment::Expr)) => {
                    let span = tokens
                        .clone()
                        .into_iter()
                        .next()
                        .map_or(expansion.span, |tree| tree.span());
                    let mut group = Group::new(Delimiter::None, tokens.clone());
                    group.set_span(span);
                    output.extend([TokenTree::Group(group)]);
                }
                Some(Binding::One(tokens, _)) => output.extend(tokens.clone()),
                Some(Binding::Many(_)) => return Err(MacroError::StillRepeating(name.to_string())),
                None => {
                    let mut dollar = Punct::new('$', Spacing::Alone);
                    dollar.set_span(expansion.span);
                    output.extend([TokenTree::Punct(dollar), TokenTree::Ident(name.clone())]);
                }
            },
            Transcriber::Crate => output.extend(expansion.crate_path.clone()),
            Transcriber::Repetition(repetition) => {
                transcribe_repetition(repetition, bindings, expansion, output)?;
            }
        }
    }
    Ok(())
}

/// A repetition writes its body once per iteration of the variables in it
/// that a repetition bound, which must agree on how many there were.
fn transcribe_repetition(
    repetition: &Repetition<Transcriber>,
    bindings: &Bindings,
    expansion: &Expansion,
    output: &mut TokenStream,
) -> Result<(), MacroError> {
    let repeating = transcriber_variables(&repetition.body)
        .into_iter()
        .filter_map(|name| match bindings.get(&name) {
            Some(Binding::Many(each)) => Some((name, each)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let Some((_, first)) = repeating.first() else {
        return Err(MacroError::UnboundRepetition);
    };
    let count = first.len();
    if repeating.iter().any(|(_, each)| each.len() != count)
        || (repetition.kleene == Kleene::ZeroOrOne && count > 1)
    {
        return Err(MacroError::UnevenRepetition);
    }
    for index in 0..count {
        if index > 0 {
            for (position, token) in repetition.separator.iter().enumerate() {
                let joint = position + 1 < repetition.separator.len();
                output.extend([token_tree(token, joint, expansion.span)]);
            }
        }
        let mut iteration = bindings.clone();
        for (name, each) in &repeating {
            iteration.insert(name.clone(), each[index].clone());
        }
        transcribe(&repetition.body, &iteration, expansion, output)?;
    }
    Ok(())
}

/// A separator's token; `joint` when the next character belongs to the same
/// operator.
fn token_tree(token: &Token, joint: bool, span: Span) -> TokenTree {
    let spacing = if joint { Spacing::Joint } else { Spacing::Alone };
    let mut tree = match token {
        Token::Ident(name) => TokenTree::Ident(Ident::new(name, span)),
        Token::Punct(character) => TokenTree::Punct(Punct::new(*character, spacing)),
        Token::Literal(text) => text
            .parse::<TokenStream>()
            .ok()
            .and_then(|stream| stream.into_iter().next())
            .expect("a literal the lexer read lexes again"),
    };
    tree.set_span(span);
    tree
}

fn transcriber_variables(transcribers: &[Transcriber]) -> Vec<String> {
    let mut names = Vec::new();
    for transcriber in transcribers {
        match transcriber {
            Transcriber::Variable(name) => names.push(name.to_string()),
            Transcriber::Group(_, inner) => names.extend(transcriber_variables(inner)),
            Transcriber::Repetition(repetition) => {
                names.extend(transcriber_variables(&repetition.body));
            }
            Transcriber::Token(_) | Transcriber::Crate => {}
        }
    }
    names
}

#[cfg(test)]
#[path = "macro_rules_tests.rs"]
mod tests;
