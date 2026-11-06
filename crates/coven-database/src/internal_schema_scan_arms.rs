//! The scan reasons as their sources write them: each arm of
//! `internal_schema_scan_reasons.rs`'s `database_reason` and of each
//! domain's `scan_reason`, so a reason whose statement or scan is gone is
//! found.

use super::Answered;
use std::path::Path;

/// One element of an arm's `(file, template, relation)` pattern: the
/// literals it names, or none when it takes any value.
type Names = Option<Vec<String>>;

struct Arm {
    file: String,
    line: usize,
    reason: String,
    alternatives: Vec<[Names; 3]>,
}

/// Each arm no audited scan answered, and each literal alternative of an
/// arm no audited scan matched.
pub(super) fn unmatched(answered: &Answered) -> Vec<String> {
    let mut failures = Vec::new();
    for arm in arms() {
        if !answered.0.iter().any(|(_, _, _, reason)| *reason == arm.reason) {
            failures.push(format!(
                "{}:{}: no scan needs the reason \"{}\"",
                arm.file, arm.line, arm.reason
            ));
            continue;
        }
        for [file, template, relation] in &arm.alternatives {
            for file in each(file) {
                for template in each(template) {
                    for relation in each(relation) {
                        let matched = answered.0.iter().any(|(f, t, r, reason)| {
                            *reason == arm.reason
                                && file.is_none_or(|file| file == f)
                                && template.is_none_or(|template| template == t)
                                && relation.is_none_or(|relation| relation == r)
                        });
                        if !matched {
                            failures.push(format!(
                                "{}:{}: no scan of {} by {}'s {} needs its reason",
                                arm.file,
                                arm.line,
                                relation.unwrap_or("any relation"),
                                file.unwrap_or("any file"),
                                template.unwrap_or("statements"),
                            ));
                        }
                    }
                }
            }
        }
    }
    failures
}

/// Each literal `names` holds, or one `None` when it takes any value.
fn each(names: &Names) -> Vec<Option<&str>> {
    match names {
        Some(names) => names.iter().map(|name| Some(name.as_str())).collect(),
        None => vec![None],
    }
}

fn arms() -> Vec<Arm> {
    let mut arms = Vec::new();
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for entry in std::fs::read_dir(&src).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_str().unwrap().to_owned();
        let function = match name.as_str() {
            "internal_schema_scan_reasons.rs" => "database_reason",
            _ if name.ends_with("_tests.rs") => "scan_reason",
            _ => continue,
        };
        let file = syn::parse_file(&std::fs::read_to_string(&path).unwrap()).unwrap();
        for item in &file.items {
            let syn::Item::Fn(item) = item else { continue };
            if item.sig.ident != function {
                continue;
            }
            let Some(syn::Stmt::Expr(syn::Expr::Match(matched), None)) = item.block.stmts.last()
            else {
                panic!("{name}: {function} ends in its match");
            };
            for arm in &matched.arms {
                if matches!(arm.pat, syn::Pat::Wild(_)) {
                    continue;
                }
                arms.push(Arm {
                    file: name.clone(),
                    line: syn::spanned::Spanned::span(&arm.pat).start().line,
                    reason: reason(&arm.body)
                        .unwrap_or_else(|| panic!("{name}: an arm's reason is a literal")),
                    alternatives: alternatives(&arm.pat, &name),
                });
            }
        }
    }
    arms
}

/// The literal an arm's `Some("…")` gives.
fn reason(body: &syn::Expr) -> Option<String> {
    if let syn::Expr::Block(block) = body {
        let [syn::Stmt::Expr(body, None)] = block.block.stmts.as_slice() else { return None };
        return reason(body);
    }
    let syn::Expr::Call(call) = body else { return None };
    let [syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(reason), .. })] =
        call.args.iter().collect::<Vec<_>>()[..]
    else {
        return None;
    };
    Some(reason.value())
}

/// Each `(file, template, relation)` tuple of an arm's pattern.
fn alternatives(pattern: &syn::Pat, name: &str) -> Vec<[Names; 3]> {
    match pattern {
        syn::Pat::Or(or) => or.cases.iter().flat_map(|case| alternatives(case, name)).collect(),
        syn::Pat::Paren(paren) => alternatives(&paren.pat, name),
        syn::Pat::Guard(guarded) => alternatives(&guarded.pat, name),
        syn::Pat::Tuple(tuple) => {
            let elements: Vec<Names> = tuple.elems.iter().map(names).collect();
            let [file, template, relation] = <[Names; 3]>::try_from(elements)
                .unwrap_or_else(|_| panic!("{name}: a reason's pattern is a triple"));
            vec![[file, template, relation]]
        }
        _ => panic!(
            "{name}:{}: a reason's pattern is a triple or an or-pattern of triples",
            syn::spanned::Spanned::span(pattern).start().line
        ),
    }
}

fn names(pattern: &syn::Pat) -> Names {
    match pattern {
        syn::Pat::Lit(syn::PatLit { lit: syn::Lit::Str(literal), .. }) => {
            Some(vec![literal.value()])
        }
        syn::Pat::Or(or) => {
            Some(or.cases.iter().flat_map(|case| names(case).unwrap_or_default()).collect())
        }
        _ => None,
    }
}
