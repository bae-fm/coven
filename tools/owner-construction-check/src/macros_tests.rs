use super::*;
use std::collections::BTreeSet;

fn mac(source: &str) -> syn::Macro {
    syn::parse_str(source).expect("parse fixture macro")
}

#[test]
fn call_arguments_parse_as_expressions() {
    assert!(matches!(
        parse_macro_body(&mac(r#"format!("{}", uuid::Uuid::new_v4())"#)),
        Some(MacroBody::Expressions(expressions)) if expressions.len() == 2
    ));
}

#[test]
fn statement_bodies_parse_as_statements() {
    assert!(matches!(
        parse_macro_body(&mac("run! { let value = compute(); store(value); }")),
        Some(MacroBody::Statements(statements)) if statements.len() == 2
    ));
}

#[test]
fn a_macro_rules_body_is_read_as_tokens() {
    let rules = syn::parse_str::<syn::ItemMacro>(
        "macro_rules! fresh { () => { ::uuid::Uuid::new_v4() }; }",
    )
    .expect("parse fixture macro_rules")
    .mac;
    assert!(parse_macro_body(&rules).is_none());
    let paths = token_paths(rules.tokens);
    assert!(paths.iter().any(|path| path.segments == ["uuid", "Uuid", "new_v4"]
        && path.followed_by == Some(Delimiter::Parenthesis)
        && path.leading_colon
        && !path.after_dot));
}

#[test]
fn token_paths_name_method_calls_and_their_receiver() {
    let paths = token_paths(mac("m!(self.build(x), handle.spawn(y))").tokens);
    let methods = paths
        .iter()
        .filter(|path| path.after_dot)
        .map(|path| (path.receiver.as_deref(), path.segments[0].as_str()))
        .collect::<Vec<_>>();
    assert_eq!(methods, [(Some("self"), "build"), (Some("handle"), "spawn")]);
}

#[test]
fn token_strings_reach_nested_groups() {
    let strings = token_strings(mac(r#"m!(a, ["DELETE FROM t", b])"#).tokens)
        .into_iter()
        .map(|string| string.value())
        .collect::<Vec<_>>();
    assert_eq!(strings, ["DELETE FROM t"]);
}

#[test]
fn token_paths_tell_types_from_values() {
    let paths = token_paths(
        mac(r#"m! {
            impl Handle for Bridge {
                fn open(setup: Option<Arc<Setup>>, runtime: &mut Runtime) -> Opened {
                    let pair: (Left, Right) = (Left::new(), Right);
                    let call: Box<dyn Fn(Argument) -> Output> = make(Value);
                    if ready { let typed: Typed = make(Value); }
                    Built { runtime: Runtime::new(), unit: Unit }
                }
            }
        }"#)
        .tokens,
    );
    let named = |type_position: bool| {
        paths
            .iter()
            .filter(|path| path.type_position == type_position && !path.after_dot)
            .map(|path| path.segments.join("::"))
            .collect::<BTreeSet<_>>()
    };
    let types = named(true);
    for name in [
        "Handle", "Bridge", "open", "Option", "Arc", "Setup", "Runtime", "Opened", "Left", "Right",
        "Box", "Fn", "Argument", "Output", "Typed",
    ] {
        assert!(types.contains(name), "{name} is a type: {types:?}");
    }
    let values = named(false);
    for name in ["Left::new", "Right", "make", "Value", "Built", "Runtime::new", "Unit"] {
        assert!(values.contains(name), "{name} is a value: {values:?}");
    }
}
