use super::*;

/// Expands `invocation` (the tokens inside `name!(…)`) with `definition`'s
/// arms, `$crate` standing for `crate`.
fn expand(definition: &str, invocation: &str) -> Result<String, MacroError> {
    let definition = syn::parse_str::<syn::ItemMacro>(definition).expect("parse definition");
    let rules = MacroRules::parse(definition.mac.tokens)?;
    let expansion =
        Expansion { crate_path: "crate".parse().expect("crate path"), span: Span::call_site() };
    let tokens = rules.expand(invocation.parse().expect("invocation tokens"), &expansion)?;
    Ok(tokens.to_string())
}

#[test]
fn repetitions_write_their_body_once_per_iteration() {
    let definition = "macro_rules! owners { ($($name:ident),* $(,)?) => { $(struct $name;)* }; }";
    assert_eq!(expand(definition, "One, Two,").unwrap(), "struct One ; struct Two ;");
    assert_eq!(expand(definition, "").unwrap(), "");
}

#[test]
fn nested_and_optional_repetitions_keep_each_iteration_apart() {
    let definition = r#"macro_rules! variants {
        ($enum:ident { $($variant:ident $({ $($field:ident),* })?),* $(,)? }) => {
            fn names() { $(let _ = $enum::$variant $({ $($field: 0),* })?;)* }
        };
    }"#;
    assert_eq!(
        expand(definition, "State { Idle, Busy { since, until }, }").unwrap(),
        "fn names () { let _ = State :: Idle ; let _ = State :: Busy { since : 0 , until : 0 } ; }"
    );
}

#[test]
fn arms_are_tried_in_order_and_literal_tokens_select_one() {
    let definition = r#"macro_rules! convert {
        ($value:expr) => { $value };
        ($value:expr, (each $to:path)) => { $value.into_iter().map(<$to>::from_core).collect() };
        ($value:expr, ($to:path)) => { <$to>::from_core($value) };
    }"#;
    assert_eq!(expand(definition, "field").unwrap(), "field");
    assert_eq!(
        expand(definition, "field, (each Bridge)").unwrap(),
        "field . into_iter () . map (< Bridge >:: from_core) . collect ()"
    );
    assert_eq!(expand(definition, "field, (Bridge)").unwrap(), "< Bridge >:: from_core (field)");
}

#[test]
fn a_repetition_backtracks_when_what_follows_needs_its_tokens() {
    let definition = "macro_rules! last { ($(a)* a) => { struct Last; }; }";
    assert_eq!(expand(definition, "a a").unwrap(), "struct Last ;");
}

#[test]
fn a_lifetime_written_in_a_body_stays_one() {
    let definition = "macro_rules! decode { ($ty:ty) => { fn get(input: &mut Decoder<'_>) -> $ty { todo!() } }; }";
    let expanded = expand(definition, "Value").unwrap();
    assert!(syn::parse_str::<syn::ItemFn>(&expanded).is_ok(), "{expanded}");
}

#[test]
fn crate_names_the_defining_crate_where_the_expansion_lands() {
    let definition = "macro_rules! outer { ($x:ident) => { $crate::inner!($x) }; }";
    assert_eq!(expand(definition, "value").unwrap(), "crate :: inner ! (value)");
}

/// An `expr` fragment stays whole, as rustc keeps it inside an invisible
/// group: `$a * 2` with `1 + 1` is `(1 + 1) * 2`.
#[test]
fn an_expression_fragment_stays_one_expression() {
    let definition =
        syn::parse_str::<syn::ItemMacro>("macro_rules! double { ($a:expr) => { $a * 2 }; }")
            .unwrap();
    let rules = MacroRules::parse(definition.mac.tokens).unwrap();
    let expansion = Expansion { crate_path: TokenStream::new(), span: Span::call_site() };
    let tokens = rules.expand("1 + 1".parse().unwrap(), &expansion).unwrap();
    let syn::Expr::Binary(product) = syn::parse2::<syn::Expr>(tokens).unwrap() else {
        panic!("a binary expression")
    };
    assert!(matches!(product.op, syn::BinOp::Mul(_)));
    assert!(matches!(*product.left, syn::Expr::Group(_)));
}

#[test]
fn a_token_tree_is_one_token_as_rustc_lexes_it() {
    let definition = "macro_rules! count { ($($token:tt)*) => { $(item!($token);)* }; }";
    assert_eq!(
        expand(definition, "a => 'b ::").unwrap(),
        "item ! (a) ; item ! (=>) ; item ! ('b) ; item ! (::) ;"
    );
}

#[test]
fn fragments_parse_as_rust_syntax() {
    let definition = r#"macro_rules! forward {
        ($(#[$meta:meta])* $vis:vis fn $name:ident($($arg:ident: $ty:ty),*) -> $ret:ty $body:block) => {
            $(#[$meta])* $vis fn $name($($arg: $ty),*) -> $ret $body
        };
    }"#;
    let invocation =
        "#[doc = \"x\"] pub(crate) fn run(a: Vec<u8>, b: &str) -> Result<(), E> { go(a, b) }";
    let expanded = expand(definition, invocation).unwrap();
    let printed = |source: &str| {
        quote::ToTokens::to_token_stream(&syn::parse_str::<syn::ItemFn>(source).unwrap())
            .to_string()
    };
    assert_eq!(printed(&expanded), printed(invocation));
}

#[test]
fn what_the_checker_cannot_expand_is_an_error() {
    assert!(matches!(
        expand("macro_rules! m { ($x:unknown) => {}; }", "a"),
        Err(MacroError::Fragment(name)) if name == "unknown"
    ));
    assert!(matches!(expand("macro_rules! m { (a) => {}; }", "b"), Err(MacroError::NoArm)));
    assert!(matches!(
        expand("macro_rules! m { ($($x:ident)*) => { $x }; }", "a b"),
        Err(MacroError::StillRepeating(name)) if name == "x"
    ));
}
