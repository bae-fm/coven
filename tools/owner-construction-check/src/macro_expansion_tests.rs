use super::*;

#[test]
fn item_declarations_keep_the_invocations_names() {
    let definition = syn::parse_str::<syn::ItemMacro>("macro_rules! owner { ($name:ident, $field:ty, $doc:literal) => { #[doc = $doc] struct $name($field); impl $name { fn get() -> Self { todo!() } } }; }").unwrap();
    let invocation = syn::parse_str::<syn::Macro>("owner!(Worker, Clock, \"worker\")").unwrap();
    let expanded = expand(&definition.mac, &invocation).expect("fixed item expansion");
    let syn::Item::Struct(item) = &expanded.items[0] else { panic!("struct declaration") };
    assert_eq!(item.ident, "Worker");
    assert_eq!(
        crate::syntax::type_names(&item.fields.iter().next().unwrap().ty),
        ["Clock".into()].into()
    );
    assert_eq!(expanded.items.len(), 2);
}

#[test]
fn unsupported_repetitions_do_not_invent_declarations() {
    let definition = syn::parse_str::<syn::ItemMacro>(
        "macro_rules! owners { ($($name:ident),*) => { $(struct $name;)* }; }",
    )
    .unwrap();
    let invocation = syn::parse_str::<syn::Macro>("owners!(One, Two)").unwrap();
    assert!(expand(&definition.mac, &invocation).is_none());
}

#[test]
fn an_unsupported_first_arm_does_not_select_a_later_arm() {
    let definition = syn::parse_str::<syn::ItemMacro>("macro_rules! owner { ($($token:tt)*) => { struct First; }; ($name:ident) => { struct $name; }; }").unwrap();
    let invocation = syn::parse_str::<syn::Macro>("owner!(Second)").unwrap();
    assert!(expand(&definition.mac, &invocation).is_none());
}

#[test]
fn commas_inside_type_arguments_are_not_macro_separators() {
    let definition = syn::parse_str::<syn::ItemMacro>(
        "macro_rules! owner { ($name:ident, $field:ty) => { struct $name($field); }; }",
    )
    .unwrap();
    let invocation = syn::parse_str::<syn::Macro>("owner!(Worker, Result<Clock, Error>)").unwrap();
    assert!(expand(&definition.mac, &invocation).is_some());
}
