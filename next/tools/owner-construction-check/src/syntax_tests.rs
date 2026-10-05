use super::*;

/// An external crate's `Store::new` does not match a local `Store`, whether
/// it is written in code or inside a macro call.
#[test]
fn external_associated_factories_do_not_match_local_owner_names() {
    let call: syn::ExprCall = syn::parse_str("apple_native_keyring_store::protected::Store::new()")
        .expect("parse external factory");
    let syn::Expr::Path(external) = call.func.as_ref() else {
        panic!("external factory is a path");
    };
    assert!(!could_be_local_associated_function_path(&path_names(
        &external.path
    )));

    for local in ["Store::new()", "crate::sync::store::Store::new()"] {
        let call: syn::ExprCall = syn::parse_str(local).expect("parse local factory");
        let syn::Expr::Path(path) = call.func.as_ref() else {
            panic!("local factory is a path");
        };
        assert!(could_be_local_associated_function_path(&path_names(
            &path.path
        )));
    }
}
