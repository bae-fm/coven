use super::*;
use crate::crate_code::checked;
use crate::owner_graph::OwnerGraph;
use crate::policy::Policy;

/// A file of `app-core`, which depends on rusqlite.
fn file(path: &str, source: &str) -> RustFile {
    let mut file = RustFile::fixture(path, source);
    file.extern_crates.insert("rusqlite".into(), "rusqlite".into());
    file
}

/// A workspace `Connection` holding a capability, which makes it an owner,
/// beside rusqlite's.
fn workspace(holders: &str) -> Vec<RustFile> {
    checked(&[
        file("crates/app-core/src/lib.rs", "mod connection; mod holders;"),
        file(
            "crates/app-core/src/connection.rs",
            "pub(crate) struct Connection { socket: Socket }",
        ),
        file("crates/app-core/src/holders.rs", holders),
    ])
}

fn owners(holders: &str) -> BTreeSet<String> {
    let policy = Policy::fixture(r#"capability_types = ["Socket"]"#);
    OwnerGraph::collect(&workspace(holders), &policy).owners
}

/// A path to rusqlite's `Connection`, written out or imported, names no
/// workspace `Connection`; one that resolves to the workspace's does.
#[test]
fn an_external_type_never_stands_for_a_workspace_type_of_its_name() {
    assert_eq!(
        owners(
            r#"
            use rusqlite::Connection as Sql;
            struct Written { connection: rusqlite::Connection }
            struct Renamed { connection: Sql }
            mod imported {
                use rusqlite::Connection;
                struct Imported { connection: Option<Connection> }
            }
            struct Local { connection: std::sync::Arc<crate::connection::Connection> }
            mod used {
                use crate::connection::Connection;
                struct Used { connection: Connection }
            }
            "#,
        ),
        BTreeSet::from(["Connection".into(), "Local".into(), "Used".into()])
    );
}

/// What the rules see: the external path by what it resolves to, and every
/// other path by its name, generic parameters included.
#[test]
fn colliding_external_paths_are_known_by_their_resolved_path() {
    let files = workspace(
        r#"
        use rusqlite::Connection;
        struct Holder<T> { connection: Connection, local: crate::connection::Connection, value: T }
        "#,
    );
    let identity = TypeIdentity::of(&files, &BTreeSet::new());
    let holder = files.iter().find(|file| file.relative_path.ends_with("holders.rs")).unwrap();
    let syn::Item::Struct(item) = &holder.syntax.items[1] else { panic!("the holder struct") };
    let names = item
        .fields
        .iter()
        .map(|field| {
            let syn::Type::Path(ty) = &field.ty else { panic!("a path type") };
            identity.in_file(&holder.relative_path).name(&ty.path).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(names, ["rusqlite::Connection", "Connection", "T"]);
}

/// A projection of an associated type names every type the workspace's
/// trait impls give it, so a generic holder holds what any of them holds.
#[test]
fn a_projection_names_every_type_impls_give_it() {
    assert_eq!(
        owners(
            r#"
            trait Mode { type Held; }
            enum Writer {}
            enum Reader {}
            impl Mode for Writer { type Held = crate::connection::Connection; }
            impl Mode for Reader { type Held = (); }
            struct Projected<M: Mode> { held: M::Held }
            struct Qualified<M: Mode> { held: Option<<M as Mode>::Held> }
            struct Plain<M: Mode> { held: M }
            "#,
        ),
        BTreeSet::from(["Connection".into(), "Projected".into(), "Qualified".into()])
    );
}

/// Two example roots that each declare `mod shared;` for one file both reach
/// its types through the module.
#[test]
fn a_module_file_two_roots_declare_resolves_from_each() {
    let files = checked(&[
        file("crates/app-core/src/lib.rs", ""),
        file(
            "crates/app-core/examples/first.rs",
            "mod shared; struct First { c: shared::Connection }",
        ),
        file(
            "crates/app-core/examples/second.rs",
            "mod shared; struct Second { c: shared::Connection }",
        ),
        file(
            "crates/app-core/examples/shared/mod.rs",
            "pub(crate) struct Connection { socket: Socket }",
        ),
    ]);
    let policy = Policy::fixture(r#"capability_types = ["Socket"]"#);
    assert_eq!(
        OwnerGraph::collect(&files, &policy).owners,
        BTreeSet::from(["Connection".into(), "First".into(), "Second".into()])
    );
}
