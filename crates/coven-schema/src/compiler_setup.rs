//! What sqlive-compose's compiler must know of coven, for the build script of
//! an app that compiles its statements (api.md E4).

use sqlive_compose::compiler::{Function, Setup};

/// coven's part of the setup the compiler runs: the SQL that makes each object
/// app SQL may read (`readable_objects_sql`), the SQL functions coven
/// registers on a store's connection (`SQL_FUNCTIONS`), and the views and
/// tables only the store's writer reads (`WRITER_ONLY`). A build script adds
/// its schema's own SQL after coven's, with the functions and collations that
/// SQL names. coven's SQL names no collation. The compiler takes no list of
/// views to read whole: sqlive decides that from each view's SQL, as it does
/// in a store.
pub fn compiler_setup() -> Setup {
    let functions = crate::SQL_FUNCTIONS
        .iter()
        .map(|function| Function {
            name: function.name.to_owned(),
            arguments: function.arguments,
            deterministic: function.deterministic,
        })
        .collect();
    Setup {
        sql: vec![crate::readable_objects_sql()],
        functions,
        collations: Vec::new(),
        writer_only: crate::WRITER_ONLY.iter().map(|name| (*name).to_owned()).collect(),
    }
}

#[cfg(test)]
#[path = "compiler_setup_tests.rs"]
mod tests;
