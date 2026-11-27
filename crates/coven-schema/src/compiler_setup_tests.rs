use sqlive_compose::compiler::{compile_crate, CompileError, Input};

use super::compiler_setup;

/// Compile the crate whose `src/lib.rs` is `lib`, against coven's setup.
fn compile(lib: &str) -> Result<(), Vec<String>> {
    let reader = |path: &str| match path {
        "src/lib.rs" => Ok(lib.to_owned()),
        _ => Err(std::io::Error::from(std::io::ErrorKind::NotFound)),
    };
    let compiled = compile_crate(Input {
        crate_name: "photos".to_owned(),
        source_reader: &reader,
        manifests: Vec::new(),
        setup: compiler_setup(),
    });
    compiled.map(drop).map_err(|errors| errors.iter().map(CompileError::to_string).collect())
}

/// With coven's setup alone, a build refuses a read-only handle's read of
/// each view and table only the writer reads, naming it.
#[test]
fn a_read_only_read_of_what_only_the_writer_reads_fails_the_build() {
    for name in crate::WRITER_ONLY {
        let lib = format!(
            "sqlive_compose::read!(Rows {{ total: i64 }}, \"SELECT count(*) AS total FROM {name}\");"
        );
        let errors = compile(&lib).expect_err(name);
        let [error] = &errors[..] else { panic!("{name}: {errors:?}") };
        assert!(error.contains(&format!("reads {name}, which only the writer reads")), "{error}");
    }
}

/// The writer's read of each compiles, as a read-only handle's read of a view
/// it may read does, with the functions coven's views call.
#[test]
fn every_other_read_of_covens_objects_compiles() {
    for name in crate::READABLE {
        let handle = if crate::WRITER_ONLY.contains(&name) { "writer " } else { "" };
        let lib = format!(
            "sqlive_compose::read!({handle}Rows {{ total: i64 }}, \"SELECT count(*) AS total FROM {name}\");"
        );
        compile(&lib).unwrap_or_else(|errors| panic!("{name}: {errors:?}"));
    }
}
