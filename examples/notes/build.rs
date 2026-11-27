//! Compiles the app's `coven::fragment!`, `coven::read!` and `coven::write!`
//! calls against its schema and coven's objects that app SQL may read
//! (coven's spec, §4.2).

use sqlive_compose::compiler::{compile_crate, Input};
use std::path::PathBuf;
use std::{env, error::Error, fs};

fn main() -> Result<(), Box<dyn Error>> {
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let out = PathBuf::from(env::var("OUT_DIR")?);
    println!("cargo::rerun-if-changed=schema.sql");

    // coven's part, then the SQL of the app's migrations in the order the app
    // declares them.
    let mut setup = coven_schema::compiler_setup();
    setup.sql.push(include_str!("schema.sql").to_owned());

    let compiled = compile_crate(Input {
        crate_name: env::var("CARGO_PKG_NAME")?,
        macros_crate: "coven".to_owned(),
        source_reader: &|path| fs::read_to_string(root.join(path)),
        manifests: Vec::new(),
        setup,
    });
    let output = match compiled {
        Ok(output) => output,
        Err(errors) => {
            for error in &errors {
                println!("cargo::error={error}");
            }
            return Ok(());
        }
    };

    let generated = out.join("sqlive_compose");
    fs::create_dir_all(&generated)?;
    for (name, text) in &output.files {
        fs::write(generated.join(name), text)?;
    }
    for path in &output.read {
        println!("cargo::rerun-if-changed={}", root.join(path).display());
    }
    for warning in &output.warnings {
        println!("cargo::warning={warning}");
    }
    Ok(())
}
