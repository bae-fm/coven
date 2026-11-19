//! cargo's workspace wrapper for the typed lints (`tools/typed-lints/clippy.sh`):
//! runs the driver beside it, clippy's own driver with the typed lints, on
//! each crate of the workspace, as cargo would run the driver itself. For a
//! compile of a crate it adds `-Zalways-encode-mir`, so rustc writes the MIR
//! of every function into the crate's metadata, and the lints read the
//! bodies of the workspace's earlier crates from it. RUSTC_BOOTSTRAP names
//! only the crate being compiled, which lets that compile take the unstable
//! option and changes nothing else. cargo compiles the dependencies with
//! plain rustc, never through a workspace wrapper.

use std::ffi::OsString;
use std::process::{Command, ExitCode};

fn main() -> ExitCode {
    let arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    let wrapper = std::env::current_exe().expect("the wrapper knows its own path");
    let driver =
        wrapper.with_file_name(format!("typed-lints-driver{}", std::env::consts::EXE_SUFFIX));
    let mut command = Command::new(&driver);
    command.args(&arguments);
    if let Some(name) = crate_name(&arguments) {
        command.arg("-Zalways-encode-mir").env("RUSTC_BOOTSTRAP", name);
    }
    match command.status() {
        Ok(status) => match status.code() {
            Some(code) => ExitCode::from(u8::try_from(code).unwrap_or(1)),
            None => ExitCode::FAILURE,
        },
        Err(error) => {
            eprintln!("typed lints: {}: {error}", driver.display());
            ExitCode::FAILURE
        }
    }
}

/// The crate a compile's arguments name (`--crate-name`): none for cargo's
/// other calls of rustc, such as `-vV` or `--print`.
fn crate_name(arguments: &[OsString]) -> Option<&OsString> {
    let position = arguments.iter().position(|argument| argument == "--crate-name")?;
    arguments.get(position + 1)
}
