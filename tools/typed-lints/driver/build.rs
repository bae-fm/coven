//! The version `--version` prints, as clippy's build script sets it, and the
//! toolchain's library folder, where the driver finds the compiler's shared
//! libraries (librustc_driver and libLLVM) when it links and when it runs.

use std::process::Command;

fn main() {
    rustc_tools_util::setup_version_info!();
    let rustc = std::env::var("RUSTC").expect("cargo names rustc in RUSTC");
    let output = Command::new(rustc)
        .args(["--print", "sysroot"])
        .output()
        .expect("rustc prints its sysroot");
    let sysroot = String::from_utf8(output.stdout).expect("the sysroot is UTF-8");
    let library = format!("{}/lib", sysroot.trim());
    println!("cargo:rustc-link-search=native={library}");
    // Windows finds a DLL through PATH, which holds the toolchain's bin folder
    // when rustup runs cargo; elsewhere the driver records the folder.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("windows") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{library}");
    }
}
