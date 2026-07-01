fn main() {
    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH")
        .expect("Cargo sets CARGO_CFG_TARGET_ARCH for build scripts");
    let experimental_wasm_enabled = std::env::var_os("CARGO_FEATURE_EXPERIMENTAL_WASM").is_some();

    if target_arch == "wasm32" && !experimental_wasm_enabled {
        panic!(
            "coven's wasm build is experimental; enable the `experimental-wasm` feature to compile it"
        );
    }
}
