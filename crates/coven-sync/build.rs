//! Refuses to build with `CONST_RANDOM_SEED` set. `PendingRecord::BUILD` is
//! drawn by `const_random!` at each compilation; a seed makes every
//! compilation draw the same value.

fn main() {
    println!("cargo:rerun-if-env-changed=CONST_RANDOM_SEED");
    if std::env::var_os("CONST_RANDOM_SEED").is_some() {
        panic!(
            "CONST_RANDOM_SEED is set, and coven-sync cannot build with it: the build id \
             on a pending record (`PendingRecord::BUILD`) must differ between builds, so a \
             refusal made by a bug is rechecked by the build that fixes it. A seed makes \
             every build draw the same id. Unset CONST_RANDOM_SEED."
        );
    }
}
