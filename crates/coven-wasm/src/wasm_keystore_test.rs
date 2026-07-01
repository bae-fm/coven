//! Headless wasm proof that the browser keystore persists the device identity.
//!
//! A keypair minted on the first `open` comes back byte-for-byte on a second
//! `open` through a *fresh* [`BrowserKeystore`] handle — so it round-tripped
//! through IndexedDB + WebCrypto (the sealed signing key and its non-extractable
//! wrapping key both survived), not a process-memory cache. `wasm-pack test`
//! gives each run a fresh browser profile, so the first open starts from an empty
//! IndexedDB. Worker-only: IndexedDB and WebCrypto run on the dedicated Worker,
//! like the rest of coven's browser stack.

use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

use crate::keys::verify_signature;
use crate::wasm_keystore::BrowserKeystore;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[wasm_bindgen_test]
async fn keypair_persists_across_keystore_reopen() {
    console_error_panic_hook::set_once();

    // First open mints and persists a new identity.
    let first = BrowserKeystore::open().await.expect("open keystore");
    let kp1 = first
        .get_or_create_user_keypair()
        .await
        .expect("mint keypair");

    // Idempotent within a handle: a second call returns the same identity, not a
    // freshly minted one.
    let kp1_again = first
        .get_or_create_user_keypair()
        .await
        .expect("reload keypair on the same handle");
    assert_eq!(
        kp1_again.signing_key, kp1.signing_key,
        "get_or_create is idempotent within one handle",
    );

    // Drop the in-memory handle so the next open can't reuse its state.
    drop(first);

    // A second, independent open reads the same identity back out of IndexedDB,
    // decrypting it with the persisted non-extractable wrapping key.
    let second = BrowserKeystore::open().await.expect("reopen keystore");
    let kp2 = second
        .get_or_create_user_keypair()
        .await
        .expect("reload keypair after reopen");

    assert_eq!(
        kp2.signing_key, kp1.signing_key,
        "the signing key survived the keystore reopen",
    );
    assert_eq!(
        kp2.public_key, kp1.public_key,
        "the public key survived the keystore reopen",
    );

    // It is a working identity, not just matching bytes: a signature made before
    // the reopen verifies under the reloaded public key.
    let message = b"a changeset signed by this device";
    let signature = kp1.sign(message);
    assert!(
        verify_signature(&signature, message, &kp2.public_key),
        "the reloaded identity verifies a signature from before the reopen",
    );
}
