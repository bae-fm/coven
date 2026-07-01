# coven on the web

Browser integration lives in the `coven-wasm` crate. The native `coven` crate
does not expose browser modules.

`coven-wasm` depends on `coven-core` and owns the JavaScript-facing API. The
production facade does not hard-code an app schema; browser assembly accepts
caller-supplied schema inputs at its boundary.

## Current support

The browser crate is still incomplete. Operations whose browser backend has not
been implemented return explicit unsupported errors.

The exported timestamp API is backed by `coven-core`'s `UpdatedAtStamper`, so
JavaScript callers do not mint timestamp strings by hand.

## Building

```sh
rustup target add wasm32-unknown-unknown
cargo check -p coven-wasm --target wasm32-unknown-unknown
```

The web example lives under `crates/coven-wasm/examples/web`.
