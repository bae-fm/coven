//! Browser keystore: persists coven's device identity (the Ed25519 user keypair)
//! across browser sessions, encrypted at rest.
//!
//! On native, the device signing key lives behind `DeviceKeys`, a
//! `keyring_core::Store` over the OS keychain. That store is synchronous and
//! `Send + Sync`; the browser's secure primitives — WebCrypto and IndexedDB — are
//! asynchronous and single-thread-bound, and there is no synchronous secure
//! storage in a browser at all. So the trait can't wrap them. coven keeps browser
//! keys a different way instead: this async keystore, which the facade awaits at
//! [`open`](crate::wasm_facade::CovenStore::open) to materialize the keypair
//! before the sync runtime starts. Everything coven needs the keypair for happens
//! after that point, so nothing downstream needs the synchronous trait.
//!
//! ## At rest
//!
//! The 64-byte Ed25519 signing key is sealed with AES-GCM under a *non-extractable*
//! `CryptoKey` that lives in IndexedDB. Because the wrapping key is non-extractable,
//! its raw bytes never enter JS or wasm memory — a dump of IndexedDB yields the
//! ciphertext plus a key handle that can only decrypt in-page, never an exportable
//! secret. The signing key itself is the only secret persisted here; the at-rest
//! encryption key and cloud credentials are supplied by the page on each `open`.
//!
//! ## Worker-only
//!
//! Like the rest of coven's browser stack, this runs on the dedicated DB Worker.
//! IndexedDB and WebCrypto are available there, and keeping all key access on the
//! one thread matches the `!Send` wasm `Database` and sync runtime.

use std::cell::RefCell;
use std::rc::Rc;

use rand::RngCore;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    AesGcmParams, AesKeyGenParams, CryptoKey, IdbDatabase, IdbObjectStore, IdbRequest,
    IdbTransactionMode, SubtleCrypto, WorkerGlobalScope,
};

use crate::keys::UserKeypair;

/// The per-origin IndexedDB database and its single object store.
const DB_NAME: &str = "coven-keystore";
const DB_VERSION: u32 = 1;
const STORE: &str = "secrets";
/// Object-store keys: the non-extractable wrapping key, and the sealed signing key.
const WRAP_KEY_ID: &str = "wrap_key";
const SIGNING_KEY_ID: &str = "user_signing_key";
/// AES-GCM standard nonce length, prepended to each ciphertext.
const IV_LEN: usize = 12;
/// Ed25519 keypair byte length (seed + public key) — what the signing key seals to.
const SIGNING_KEY_LEN: usize = 64;

/// An open browser keystore: the IndexedDB handle, WebCrypto's `subtle` interface,
/// and the non-extractable wrapping key that seals secrets at rest.
pub struct BrowserKeystore {
    db: IdbDatabase,
    subtle: SubtleCrypto,
    wrap_key: CryptoKey,
}

impl BrowserKeystore {
    /// Open (creating on first use) the per-origin keystore and ensure its
    /// non-extractable AES-GCM wrapping key exists. Every failure is a `String`
    /// the caller can surface — there is no silent fallback to an unencrypted or
    /// ephemeral key, which would either leak the identity or lose it.
    pub async fn open() -> Result<Self, String> {
        let global = worker_global()?;
        let subtle = global.crypto().map_err(|e| err_str(&e))?.subtle();
        let db = open_db(&global).await?;
        let wrap_key = load_or_create_wrap_key(&db, &subtle).await?;
        Ok(Self {
            db,
            subtle,
            wrap_key,
        })
    }

    /// Load the device's Ed25519 keypair, generating and persisting one on first
    /// use. The same identity is returned on every later `open`, so a reopened tab
    /// keeps the membership identity other devices already trust.
    pub async fn get_or_create_user_keypair(&self) -> Result<UserKeypair, String> {
        if let Some(val) = get(&self.db, SIGNING_KEY_ID).await? {
            let blob = val
                .dyn_into::<js_sys::Uint8Array>()
                .map_err(|_| "stored signing key is not a byte array".to_string())?
                .to_vec();
            let signing_key = self.unseal(&blob).await?;
            return UserKeypair::from_signing_key_bytes(&signing_key)
                .map_err(|e| format!("stored signing key is not a valid keypair: {e}"));
        }

        let keypair = UserKeypair::generate();
        let blob = self.seal(&keypair.to_keypair_bytes()).await?;
        put(
            &self.db,
            SIGNING_KEY_ID,
            js_sys::Uint8Array::from(blob.as_slice()).as_ref(),
        )
        .await?;
        Ok(keypair)
    }

    /// Seal `plaintext` with AES-GCM under the wrapping key, returning `iv || ct`.
    async fn seal(&self, plaintext: &[u8]) -> Result<Vec<u8>, String> {
        let mut iv = [0u8; IV_LEN];
        rand::rng().fill_bytes(&mut iv);
        let params = AesGcmParams::new_with_u8_slice("AES-GCM", &mut iv);
        let promise = self
            .subtle
            .encrypt_with_object_and_u8_array(params.as_ref(), &self.wrap_key, plaintext)
            .map_err(|e| err_str(&e))?;
        let buf = JsFuture::from(promise).await.map_err(|e| err_str(&e))?;
        let ct = js_sys::Uint8Array::new(&buf).to_vec();

        let mut blob = Vec::with_capacity(IV_LEN + ct.len());
        blob.extend_from_slice(&iv);
        blob.extend_from_slice(&ct);
        Ok(blob)
    }

    /// Reverse [`seal`](Self::seal): split `iv || ct`, AES-GCM decrypt, and require
    /// exactly a signing key's worth of bytes back.
    async fn unseal(&self, blob: &[u8]) -> Result<[u8; SIGNING_KEY_LEN], String> {
        if blob.len() <= IV_LEN {
            return Err(format!(
                "stored signing-key blob is too short ({} bytes)",
                blob.len()
            ));
        }
        let (iv, ct) = blob.split_at(IV_LEN);
        let mut iv = iv.to_vec();
        let params = AesGcmParams::new_with_u8_slice("AES-GCM", &mut iv);
        let promise = self
            .subtle
            .decrypt_with_object_and_u8_array(params.as_ref(), &self.wrap_key, ct)
            .map_err(|e| err_str(&e))?;
        let buf = JsFuture::from(promise).await.map_err(|e| err_str(&e))?;
        js_sys::Uint8Array::new(&buf)
            .to_vec()
            .try_into()
            .map_err(|v: Vec<u8>| {
                format!(
                    "decrypted signing key is {} bytes, expected {SIGNING_KEY_LEN}",
                    v.len()
                )
            })
    }
}

/// The dedicated Worker's global scope, which exposes both IndexedDB and WebCrypto.
fn worker_global() -> Result<WorkerGlobalScope, String> {
    js_sys::global()
        .dyn_into::<WorkerGlobalScope>()
        .map_err(|_| "the browser keystore requires a dedicated Worker global scope".to_string())
}

/// Stringify a rejected JS value without dropping its message — a `DOMException`
/// or `Error` carries the real reason, which `{:?}` alone would hide.
fn err_str(e: &JsValue) -> String {
    e.as_string()
        .or_else(|| e.dyn_ref::<web_sys::DomException>().map(|d| d.message()))
        .or_else(|| {
            e.dyn_ref::<js_sys::Error>()
                .map(|er| String::from(er.message()))
        })
        .unwrap_or_else(|| format!("{e:?}"))
}

/// Open the keystore database, creating its object store on the first-ever open.
async fn open_db(global: &WorkerGlobalScope) -> Result<IdbDatabase, String> {
    let factory = global
        .indexed_db()
        .map_err(|e| err_str(&e))?
        .ok_or_else(|| "IndexedDB is unavailable in this context".to_string())?;
    let open_req = factory
        .open_with_u32(DB_NAME, DB_VERSION)
        .map_err(|e| err_str(&e))?;

    // `onupgradeneeded` fires once (version 0 -> 1) before `onsuccess`, the only
    // place an object store may be created. The store does not exist yet at v1, so
    // create it unconditionally. A failure here can't be returned from a JS
    // callback, so record it and surface it after the open completes — otherwise a
    // swallowed error would leave a storeless database that fails confusingly on
    // the first `get`/`put`.
    let upgrade_err: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
    let req_for_upgrade = open_req.clone();
    let err_slot = upgrade_err.clone();
    let onupgrade = Closure::<dyn FnMut()>::new(move || {
        let result = match req_for_upgrade.result() {
            Ok(r) => r,
            Err(e) => {
                *err_slot.borrow_mut() =
                    Some(format!("reading open request result: {}", err_str(&e)));
                return;
            }
        };
        let db = match result.dyn_into::<IdbDatabase>() {
            Ok(db) => db,
            Err(_) => {
                *err_slot.borrow_mut() = Some("upgrade result is not an IdbDatabase".to_string());
                return;
            }
        };
        if let Err(e) = db.create_object_store(STORE) {
            *err_slot.borrow_mut() = Some(format!("create object store {STORE}: {}", err_str(&e)));
        }
    });
    open_req.set_onupgradeneeded(Some(onupgrade.as_ref().unchecked_ref()));

    let open_result = await_request(&open_req).await;
    drop(onupgrade);
    if let Some(e) = upgrade_err.borrow_mut().take() {
        return Err(format!("keystore schema upgrade failed: {e}"));
    }
    open_result?
        .dyn_into::<IdbDatabase>()
        .map_err(|_| "opening the keystore did not yield an IdbDatabase".to_string())
}

/// Load the non-extractable AES-GCM wrapping key, generating and storing one on
/// first use. Generated with `extractable = false`, so its raw bytes never leave
/// the browser's key store even though the `CryptoKey` object is persisted.
async fn load_or_create_wrap_key(
    db: &IdbDatabase,
    subtle: &SubtleCrypto,
) -> Result<CryptoKey, String> {
    if let Some(val) = get(db, WRAP_KEY_ID).await? {
        return val
            .dyn_into::<CryptoKey>()
            .map_err(|_| "the stored wrapping key is not a CryptoKey".to_string());
    }

    let algo = AesKeyGenParams::new("AES-GCM", 256);
    let usages = js_sys::Array::of2(&JsValue::from_str("encrypt"), &JsValue::from_str("decrypt"));
    let promise = subtle
        .generate_key_with_object(algo.as_ref(), false, usages.as_ref())
        .map_err(|e| err_str(&e))?;
    let key = JsFuture::from(promise)
        .await
        .map_err(|e| err_str(&e))?
        .dyn_into::<CryptoKey>()
        .map_err(|_| "generateKey did not return a CryptoKey".to_string())?;

    put(db, WRAP_KEY_ID, key.as_ref()).await?;
    Ok(key)
}

/// Read one value by key. `Ok(None)` if the key is absent (`get` resolves to
/// `undefined`); `Err` only on a real IndexedDB failure.
async fn get(db: &IdbDatabase, key: &str) -> Result<Option<JsValue>, String> {
    let store = object_store(db, IdbTransactionMode::Readonly)?;
    let req = store
        .get(&JsValue::from_str(key))
        .map_err(|e| err_str(&e))?;
    let val = await_request(&req).await?;
    Ok((!val.is_undefined() && !val.is_null()).then_some(val))
}

/// Write one value by key, awaiting the request so the transaction commits before
/// returning.
async fn put(db: &IdbDatabase, key: &str, value: &JsValue) -> Result<(), String> {
    let store = object_store(db, IdbTransactionMode::Readwrite)?;
    let req = store
        .put_with_key(value, &JsValue::from_str(key))
        .map_err(|e| err_str(&e))?;
    await_request(&req).await?;
    Ok(())
}

/// A one-shot transaction over the single store in the given mode, returning the
/// store handle. The transaction stays alive in JS as long as its request is
/// pending, so the caller just awaits the request.
fn object_store(db: &IdbDatabase, mode: IdbTransactionMode) -> Result<IdbObjectStore, String> {
    db.transaction_with_str_and_mode(STORE, mode)
        .and_then(|tx| tx.object_store(STORE))
        .map_err(|e| err_str(&e))
}

/// Await an IndexedDB request, resolving with its result on success and a message
/// on failure. IndexedDB is event-based, so this bridges `onsuccess`/`onerror` to
/// a future; the closures are held alive until the handler fires.
async fn await_request(req: &IdbRequest) -> Result<JsValue, String> {
    let (tx, rx) = tokio::sync::oneshot::channel::<Result<JsValue, String>>();
    let tx = Rc::new(RefCell::new(Some(tx)));

    let req_ok = req.clone();
    let tx_ok = tx.clone();
    let onsuccess = Closure::<dyn FnMut()>::new(move || {
        if let Some(tx) = tx_ok.borrow_mut().take() {
            // A send error means the awaiter was dropped (the future was cancelled);
            // our callers await sequentially so that shouldn't happen, but log it
            // rather than discard it silently if it ever does.
            if tx.send(req_ok.result().map_err(|e| err_str(&e))).is_err() {
                tracing::debug!("keystore request completed but its awaiter was gone");
            }
        }
    });

    let req_err = req.clone();
    let tx_err = tx;
    let onerror = Closure::<dyn FnMut()>::new(move || {
        if let Some(tx) = tx_err.borrow_mut().take() {
            // Read the failure's detail; if even reading `.error` fails, say so
            // rather than hide it behind a generic message.
            let msg = match req_err.error() {
                Ok(Some(e)) => e.message(),
                Ok(None) => "IndexedDB request failed (no error detail)".to_string(),
                Err(e) => format!(
                    "IndexedDB request failed; reading its error detail also failed: {}",
                    err_str(&e)
                ),
            };
            if tx.send(Err(msg)).is_err() {
                tracing::debug!("keystore request failed but its awaiter was gone");
            }
        }
    });

    req.set_onsuccess(Some(onsuccess.as_ref().unchecked_ref()));
    req.set_onerror(Some(onerror.as_ref().unchecked_ref()));

    let out = rx
        .await
        .map_err(|_| "IndexedDB request was dropped before completing".to_string())?;
    // Keep the handlers alive until exactly one of them fired; dropping them here
    // (after the await) frees them without racing the event.
    drop(onsuccess);
    drop(onerror);
    out
}
