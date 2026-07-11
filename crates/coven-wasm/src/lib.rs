use std::{
    cell::RefCell,
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

#[cfg(target_arch = "wasm32")]
pub mod local_blob;
#[cfg(target_arch = "wasm32")]
pub mod wasm;
#[cfg(target_arch = "wasm32")]
pub mod wasm_facade;
#[cfg(target_arch = "wasm32")]
pub mod wasm_keystore;

pub mod blob {
    pub use coven_core::blob::*;
}

pub mod clock {
    pub use coven_core::clock::*;
}

pub mod config {
    pub use coven_core::config::*;
}

pub mod database {
    pub use coven_core::database::*;
}

pub mod encryption {
    pub use coven_core::encryption::*;
}

pub mod keys {
    pub use coven_core::keys::*;
}

pub mod store_dir {
    pub use coven_core::store_dir::*;
}

#[cfg(not(target_arch = "wasm32"))]
pub mod local_blob {
    pub use coven_core::local_blob::*;
}

pub mod migration {
    pub use coven_core::migration::*;
}

pub mod storage {
    pub use coven_core::storage::*;

    pub mod cloud {
        pub use coven_core::storage::cloud::*;

        pub mod s3_common {
            pub use coven_core::storage::cloud::s3_common::*;
        }

        #[cfg(test)]
        pub mod test_utils {
            pub use coven_core::storage::cloud::test_utils::*;
        }

        #[cfg(target_arch = "wasm32")]
        pub mod s3_wasm;
    }
}

pub mod sync {
    pub use coven_core::sync::*;

    #[cfg(test)]
    pub mod test_helpers {
        pub use coven_core::sync::test_helpers::*;
    }

    #[cfg(target_arch = "wasm32")]
    pub mod wasm_runtime;
}

#[cfg(target_arch = "wasm32")]
pub use wasm::install_browser_storage;
#[cfg(target_arch = "wasm32")]
pub use wasm_facade::CovenStore;
#[cfg(target_arch = "wasm32")]
pub use wasm_keystore::BrowserKeystore;

use coven_core::{Hlc, UpdatedAtStamper};
use wasm_bindgen::prelude::*;

thread_local! {
    static STANDALONE_HLCS: RefCell<HashMap<String, Arc<Hlc>>> = RefCell::new(HashMap::new());
}

/// Mint an HLC stamp for callers that do not have an open [`CovenStore`].
///
/// This process-local clock is monotonic per `device_id`, but it is not seeded
/// from an open database and is not advanced by pull. Synced-row writes through an
/// open store use [`CovenStore::stamp`].
#[wasm_bindgen]
pub fn stamp(device_id: String) -> Result<String, JsValue> {
    install_platform();
    let stamper = UpdatedAtStamper::new(
        standalone_hlc_for_device(device_id)
            .map_err(|e| JsValue::from_str(&format!("invalid browser config: device_id {e}")))?,
    );
    Ok(stamper.stamp())
}

fn standalone_hlc_for_device(
    device_id: String,
) -> Result<Arc<Hlc>, crate::store_dir::PathTokenError> {
    STANDALONE_HLCS.with(|hlcs| {
        let mut hlcs = hlcs.borrow_mut();
        match hlcs.entry(device_id.clone()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let hlc = Arc::new(Hlc::try_new(device_id)?);
                Ok(entry.insert(hlc).clone())
            }
        }
    })
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(start)]
pub fn start() {
    install_platform();
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn install_platform() {
    local_blob::install_platform_backend();
    coven_core::database::register_platform_connection_opener(wasm::open_platform_connection);
    coven_core::sync::hlc::register_platform_wall_clock(wall_clock_ms);
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn install_platform() {}

#[cfg(target_arch = "wasm32")]
fn wall_clock_ms() -> u64 {
    js_sys::Date::now() as u64
}

#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_blob_opfs_test;
#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_facade_test;
#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_keystore_test;
#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_opfs_test;
#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_runtime_test;
#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_sync_test;
#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_test_support;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use coven_core::Timestamp;

    use super::{stamp, standalone_hlc_for_device};

    #[test]
    fn stamp_returns_a_core_hlc_stamp() {
        let stamp = stamp("browser-device".to_string()).expect("stamp");
        let parsed = Timestamp::parse(&stamp).expect("parse stamp");
        assert_eq!(parsed.device_id, "browser-device");
    }

    #[test]
    fn same_device_stamps_are_strictly_increasing() {
        let mut previous = stamp("browser-device-monotonic".to_string()).expect("first stamp");
        for _ in 0..1024 {
            let next = stamp("browser-device-monotonic".to_string()).expect("next stamp");
            assert!(
                next > previous,
                "stamp {next} must sort after previous stamp {previous}"
            );
            previous = next;
        }
    }

    #[test]
    fn standalone_hlc_is_shared_per_device() {
        let first = standalone_hlc_for_device("shared-hlc-device".to_string()).expect("first HLC");
        let second =
            standalone_hlc_for_device("shared-hlc-device".to_string()).expect("second HLC");
        let other =
            standalone_hlc_for_device("other-shared-hlc-device".to_string()).expect("other HLC");

        assert!(Arc::ptr_eq(&first, &second));
        assert!(!Arc::ptr_eq(&first, &other));
    }
}
