use std::sync::Arc;

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

pub mod library_dir {
    pub use coven_core::library_dir::*;
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
pub use wasm_facade::CovenLibrary;
#[cfg(target_arch = "wasm32")]
pub use wasm_keystore::BrowserKeystore;

use coven_core::{Hlc, UpdatedAtStamper};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn stamp(device_id: String) -> Result<String, JsValue> {
    install_platform();
    if device_id.is_empty() {
        return Err(JsValue::from_str(
            "invalid browser config: device_id must not be empty",
        ));
    }
    let stamper = UpdatedAtStamper::new(Arc::new(Hlc::new(device_id)));
    Ok(stamper.stamp())
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

#[cfg(test)]
mod tests {
    use coven_core::Timestamp;

    use super::stamp;

    #[test]
    fn stamp_returns_a_core_hlc_stamp() {
        let stamp = stamp("browser-device".to_string()).expect("stamp");
        let parsed = Timestamp::parse(&stamp).expect("parse stamp");
        assert_eq!(parsed.device_id, "browser-device");
    }
}
