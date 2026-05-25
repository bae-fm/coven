//! Local managed blob storage.
//!
//! Files are stored at content-addressed paths under
//! `storage/{ab}/{cd}/{file_id}` as plaintext. Local files are never encrypted —
//! encryption happens only on upload to the cloud home.
mod traits;

pub use traits::BlobStore;

/// Hash-based storage path for a file: `storage/{ab}/{cd}/{file_id}`
///
/// Deterministic from the file_id alone. Used for both local storage
/// (relative to the library dir) and cloud keys.
pub fn storage_path(file_id: &str) -> String {
    crate::library_dir::LibraryDir::hashed_path("storage", file_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_path_uses_first_four_hex_chars() {
        let id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";
        let path = storage_path(id);
        assert_eq!(path, format!("storage/a1/b2/{}", id));
    }

    #[test]
    fn storage_path_preserves_original_id_with_dashes() {
        let id = "12345678-aaaa-bbbb-cccc-ddddeeeeaaaa";
        let path = storage_path(id);
        // prefix from dashless hex: "12" and "34"
        assert_eq!(path, format!("storage/12/34/{}", id));
        // The full id with dashes is the filename
        assert!(path.ends_with(id));
    }
}
