/// Hash-based storage path for a file: `storage/{ab}/{cd}/{file_id}`.
pub fn storage_path(file_id: &str) -> Result<String, crate::store_dir::PathTokenError> {
    crate::store_dir::StoreDir::hashed_path("storage", file_id)
}
