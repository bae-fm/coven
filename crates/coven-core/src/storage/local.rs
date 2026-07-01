/// Hash-based storage path for a file: `storage/{ab}/{cd}/{file_id}`.
pub fn storage_path(file_id: &str) -> Result<String, crate::library_dir::PathTokenError> {
    crate::library_dir::LibraryDir::hashed_path("storage", file_id)
}
