use crate::{StorageError, StorageFailure};

pub use coven_format::path::*;

pub(crate) fn validate_root(root: &str) -> Result<(), StorageError> {
    if !root.is_empty() {
        for part in root.split('/') {
            segment(part)?;
        }
    }
    Ok(())
}
fn segment(part: &str) -> Result<(), StorageError> {
    if part.is_empty()
        || part.len() > 255
        || !part
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
    {
        return Err(StorageFailure::InvalidPath.into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "path_tests.rs"]
mod tests;
