//! Flat key encoding shared by Google Drive and OneDrive.
//!
//! Both store every object flat in a single folder, encoding path separators as
//! `__` so a `/`-bearing CloudHome key maps to one flat filename and back. The
//! encoding was byte-identical in both backends; this is the one copy.

/// `changes/dev1/42.enc` → `changes__dev1__42.enc`.
pub fn encode_key(key: &str) -> String {
    key.replace('/', "__")
}

/// `changes__dev1__42.enc` → `changes/dev1/42.enc`.
pub fn decode_key(filename: &str) -> String {
    filename.replace("__", "/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        for key in [
            "snapshot/abc/0.db.enc",
            "snapshot/current.json.enc",
            "changes/device-abc/1.enc",
            "heads/device-abc.json.enc",
            "images/cover.jpg",
        ] {
            assert_eq!(decode_key(&encode_key(key)), key);
        }
    }

    #[test]
    fn encode_replaces_slashes() {
        assert_eq!(encode_key("changes/dev1/42.enc"), "changes__dev1__42.enc");
    }
}
