//! A path on this device as coven records it wherever it reopens the file,
//! and the text that shows one (§17.3).
//!
//! A path is the operating system's own bytes, which needn't be UTF-8: Linux
//! takes any bytes but `/` and NUL in a name, and Windows any UTF-16 units,
//! unpaired surrogates included. Coven records those bytes exactly, so a file
//! whose name isn't valid Unicode still opens. Text that shows a path is
//! made from the same bytes, one way only: it never opens a file.

use std::path::{Path, PathBuf};

/// The path's bytes as coven records them. On Unix they are the path's own
/// bytes. On Windows they are its UTF-16 units in WTF-8: UTF-8, with each
/// unpaired surrogate written as the three bytes UTF-8 would give its code
/// point. Rust's `OsStr::as_encoded_bytes` holds the same bytes today, but
/// its encoding is unspecified and not to be stored, so coven writes them
/// itself. A path that is valid Unicode is its UTF-8 on every platform.
pub fn path_bytes(path: &Path) -> Vec<u8> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        path.as_os_str().as_bytes().to_vec()
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        wtf8(path.as_os_str().encode_wide())
    }
}

/// The path `path_bytes` recorded. Bytes it never gives fail with
/// `PathBytesError`: on Windows, bytes that aren't WTF-8, such as Latin-1
/// from a Linux device. On Unix any bytes are a path.
pub fn path_from_bytes(bytes: Vec<u8>) -> Result<PathBuf, PathBytesError> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        Ok(std::ffi::OsString::from_vec(bytes).into())
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt;
        units(&bytes)
            .map(|units| std::ffi::OsString::from_wide(&units).into())
            .ok_or(PathBytesError)
    }
}

/// Bytes that `path_bytes` never gives on this operating system, so no path
/// here: on Windows, bytes that aren't WTF-8. Unix takes any bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("the bytes are not a path recorded on this operating system")]
pub struct PathBytesError;

/// The text that shows `path`: `path_bytes_text` of its bytes.
pub fn path_text(path: &Path) -> String {
    path_bytes_text(&path_bytes(path))
}

/// The text that shows a path from its recorded bytes, as SQL's
/// `coven_path_text` gives it: valid UTF-8 as it is, and each byte that
/// isn't as `\x` and two uppercase hex digits. `Caf\xE9.flac` shows a Latin-1
/// name. A backslash already in the name stays as it is, so a name spelled
/// `Caf\xE9.flac` shows the same: the text can't be turned back into the
/// path, and nothing opens a file by it.
pub fn path_bytes_text(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut text = String::with_capacity(bytes.len());
    for chunk in bytes.utf8_chunks() {
        text.push_str(chunk.valid());
        for byte in chunk.invalid() {
            write!(text, "\\x{byte:02X}").expect("writing to a String succeeds");
        }
    }
    text
}

/// UTF-16 units in WTF-8.
#[cfg(any(windows, test))]
fn wtf8(units: impl Iterator<Item = u16>) -> Vec<u8> {
    let mut bytes = Vec::new();
    for unit in char::decode_utf16(units) {
        match unit {
            Ok(character) => {
                let mut buffer = [0; 4];
                bytes.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
            }
            Err(unpaired) => {
                let surrogate = unpaired.unpaired_surrogate();
                bytes.extend([
                    0xE0 | (surrogate >> 12) as u8,
                    0x80 | ((surrogate >> 6) & 0x3F) as u8,
                    0x80 | (surrogate & 0x3F) as u8,
                ]);
            }
        }
    }
    bytes
}

/// The UTF-16 units `wtf8` wrote as `bytes`, or `None` for bytes it never
/// writes: anything but UTF-8 and unpaired surrogates, and a lead surrogate
/// followed by a trail one, which is one character.
#[cfg(any(windows, test))]
fn units(bytes: &[u8]) -> Option<Vec<u16>> {
    let mut units = Vec::with_capacity(bytes.len());
    let mut rest = bytes;
    loop {
        let (valid, after) = match std::str::from_utf8(rest) {
            Ok(valid) => (valid, &[][..]),
            Err(error) => {
                let (valid, after) = rest.split_at(error.valid_up_to());
                (
                    std::str::from_utf8(valid).expect("the bytes up to the error are UTF-8"),
                    after,
                )
            }
        };
        units.extend(valid.encode_utf16());
        let [0xED, second @ 0xA0..=0xBF, third @ 0x80..=0xBF, after @ ..] = after else {
            return after.is_empty().then_some(units);
        };
        let surrogate = 0xD000 | (u16::from(second & 0x3F) << 6) | u16::from(third & 0x3F);
        let trail = (0xDC00..=0xDFFF).contains(&surrogate);
        if trail
            && units
                .last()
                .is_some_and(|unit| (0xD800..=0xDBFF).contains(unit))
        {
            return None;
        }
        units.push(surrogate);
        rest = after;
    }
}

#[cfg(test)]
#[path = "path_bytes_tests.rs"]
mod tests;
