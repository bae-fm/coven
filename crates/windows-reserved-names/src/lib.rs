//! The names Windows keeps for devices.
//!
//! Windows opens a device, not a file, for a name such as `NUL` or `COM1`.
//! [`windows_device`] tells whether a name is one of them, for code that
//! makes file names and for code that hands paths to Windows.

/// Whether `base`, a name's part before its first `.` without trailing
/// spaces, names a Windows device: Windows opens the device for it, with any
/// extension on some versions, and in any ASCII case. `¹`, `²` and `³` count
/// as digits.
pub fn windows_device(base: &str) -> bool {
    let base = base.to_ascii_uppercase();
    if matches!(base.as_str(), "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$") {
        return true;
    }
    let Some(number) = base.strip_prefix("COM").or_else(|| base.strip_prefix("LPT")) else {
        return false;
    };
    let mut number = number.chars();
    matches!((number.next(), number.next()), (Some('1'..='9' | '¹' | '²' | '³'), None))
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
