use super::windows_device;

/// Windows keeps each device name in any ASCII case, and counts `¹`, `²` and
/// `³` as digits of a port's number.
#[test]
fn a_device_name_names_a_device_in_any_case() {
    for base in
        ["CON", "con", "Prn", "aux", "NUL", "COM1", "lpt9", "COM¹", "LPT²", "CONIN$", "conout$"]
    {
        assert!(windows_device(base), "{base:?}");
    }
}

/// A name that only looks like a device name names a file.
#[test]
fn a_name_close_to_a_device_name_names_a_file() {
    for base in ["COM0", "COM10", "LPT", "CONSOLE", "ACON", "ＣＯＮ", ""] {
        assert!(!windows_device(base), "{base:?}");
    }
}
