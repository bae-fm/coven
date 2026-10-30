use super::*;

#[test]
fn text_escapes_each_byte_that_isnt_utf8_and_keeps_backslashes() {
    for (bytes, text) in [
        (
            &b"Abbey Road/01 Come Together.flac"[..],
            "Abbey Road/01 Come Together.flac",
        ),
        ("Björk/Jóga.flac".as_bytes(), "Björk/Jóga.flac"),
        (b"Caf\xE9.flac", r"Caf\xE9.flac"),
        // A name already spelled with an escape shows the same text.
        (br"Caf\xE9.flac", r"Caf\xE9.flac"),
        (br"back\slash", r"back\slash"),
        (b"\xFF\xFE", r"\xFF\xFE"),
        // A UTF-8 character cut short is each of its bytes.
        (b"Bj\xC3", r"Bj\xC3"),
        (b"\xF0\x9F\x8E", r"\xF0\x9F\x8E"),
        // A surrogate, as WTF-8 writes an unpaired one, isn't UTF-8.
        (b"\xED\xA0\x80.flac", r"\xED\xA0\x80.flac"),
        (b"", ""),
    ] {
        assert_eq!(path_bytes_text(bytes), text, "{bytes:?}");
    }
}

#[test]
fn a_unicode_path_is_its_utf8() {
    let path = Path::new("Björk").join("Homogenic").join("Jóga.flac");
    let bytes = path_bytes(&path);
    assert_eq!(bytes, path.to_str().unwrap().as_bytes());
    assert_eq!(path_text(&path), path.to_str().unwrap());
    assert_eq!(path_from_bytes(bytes), Some(path));
}

#[cfg(unix)]
#[test]
fn a_unix_path_is_its_own_bytes() {
    use std::os::unix::ffi::OsStrExt;
    let path = Path::new(std::ffi::OsStr::from_bytes(b"/music/Caf\xE9.flac"));
    let bytes = path_bytes(path);
    assert_eq!(bytes, b"/music/Caf\xE9.flac");
    assert_eq!(path_text(path), r"/music/Caf\xE9.flac");
    assert_eq!(path_from_bytes(bytes).as_deref(), Some(path));
}

/// WTF-8 round-trips any UTF-16, unpaired surrogates included, and refuses
/// what it never writes.
#[test]
fn wtf8_holds_any_utf16_and_refuses_what_it_never_writes() {
    let cases: [&[u16]; 6] = [
        &[],
        &[0x43, 0x61, 0x66, 0xE9],
        // A paired surrogate is one character, four bytes.
        &[0xD83C, 0xDFB5],
        &[0xD800],
        &[0x61, 0xDC00, 0x62],
        // A trail before a lead pairs with nothing.
        &[0xDC00, 0xD800],
    ];
    for case in cases {
        let bytes = wtf8(case.iter().copied());
        assert_eq!(units(&bytes).as_deref(), Some(case), "{case:x?}");
    }
    assert_eq!(wtf8([0xD83C, 0xDFB5].into_iter()), "🎵".as_bytes());
    assert_eq!(wtf8([0xD800].into_iter()), b"\xED\xA0\x80");
    assert_eq!(wtf8([0xDFFF].into_iter()), b"\xED\xBF\xBF");
    for bytes in [
        &b"\xFF"[..],
        b"Caf\xE9",
        b"\xED\xA0",
        b"\xED\xC0\x80",
        // A lead and a trail written apart are one character, written whole.
        b"\xED\xA0\x80\xED\xB0\x80",
    ] {
        assert_eq!(units(bytes), None, "{bytes:x?}");
    }
}

#[cfg(windows)]
#[test]
fn a_windows_path_with_an_unpaired_surrogate_is_its_wtf8() {
    use std::os::windows::ffi::OsStringExt;
    let path = PathBuf::from(std::ffi::OsString::from_wide(&[
        0x43, 0x3A, 0x5C, 0x61, 0xD800, 0x2E, 0x66,
    ]));
    let bytes = path_bytes(&path);
    assert_eq!(bytes, b"C:\\a\xED\xA0\x80.f");
    assert_eq!(path_text(&path), r"C:\a\xED\xA0\x80.f");
    assert_eq!(path_from_bytes(bytes), Some(path));
    assert_eq!(path_from_bytes(b"\xFF".to_vec()), None);
}
