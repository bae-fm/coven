use super::*;

#[test]
fn a_relative_path_is_names_that_stay_inside_the_folder() {
    let album = RelativePath::new("Abbey Road/01 Come Together.flac").unwrap();
    assert_eq!(
        album.as_path(),
        Path::new("Abbey Road").join("01 Come Together.flac")
    );
    assert_eq!(
        RelativePath::new("Björk//Homogenic/").unwrap().as_path(),
        Path::new("Björk").join("Homogenic")
    );
    for (path, error) in [
        ("", RelativePathError::Empty),
        ("/etc/passwd", RelativePathError::Absolute),
        ("../outside.flac", RelativePathError::Parent),
        ("album/../../outside.flac", RelativePathError::Parent),
        ("./track.flac", RelativePathError::Current),
    ] {
        assert_eq!(RelativePath::new(path), Err(error), "{path}");
    }
}

#[cfg(unix)]
#[test]
fn a_name_that_isnt_unicode_is_refused() {
    use std::os::unix::ffi::OsStrExt;
    let name = std::ffi::OsStr::from_bytes(b"track\xff.flac");
    assert_eq!(RelativePath::new(name), Err(RelativePathError::NotUnicode));
}

#[cfg(windows)]
#[test]
fn a_drive_is_absolute() {
    for path in [
        r"C:\Music\track.flac",
        r"C:track.flac",
        r"\\server\share\track.flac",
    ] {
        assert_eq!(
            RelativePath::new(path),
            Err(RelativePathError::Absolute),
            "{path}"
        );
    }
}
