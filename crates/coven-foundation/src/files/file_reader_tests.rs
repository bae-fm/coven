use super::*;

#[tokio::test]
async fn readers_distinguish_missing_files_from_other_errors() {
    let directory = tempfile::tempdir().unwrap();
    for name in ["missing", "invalid\0path"] {
        let path = directory.path().join(name);
        let range_error = match FileReader::open(&path) {
            Ok(_) => panic!("invalid path opened"),
            Err(error) => error,
        };
        let original_error = crate::files::observe_file(&path, |_| {}).await.unwrap_err();
        for (error, expected) in [
            (range_error, "read file"),
            (original_error, "read original"),
        ] {
            if name == "missing" {
                assert!(matches!(error, ObservationError::Missing(found) if found == path));
            } else {
                assert!(matches!(error,
                    ObservationError::File(FileError::Io { operation, path: found, source })
                    if operation == expected && found == path && source.kind() == io::ErrorKind::InvalidInput
                ));
            }
        }
    }
}

#[test]
fn positioned_reads_keep_the_opened_file_after_replacement() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("file");
    std::fs::write(&path, b"abcdef").unwrap();
    let reader = FileReader::open(&path).unwrap();
    let mut scanned = Vec::new();
    reader
        .scan(|bytes| scanned.extend_from_slice(bytes))
        .unwrap();
    assert_eq!(scanned, b"abcdef");
    crate::files::AtomicFile::new(path.clone())
        .replace(b"new")
        .unwrap();
    assert_eq!(reader.read_at(2, 3).unwrap(), b"cde");
    assert_eq!(reader.read_at(6, 0).unwrap(), b"");
    assert!(reader.read_at(5, 2).is_err());
}

#[test]
fn original_facts_and_later_changes_are_checked() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("file");
    std::fs::write(&path, b"abc").unwrap();
    let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    assert!(matches!(
        FileReader::open_original(&path, 4, modified),
        Err(ObservationError::Changed(_))
    ));
    let reader = FileReader::open_original(&path, 3, modified).unwrap();
    std::fs::write(&path, b"changed").unwrap();
    assert!(matches!(
        reader.read_at(0, 1),
        Err(ObservationError::Changed(_))
    ));
}
