use super::*;

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
