use super::*;

/// A deletion's record reads back the choice it was made with, recording
/// again replaces it, and forgetting it, present or not, leaves none.
#[test]
fn a_deletion_record_reads_back_until_forgotten() {
    let directory = tempfile::tempdir().unwrap();
    assert_eq!(read(directory.path()).unwrap(), None);
    record(directory.path(), IfUnsent::Refuse).unwrap();
    assert_eq!(read(directory.path()).unwrap(), Some(IfUnsent::Refuse));
    record(directory.path(), IfUnsent::Discard).unwrap();
    assert_eq!(read(directory.path()).unwrap(), Some(IfUnsent::Discard));
    forget(directory.path()).unwrap();
    forget(directory.path()).unwrap();
    assert_eq!(read(directory.path()).unwrap(), None);
}
