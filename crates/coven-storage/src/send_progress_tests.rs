use super::*;
use std::sync::Mutex;

fn told() -> (SendProgress, Arc<Mutex<Vec<u64>>>) {
    let counts = Arc::new(Mutex::new(Vec::new()));
    let progress = SendProgress::new({
        let counts = counts.clone();
        move |count| counts.lock().unwrap().push(count)
    });
    (progress, counts)
}

/// A part's body counts from where the part starts in its object, so the
/// caller hears how far through the object the upload has got.
#[test]
fn a_part_counts_from_its_place_in_the_object() {
    let (progress, counts) = told();
    progress.taken(10);
    let part = progress.starting_at(100);
    part.taken(5);
    part.starting_at(20).taken(1);
    assert_eq!(*counts.lock().unwrap(), [10, 105, 121]);
}

/// Unread progress tells nothing and keeps nothing.
#[test]
fn unread_progress_tells_no_one() {
    let progress = SendProgress::unread();
    progress.taken(10);
    progress.starting_at(5).taken(1);
    assert!(format!("{progress:?}").contains("read: false"));
}
