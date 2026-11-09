use super::*;
use crate::files::{FileArea, FileName, StoreDirAccess, StoreDirectories};
use crate::id_source::{StoreId, UuidIds};
use std::sync::atomic::{AtomicUsize, Ordering};

fn store(root: &tempfile::TempDir) -> crate::files::StoreDir {
    StoreDirectories::new(root.path().to_owned(), crate::task::HeardBugs::shared())
        .create_store_dir(StoreId(uuid::Uuid::from_u128(123)), &UuidIds)
        .unwrap()
}

/// Write `bytes` as `name` in `area`, finished into `batch`.
async fn write(store: &crate::files::StoreDir, area: FileArea, name: &str, batch: &mut FlushBatch) {
    store
        .create_writer(area, &FileName::new(name).unwrap())
        .unwrap()
        .write_from(&mut b"bytes".as_slice(), |_| {}, batch)
        .await
        .unwrap();
}

/// Each folder is synced once however many files the batch made there.
/// Until the flush ends, the batch holds the store read lock of a file in
/// each folder: the flush is still part of those files' writes.
#[tokio::test]
async fn a_batch_keeps_each_folder_once_and_the_store_until_it_is_flushed() {
    let root = tempfile::tempdir().unwrap();
    let store = store(&root);
    let mut batch = FlushBatch::new(crate::task::HeardBugs::shared());
    for name in ["one", "two", "three"] {
        write(&store, FileArea::AppProvided, name, &mut batch).await;
    }
    write(&store, FileArea::Cache, "four", &mut batch).await;
    assert_eq!(batch.files, 4);
    #[cfg(unix)]
    {
        assert_eq!(batch.folders.len(), 2);
        assert!(matches!(
            store.lock_for_deletion(),
            Err(crate::files::StoreLockError::AlreadyOpen(_))
        ));
    }
    let _flushed = batch.flush().await.unwrap();
    assert!(store.lock_for_deletion().unwrap().is_some());
}

/// The files of one held folder share its single read lock: with the folder
/// let go, the store stays held until the batch they finished into flushes.
#[cfg(unix)]
#[tokio::test]
async fn files_made_in_a_held_folder_keep_the_store_until_their_batch_flushes() {
    let root = tempfile::tempdir().unwrap();
    let store = store(&root);
    let folder = store.writer_folder(FileArea::AppProvided).unwrap();
    let mut batch = FlushBatch::new(crate::task::HeardBugs::shared());
    for name in ["one", "two", "three"] {
        folder
            .create(&FileName::new(name).unwrap())
            .unwrap()
            .write_from(&mut b"bytes".as_slice(), |_| {}, &mut batch)
            .await
            .unwrap();
    }
    drop(folder);
    assert_eq!(batch.files, 3);
    assert!(matches!(store.lock_for_deletion(), Err(crate::files::StoreLockError::AlreadyOpen(_))));
    let _flushed = batch.flush().await.unwrap();
    assert!(store.lock_for_deletion().unwrap().is_some());
}

/// A whole file made in one call is synced, counted in the batch, and never
/// replaces a file already there.
#[tokio::test]
async fn a_whole_file_is_counted_in_the_batch_and_never_replaces_another() {
    let root = tempfile::tempdir().unwrap();
    let store = store(&root);
    let folder = store.writer_folder(FileArea::AppProvided).unwrap();
    let name = FileName::new("whole").unwrap();
    let mut batch = FlushBatch::new(crate::task::HeardBugs::shared());
    folder.write_whole(&name, b"all of it", &mut batch).unwrap();
    assert_eq!(batch.files, 1);
    assert_eq!(
        store.file(FileArea::AppProvided, &name).read_optional().unwrap().unwrap(),
        b"all of it"
    );
    assert!(folder.write_whole(&name, b"other", &mut batch).is_err());
    assert_eq!(batch.files, 1);
    assert_eq!(
        store.file(FileArea::AppProvided, &name).read_optional().unwrap().unwrap(),
        b"all of it"
    );
    let _flushed = batch.flush().await.unwrap();
}

/// A flush that fails fails the batch, so the commit that would record its
/// files never gets the `Flushed` it takes. An empty batch flushes nothing.
#[tokio::test]
async fn a_failed_flush_fails_the_batch_and_an_empty_one_flushes_nothing() {
    let calls = std::sync::Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let hook: FlushHook = std::sync::Arc::new(move || {
        counted.fetch_add(1, Ordering::SeqCst);
        Err(std::io::Error::other("the drive refused the flush"))
    });
    let _flushed = FlushBatch::new(crate::task::HeardBugs::shared())
        .with_hook(Some(hook.clone()))
        .flush()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let root = tempfile::tempdir().unwrap();
    let store = store(&root);
    let mut batch = FlushBatch::new(crate::task::HeardBugs::shared()).with_hook(Some(hook));
    write(&store, FileArea::AppProvided, "one", &mut batch).await;
    let error = batch.flush().await.unwrap_err();
    assert!(error.to_string().contains("the drive refused the flush"), "{error}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// What a batch costs as the files area fills, under three layouts of its
/// copies. Unix only: Windows syncs no folder.
#[cfg(unix)]
mod staged_batch_cost {
    use super::*;

    /// How the app-provided copies' files are laid out in the area's folder.
    #[derive(Clone, Copy)]
    enum Layout {
        /// Every copy at the top of `files/`.
        Flat,
        /// A new folder in `files/` for each batch's copies.
        FolderPerBatch,
        /// 256 folders in `files/`, a copy in the one its name's first two hex
        /// digits name.
        Fanout,
    }

    const COPIES: usize = 12;

    /// The folder of the 256 a name's first two hex digits choose.
    fn fanout(name: &str) -> String {
        name[..2].to_owned()
    }

    /// Put `count` one-byte copies in `area`, laid out by `layout`. Each is
    /// synced as it is made, and the folders and the drive's cache after, so
    /// no write-back of the filling is still running while a batch is timed.
    fn fill(area: &std::path::Path, layout: Layout, count: usize) {
        let mut folder = area.to_owned();
        let mut folders = std::collections::BTreeSet::from([area.to_owned()]);
        for made in 0..count {
            let name = uuid::Uuid::new_v4().to_string();
            let path = match layout {
                Layout::Flat => area.join(&name),
                Layout::FolderPerBatch => {
                    if made % COPIES == 0 {
                        folder = area.join(uuid::Uuid::new_v4().to_string());
                        std::fs::create_dir(&folder).unwrap();
                    }
                    folder.join(&name)
                }
                Layout::Fanout => area.join(fanout(&name)).join(&name),
            };
            let mut file = std::fs::File::create_new(&path).unwrap();
            std::io::Write::write_all(&mut file, b"x").unwrap();
            rustix::fs::fsync(&file).unwrap();
            folders.insert(path.parent().unwrap().to_owned());
        }
        for folder in &folders {
            let folder = BatchFolder::open(folder.clone(), None).unwrap();
            rustix::fs::fsync(&*folder.handle).unwrap();
        }
        #[cfg(target_vendor = "apple")]
        {
            let area = BatchFolder::open(area.to_owned(), None).unwrap();
            rustix::fs::fcntl_fullfsync(&*area.handle).unwrap();
        }
    }

    /// One batch's steps, timed apart.
    struct Sample {
        /// Making, writing and syncing every copy, with its folder for a
        /// folder per batch.
        copies: std::time::Duration,
        /// Syncing each folder a copy was made in, and `files/` when the batch
        /// made a folder there.
        folders: std::time::Duration,
        /// Flushing each device's cache once.
        cache: std::time::Duration,
    }

    /// Make one batch of copies of `bytes` laid out by `layout`, and flush it
    /// through the batch's own steps, timing each.
    fn sample(
        store: &crate::files::StoreDir,
        area: &std::path::Path,
        layout: Layout,
        folders: &std::collections::BTreeMap<String, crate::files::WriterFolder>,
        bytes: &[u8],
    ) -> Sample {
        use crate::files::WriterFolder;
        let bugs = crate::task::HeardBugs::shared();
        let mut batch = FlushBatch::new(bugs.clone());
        let name = || FileName::new(uuid::Uuid::new_v4().to_string()).unwrap();
        let started = std::time::Instant::now();
        match layout {
            Layout::Flat => {
                for _ in 0..COPIES {
                    folders[""].write_whole(&name(), bytes, &mut batch).unwrap();
                }
            }
            Layout::FolderPerBatch => {
                let path = area.join(uuid::Uuid::new_v4().to_string());
                std::fs::create_dir(&path).unwrap();
                let folder =
                    WriterFolder::open(&path, store.lock_read_only().unwrap(), bugs).unwrap();
                for _ in 0..COPIES {
                    folder.write_whole(&name(), bytes, &mut batch).unwrap();
                }
                // The batch's folder is a new entry in `files/`, which its
                // flush syncs with the folder.
                batch.folders.push(BatchFolder::open(area.to_owned(), None).unwrap());
            }
            Layout::Fanout => {
                for _ in 0..COPIES {
                    let name = name();
                    folders[&fanout(name.as_str())].write_whole(&name, bytes, &mut batch).unwrap();
                }
            }
        }
        let copies = started.elapsed();
        let started = std::time::Instant::now();
        batch.sync_folders().unwrap();
        let folders = started.elapsed();
        let started = std::time::Instant::now();
        #[cfg(target_vendor = "apple")]
        batch.flush_devices().unwrap();
        let cache = started.elapsed();
        Sample { copies, folders, cache }
    }

    /// One store whose batches are timed, laid out by `layout`, with the
    /// folders its copies are made in held open as staging holds them.
    struct Measured {
        label: String,
        _root: tempfile::TempDir,
        store: crate::files::StoreDir,
        layout: Layout,
        folders: std::collections::BTreeMap<String, crate::files::WriterFolder>,
        samples: Vec<Sample>,
    }

    /// A new store holding `count` copies laid out by `layout`.
    fn measured(layout: Layout, count: usize, label: &str) -> Measured {
        let root = tempfile::tempdir().unwrap();
        let store = store(&root);
        let area = store.area_path(FileArea::AppProvided);
        let mut folders = std::collections::BTreeMap::new();
        match layout {
            Layout::Flat => {
                folders.insert(String::new(), store.writer_folder(FileArea::AppProvided).unwrap());
            }
            Layout::FolderPerBatch => {}
            Layout::Fanout => {
                for digits in 0..=255u8 {
                    let name = format!("{digits:02x}");
                    std::fs::create_dir(area.join(&name)).unwrap();
                    let folder = crate::files::WriterFolder::open(
                        &area.join(&name),
                        store.lock_read_only().unwrap(),
                        crate::task::HeardBugs::shared(),
                    )
                    .unwrap();
                    folders.insert(name, folder);
                }
            }
        }
        fill(&area, layout, count);
        Measured {
            label: label.to_owned(),
            _root: root,
            store,
            layout,
            folders,
            samples: Vec::new(),
        }
    }

    impl Measured {
        fn sample(&self, bytes: &[u8]) -> Sample {
            let area = self.store.area_path(FileArea::AppProvided);
            sample(&self.store, &area, self.layout, &self.folders, bytes)
        }

        /// Print each step's median, and the batch's middle half.
        fn report(&self) {
            let sorted = |step: &dyn Fn(&Sample) -> std::time::Duration| {
                let mut times: Vec<_> = self.samples.iter().map(step).collect();
                times.sort();
                times
            };
            let ms = |time: std::time::Duration| time.as_secs_f64() * 1e3;
            let median = |times: &[std::time::Duration]| times[times.len() / 2];
            let batch = sorted(&|sample| sample.copies + sample.folders + sample.cache);
            println!(
                "{:<34} per copy {:>6.1} µs  folders {:>5.2} ms  cache {:>5.2} ms  \
                 batch {:>5.2} ms ({:.2} to {:.2})",
                self.label,
                median(&sorted(&|sample| sample.copies)).as_secs_f64() * 1e6 / COPIES as f64,
                ms(median(&sorted(&|sample| sample.folders))),
                ms(median(&sorted(&|sample| sample.cache))),
                ms(median(&batch)),
                ms(batch[batch.len() / 4]),
                ms(batch[batch.len() * 3 / 4]),
            );
        }
    }

    /// What one batch of 12 staged copies of 64 KiB costs as the app-provided
    /// files' folder fills, and at 240,000 files in two other layouts: a folder
    /// per batch, and 256 folders chosen by the name's first two hex digits
    /// (the names are random, as hashing a name makes them). Each batch makes
    /// its copies through the real writer, as staging does, and flushes them
    /// through the batch's own steps, timing apart making, writing and syncing
    /// each copy, syncing the folders, and flushing the drive's cache. The
    /// stores take turns, a batch each, so a machine busy with other work
    /// slows them alike, after 3 rounds that warm them. Prints the median of
    /// 21 batches each, and the middle half of the batch's times. Run with
    /// `--ignored --nocapture`.
    #[test]
    #[ignore = "measurement: run with --ignored --nocapture"]
    fn a_staged_batch_costs_in_a_full_folder() {
        let mut stores = vec![
            measured(Layout::Flat, 0, "one folder, 0 files"),
            measured(Layout::Flat, 60_000, "one folder, 60000 files"),
            measured(Layout::Flat, 240_000, "one folder, 240000 files"),
            measured(Layout::FolderPerBatch, 240_000, "a folder per batch, 240000 files"),
            measured(Layout::Fanout, 240_000, "256 folders, 240000 files"),
        ];
        let bytes = vec![7; 64 * 1024];
        for round in 0..24 {
            for measured in &mut stores {
                let sample = measured.sample(&bytes);
                if round >= 3 {
                    measured.samples.push(sample);
                }
            }
        }
        for measured in &stores {
            measured.report();
        }
    }
}
