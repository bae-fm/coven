use super::*;
use crate::crate_code::checked;

fn policy() -> Policy {
    Policy::fixture(
        r#"
        rules = ["read-shapes"]
        [database]
        capability = "SQLite"
        test_support_features = ["test-utils"]
        [read_shapes]
        read_types = ["SqlReadContext", "LiveQuery"]
        live_query_types = ["LiveQuery", "LiveRead"]
        value_stream_types = ["TransferWatch"]
        kept_state_types = ["Mutex", "Cell"]
        plumbing = ["crates/app-store/src/live.rs"]
        rule = "the read rule"
        "#,
    )
}

const LIVE: (&str, &str) = (
    "crates/app-store/src/live.rs",
    r#"
    pub struct LiveRead<Q, V> { query: LiveQuery<Q>, state: Mutex<V> }
    pub struct LiveQuery<Q> { requests: Mutex<Q> }
    impl<Q> LiveQuery<Q> {
        pub fn process<F>(&self, stage: F) where F: Fn(SqlReadContext<'_>) {}
        pub fn next(&mut self) {}
    }
    pub struct Store;
    impl Store {
        pub fn subscribe<F>(&self, read: F) where F: Fn(SqlReadContext<'_>) {}
    }
    "#,
);

/// Each finding of the files as its item and what it says, sorted.
fn found(files: &[(&str, &str)]) -> Vec<(String, String)> {
    let files = files.iter().map(|(path, text)| RustFile::fixture(path, text)).collect::<Vec<_>>();
    let files = checked(&files);
    let policy = policy();
    let shapes = policy.read_shapes.as_ref().expect("rows");
    let read = ReadCode::collect(&files, &policy, shapes);
    let mut hits = kept_state(&read, &files, &policy, shapes)
        .into_iter()
        .map(|hit| {
            assert_eq!(hit.shape.id(), "kept-state");
            (hit.item, hit.what)
        })
        .collect::<Vec<_>>();
    hits.sort();
    hits
}

fn items(files: &[(&str, &str)]) -> Vec<String> {
    found(files).into_iter().map(|(item, _)| item).collect()
}

const A: &str = "crates/app/src/a.rs";

#[test]
fn a_holder_of_a_live_query_keeping_a_lock_beside_it_is_found_at_the_type() {
    let found = found(&[
        LIVE,
        (
            A,
            r#"
            struct Told { last: Mutex<u8> }
            struct Holder { query: LiveRead<u8, u8>, told: Told }
            "#,
        ),
    ]);
    assert_eq!(
        found,
        [(
            "Holder".to_string(),
            "keeps `told`, which holds `Mutex` state, beside its live query".to_string()
        )]
    );
}

#[test]
fn a_value_stream_beside_the_live_query_is_not_kept_state() {
    assert_eq!(
        items(&[
            LIVE,
            (
                A,
                r#"
                struct TransferWatch { latest: Mutex<u8> }
                struct Holder { query: LiveQuery<u8>, transfers: TransferWatch }
                "#,
            ),
        ]),
        Vec::<String>::new()
    );
}

#[test]
fn a_holder_nested_two_types_deep_is_found() {
    assert_eq!(
        items(&[
            LIVE,
            (
                A,
                r#"
                struct Told { last: Cell<u8> }
                struct Sending { query: LiveQuery<u8> }
                struct Watch { transfers: Sending, told: Option<Told> }
                "#,
            ),
        ]),
        ["Watch"]
    );
}

#[test]
fn the_live_query_types_own_locks_are_request_plumbing() {
    assert_eq!(items(&[LIVE]), Vec::<String>::new());
    assert_eq!(
        items(&[(
            A,
            "struct LiveRead<Q> { query: LiveQuery<Q>, state: Mutex<u8> } struct LiveQuery<Q>(Mutex<Q>);"
        )]),
        Vec::<String>::new()
    );
}

#[test]
fn a_method_taking_self_that_writes_a_field_beside_the_live_query_is_found() {
    let found = found(&[
        LIVE,
        (
            A,
            r#"
            struct List; impl List { fn push(&mut self) {} fn len(&self) {} }
            struct Inner { x: u8 }
            struct Holder { query: LiveQuery<u8>, told: Option<u8>, inner: Inner, count: u8, list: List }
            impl Holder {
                fn new(query: LiveQuery<u8>) -> Self { let mut holder = Self { query, told: None, inner: Inner { x: 0 }, count: 0, list: List }; holder.told = None; holder }
                fn assigns(&mut self, v: u8) { self.told = Some(v); }
                fn assigns_through(&mut self) { self.inner.x = 1; }
                fn dereferences(&mut self) { *self.told = 1; }
                fn compounds(&mut self) { self.count += 1; }
                fn calls_mut(&mut self) { self.list.push(); }
                fn only_reads(&self) { self.list.len(); }
                fn advances(&mut self) { self.query.next(); }
            }
            "#,
        ),
    ]);
    let items = found.iter().map(|(item, _)| item.as_str()).collect::<Vec<_>>();
    assert_eq!(
        items,
        [
            "Holder::assigns",
            "Holder::assigns_through",
            "Holder::calls_mut",
            "Holder::compounds",
            "Holder::dereferences",
        ]
    );
    assert_eq!(found[0].1, "writes `self.told` after construction");
}

#[test]
fn a_closure_capturing_a_binding_that_keeps_state_is_found() {
    let found = found(&[
        LIVE,
        (
            A,
            r#"
            struct Places { seen: Mutex<u8> }
            impl Places { fn new() -> Places { Places { seen: Mutex::new(0) } } fn record(&self) {} }
            struct Screen { store: Store }
            impl Screen {
                fn open(&self) {
                    let places = Places::new();
                    self.store.subscribe(|sql| { places.record(); });
                }
                fn clean(&self) {
                    let limit = 5u8;
                    self.store.subscribe(|sql| { sql.take(limit); });
                }
            }
            "#,
        ),
    ]);
    // `record` is read code too, called from the closure, on a type that keeps state.
    assert_eq!(
        found,
        [
            (
                "Places::record".to_string(),
                "reads through `Places`, which keeps `Mutex` state".to_string()
            ),
            (
                "Screen::open".to_string(),
                "captures `places`, which keeps `Mutex` state".to_string()
            ),
        ]
    );
}

#[test]
fn a_resolver_assigning_a_captured_binding_is_found_and_one_reading_a_capture_is_not() {
    let found = found(&[
        LIVE,
        (
            A,
            r#"
            struct Screen { query: LiveQuery<u8> }
            impl Screen {
                fn tells(&self) {
                    let mut told = None;
                    self.query.process(|rows| { told = Some(rows); });
                }
                fn reads(&self) {
                    let threshold = 3;
                    self.query.process(|rows| { rows.over(threshold); });
                }
            }
            "#,
        ),
    ]);
    assert_eq!(found, [("Screen::tells".to_string(), "assigns the captured `told`".to_string())]);
}

#[test]
fn a_local_of_a_read_function_is_no_kept_state_to_a_closure_inside_the_same_call() {
    let found = found(&[(
        A,
        r#"
        struct Tally; impl Tally { fn bump(&mut self) {} fn new() -> Tally { Tally } }
        fn of(sql: &SqlReadContext<'_>, rows: Vec<u8>) {
            let mut count = 0;
            let mut tally = Tally::new();
            rows.iter().for_each(|row| {
                count += 1;
                tally.bump();
            });
        }
        "#,
    )]);
    assert_eq!(found, []);
}

#[test]
fn a_closure_calling_a_mutating_method_on_a_capture_is_found() {
    let found = found(&[
        LIVE,
        (
            A,
            r#"
            struct Tally; impl Tally { fn bump(&mut self) {} fn get(&self) {} fn new() -> Tally { Tally } }
            struct Screen { query: LiveQuery<u8> }
            impl Screen {
                fn counts(&self) {
                    let mut tally = Tally::new();
                    self.query.process(|rows| { tally.bump(); });
                }
                fn looks(&self) {
                    let tally = Tally::new();
                    self.query.process(|rows| { tally.get(); });
                }
            }
            "#,
        ),
    ]);
    assert_eq!(
        found,
        [("Screen::counts".to_string(), "changes the captured `tally` with `bump`".to_string())]
    );
}

#[test]
fn self_state_used_inside_a_read_closure_is_found_as_a_capture_is() {
    let found = found(&[
        LIVE,
        (
            A,
            r#"
            struct Screen { query: LiveQuery<u8>, cache: Mutex<u8>, plain: u8 }
            impl Screen {
                fn open(&self) {
                    self.query.process(|rows| { self.cache.lock(); self.plain.max(1); });
                }
            }
            "#,
        ),
    ]);
    assert!(
        found.contains(&(
            "Screen::open".to_string(),
            "captures `self.cache`, which keeps `Mutex` state".to_string()
        )),
        "{found:?}"
    );
    assert_eq!(found.iter().filter(|(item, _)| item == "Screen::open").count(), 1);
}

#[test]
fn a_read_method_on_a_type_keeping_state_is_found() {
    let found = found(&[(
        A,
        r#"
        struct SectionsDelivery { sent: Mutex<u8> }
        impl SectionsDelivery {
            fn read(&self, sql: &SqlReadContext<'_>) {}
            fn write(&self) {}
        }
        struct Pure { n: u8 }
        impl Pure { fn read(&self, sql: &SqlReadContext<'_>) {} }
        "#,
    )]);
    assert_eq!(
        found,
        [(
            "SectionsDelivery::read".to_string(),
            "reads through `SectionsDelivery`, which keeps `Mutex` state".to_string()
        )]
    );
}

#[test]
fn read_code_naming_a_static_that_keeps_state_is_found() {
    let found = found(&[(
        A,
        r#"
        static SEEN: Mutex<u8> = Mutex::new(0);
        static NAMES: &str = "x";
        fn read(sql: &SqlReadContext<'_>) { SEEN.lock(); let n = NAMES; }
        fn elsewhere() { SEEN.lock(); }
        "#,
    )]);
    assert_eq!(
        found,
        [("read".to_string(), "reads the static `SEEN`, which keeps `Mutex` state".to_string())]
    );
}
