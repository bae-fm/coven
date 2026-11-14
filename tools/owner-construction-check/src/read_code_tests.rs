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
        live_query_types = ["LiveQuery"]
        plumbing = ["crates/app-store/src/store.rs", "crates/app-store/src/live/"]
        rule = "the read rule"
        "#,
    )
}

const STORE: (&str, &str) = (
    "crates/app-store/src/store.rs",
    r#"
    pub struct Store;
    impl Store {
        pub fn subscribe<F, R>(&self, read: F) where F: Fn(SqlReadContext<'_>) -> R {}
        pub fn plain<F>(&self, run: F) where F: Fn(Other) {}
    }
    pub fn plumbing_helper(sql: &SqlReadContext) {}
    "#,
);

/// Each unit of read code as its file, its item and why it is read code,
/// sorted.
fn units(files: &[(&str, &str)]) -> Vec<(String, String, Why)> {
    let files = files.iter().map(|(path, text)| RustFile::fixture(path, text)).collect::<Vec<_>>();
    let files = checked(&files);
    let policy = policy();
    let shapes = policy.read_shapes.as_ref().expect("fixture read_shapes");
    let mut found = ReadCode::collect(&files, &policy, shapes)
        .units()
        .iter()
        .map(|unit| (unit.file.relative_path.clone(), unit.item.clone(), unit.why))
        .collect::<Vec<_>>();
    found.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    found
}

fn unit(item: &str, why: Why) -> (String, String, Why) {
    ("crates/app/src/a.rs".to_string(), item.to_string(), why)
}

#[test]
fn a_function_taking_a_read_type_is_read_code_and_one_taking_a_write_is_not() {
    let found = units(&[(
        "crates/app/src/a.rs",
        "fn reads(sql: &SqlReadContext<'_>) {} fn writes(tx: &WriteTransaction) {}",
    )]);
    assert_eq!(found, [unit("reads", Why::Root)]);
}

#[test]
fn a_function_taking_a_live_query_is_read_code_only_in_a_type_that_holds_one() {
    let found = units(&[(
        "crates/app/src/a.rs",
        r#"
        struct Holder { query: LiveQuery<u8> }
        impl Holder { fn of(query: LiveQuery<u8>) -> Holder { Holder { query } } }
        struct Outer { inner: Holder }
        impl Outer { fn of(query: LiveQuery<u8>) -> Outer { todo!() } }
        struct Service;
        impl Service { fn until_closed(&self, query: LiveQuery<u8>) {} }
        fn start(query: &LiveQuery<u8>) {}
        fn reads(sql: &SqlReadContext<'_>) {}
        "#,
    )]);
    assert_eq!(
        found,
        [unit("Holder::of", Why::Root), unit("Outer::of", Why::Root), unit("reads", Why::Root)]
    );
}

#[test]
fn a_callback_that_is_given_a_read_type_makes_its_taker_no_read_code() {
    let found = units(&[(
        "crates/app/src/a.rs",
        "fn takes(read: impl Fn(&SqlReadContext<'_>)) {} fn pointer(read: fn(&SqlReadContext)) {}",
    )]);
    assert_eq!(found, []);
}

#[test]
fn an_untyped_closure_passed_to_a_callee_that_takes_a_read_closure_is_read_code() {
    let found = units(&[
        STORE,
        (
            "crates/app/src/a.rs",
            r#"
            struct Screen { store: Store }
            impl Screen {
                fn open(&self) { self.store.subscribe(|sql| { sql.go(); }); }
                fn other(&self) { self.store.plain(|x| { x.go(); }); }
            }
            "#,
        ),
    ]);
    assert_eq!(found, [unit("Screen::open", Why::PassedToReadCallee)]);
}

#[test]
fn a_callee_names_a_read_type_directly_through_impl_trait_or_its_generics_bound() {
    let found = units(&[(
        "crates/app/src/a.rs",
        r#"
        struct LiveRead<F>(F);
        impl<F: Fn(SqlReadContext<'_>)> LiveRead<F> { fn new(read: F) -> Self { Self(read) } }
        fn direct(read: impl Fn(&SqlReadContext<'_>)) {}
        fn bounded<F>(read: F) where F: FnOnce(&SqlReadContext<'_>) {}
        fn unrelated<F: Fn(u8)>(run: F) {}
        fn caller() {
            LiveRead::new(|sql| { sql.x(); });
            direct(|sql| { sql.x(); });
            bounded(|sql| { sql.x(); });
            unrelated(|n| { n.x(); });
        }
        "#,
    )]);
    assert_eq!(
        found,
        [
            unit("caller", Why::PassedToReadCallee),
            unit("caller", Why::PassedToReadCallee),
            unit("caller", Why::PassedToReadCallee),
        ]
    );
}

#[test]
fn a_function_path_passed_to_a_read_callee_is_read_code() {
    let found = units(&[
        STORE,
        (
            "crates/app/src/a.rs",
            r#"
            struct Screen { store: Store }
            impl Screen {
                fn open(&self) { self.store.subscribe(Self::read); }
                fn read(sql: SqlReadContext<'_>) {}
            }
            "#,
        ),
    ]);
    // `read` takes a read type itself, so it is a root.
    assert_eq!(found, [unit("Screen::read", Why::Root)]);

    let found = units(&[
        STORE,
        (
            "crates/app/src/a.rs",
            r#"
            struct Screen { store: Store }
            impl Screen {
                fn open(&self) { self.store.subscribe(stage); }
            }
            fn stage(not_named: Rows) {}
            "#,
        ),
    ]);
    assert_eq!(found, [unit("stage", Why::PassedToReadCallee)]);
}

#[test]
fn a_closure_passed_to_a_method_of_a_read_type_is_read_code() {
    let found = units(&[(
        "crates/app/src/a.rs",
        r#"
        struct Builder;
        impl Builder { fn build(&self) -> Result<LiveQuery<u8>, Error> { todo!() } }
        struct Holder { query: std::sync::Arc<LiveQuery<u8>> }
        fn from_call(builder: &Builder) -> Result<(), Error> {
            let query = builder.build()?;
            query.process(|rows| { rows.x(); });
            Ok(())
        }
        impl Holder {
            fn from_field(&self) { self.query.process(|rows| { rows.x(); }); }
            fn from_function_path(&self) { self.query.process(Self::stage); }
            fn stage(rows: Rows) {}
        }
        fn not_a_read(other: &Other) { other.process(|rows| { rows.x(); }); }
        "#,
    )]);
    assert_eq!(
        found,
        [
            unit("Holder::from_field", Why::PassedToReadReceiver),
            unit("Holder::stage", Why::PassedToReadReceiver),
            unit("from_call", Why::PassedToReadReceiver),
        ]
    );
}

#[test]
fn a_closure_with_a_typed_read_parameter_is_read_code_wherever_it_is_passed() {
    let found = units(&[(
        "crates/app/src/a.rs",
        "fn run(pool: &Pool) { pool.with(|sql: &SqlReadContext<'_>| { sql.x(); }); }",
    )]);
    assert_eq!(found, [unit("run", Why::Root)]);
}

#[test]
fn a_closure_inside_read_code_is_part_of_it() {
    let found = units(&[
        (
            "crates/app/src/a.rs",
            r#"
        struct Screen { store: Store }
        impl Screen {
            fn read(&self, sql: &SqlReadContext<'_>) { self.store.subscribe(|inner| { inner.x(); }); }
        }
        "#,
        ),
        STORE,
    ]);
    assert_eq!(found, [unit("Screen::read", Why::Root)]);
}

#[test]
fn a_helper_read_code_calls_is_read_code_when_the_call_resolves() {
    let found = units(&[(
        "crates/app/src/a.rs",
        r#"
        struct Reads { table: std::sync::Arc<Table> }
        struct Table;
        impl Table {
            fn new() -> Table { Table }
            fn rows(&self) {}
            fn extra(&self) {}
            fn unused(&self) {}
        }
        fn by_path() {}
        impl Reads {
            fn read(&self, sql: &SqlReadContext<'_>) {
                by_path();
                Self::assoc();
                self.table.rows();
                let table = Table::new();
                table.extra();
            }
            fn assoc() {}
        }
        "#,
    )]);
    assert_eq!(
        found,
        [
            unit("Reads::assoc", Why::Reached),
            unit("Reads::read", Why::Root),
            unit("Table::extra", Why::Reached),
            unit("Table::new", Why::Reached),
            unit("Table::rows", Why::Reached),
            unit("by_path", Why::Reached),
        ]
    );
}

#[test]
fn a_helper_is_reached_through_a_typed_let_a_parameter_and_a_resolved_return_type() {
    let found = units(&[(
        "crates/app/src/a.rs",
        r#"
        struct Table;
        impl Table { fn first(&self) {} fn second(&self) {} fn third(&self) {} fn fourth(&self) {} }
        struct Maker;
        impl Maker { fn make(&self) -> Option<Table> { None } }
        fn read(sql: &SqlReadContext<'_>, table: &Table, maker: &Maker) {
            table.first();
            let typed: Table = todo!();
            typed.second();
            let made = maker.make().unwrap();
            made.third();
            let literal = Table {};
            literal.fourth();
        }
        "#,
    )]);
    let items = found.iter().map(|found| found.1.as_str()).collect::<Vec<_>>();
    assert_eq!(
        items,
        ["Maker::make", "Table::first", "Table::fourth", "Table::second", "Table::third", "read"]
    );
}

#[test]
fn a_closure_parameter_and_a_pattern_binding_take_the_type_of_what_they_come_from() {
    let found = units(&[(
        "crates/app/src/a.rs",
        r#"
        use std::collections::HashMap;
        struct Card;
        impl Card {
            fn mapped(&self) {} fn each(&self) {} fn looped(&self) {} fn destructured(&self) {}
            fn matched(&self) {} fn iflet(&self) {} fn valued(&self) {} fn folded(&self) {}
            fn projected(&self) {}
        }
        struct Stored { card: Card }
        fn loaded(sql: &SqlReadContext<'_>) -> Option<Stored> { None }
        fn read(
            sql: &SqlReadContext<'_>,
            cards: &[Card],
            one: Option<&Card>,
            by_key: HashMap<String, Card>,
        ) {
            cards.iter().map(|card| card.mapped());
            one.map(|card| card.each());
            for card in cards { card.looped(); }
            let Some(card) = one else { return };
            card.destructured();
            match one { Some(card) => card.matched(), None => {} }
            if let Some(card) = one { card.iflet(); }
            by_key.values().for_each(|card| card.valued());
            let held = loaded(sql).map(|stored| stored.card).filter(|c| c.ok).ok_or(1)?;
            held.projected();
            cards.iter().fold(0, |sum, card| { card.folded(); sum });
        }
        "#,
    )]);
    let items = found.iter().map(|found| found.1.as_str()).collect::<Vec<_>>();
    assert_eq!(
        items,
        [
            "Card::destructured",
            "Card::each",
            "Card::folded",
            "Card::iflet",
            "Card::looped",
            "Card::mapped",
            "Card::matched",
            "Card::projected",
            "Card::valued",
            "loaded",
            "read"
        ]
    );
}

#[test]
fn a_method_on_an_unknown_receiver_is_followed_by_its_name_only_when_one_method_has_it() {
    let found = units(&[(
        "crates/app/src/a.rs",
        r#"
        struct First; struct Second;
        impl First { fn shared<F: Fn(u8)>(&self, f: F) {} fn only_here<F: Fn(u8)>(&self, f: F) {} }
        impl Second { fn shared<F: Fn(u8)>(&self, f: F) {} }
        fn read(sql: &SqlReadContext<'_>) {
            let mystery = unknown_source();
            mystery.shared(|n| n);
            mystery.only_here(|n| n);
        }
        "#,
    )]);
    assert_eq!(found, [unit("First::only_here", Why::Reached), unit("read", Why::Root)]);
}

#[test]
fn a_method_on_an_unknown_receiver_is_followed_by_its_name_only_when_it_takes_a_closure() {
    let found = units(&[(
        "crates/app/src/a.rs",
        r#"
        struct Task; struct Words;
        impl Task { fn cancel(&self) {} fn each<F: Fn(u8)>(&self, f: F) {} }
        impl Words { fn find(&self, text: &str) {} }
        fn read(sql: &SqlReadContext<'_>) {
            let mystery = unknown_source();
            mystery.cancel();
            mystery.find("x");
            mystery.each(|n| n);
        }
        "#,
    )]);
    assert_eq!(found, [unit("Task::each", Why::Reached), unit("read", Why::Root)]);
}

#[test]
fn an_unknown_receiver_passes_a_closure_to_a_read_callee_only_when_every_method_of_the_name_agrees()
{
    let found = units(&[(
        "crates/app/src/a.rs",
        r#"
        struct First; struct Second; struct Third;
        impl First { fn run<F: Fn(SqlReadContext<'_>)>(&self, f: F) {} }
        impl Second { fn run<F: Fn(SqlReadContext<'_>)>(&self, f: F) {} }
        impl Third { fn run<F: Fn(u8)>(&self, f: F) {} fn solo<F: Fn(SqlReadContext<'_>)>(&self, f: F) {} }
        fn agree_not() { let mystery = unknown_source(); mystery.run(|x| { x.go(); }); }
        fn agree() { let mystery = unknown_source(); mystery.solo(|x| { x.go(); }); }
        "#,
    )]);
    assert_eq!(found, [unit("agree", Why::PassedToReadCallee)]);
}

#[test]
fn plumbing_and_test_support_items_are_no_read_code() {
    let found = units(&[
        STORE,
        ("crates/app-store/src/live/query.rs", "pub fn inside(sql: &SqlReadContext<'_>) {}"),
        (
            "crates/app/src/a.rs",
            r#"
            #[cfg(feature = "test-utils")]
            fn gated(sql: &SqlReadContext<'_>) {}
            #[cfg(any(test, feature = "test-utils"))]
            mod support { pub fn also_gated(sql: &SqlReadContext<'_>) {} }
            #[cfg(not(test))]
            fn production(sql: &SqlReadContext<'_>) {}
            fn calls_plumbing(sql: &SqlReadContext<'_>) { plumbing_helper(sql); }
            "#,
        ),
    ]);
    assert_eq!(found, [unit("calls_plumbing", Why::Root), unit("production", Why::Root)]);
}

#[test]
fn a_file_only_a_test_support_mod_line_reaches_is_no_read_code() {
    let found = units(&[
        (
            "crates/app/src/lib.rs",
            r#"
            #[cfg(any(test, feature = "test-utils"))]
            mod support;
            #[cfg(feature = "test-utils")]
            pub mod fixtures;
            mod real;
            "#,
        ),
        ("crates/app/src/support.rs", "pub fn gated(sql: &SqlReadContext<'_>) {}"),
        ("crates/app/src/fixtures.rs", "pub fn also_gated(sql: &SqlReadContext<'_>) {}"),
        ("crates/app/src/real.rs", "pub fn reads(sql: &SqlReadContext<'_>) {}"),
    ]);
    assert_eq!(found, [("crates/app/src/real.rs".to_string(), "reads".to_string(), Why::Root)]);
}

#[test]
fn a_call_that_hands_over_a_read_value_is_known_by_its_place() {
    let files = [RustFile::fixture(
        "crates/app/src/a.rs",
        "fn outer(sql: &SqlReadContext<'_>) {\n    inner(sql);\n    other(3);\n}\nfn inner(sql: &SqlReadContext<'_>) {}\nfn other(n: u8) {}",
    )];
    let files = checked(&files);
    let policy = policy();
    let read = ReadCode::collect(&files, &policy, policy.read_shapes.as_ref().expect("rows"));
    assert!(read.hands_read_value("crates/app/src/a.rs", (2, 4)));
    assert!(!read.hands_read_value("crates/app/src/a.rs", (3, 4)));
}
