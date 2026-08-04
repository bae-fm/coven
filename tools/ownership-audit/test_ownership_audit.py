import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest


MODULE_PATH = Path(__file__).with_name("ownership_audit.py")
SPEC = importlib.util.spec_from_file_location("ownership_audit", MODULE_PATH)
assert SPEC is not None
assert SPEC.loader is not None
ownership_audit = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = ownership_audit
SPEC.loader.exec_module(ownership_audit)


class SyntaxInventoryTests(unittest.TestCase):
    def inventory(self, source: str):
        with tempfile.TemporaryDirectory() as directory:
            path = (
                Path(directory)
                / "crates"
                / "coven-core"
                / "src"
                / "sample.rs"
            )
            path.parent.mkdir(parents=True)
            path.write_text(source)
            source_file = ownership_audit.parse_source(path, source)
            original_root = ownership_audit.ROOT
            ownership_audit.ROOT = Path(directory)
            try:
                return ownership_audit.inventory_file(source_file)
            finally:
                ownership_audit.ROOT = original_root

    def test_inventory_distinguishes_free_associated_and_receiver_methods(self):
        records = self.inventory(
            """
fn transform(value: u64) -> u64 { value + 1 }
struct Store;
impl Store {
    fn open() -> Self { Self }
    fn read(&self) {}
}
"""
        )
        kinds = {record["name"]: record["kind"] for record in records}
        self.assertEqual(kinds["transform"], "free")
        self.assertEqual(kinds["open"], "associated")
        self.assertEqual(kinds["read"], "method")
        transform = next(record for record in records if record["name"] == "transform")
        self.assertEqual(
            transform["parameters"],
            [{"name": "value", "type": "u64"}],
        )
        self.assertEqual(transform["return_type"], "u64")

    def test_callable_signature_excludes_leading_comments_and_attributes(self):
        records = self.inventory(
            """
/// Explain the operation.
#[cfg(test)]
pub(crate) async fn load(value: u64) -> u64 { value }
"""
        )

        self.assertEqual(
            records[0]["signature"],
            "pub(crate) async fn load(value: u64) -> u64",
        )

    def test_inventory_marks_receiver_constructors(self):
        records = self.inventory(
            """
struct Store {
    database: Database,
}
struct Database;
impl Store {
    fn new(database: Database) -> Self {
        Self { database }
    }

    fn checked(database: Database) -> Self {
        assert!(database.is_valid());
        Self { database }
    }

    fn loaded(database: Database) -> Self {
        Self {
            database: load(database),
        }
    }

    fn explicit(database: Database) -> Store {
        Self { database }
    }
}
impl Database {
    fn is_valid(&self) -> bool { true }
}
fn load(database: Database) -> Database { database }
"""
        )
        constructors = {
            record["name"]: record["receiver_constructor"]
            for record in records
            if record["receiver_type"] == "Store"
        }
        self.assertEqual(
            constructors,
            {
                "new": True,
                "checked": True,
                "loaded": True,
                "explicit": True,
            },
        )

    def test_inventory_separates_parameter_and_return_dependencies(self):
        records = self.inventory(
            """
fn open_image(image: &[u8]) -> Result<Connection, DbError> {
    todo!()
}
fn load_root(conn: &Connection) -> Result<StoreRootRef, DbError> {
    todo!()
}
"""
        )
        by_name = {
            record["name"]: record
            for record in records
        }
        self.assertEqual(by_name["open_image"]["parameter_dependencies"], [])
        self.assertEqual(
            by_name["open_image"]["return_dependencies"],
            ["database"],
        )
        self.assertEqual(
            by_name["load_root"]["parameter_dependencies"],
            ["database"],
        )
        self.assertEqual(
            by_name["load_root"]["return_dependencies"],
            ["authority"],
        )

    def test_inventory_uses_the_implemented_type_as_receiver(self):
        records = self.inventory(
            """
struct Store<'a>(&'a ());
impl<'a> Store<'a> {
    fn read(&self) {}
}
#[async_trait]
impl<'a> Reader for Store<'a> {
    fn load(&self) {}
}
"""
        )
        read = next(record for record in records if record["name"] == "read")
        load = next(record for record in records if record["name"] == "load")
        self.assertEqual(read["receiver_type"], "Store<'a>")
        self.assertEqual(load["receiver_type"], "Reader for Store<'a>")
        self.assertEqual(
            read["symbol"],
            "coven_core::sample::<Store<'a>>::read",
        )
        self.assertEqual(
            load["symbol"],
            "coven_core::sample::<Reader for Store<'a>>::load",
        )

    def test_inventory_finds_retained_dependencies_and_ambient_access(self):
        records = self.inventory(
            """
fn publish(database: &Database, storage: &dyn SyncStorage) {
    let now = SystemTime::now();
    storage.create_object(database.load(now));
}
"""
        )
        record = records[0]
        self.assertEqual(record["retained_dependencies"], ["database", "storage"])
        self.assertEqual(record["ambient_dependencies"], ["clock"])
        self.assertIn("storage-write", record["effects"])

    def test_service_exposure_report_uses_syntax_tree_boundaries(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "crates" / "coven-core" / "src" / "sample.rs"
            path.parent.mkdir(parents=True)
            source = """
pub(crate) struct History<'a> {
    pub(super) database: StoreDatabase,
    history_verifier: MergeHistoryVerifier<'a>,
    pub label: String,
}
impl History<'_> {
    pub(super) fn database(&self) -> &StoreDatabase {
        &self.database
    }

    fn storage(&self) -> &dyn SyncStorage {
        todo!()
    }

    fn verify(&mut self) {
        let _ = &mut self.history.history_verifier;
        let _ = &self.history.root;
    }
}
"""
            path.write_text(source)
            source_file = ownership_audit.parse_source(path, source)
            original_root = ownership_audit.ROOT
            ownership_audit.ROOT = root
            try:
                findings = ownership_audit.service_exposures_for_source(source_file)
            finally:
                ownership_audit.ROOT = original_root

        self.assertEqual(
            [
                (
                    finding["kind"],
                    finding["name"],
                    finding["dependencies"],
                )
                for finding in findings
            ],
            [
                ("service-field", "History::database", ["database"]),
                (
                    "service-getter",
                    "coven_core::sample::<History<'_>>::database",
                    ["database"],
                ),
                (
                    "nested-service-reach",
                    "coven_core::sample::<History<'_>>::verify",
                    ["verification"],
                ),
            ],
        )

    def test_service_exposure_report_finds_anonymous_owner_bundles(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "crates" / "coven-core" / "src" / "sample.rs"
            path.parent.mkdir(parents=True)
            source = """
fn founder_authority() -> Result<
    (StoreDeviceRegistrationRef, StoreDeviceRegistration, UserKeypair),
    Error,
> {
    todo!()
}

fn database_runtime() -> (Database, RuntimeHandle) {
    todo!()
}

fn nested_runtime() -> Option<(HashMap<String, Vec<u8>>, RuntimeHandle)> {
    todo!()
}

fn callback_factory() -> impl Fn(Database, RuntimeHandle) {
    todo!()
}

fn selected_position() -> (String, u64) {
    todo!()
}
"""
            path.write_text(source)
            source_file = ownership_audit.parse_source(path, source)
            original_root = ownership_audit.ROOT
            ownership_audit.ROOT = root
            try:
                findings = ownership_audit.service_exposures_for_source(source_file)
            finally:
                ownership_audit.ROOT = original_root

        bundles = [
            finding
            for finding in findings
            if finding["kind"] == "service-bundle"
        ]
        self.assertEqual(
            [
                (
                    finding["name"],
                    finding["dependencies"],
                    finding["members"],
                )
                for finding in bundles
            ],
            [
                (
                    "coven_core::sample::founder_authority",
                    ["authority", "identity"],
                    [
                        "StoreDeviceRegistrationRef",
                        "StoreDeviceRegistration",
                        "UserKeypair",
                    ],
                ),
                (
                    "coven_core::sample::database_runtime",
                    ["database", "runtime"],
                    ["Database", "RuntimeHandle"],
                ),
                (
                    "coven_core::sample::nested_runtime",
                    ["runtime"],
                    ["HashMap<String, Vec<u8>>", "RuntimeHandle"],
                ),
            ],
        )

    def test_call_site_records_tuple_destructuring(self):
        records = self.inventory(
            """
fn database_runtime() -> (Database, RuntimeHandle) { todo!() }
fn consume(_: Database, _: RuntimeHandle) {}
fn caller() {
    let (database, runtime) = database_runtime();
    consume(database, runtime);
}
"""
        )
        caller = next(record for record in records if record["name"] == "caller")
        site = next(
            call
            for call in caller["calls"]
            if call["callee_text"] == "database_runtime"
        )

        self.assertEqual(
            site["result_binding"],
            {
                "kind": "TUPLE_PAT",
                "text": "(database, runtime)",
                "members": [
                    {"position": 0, "text": "database", "bindings": ["database"]},
                    {"position": 1, "text": "runtime", "bindings": ["runtime"]},
                ],
            },
        )

    def test_inventory_records_closures_under_their_enclosing_callable(self):
        records = self.inventory(
            "fn run(database: &Database) { let load = || database.load(); load(); }"
        )
        closure = next(record for record in records if record["kind"] == "closure")
        self.assertIn("::run::<closure@", closure["symbol"])
        self.assertEqual(closure["binding"], "load")
        self.assertTrue(closure["calls"])

    def test_closure_captures_only_outer_lexical_bindings(self):
        records = self.inventory(
            """
fn run(database: &Database, ignored: u64) {
    let offset = 3;
    let callback = move |value: u64| {
        let inner = value + 1;
        database.load(offset + inner)
    };
    callback(ignored);
}
"""
        )
        closure = next(record for record in records if record["kind"] == "closure")
        self.assertEqual(
            closure["captured_values"],
            [
                {
                    "declared_by": "coven_core::sample::run",
                    "kind": "parameter",
                    "name": "database",
                    "type": "&Database",
                },
                {
                    "declared_by": "coven_core::sample::run",
                    "kind": "local",
                    "name": "offset",
                    "type": "",
                },
            ],
        )

    def test_async_block_records_outer_captures(self):
        records = self.inventory(
            """
fn run(database: Database) {
    let task = async move { database.load().await };
    consume(task);
}
"""
        )
        block = next(record for record in records if record["kind"] == "async-block")
        self.assertEqual(
            [capture["name"] for capture in block["captured_values"]],
            ["database"],
        )

    def test_unicode_before_callable_does_not_shift_parser_offsets(self):
        records = self.inventory(
            """
/// An em dash — occupies three UTF-8 bytes.
struct Store;
impl Store {
    fn actual_name(&self) {}
}
"""
        )
        record = next(record for record in records if record["name"] == "actual_name")
        self.assertEqual(record["kind"], "method")
        self.assertEqual(record["symbol"], "coven_core::sample::<Store>::actual_name")

    def test_nested_syntax_nodes_record_one_macro_call(self):
        records = self.inventory(
            'fn run() { tracing::info!(value = load(), "loaded"); }'
        )
        run = next(record for record in records if record["name"] == "run")
        macro_calls = [
            call
            for call in run["calls"]
            if call["text"] == 'tracing::info!(value = load(), "loaded")'
        ]
        self.assertEqual(len(macro_calls), 1)

    def test_source_call_site_uses_the_enclosing_call_on_its_line(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.rs"
            source_file = ownership_audit.parse_source(
                path,
                "fn run() {\n    outer(inner());\n    elsewhere();\n}\n",
            )
            site = ownership_audit.source_call_site(
                {path.resolve(): source_file},
                path.resolve(),
                {
                    "start": {"line": 1, "character": 10},
                    "end": {"line": 1, "character": 15},
                },
            )
        self.assertEqual(site["callee_text"], "inner")
        self.assertEqual(site["expression"], "inner()")

    def test_item_macro_expansion_callables_are_attributed_to_invocation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            path = root / "crates" / "coven-core" / "src" / "sample.rs"
            path.parent.mkdir(parents=True)
            source = 'mod ids { generated_id!(CircleId); }'
            path.write_text(source)
            source_file = ownership_audit.parse_source(path, source)
            original_root = ownership_audit.ROOT
            ownership_audit.ROOT = root
            try:
                invocation = ownership_audit.item_macro_invocations(source_file)[0]
                records = ownership_audit.inventory_macro_expansion(
                    invocation,
                    "struct CircleId; impl CircleId { fn parse() {} }",
                )
            finally:
                ownership_audit.ROOT = original_root
        parse = next(record for record in records if record["name"] == "parse")
        self.assertEqual(parse["module"], "coven_core::sample::ids")
        self.assertEqual(parse["macro_origin"]["name"], "generated_id")
        self.assertEqual(
            parse["symbol"],
            "coven_core::sample::ids::<CircleId>::parse",
        )

    def test_macro_generated_target_uses_the_call_receiver(self):
        records = [
            {
                "symbol": "sample::<First>::generate",
                "name": "generate",
                "receiver_type": "First",
                "signature": "fn generate() -> Self",
                "macro_origin": {},
            },
            {
                "symbol": "sample::<Second>::generate",
                "name": "generate",
                "receiver_type": "Second",
                "signature": "fn generate() -> Self",
                "macro_origin": {},
            },
        ]
        target = {
            "name": "generate",
            "detail": "fn generate() -> Self",
        }
        self.assertEqual(
            ownership_audit.macro_generated_target(
                records,
                target,
                {"callee_text": "Second::generate"},
            ),
            "sample::<Second>::generate",
        )

    def test_trait_call_lists_macro_generated_implementations(self):
        trait = {
            "name": "from_action",
            "receiver_type": "trait DeviceJoinArtifact",
        }
        records = [
            trait,
            {
                "symbol": "sample::<DeviceJoinArtifact for Activation>::from_action",
                "name": "from_action",
                "receiver_type": "DeviceJoinArtifact for Activation",
            },
            {
                "symbol": "sample::<Unrelated for Activation>::from_action",
                "name": "from_action",
                "receiver_type": "Unrelated for Activation",
            },
        ]
        self.assertEqual(
            ownership_audit.trait_implementation_candidates(
                trait,
                records,
                "T::from_action",
            ),
            ["sample::<DeviceJoinArtifact for Activation>::from_action"],
        )

    def test_concrete_trait_call_narrows_generated_implementation(self):
        trait = {
            "name": "from_action",
            "receiver_type": "trait DeviceJoinArtifact",
        }
        records = [
            trait,
            {
                "symbol": "sample::<DeviceJoinArtifact for Activation>::from_action",
                "name": "from_action",
                "receiver_type": "DeviceJoinArtifact for Activation",
            },
            {
                "symbol": "sample::<DeviceJoinArtifact for Cancellation>::from_action",
                "name": "from_action",
                "receiver_type": "DeviceJoinArtifact for Cancellation",
            },
        ]
        self.assertEqual(
            ownership_audit.trait_implementation_candidates(
                trait,
                records,
                "Activation::from_action",
            ),
            ["sample::<DeviceJoinArtifact for Activation>::from_action"],
        )

    def test_macro_syntactic_target_requires_same_module_free_callable(self):
        record = {
            "module": "sample::ids",
            "macro_origin": {},
        }
        records = [
            {
                "symbol": "sample::ids::generated_bytes",
                "module": "sample::ids",
                "kind": "free",
                "name": "generated_bytes",
            },
            {
                "symbol": "sample::storage::<Storage>::get",
                "module": "sample::storage",
                "kind": "method",
                "name": "get",
            },
        ]
        self.assertEqual(
            ownership_audit.macro_syntactic_target(
                record,
                {"callee_text": "generated_bytes"},
                records,
            ),
            "sample::ids::generated_bytes",
        )
        self.assertIsNone(
            ownership_audit.macro_syntactic_target(
                record,
                {"callee_text": "get"},
                records,
            )
        )

    def test_call_inventory_records_top_level_argument_expressions(self):
        records = self.inventory(
            "fn run() { target(value, || nested()); }"
        )
        run = next(record for record in records if record["name"] == "run")
        target = next(call for call in run["calls"] if call["callee_text"] == "target")
        self.assertEqual(
            [argument["text"] for argument in target["arguments"]],
            ["value", "|| nested()"],
        )

    def test_configuration_variants_have_distinct_semantic_identities(self):
        records = self.inventory(
            """
#[cfg(target_os = "macos")]
fn install() {}
#[cfg(
    target_os = "linux"
)]
fn install() {}
"""
        )
        symbols = {record["symbol"] for record in records}
        self.assertEqual(len(symbols), 2)
        self.assertTrue(all("@#[cfg" in symbol for symbol in symbols))

    def test_impl_configuration_distinguishes_method_identities(self):
        records = self.inventory(
            """
struct Store;
#[cfg(target_os = "macos")]
impl Store { fn install(&self) {} }
#[cfg(target_os = "linux")]
impl Store { fn install(&self) {} }
"""
        )
        installs = [
            record["symbol"]
            for record in records
            if record["name"] == "install"
        ]
        self.assertEqual(len(set(installs)), 2)
        self.assertTrue(all("@#[cfg" in symbol for symbol in installs))

    def test_callable_and_call_conditions_include_ancestor_cfg(self):
        records = self.inventory(
            """
#[cfg(feature = "provider")]
mod provider {
    fn run() {
        #[cfg(test)]
        target();
    }
}
"""
        )
        run = next(record for record in records if record["name"] == "run")
        self.assertEqual(run["cfg"], ['#[cfg(feature = "provider")]'])
        target = next(call for call in run["calls"] if call["callee_text"] == "target")
        self.assertEqual(target["cfg"], ["#[cfg(test)]"])

    def test_nested_callables_inherit_test_context(self):
        records = self.inventory(
            """
#[tokio::test]
async fn writes() {
    run(|| save());
}
"""
        )
        self.assertTrue(records)
        self.assertTrue(all(record["test_context"] for record in records))

    def test_inventory_distinguishes_test_entries_from_test_helpers(self):
        records = self.inventory(
            """
#[cfg(test)]
fn helper() {}

#[test]
fn direct_test() { helper(); }

#[tokio::test]
async fn async_test() { helper(); }

#[rstest]
fn generated_test() { helper(); }
"""
        )
        by_name = {record["name"]: record for record in records}

        self.assertFalse(by_name["helper"]["test_entry"])
        self.assertTrue(by_name["direct_test"]["test_entry"])
        self.assertTrue(by_name["async_test"]["test_entry"])
        self.assertTrue(by_name["generated_test"]["test_entry"])

    def test_nested_functions_belong_to_their_enclosing_callable(self):
        records = self.inventory(
            """
fn first() { fn transform() {} transform(); }
fn second() { fn transform() {} transform(); }
"""
        )
        transforms = [
            record
            for record in records
            if record["name"] == "transform"
        ]
        self.assertEqual(
            {record["symbol"] for record in transforms},
            {
                "coven_core::sample::first::<local>::transform",
                "coven_core::sample::second::<local>::transform",
            },
        )
        self.assertTrue(all(record["kind"] == "free" for record in transforms))


class DecisionLedgerTests(unittest.TestCase):
    def test_parse_decisions_accepts_the_supported_toml_shape(self):
        ledger = ownership_audit.parse_decisions(
            """
[[decision]]
symbol = "coven_core::sample::transform"
signature = "fn transform(value: u64) -> u64"
classification = "transformation"
reason = "The output depends only on the explicit value."
status = "verified"
"""
        )
        self.assertEqual(
            ledger["decision"][0]["classification"],
            "transformation",
        )

    def test_absent_local_decision_ledger_starts_empty(self):
        with tempfile.TemporaryDirectory() as directory:
            original_path = ownership_audit.DECISIONS_PATH
            ownership_audit.DECISIONS_PATH = Path(directory) / "decisions.toml"
            try:
                self.assertEqual(ownership_audit.read_decisions(), {})
            finally:
                ownership_audit.DECISIONS_PATH = original_path

    def test_decision_signature_ignores_rust_formatting_whitespace(self):
        with tempfile.TemporaryDirectory() as directory:
            original_path = ownership_audit.DECISIONS_PATH
            ownership_audit.DECISIONS_PATH = Path(directory) / "decisions.toml"
            ownership_audit.DECISIONS_PATH.write_text(
                """
[[decision]]
symbol = "sample::transform"
signature = "fn transform(value: u64) -> Result<u64, Error>"
classification = "transformation"
reason = "The explicit value completely determines the result."
status = "verified"
"""
            )
            try:
                decisions = ownership_audit.read_decisions()
            finally:
                ownership_audit.DECISIONS_PATH = original_path

        current = {
            "symbol": "sample::transform",
            "signature": (
                "fn transform( value: u64, ) "
                "-> Result<u64, Error>"
            ),
        }
        self.assertEqual(
            ownership_audit.unclassified(
                {"callables": [current]},
                decisions,
            ),
            [],
        )

    def test_test_entries_and_lexical_bodies_do_not_need_ledger_rows(self):
        test_entry = {
            "symbol": "sample::writes",
            "signature": "fn writes()",
            "kind": "free",
            "test_entry": True,
        }
        closure = {
            "symbol": "sample::writes::<closure@2:5>",
            "signature": "|| persist()",
            "kind": "closure",
            "enclosing_callable": test_entry["symbol"],
        }
        helper = {
            "symbol": "sample::persist",
            "signature": "fn persist()",
            "kind": "free",
            "test_entry": False,
        }

        self.assertEqual(
            ownership_audit.unclassified(
                {"callables": [test_entry, closure, helper]},
                {},
            ),
            [helper],
        )

    def test_receivers_and_receiver_constructors_are_code_dispositions(self):
        method = {
            "symbol": "sample::<Store>::read",
            "signature": "fn read(&self)",
            "kind": "method",
            "receiver_type": "Store",
            "receiver_constructor": False,
            "parameters": [{"name": "self", "type": "&self"}],
            "test_entry": False,
        }
        constructor = {
            "symbol": "sample::<Store>::new",
            "signature": "fn new() -> Store",
            "kind": "associated",
            "receiver_type": "Store",
            "receiver_constructor": True,
            "parameters": [],
            "test_entry": False,
        }
        free = {
            "symbol": "sample::read",
            "signature": "fn read()",
            "kind": "free",
            "receiver_type": None,
            "receiver_constructor": False,
            "parameters": [],
            "test_entry": False,
        }

        self.assertEqual(
            ownership_audit.unclassified(
                {"callables": [method, constructor, free]},
                {},
            ),
            [free],
        )

    def test_unresolved_call_requires_an_exact_reviewed_edge_decision(self):
        record = {
            "symbol": "sample::map",
            "signature": "fn map(convert: impl FnOnce(u64) -> u64)",
            "call_hierarchy": "resolved",
            "call_hierarchy_views": {"library-default": "resolved"},
            "unresolved_calls": [
                {
                    "text": "convert(value)",
                    "resolution": "callable-parameter",
                    "dynamic_dispatch_candidates": ["external-caller-supplied"],
                }
            ],
        }
        index = {"callables": [record]}

        evidence = ownership_audit.unresolved_edge_evidence(index)
        self.assertEqual(len(evidence), 1)
        self.assertEqual(
            ownership_audit.unreviewed_edges(index, {}),
            evidence,
        )

        decision = {
            **evidence[0],
            "reason": "The callback is supplied by the caller and invoked once.",
            "status": "reviewed",
        }
        reviewed = {ownership_audit.edge_decision_key(decision): decision}
        self.assertEqual(ownership_audit.unreviewed_edges(index, reviewed), [])

        record["unresolved_calls"][0]["dynamic_dispatch_candidates"] = [
            "sample::convert"
        ]
        self.assertEqual(len(ownership_audit.unreviewed_edges(index, reviewed)), 1)

    def test_unavailable_call_hierarchy_requires_a_reviewed_disposition(self):
        record = {
            "symbol": "sample::platform_only",
            "signature": "fn platform_only()",
            "call_hierarchy": "unavailable",
            "call_hierarchy_views": {"library-default": "unavailable"},
            "unresolved_calls": [],
        }
        evidence = ownership_audit.unresolved_edge_evidence(
            {"callables": [record]}
        )

        self.assertEqual(
            evidence,
            [
                {
                    "caller": "sample::platform_only",
                    "signature": "fn platform_only()",
                    "kind": "call-hierarchy",
                    "expression": "<call-hierarchy>",
                    "resolution": "unavailable",
                    "candidates": ["library-default:unavailable"],
                }
            ],
        )


class SemanticCacheTests(unittest.TestCase):
    def test_semantic_workspace_cache_only_globally_tracks_analyzer_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            crate = root / "crates" / "sample"
            manifest = crate / "Cargo.toml"
            library_path = crate / "src" / "lib.rs"
            build_path = crate / "build.rs"
            tool_path = root / "ownership_audit.py"
            library_path.parent.mkdir(parents=True)
            manifest.write_text("[package]\nname = \"sample\"\n")
            library_path.write_text("struct Model { value: u8 }\n")
            build_path.write_text("fn main() { first(); }\n")
            tool_path.write_text("graph assembly version one\n")
            metadata = {
                "packages": [
                    {
                        "manifest_path": str(manifest),
                        "targets": [
                            {
                                "kind": ["lib"],
                                "src_path": str(library_path),
                            },
                            {
                                "kind": ["custom-build"],
                                "src_path": str(build_path),
                            },
                        ],
                    }
                ]
            }

            def fingerprint() -> str:
                sources = {
                    path.resolve(): ownership_audit.parse_source(path)
                    for path in (library_path, build_path)
                }
                return ownership_audit.semantic_workspace_cache_fingerprint(
                    metadata,
                    sources,
                    "rust-analyzer 1",
                )

            original_root = ownership_audit.ROOT
            original_file = ownership_audit.__file__
            ownership_audit.ROOT = root
            ownership_audit.__file__ = str(tool_path)
            try:
                baseline = fingerprint()
                tool_path.write_text("graph assembly version two\n")
                changed_tool = fingerprint()
                library_path.write_text("struct Model { value: u16 }\n")
                changed_library = fingerprint()
                build_path.write_text("fn main() { second(); }\n")
                changed_build = fingerprint()
            finally:
                ownership_audit.ROOT = original_root
                ownership_audit.__file__ = original_file

        self.assertEqual(baseline, changed_tool)
        self.assertEqual(baseline, changed_library)
        self.assertNotEqual(changed_library, changed_build)

    def test_semantic_entry_tracks_referenced_declaration_surfaces(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            source_root = root / "crates" / "sample" / "src"
            caller_path = source_root / "lib.rs"
            model_path = source_root / "model.rs"
            source_root.mkdir(parents=True)
            caller_path.write_text(
                "use crate::model::Wrapper;\n"
                "fn caller(value: &Wrapper) { value.child.read(); }\n"
                "fn stable() { same_target(); }\n"
            )
            model_path.write_text(
                "/// First wording.\n"
                "struct Wrapper { child: Alias }\n"
                "type Alias = First;\n"
                "struct Unrelated { value: u8 }\n"
            )

            def fingerprints() -> dict[str, str]:
                source_files = {
                    path.resolve(): ownership_audit.parse_source(path)
                    for path in (caller_path, model_path)
                }
                records = [
                    record
                    for source_file in source_files.values()
                    for record in ownership_audit.inventory_file(source_file)
                ]
                declaration_surfaces = (
                    ownership_audit.semantic_declaration_surfaces(source_files)
                )
                source_fingerprints = (
                    ownership_audit.semantic_entry_source_fingerprints(
                        records,
                        records,
                        source_files,
                        declaration_surfaces,
                    )
                )
                return {
                    record["name"]: source_fingerprints[record["symbol"]]
                    for record in records
                    if record["path"] == "crates/sample/src/lib.rs"
                }

            original_root = ownership_audit.ROOT
            ownership_audit.ROOT = root
            try:
                baseline = fingerprints()
                model_path.write_text(
                    "/// Second wording.\n"
                    "struct Wrapper { child: Alias }\n"
                    "type Alias = First;\n"
                    "struct Unrelated { value: u8 }\n"
                )
                changed_comment = fingerprints()
                model_path.write_text(
                    "/// Second wording.\n"
                    "struct Wrapper { child: Alias }\n"
                    "type Alias = Second;\n"
                    "struct Unrelated { value: u8 }\n"
                )
                changed_dependency = fingerprints()
                model_path.write_text(
                    "/// Second wording.\n"
                    "struct Wrapper { child: Alias }\n"
                    "type Alias = Second;\n"
                    "struct Unrelated { value: u16 }\n"
                )
                changed_unrelated = fingerprints()
            finally:
                ownership_audit.ROOT = original_root

        self.assertEqual(baseline["caller"], changed_comment["caller"])
        self.assertEqual(baseline["stable"], changed_comment["stable"])
        self.assertNotEqual(baseline["caller"], changed_dependency["caller"])
        self.assertEqual(baseline["stable"], changed_dependency["stable"])
        self.assertEqual(
            changed_dependency["caller"],
            changed_unrelated["caller"],
        )
        self.assertEqual(
            changed_dependency["stable"],
            changed_unrelated["stable"],
        )

    def test_unrelated_grouped_import_does_not_invalidate_a_type_user(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            source_root = root / "crates" / "sample" / "src"
            caller_path = source_root / "lib.rs"
            model_path = source_root / "model.rs"
            unrelated_path = source_root / "unrelated.rs"
            source_root.mkdir(parents=True)
            caller_path.write_text(
                "use crate::model::Wrapper;\n"
                "fn caller(value: &Wrapper) { value.read(); }\n"
                "fn stable() { same_target(); }\n"
            )
            model_path.write_text(
                "struct Wrapper;\n"
                "struct First;\n"
                "struct Second;\n"
            )

            def fingerprints() -> dict[str, str]:
                source_files = {
                    path.resolve(): ownership_audit.parse_source(path)
                    for path in (caller_path, model_path, unrelated_path)
                }
                records = [
                    record
                    for source_file in source_files.values()
                    for record in ownership_audit.inventory_file(source_file)
                ]
                declaration_surfaces = (
                    ownership_audit.semantic_declaration_surfaces(source_files)
                )
                source_fingerprints = (
                    ownership_audit.semantic_entry_source_fingerprints(
                        records,
                        records,
                        source_files,
                        declaration_surfaces,
                    )
                )
                return {
                    record["name"]: source_fingerprints[record["symbol"]]
                    for record in records
                    if record["path"] == "crates/sample/src/lib.rs"
                }

            original_root = ownership_audit.ROOT
            ownership_audit.ROOT = root
            try:
                unrelated_path.write_text(
                    "use crate::model::{Wrapper, First};\n"
                )
                baseline = fingerprints()
                unrelated_path.write_text(
                    "use crate::model::{Wrapper, Second};\n"
                )
                changed = fingerprints()
            finally:
                ownership_audit.ROOT = original_root

        self.assertEqual(baseline["caller"], changed["caller"])
        self.assertEqual(baseline["stable"], changed["stable"])

    def test_moving_an_unchanged_declaration_does_not_invalidate_its_dependents(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            source_root = root / "crates" / "sample" / "src"
            caller_path = source_root / "caller.rs"
            container_path = source_root / "container.rs"
            old_path = source_root / "old.rs"
            new_path = source_root / "new.rs"
            source_root.mkdir(parents=True)
            caller_path.write_text(
                "use crate::container::Container;\n"
                "fn caller(value: &Container) { value.read(); }\n"
            )

            def fingerprint(import_path: str, moved_path: Path) -> str:
                container_path.write_text(
                    f"use crate::{import_path}::Moved;\n"
                    "struct Container { value: Moved }\n"
                )
                moved_path.write_text("struct Moved { value: u8 }\n")
                source_files = {
                    path.resolve(): ownership_audit.parse_source(path)
                    for path in (caller_path, container_path, moved_path)
                }
                records = [
                    record
                    for source_file in source_files.values()
                    for record in ownership_audit.inventory_file(source_file)
                ]
                caller = next(
                    record for record in records if record["name"] == "caller"
                )
                return ownership_audit.semantic_entry_source_fingerprints(
                    records,
                    records,
                    source_files,
                    ownership_audit.semantic_declaration_surfaces(source_files),
                )[caller["symbol"]]

            original_root = ownership_audit.ROOT
            ownership_audit.ROOT = root
            try:
                baseline = fingerprint("old", old_path)
                old_path.unlink()
                moved = fingerprint("new", new_path)
            finally:
                ownership_audit.ROOT = original_root

        self.assertEqual(baseline, moved)

    def test_moving_an_unrelated_same_named_callable_keeps_caller_fingerprint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            source_root = root / "crates" / "sample" / "src"
            caller_path = source_root / "caller.rs"
            stable_path = source_root / "stable.rs"
            old_path = source_root / "old.rs"
            new_path = source_root / "new.rs"
            source_root.mkdir(parents=True)
            caller_path.write_text("fn caller(value: &Stable) { value.run(); }\n")
            stable_path.write_text("struct Stable; impl Stable { fn run(&self) {} }\n")

            def fingerprint(moved_path: Path) -> str:
                moved_path.write_text("struct Other; impl Other { fn run(&self) {} }\n")
                source_files = {
                    path.resolve(): ownership_audit.parse_source(path)
                    for path in (caller_path, stable_path, moved_path)
                }
                records = [
                    record
                    for source_file in source_files.values()
                    for record in ownership_audit.inventory_file(source_file)
                ]
                caller = next(
                    record for record in records if record["name"] == "caller"
                )
                return ownership_audit.semantic_entry_source_fingerprints(
                    records,
                    records,
                    source_files,
                    ownership_audit.semantic_declaration_surfaces(source_files),
                )[caller["symbol"]]

            original_root = ownership_audit.ROOT
            ownership_audit.ROOT = root
            try:
                baseline = fingerprint(old_path)
                old_path.unlink()
                moved = fingerprint(new_path)
            finally:
                ownership_audit.ROOT = original_root

        self.assertEqual(baseline, moved)

    def test_unrelated_impl_header_does_not_invalidate_type_user(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            source_root = root / "crates" / "sample" / "src"
            caller_path = source_root / "caller.rs"
            model_path = source_root / "model.rs"
            impl_path = source_root / "implementation.rs"
            source_root.mkdir(parents=True)
            caller_path.write_text(
                "fn caller(value: &Wrapper<Stable>) { same_target(); }\n"
            )
            model_path.write_text(
                "struct Stable;\n"
                "struct Wrapper<T>(T);\n"
            )

            def fingerprint(implementation: str) -> str:
                impl_path.write_text(implementation)
                source_files = {
                    path.resolve(): ownership_audit.parse_source(path)
                    for path in (caller_path, model_path, impl_path)
                }
                records = [
                    record
                    for source_file in source_files.values()
                    for record in ownership_audit.inventory_file(source_file)
                ]
                caller = next(
                    record for record in records if record["name"] == "caller"
                )
                return ownership_audit.semantic_entry_source_fingerprints(
                    records,
                    records,
                    source_files,
                    ownership_audit.semantic_declaration_surfaces(source_files),
                )[caller["symbol"]]

            original_root = ownership_audit.ROOT
            ownership_audit.ROOT = root
            try:
                baseline = fingerprint(
                    "trait First {}\n"
                    "impl<T> First for Wrapper<T> {}\n"
                )
                changed = fingerprint(
                    "trait Second {}\n"
                    "impl<T> Second for Wrapper<T> {}\n"
                )
            finally:
                ownership_audit.ROOT = original_root

        self.assertEqual(baseline, changed)

    def test_impl_header_change_invalidates_its_method_and_callers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            source_root = root / "crates" / "sample" / "src"
            caller_path = source_root / "caller.rs"
            model_path = source_root / "model.rs"
            source_root.mkdir(parents=True)
            caller_path.write_text(
                "fn caller(value: &Wrapper) { value.run(); }\n"
                "fn stable() { same_target(); }\n"
            )

            def fingerprints(trait_name: str) -> dict[str, str]:
                model_path.write_text(
                    "trait First {}\n"
                    "trait Second {}\n"
                    "struct Wrapper;\n"
                    f"impl {trait_name} for Wrapper {{ fn run(&self) {{}} }}\n"
                )
                source_files = {
                    path.resolve(): ownership_audit.parse_source(path)
                    for path in (caller_path, model_path)
                }
                records = [
                    record
                    for source_file in source_files.values()
                    for record in ownership_audit.inventory_file(source_file)
                ]
                source_fingerprints = (
                    ownership_audit.semantic_entry_source_fingerprints(
                        records,
                        records,
                        source_files,
                        ownership_audit.semantic_declaration_surfaces(source_files),
                    )
                )
                return {
                    record["name"]: source_fingerprints[record["symbol"]]
                    for record in records
                }

            original_root = ownership_audit.ROOT
            ownership_audit.ROOT = root
            try:
                baseline = fingerprints("First")
                changed = fingerprints("Second")
            finally:
                ownership_audit.ROOT = original_root

        self.assertNotEqual(baseline["run"], changed["run"])
        self.assertNotEqual(baseline["caller"], changed["caller"])
        self.assertEqual(baseline["stable"], changed["stable"])

    def test_semantic_cache_invalidates_only_the_callable_whose_body_changed(self):
        def fingerprints(root: Path, path: Path, source: str):
            source_file = ownership_audit.parse_source(path, source)
            records = ownership_audit.inventory_file(source_file)
            surface = ownership_audit.semantic_workspace_cache_fingerprint(
                {"packages": []},
                {path: source_file},
                "rust-analyzer 1",
            )
            view = ownership_audit.SEMANTIC_VIEWS[0]
            view_fingerprint = ownership_audit.view_cache_fingerprint(surface, view)
            declaration_surfaces = ownership_audit.semantic_declaration_surfaces(
                {path: source_file}
            )
            source_fingerprints = (
                ownership_audit.semantic_entry_source_fingerprints(
                    records,
                    records,
                    {path: source_file},
                    declaration_surfaces,
                )
            )
            return surface, {
                record["name"]: ownership_audit.semantic_entry_cache_fingerprint(
                    view_fingerprint,
                    source_fingerprints[record["symbol"]],
                )
                for record in records
            }

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            path = (root / "crates" / "sample" / "src" / "lib.rs").resolve()
            path.parent.mkdir(parents=True)
            original_root = ownership_audit.ROOT
            ownership_audit.ROOT = root
            try:
                baseline_surface, baseline = fingerprints(
                    root,
                    path,
                    "fn changed() { old_target(); } fn stable() { same_target(); }",
                )
                changed_surface, changed = fingerprints(
                    root,
                    path,
                    "fn changed() {\n"
                    "    new_target();\n"
                    "    another_target();\n"
                    "}\n"
                    "fn stable() { same_target(); }",
                )
            finally:
                ownership_audit.ROOT = original_root

        self.assertEqual(baseline_surface, changed_surface)
        self.assertNotEqual(baseline["changed"], changed["changed"])
        self.assertEqual(baseline["stable"], changed["stable"])

    def test_input_hash_changes_with_source_metadata_and_tool_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "sample.rs"
            source.write_text("fn first() {}")
            original_root = ownership_audit.ROOT
            ownership_audit.ROOT = root
            try:
                baseline = ownership_audit.hash_cache_inputs(
                    [source], {"packages": []}, "rust-analyzer 1"
                )
                source.write_text("fn second() {}")
                changed_source = ownership_audit.hash_cache_inputs(
                    [source], {"packages": []}, "rust-analyzer 1"
                )
                changed_metadata = ownership_audit.hash_cache_inputs(
                    [source], {"packages": [{"name": "sample"}]}, "rust-analyzer 1"
                )
                changed_tool = ownership_audit.hash_cache_inputs(
                    [source], {"packages": []}, "rust-analyzer 2"
                )
            finally:
                ownership_audit.ROOT = original_root
        self.assertNotEqual(baseline, changed_source)
        self.assertNotEqual(changed_source, changed_metadata)
        self.assertNotEqual(changed_source, changed_tool)

    def test_semantic_view_cache_rejects_another_fingerprint(self):
        with tempfile.TemporaryDirectory() as directory:
            original_cache_dir = ownership_audit.CACHE_DIR
            ownership_audit.CACHE_DIR = Path(directory)
            view = ownership_audit.SEMANTIC_VIEWS[0]
            try:
                ownership_audit.write_semantic_view_cache(
                    view,
                    "first",
                    {
                        "sample::run": {
                            "fingerprint": "entry-one",
                            "calls": [],
                        }
                    },
                )
                self.assertEqual(
                    ownership_audit.read_semantic_view_cache(
                        view,
                        "first",
                        {"sample::run": "entry-one"},
                    ),
                    {
                        "sample::run": {
                            "fingerprint": "entry-one",
                            "calls": [],
                        }
                    },
                )
                self.assertEqual(
                    ownership_audit.read_semantic_view_cache(
                        view,
                        "second",
                        {"sample::run": "entry-one"},
                    ),
                    {},
                )
            finally:
                ownership_audit.CACHE_DIR = original_cache_dir

    def test_semantic_view_collection_requeries_callers_when_a_callee_changes_shape(self):
        class FakeAnalyzer:
            queried = []

            def __init__(self, root, view):
                pass

            def initialize(self):
                pass

            def wait_until_ready(self):
                pass

            def open_document(self, path, source):
                pass

            def outgoing(self, path, position):
                self.queried.append(position)
                return []

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            path = (root / "crates" / "sample" / "src" / "lib.rs").resolve()
            path.parent.mkdir(parents=True)
            original_root = ownership_audit.ROOT
            original_cache_dir = ownership_audit.CACHE_DIR
            original_analyzer = ownership_audit.RustAnalyzer
            ownership_audit.ROOT = root
            ownership_audit.CACHE_DIR = root / "cache"
            ownership_audit.RustAnalyzer = FakeAnalyzer
            view = ownership_audit.SEMANTIC_VIEWS[0]

            def collect(source: str):
                source_file = ownership_audit.parse_source(path, source)
                records = ownership_audit.inventory_file(source_file)
                declaration_surfaces = (
                    ownership_audit.semantic_declaration_surfaces(
                        {path: source_file}
                    )
                )
                entries = ownership_audit.collect_semantic_view_calls(
                    view,
                    records,
                    records,
                    {path: records},
                    {path: source_file},
                    "workspace",
                    ownership_audit.semantic_entry_source_fingerprints(
                        records,
                        records,
                        {path: source_file},
                        declaration_surfaces,
                    ),
                )
                return records, entries

            try:
                collect(
                    "fn target(value: u8) {}\n"
                    "fn caller() { target(1); }\n"
                    "fn stable() {}\n"
                )
                FakeAnalyzer.queried.clear()
                changed_records, _ = collect(
                    "fn target(value: i8) {}\n"
                    "fn caller() { target(1); }\n"
                    "fn stable() {}\n"
                )
            finally:
                ownership_audit.RustAnalyzer = original_analyzer
                ownership_audit.CACHE_DIR = original_cache_dir
                ownership_audit.ROOT = original_root

        changed_by_name = {
            record["name"]: record
            for record in changed_records
        }
        self.assertEqual(
            FakeAnalyzer.queried,
            [
                changed_by_name["target"]["name_range"]["start"],
                changed_by_name["caller"]["name_range"]["start"],
            ],
        )

    def test_cached_call_ranges_follow_an_unchanged_callable_that_moves(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            path = (root / "crates" / "sample" / "src" / "lib.rs").resolve()
            path.parent.mkdir(parents=True)
            original_root = ownership_audit.ROOT
            ownership_audit.ROOT = root
            try:
                baseline_file = ownership_audit.parse_source(
                    path,
                    "fn caller() { target(); }\n",
                )
                baseline_records = ownership_audit.inventory_file(baseline_file)
                baseline = baseline_records[0]
                call_range = baseline["calls"][0]["callee_range"]
                normalized = ownership_audit.normalize_semantic_calls(
                    baseline,
                    [
                        {
                            "to": {
                                "name": "target",
                                "detail": "fn target()",
                                "uri": "file:///outside.rs",
                                "selectionRange": {
                                    "start": {"line": 0, "character": 0},
                                },
                            },
                            "fromRanges": [call_range],
                        }
                    ],
                    baseline_records,
                    {path: baseline_records},
                    {path: baseline_file},
                )

                moved_file = ownership_audit.parse_source(
                    path,
                    "const PREFIX: u8 = 0;\nfn caller() { target(); }\n",
                )
                moved_records = ownership_audit.inventory_file(moved_file)
                moved = moved_records[0]
                matched_sites = {moved["symbol"]: []}
                ownership_audit.attach_semantic_view_calls(
                    ownership_audit.SEMANTIC_VIEWS[0],
                    moved_records,
                    {moved["symbol"]: {"calls": normalized}},
                    moved_records,
                    {path: moved_records},
                    {path: moved_file},
                    matched_sites,
                )
            finally:
                ownership_audit.ROOT = original_root

        self.assertEqual(
            moved["callees"][0]["sites"][0]["range"]["start"]["line"],
            1,
        )
        self.assertEqual(
            moved["callees"][0]["sites"][0]["expression"],
            "target()",
        )

    def test_semantic_view_collection_queries_only_uncached_callables(self):
        class FakeAnalyzer:
            queried = []

            def __init__(self, root, view):
                pass

            def initialize(self):
                pass

            def wait_until_ready(self):
                pass

            def open_document(self, path, source):
                pass

            def outgoing(self, path, position):
                self.queried.append(position)
                return []

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            path = (root / "crates" / "sample" / "src" / "lib.rs").resolve()
            path.parent.mkdir(parents=True)
            original_root = ownership_audit.ROOT
            original_cache_dir = ownership_audit.CACHE_DIR
            original_analyzer = ownership_audit.RustAnalyzer
            ownership_audit.ROOT = root
            ownership_audit.CACHE_DIR = root / "cache"
            ownership_audit.RustAnalyzer = FakeAnalyzer
            view = ownership_audit.SEMANTIC_VIEWS[0]
            fingerprint = ownership_audit.view_cache_fingerprint("workspace", view)
            try:
                source_file = ownership_audit.parse_source(
                    path,
                    "fn cached() {}\nfn missing() {}\n",
                )
                named = ownership_audit.inventory_file(source_file)
                by_name = {record["name"]: record for record in named}
                declaration_surfaces = ownership_audit.semantic_declaration_surfaces(
                    {path: source_file}
                )
                source_fingerprints = (
                    ownership_audit.semantic_entry_source_fingerprints(
                        named,
                        named,
                        {path: source_file},
                        declaration_surfaces,
                    )
                )
                entry_fingerprints = {
                    record["symbol"]: ownership_audit.semantic_entry_cache_fingerprint(
                        fingerprint,
                        source_fingerprints[record["symbol"]],
                    )
                    for record in named
                }
                ownership_audit.write_semantic_view_cache(
                    view,
                    fingerprint,
                    {
                        by_name["cached"]["symbol"]: {
                            "fingerprint": entry_fingerprints[
                                by_name["cached"]["symbol"]
                            ],
                            "calls": None,
                        }
                    },
                )
                entries = ownership_audit.collect_semantic_view_calls(
                    view,
                    named,
                    named,
                    {path: named},
                    {path: source_file},
                    "workspace",
                    source_fingerprints,
                )
            finally:
                ownership_audit.RustAnalyzer = original_analyzer
                ownership_audit.CACHE_DIR = original_cache_dir
                ownership_audit.ROOT = original_root
        self.assertEqual(
            FakeAnalyzer.queried,
            [by_name["missing"]["name_range"]["start"]],
        )
        self.assertEqual(
            set(entries),
            {by_name["cached"]["symbol"], by_name["missing"]["symbol"]},
        )

    def test_semantic_view_cache_requeries_a_caller_whose_target_moved(self):
        class FakeAnalyzer:
            queried = []

            def __init__(self, root, view):
                pass

            def initialize(self):
                pass

            def wait_until_ready(self):
                pass

            def open_document(self, path, source):
                pass

            def outgoing(self, path, position):
                self.queried.append(position)
                return []

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            path = (root / "crates" / "sample" / "src" / "lib.rs").resolve()
            path.parent.mkdir(parents=True)
            original_root = ownership_audit.ROOT
            original_cache_dir = ownership_audit.CACHE_DIR
            original_analyzer = ownership_audit.RustAnalyzer
            ownership_audit.ROOT = root
            ownership_audit.CACHE_DIR = root / "cache"
            ownership_audit.RustAnalyzer = FakeAnalyzer
            view = ownership_audit.SEMANTIC_VIEWS[0]
            fingerprint = ownership_audit.view_cache_fingerprint("workspace", view)
            try:
                source_file = ownership_audit.parse_source(
                    path,
                    "fn caller() { moved_target(); }\n",
                )
                records = ownership_audit.inventory_file(source_file)
                caller = records[0]
                source_fingerprints = (
                    ownership_audit.semantic_entry_source_fingerprints(
                        records,
                        records,
                        {path: source_file},
                        ownership_audit.semantic_declaration_surfaces(
                            {path: source_file}
                        ),
                    )
                )
                entry_fingerprint = ownership_audit.semantic_entry_cache_fingerprint(
                    fingerprint,
                    source_fingerprints[caller["symbol"]],
                )
                ownership_audit.write_semantic_view_cache(
                    view,
                    fingerprint,
                    {
                        caller["symbol"]: {
                            "fingerprint": entry_fingerprint,
                            "calls": [
                                {
                                    "target": "sample::old::moved_target",
                                    "relativeRanges": [],
                                }
                            ],
                        }
                    },
                )
                ownership_audit.collect_semantic_view_calls(
                    view,
                    records,
                    records,
                    {path: records},
                    {path: source_file},
                    "workspace",
                    source_fingerprints,
                )
            finally:
                ownership_audit.RustAnalyzer = original_analyzer
                ownership_audit.CACHE_DIR = original_cache_dir
                ownership_audit.ROOT = original_root

        self.assertEqual(FakeAnalyzer.queried, [caller["name_range"]["start"]])


class SemanticIndexTests(unittest.TestCase):
    def graph_record(self, symbol, **overrides):
        record = {
            "symbol": symbol,
            "name": symbol.rsplit("::", 1)[-1],
            "signature": "fn call()",
            "module": symbol.rsplit("::", 1)[0],
            "crate": "sample",
            "path": "crates/sample/src/lib.rs",
            "range": {
                "start": {"line": 0, "character": 0},
                "end": {"line": 0, "character": 1},
            },
            "kind": "free",
            "receiver_type": None,
            "parameters": [],
            "retained_dependencies": [],
            "ambient_dependencies": [],
            "effects": [],
            "captured_values": [],
            "unresolved_calls": [],
            "semantic_views": ["library-default"],
            "callers": [],
            "enclosing_callable": None,
            "callees": [],
        }
        record.update(overrides)
        return record

    def test_stale_decisions_index_current_callables_once(self):
        class OnePassCallables(list):
            iterations = 0

            def __iter__(self):
                self.iterations += 1
                if self.iterations > 1:
                    raise AssertionError("callables were scanned more than once")
                return super().__iter__()

        current = self.graph_record("sample::current")
        callables = OnePassCallables([current])
        decisions = {
            ("sample::current", "fn call()"): {
                "classification": "boundary",
            },
            ("sample::absent", "fn call()"): {
                "classification": "boundary",
            },
            ("sample::deleted", "fn call()"): {
                "classification": "delete",
            },
        }

        stale = ownership_audit.stale_decisions(
            {"callables": callables}, decisions
        )

        self.assertEqual(stale, [decisions[("sample::absent", "fn call()")]])
        self.assertEqual(callables.iterations, 1)

    def test_graph_data_exposes_bottom_up_ready_stateful_queue(self):
        owner = self.graph_record(
            "sample::store::<Store>::persist",
            module="sample::store",
            kind="method",
            receiver_type="Store",
            parameters=[{"name": "self", "type": "&self"}],
            effects=["database-write"],
        )
        leaf = self.graph_record(
            "sample::store::publish_blob",
            retained_dependencies=["storage"],
            effects=["storage-write"],
            callers=[
                {
                    "symbol": "sample::store::publish_snapshot",
                    "sites": [{}],
                }
            ],
            callees=[{"symbol": owner["symbol"], "sites": [{}, {}]}],
        )
        parent = self.graph_record(
            "sample::store::publish_snapshot",
            callers=[],
            callees=[{"symbol": leaf["symbol"], "sites": [{}]}],
        )
        owner["callers"] = [
            {"symbol": leaf["symbol"], "sites": [{}, {}]}
        ]
        graph = ownership_audit.build_graph_data(
            {"callables": [parent, leaf, owner], "reach_throughs": {}}
        )
        nodes = {node["label"]: node for node in graph["nodes"]}
        self.assertEqual(nodes["Store"]["kind"], "owner")
        self.assertTrue(nodes["store::publish_blob"]["ready"])
        self.assertEqual(nodes["store::publish_snapshot"]["blockers"], [
            nodes["store::publish_blob"]["id"]
        ])
        self.assertFalse(nodes["store::publish_snapshot"]["ready"])
        self.assertEqual(graph["summary"]["ready"], 1)

    def test_graph_exposes_anonymous_bundle_consumers(self):
        producer = self.graph_record(
            "sample::database_runtime",
            return_type="(Database, RuntimeHandle)",
            retained_dependencies=["database", "runtime"],
            anonymous_owner_bundles=[
                {
                    "members": ["Database", "RuntimeHandle"],
                    "dependencies": ["database", "runtime"],
                }
            ],
            callers=[
                {
                    "symbol": "sample::Store::load",
                    "sites": [
                        {
                            "expression": "database_runtime()",
                            "result_binding": {
                                "kind": "TUPLE_PAT",
                                "text": "(database, runtime)",
                                "members": [
                                    {
                                        "position": 0,
                                        "text": "database",
                                        "bindings": ["database"],
                                    },
                                    {
                                        "position": 1,
                                        "text": "runtime",
                                        "bindings": ["runtime"],
                                    },
                                ],
                            },
                        }
                    ],
                }
            ],
        )
        caller = self.graph_record(
            "sample::Store::load",
            kind="method",
            receiver_type="Store",
            parameters=[{"name": "self", "type": "&self"}],
            calls=[
                {
                    "callee_text": "consume",
                    "arguments": [
                        {"text": "database"},
                        {"text": "runtime"},
                    ],
                }
            ],
            callees=[
                {"symbol": producer["symbol"], "sites": [{}]},
            ],
        )

        graph = ownership_audit.build_graph_data(
            {"callables": [producer, caller], "reach_throughs": {}}
        )
        producer_node = next(
            node
            for node in [
                *graph["nodes"],
                *graph["construction_boundaries"],
            ]
            if any(
                callable_record["symbol"] == producer["symbol"]
                for callable_record in node["callables"]
            )
        )
        bundle = next(
            callable_record["anonymous_owner_bundles"][0]
            for callable_record in producer_node["callables"]
            if callable_record["symbol"] == producer["symbol"]
        )

        self.assertEqual(bundle["nearest_owner"], "Store")
        self.assertEqual(
            bundle["consumers"][0]["downstream_arguments"],
            [
                {
                    "member": 0,
                    "binding": "database",
                    "callee": "consume",
                    "argument": 0,
                    "expression": "database",
                },
                {
                    "member": 1,
                    "binding": "runtime",
                    "callee": "consume",
                    "argument": 1,
                    "expression": "runtime",
                },
            ],
        )
        html = ownership_audit.render_graph_html(graph)
        self.assertIn("Anonymous owner bundles", html)
        self.assertIn("nearest retained owner", html)

    def test_cfg_realm_distinguishes_production_alternatives_from_test_only_paths(self):
        oauth = self.graph_record(
            "sample::oauth::generate_code_verifier",
            effects=["randomness"],
            cfg_paths=[
                ['#[cfg(any(test, feature = "oauth-providers"))]'],
            ],
        )
        merged_variants = self.graph_record(
            "sample::protocol_root::load_pinned",
            effects=["storage-read"],
            cfg_paths=[
                ['#[cfg(any(test, feature = "test-utils"))]'],
                ['#[cfg(not(any(test, feature = "test-utils")))]'],
            ],
        )
        test_only = self.graph_record(
            "sample::fixtures::install",
            effects=["database-write"],
            cfg_paths=[
                ['#[cfg(any(test, feature = "test-utils"))]'],
            ],
        )

        graph = ownership_audit.build_graph_data(
            {
                "callables": [oauth, merged_variants, test_only],
                "reach_throughs": {},
            }
        )
        nodes = {
            node["component"]: node
            for node in graph["nodes"]
            if node["kind"] == "unbound"
        }

        self.assertEqual(nodes[oauth["symbol"]]["realm"], "production")
        self.assertEqual(
            nodes[merged_variants["symbol"]]["realm"], "production"
        )
        self.assertEqual(nodes[test_only["symbol"]]["realm"], "test")

    def test_receiverless_associated_stateful_function_is_unbound(self):
        constructor = self.graph_record(
            "sample::store::<Store>::open",
            module="sample::store",
            kind="associated",
            receiver_type="Store",
            retained_dependencies=["database"],
            effects=["database-read"],
        )
        graph = ownership_audit.build_graph_data(
            {"callables": [constructor], "reach_throughs": {}}
        )
        node = next(
            node
            for node in graph["nodes"]
            if node["kind"] == "unbound"
        )
        self.assertEqual(node["label"], "store::open")
        self.assertTrue(node["ready"])

    def test_receiver_constructor_is_not_a_workflow(self):
        constructor = self.graph_record(
            "sample::store::<Store>::new",
            module="sample::store",
            kind="associated",
            receiver_type="Store",
            return_type="Self",
            retained_dependencies=["database"],
            receiver_constructor=True,
        )
        graph = ownership_audit.build_graph_data(
            {"callables": [constructor], "reach_throughs": {}}
        )
        self.assertEqual(graph["summary"]["stateful_components"], 0)
        self.assertFalse(
            any(node["kind"] != "capability" for node in graph["nodes"])
        )
        self.assertEqual(
            [
                boundary["callables"][0]["symbol"]
                for boundary in graph["construction_boundaries"]
            ],
            [constructor["symbol"]],
        )

    def test_construction_boundary_exposes_its_direct_callers(self):
        constructor = self.graph_record(
            "sample::store::<Store>::new",
            module="sample::store",
            kind="associated",
            receiver_type="Store",
            return_type="Self",
            receiver_constructor=True,
        )
        caller = self.graph_record(
            "sample::application::open_store",
            module="sample::application",
            callees=[{"symbol": constructor["symbol"], "sites": [{}, {}]}],
        )
        constructor["callers"] = [
            {"symbol": caller["symbol"], "sites": [{}, {}]}
        ]
        graph = ownership_audit.build_graph_data(
            {
                "callables": [caller, constructor],
                "reach_throughs": {},
            }
        )
        boundary = graph["construction_boundaries"][0]
        self.assertEqual(
            [record["symbol"] for record in boundary["construction_callers"]],
            [caller["symbol"]],
        )
        self.assertEqual(graph["summary"]["construction_boundaries"], 1)

    def test_free_factory_is_not_a_workflow(self):
        constructor = self.graph_record(
            "sample::store::<Store>::new",
            module="sample::store",
            kind="associated",
            receiver_type="Store",
            return_type="Self",
            receiver_constructor=True,
        )
        factory = self.graph_record(
            "sample::store::open_store",
            return_type="Result<Store, OpenError>",
            retained_dependencies=["database"],
            callees=[{"symbol": constructor["symbol"], "sites": [{}]}],
        )
        constructor["callers"] = [
            {"symbol": factory["symbol"], "sites": [{}]}
        ]
        graph = ownership_audit.build_graph_data(
            {
                "callables": [factory, constructor],
                "reach_throughs": {},
            }
        )
        self.assertEqual(graph["summary"]["stateful_components"], 0)
        self.assertFalse(
            any(node["kind"] != "capability" for node in graph["nodes"])
        )

    def test_free_retained_capability_constructor_is_not_a_workflow(self):
        boundary = self.graph_record(
            "sample::database::open_image",
            return_type="Result<Connection, DbError>",
            retained_dependencies=["database"],
            effects=["database-write"],
        )
        graph = ownership_audit.build_graph_data(
            {"callables": [boundary], "reach_throughs": {}}
        )
        self.assertEqual(graph["summary"]["stateful_components"], 0)
        self.assertFalse(
            any(node["kind"] != "capability" for node in graph["nodes"])
        )

    def test_loader_from_retained_state_is_not_a_constructor(self):
        loader = self.graph_record(
            "sample::database::load_store_root",
            parameters=[{"name": "conn", "type": "&Connection"}],
            return_type="Result<StoreRootRef, DbError>",
            retained_dependencies=["database", "authority"],
            effects=["database-read"],
        )
        graph = ownership_audit.build_graph_data(
            {"callables": [loader], "reach_throughs": {}}
        )
        self.assertEqual(graph["summary"]["unbound"], 1)
        self.assertEqual(graph["construction_boundaries"], [])

    def test_factory_boundary_propagates_through_factory_wrappers(self):
        constructor = self.graph_record(
            "sample::store::<Store>::new",
            module="sample::store",
            kind="associated",
            receiver_type="Store",
            return_type="Self",
            receiver_constructor=True,
        )
        factory = self.graph_record(
            "sample::store::open_store",
            return_type="Result<Store, OpenError>",
            callees=[{"symbol": constructor["symbol"], "sites": [{}]}],
        )
        wrapper = self.graph_record(
            "sample::store::open_test_store",
            return_type="Result<Store, OpenError>",
            retained_dependencies=["storage"],
            callees=[{"symbol": factory["symbol"], "sites": [{}]}],
        )
        graph = ownership_audit.build_graph_data(
            {
                "callables": [wrapper, factory, constructor],
                "reach_throughs": {},
            }
        )
        self.assertEqual(graph["summary"]["stateful_components"], 0)
        self.assertFalse(
            any(node["kind"] != "capability" for node in graph["nodes"])
        )

    def test_workflow_returning_another_type_is_not_a_factory_boundary(self):
        constructor = self.graph_record(
            "sample::store::<Store>::new",
            module="sample::store",
            kind="associated",
            receiver_type="Store",
            return_type="Self",
            receiver_constructor=True,
        )
        workflow = self.graph_record(
            "sample::store::create_account",
            return_type="Result<Account, OpenError>",
            retained_dependencies=["database"],
            callees=[{"symbol": constructor["symbol"], "sites": [{}]}],
        )
        graph = ownership_audit.build_graph_data(
            {
                "callables": [workflow, constructor],
                "reach_throughs": {},
            }
        )
        self.assertEqual(graph["summary"]["unbound"], 1)
        self.assertEqual(
            [
                node["label"]
                for node in graph["nodes"]
                if node["kind"] == "unbound"
            ],
            ["store::create_account"],
        )

    def test_workflow_that_calls_a_constructor_is_not_a_factory_boundary(self):
        constructor = self.graph_record(
            "sample::store::<Store>::new",
            module="sample::store",
            kind="associated",
            receiver_type="Store",
            return_type="Self",
            receiver_constructor=True,
        )
        load = self.graph_record(
            "sample::store::load_database",
            effects=["database-read"],
        )
        workflow = self.graph_record(
            "sample::store::load_store",
            return_type="Result<Store, OpenError>",
            retained_dependencies=["database"],
            callees=[
                {"symbol": load["symbol"], "sites": [{}]},
                {"symbol": constructor["symbol"], "sites": [{}]},
            ],
        )
        graph = ownership_audit.build_graph_data(
            {
                "callables": [workflow, load, constructor],
                "reach_throughs": {},
            }
        )
        self.assertEqual(
            {
                callable_record["symbol"]
                for node in graph["nodes"]
                if node["kind"] == "unbound"
                for callable_record in node["callables"]
            },
            {workflow["symbol"], load["symbol"]},
        )

    def test_constructor_lexical_body_is_part_of_the_boundary(self):
        constructor = self.graph_record(
            "sample::store::<Store>::new",
            module="sample::store",
            kind="associated",
            receiver_type="Store",
            return_type="Self",
            receiver_constructor=True,
        )
        closure = self.graph_record(
            "sample::store::<Store>::new::<closure@1:1>",
            kind="closure",
            enclosing_callable=constructor["symbol"],
            effects=["database-read"],
        )
        graph = ownership_audit.build_graph_data(
            {
                "callables": [constructor, closure],
                "reach_throughs": {},
            }
        )
        self.assertEqual(graph["summary"]["stateful_components"], 0)
        self.assertFalse(
            any(node["kind"] != "capability" for node in graph["nodes"])
        )

    def test_constructor_boundary_stops_stateful_propagation(self):
        helper = self.graph_record(
            "sample::store::load",
            effects=["database-read"],
        )
        constructor = self.graph_record(
            "sample::store::<Store>::open",
            module="sample::store",
            kind="associated",
            receiver_type="Store",
            receiver_constructor=True,
            callees=[{"symbol": helper["symbol"], "sites": [{}]}],
        )
        entry = self.graph_record(
            "sample::open_store",
            callees=[{"symbol": constructor["symbol"], "sites": [{}]}],
        )
        helper["callers"] = [
            {"symbol": constructor["symbol"], "sites": [{}]}
        ]
        constructor["callers"] = [
            {"symbol": entry["symbol"], "sites": [{}]}
        ]
        graph = ownership_audit.build_graph_data(
            {
                "callables": [entry, constructor, helper],
                "reach_throughs": {},
            }
        )
        self.assertEqual(
            [
                callable_record["symbol"]
                for node in graph["nodes"]
                for callable_record in node["callables"]
            ],
            [helper["symbol"]],
        )

    def test_constructor_is_removed_from_recursive_workflow_group(self):
        constructor = self.graph_record(
            "sample::store::<Store>::parse",
            module="sample::store",
            kind="associated",
            receiver_type="Store",
            receiver_constructor=True,
        )
        verifier = self.graph_record(
            "sample::store::verify",
            effects=["database-read"],
        )
        constructor["callees"] = [
            {"symbol": verifier["symbol"], "sites": [{}]}
        ]
        constructor["callers"] = [
            {"symbol": verifier["symbol"], "sites": [{}]}
        ]
        verifier["callees"] = [
            {"symbol": constructor["symbol"], "sites": [{}]}
        ]
        verifier["callers"] = [
            {"symbol": constructor["symbol"], "sites": [{}]}
        ]
        graph = ownership_audit.build_graph_data(
            {
                "callables": [constructor, verifier],
                "reach_throughs": {},
            }
        )
        self.assertEqual(
            [
                callable_record["symbol"]
                for node in graph["nodes"]
                for callable_record in node["callables"]
            ],
            [verifier["symbol"]],
        )

    def test_nested_stateful_callable_inherits_receiver_owner(self):
        method = self.graph_record(
            "sample::store::<Store>::sync",
            module="sample::store",
            kind="method",
            receiver_type="Store",
            parameters=[{"name": "self", "type": "&self"}],
        )
        closure = self.graph_record(
            "sample::store::<Store>::sync::$closure@1",
            module="sample::store",
            kind="closure",
            enclosing_callable=method["symbol"],
            effects=["storage-write"],
        )
        method["callees"] = [{"symbol": closure["symbol"], "sites": [{}]}]
        closure["callers"] = [{"symbol": method["symbol"], "sites": [{}]}]
        graph = ownership_audit.build_graph_data(
            {"callables": [method, closure], "reach_throughs": {}}
        )
        owners = [
            node
            for node in graph["nodes"]
            if node["kind"] == "owner"
        ]
        self.assertEqual([owner["label"] for owner in owners], ["Store"])
        self.assertEqual(graph["summary"]["unbound"], 0)

    def test_nested_unowned_stateful_callable_is_part_of_its_parent(self):
        parent = self.graph_record("sample::run")
        closure = self.graph_record(
            "sample::run::<closure@1:1>",
            kind="closure",
            enclosing_callable=parent["symbol"],
            effects=["database-read"],
        )
        graph = ownership_audit.build_graph_data(
            {"callables": [parent, closure], "reach_throughs": {}}
        )
        workflow = next(
            node
            for node in graph["nodes"]
            if node["kind"] == "unbound"
        )
        self.assertEqual(
            {record["symbol"] for record in workflow["callables"]},
            {parent["symbol"], closure["symbol"]},
        )
        self.assertTrue(workflow["ready"])
        self.assertEqual(workflow["effects"], ["database-read"])
        self.assertEqual(graph["summary"]["ready"], 1)

    def test_verified_parent_disposition_covers_its_lexical_closure(self):
        parent = self.graph_record("sample::run")
        closure = self.graph_record(
            "sample::run::<closure@1:1>",
            kind="closure",
            enclosing_callable=parent["symbol"],
            effects=["database-read"],
        )
        decisions = {
            (parent["symbol"], parent["signature"]): {
                "classification": "boundary",
                "status": "verified",
            }
        }

        graph = ownership_audit.build_graph_data(
            {"callables": [parent, closure], "reach_throughs": {}},
            decisions,
        )

        workflow = next(
            node
            for node in graph["nodes"]
            if node["kind"] != "capability"
        )
        self.assertEqual(workflow["kind"], "resolved")
        self.assertEqual(workflow["classifications"], ["boundary"])

    def test_ready_group_keeps_adjacent_receiver_as_candidate_not_owner(self):
        owner = self.graph_record(
            "sample::store::<Store>::sync",
            module="sample::store",
            kind="method",
            receiver_type="Store",
            parameters=[{"name": "self", "type": "&self"}],
            callees=[
                {"symbol": "sample::store::publish", "sites": [{}, {}]}
            ],
        )
        helper = self.graph_record(
            "sample::store::publish",
            retained_dependencies=["storage"],
            effects=["storage-write"],
            callers=[
                {"symbol": owner["symbol"], "sites": [{}, {}]}
            ],
        )
        graph = ownership_audit.build_graph_data(
            {"callables": [owner, helper], "reach_throughs": {}}
        )
        workflow = next(
            node
            for node in graph["nodes"]
            if node["kind"] == "unbound"
        )
        self.assertEqual(workflow["candidate_owner"]["label"], "Store")
        self.assertEqual(
            workflow["candidate_owner"]["id"],
            "owner|production|sample|Store",
        )

    def test_verified_boundary_does_not_block_its_caller(self):
        boundary = self.graph_record(
            "sample::sync::run",
            effects=["network"],
        )
        caller = self.graph_record(
            "sample::main",
            callees=[{"symbol": boundary["symbol"], "sites": [{}]}],
        )
        boundary["callers"] = [
            {"symbol": caller["symbol"], "sites": [{}]}
        ]
        decisions = {
            (boundary["symbol"], boundary["signature"]): {
                "classification": "boundary",
                "status": "verified",
            }
        }
        graph = ownership_audit.build_graph_data(
            {"callables": [caller, boundary], "reach_throughs": {}},
            decisions,
        )
        nodes = {node["label"]: node for node in graph["nodes"]}
        self.assertEqual(nodes["sync::run"]["kind"], "resolved")
        self.assertTrue(nodes["sample::main"]["ready"])

    def test_test_entry_is_a_boundary_without_hiding_its_helper(self):
        helper = self.graph_record(
            "sample::tests::persist_fixture",
            effects=["database-write"],
            test_context=True,
        )
        test_entry = self.graph_record(
            "sample::tests::writes",
            test_context=True,
            test_entry=True,
            callees=[{"symbol": helper["symbol"], "sites": [{}]}],
        )
        helper["callers"] = [
            {"symbol": test_entry["symbol"], "sites": [{}]}
        ]

        graph = ownership_audit.build_graph_data(
            {"callables": [test_entry, helper], "reach_throughs": {}},
        )
        nodes = {
            node["component"]: node
            for node in graph["nodes"]
            if node["kind"] != "capability"
        }

        self.assertEqual(nodes[test_entry["symbol"]]["kind"], "resolved")
        self.assertEqual(
            nodes[test_entry["symbol"]]["classifications"],
            ["boundary"],
        )
        self.assertEqual(nodes[helper["symbol"]]["kind"], "unbound")
        self.assertTrue(nodes[helper["symbol"]]["ready"])

    def test_verified_transformation_does_not_block_its_caller(self):
        transformation = self.graph_record(
            "sample::sync::state_hash",
            effects=["cryptography"],
        )
        caller = self.graph_record(
            "sample::sync::verify",
            callees=[{"symbol": transformation["symbol"], "sites": [{}]}],
        )
        transformation["callers"] = [
            {"symbol": caller["symbol"], "sites": [{}]}
        ]
        decisions = {
            (transformation["symbol"], transformation["signature"]): {
                "classification": "transformation",
                "status": "verified",
            }
        }
        graph = ownership_audit.build_graph_data(
            {"callables": [caller, transformation], "reach_throughs": {}},
            decisions,
        )
        nodes = {node["label"]: node for node in graph["nodes"]}
        self.assertEqual(nodes["sync::state_hash"]["kind"], "resolved")
        self.assertTrue(nodes["sync::verify"]["ready"])

    def test_owner_method_decision_does_not_resolve_receiverless_function(self):
        helper = self.graph_record(
            "sample::store::publish",
            retained_dependencies=["storage"],
            effects=["storage-write"],
        )
        decisions = {
            (helper["symbol"], helper["signature"]): {
                "classification": "owner-method",
                "status": "verified",
            }
        }
        graph = ownership_audit.build_graph_data(
            {"callables": [helper], "reach_throughs": {}},
            decisions,
        )
        workflow = next(
            node
            for node in graph["nodes"]
            if node["kind"] == "unbound"
        )
        self.assertTrue(workflow["ready"])

    def test_verified_receiver_cycle_is_resolved(self):
        history = self.graph_record(
            "sample::history::<History>::verify",
            module="sample::history",
            kind="method",
            receiver_type="History",
            parameters=[{"name": "self", "type": "&mut self"}],
            effects=["database-read"],
        )
        activation = self.graph_record(
            "sample::history::<Activation>::load",
            module="sample::history",
            kind="method",
            receiver_type="Activation",
            parameters=[{"name": "self", "type": "&mut self"}],
            effects=["cryptography"],
        )
        history["callees"] = [
            {"symbol": activation["symbol"], "sites": [{}]}
        ]
        history["callers"] = [
            {"symbol": activation["symbol"], "sites": [{}]}
        ]
        activation["callees"] = [
            {"symbol": history["symbol"], "sites": [{}]}
        ]
        activation["callers"] = [
            {"symbol": history["symbol"], "sites": [{}]}
        ]
        decisions = {
            (history["symbol"], history["signature"]): {
                "classification": "owner-method",
                "owner": "History",
                "status": "verified",
            },
            (activation["symbol"], activation["signature"]): {
                "classification": "operation-method",
                "owner": "Activation",
                "status": "verified",
            },
        }

        graph = ownership_audit.build_graph_data(
            {"callables": [history, activation], "reach_throughs": {}},
            decisions,
        )

        self.assertEqual(graph["summary"]["unbound"], 0)
        resolved = next(
            node for node in graph["nodes"] if node["kind"] == "resolved"
        )
        self.assertEqual(
            resolved["classifications"],
            ["operation-method", "owner-method"],
        )

    def test_callback_body_does_not_block_the_executor(self):
        caller = self.graph_record(
            "sample::run",
            callees=[
                {"symbol": "sample::execute", "sites": [{}]},
                {"symbol": "sample::run::<closure@1:1>", "sites": [{}]},
            ],
        )
        callback = self.graph_record(
            "sample::run::<closure@1:1>",
            kind="closure",
            enclosing_callable=caller["symbol"],
            effects=["database-write"],
        )
        executor = self.graph_record(
            "sample::execute",
            effects=["database-write"],
            callers=[{"symbol": caller["symbol"], "sites": [{}]}],
            callees=[
                {
                    "symbol": callback["symbol"],
                    "sites": [{"role": "callable-argument"}],
                }
            ],
        )

        graph = ownership_audit.build_graph_data(
            {"callables": [caller, callback, executor], "reach_throughs": {}}
        )
        nodes = {node["label"]: node for node in graph["nodes"]}

        self.assertTrue(nodes["sample::execute"]["ready"])
        self.assertEqual(
            nodes["sample::run"]["blockers"],
            [nodes["sample::execute"]["id"]],
        )

    def test_graph_html_contains_svg_and_embedded_index(self):
        html = ownership_audit.render_graph_html(
            {
                "nodes": [],
                "construction_boundaries": [],
                "call_edges": [],
                "construction_edges": [],
                "supporting_edges": [],
                "summary": {
                    "callables": 0,
                    "ready": 0,
                    "unbound": 0,
                    "owners": 0,
                    "construction_boundaries": 0,
                },
            }
        )
        self.assertIn('<div id="ownership-list"', html)
        self.assertIn('<svg id="graph"', html)
        self.assertIn("Ready ownership queue", html)
        self.assertIn("a blocked caller remains in place", html)
        self.assertIn("This does not make its callers ready", html)
        self.assertIn("Construction boundaries", html)
        self.assertIn('"callables": 0', html)

    def test_reach_through_reports_cover_static_calls_and_field_bundles(self):
        caller = {
            "symbol": "sample::owner::child::<Worker>::run",
            "kind": "method",
            "module": "sample::owner::child",
            "parameters": [
                {"name": "self", "type": "&self"},
                {"name": "database", "type": "&Database"},
            ],
            "paths": [
                {
                    "text": "super::super::history::load",
                    "range": {
                        "start": {"line": 3, "character": 8},
                        "end": {"line": 3, "character": 35},
                    },
                }
            ],
            "calls": [
                {
                    "text": "Store::open(self.database, self.storage)",
                    "arguments": [
                        {"text": "self.database"},
                        {"text": "self.storage"},
                    ],
                }
            ],
            "callees": [
                {
                    "symbol": "sample::store::<Store>::open",
                    "sites": [
                        {
                            "range": {
                                "start": {"line": 4, "character": 15},
                                "end": {"line": 4, "character": 19},
                            },
                            "expression": "Store::open(self.database, self.storage)",
                        }
                    ],
                }
            ],
        }
        target = {
            "symbol": "sample::store::<Store>::open",
            "name": "open",
            "kind": "associated",
            "retained_dependencies": ["database", "storage"],
            "ambient_dependencies": [],
            "effects": ["database-read"],
        }
        reports = ownership_audit.build_reach_through_reports(
            [caller, target],
            {},
        )
        self.assertEqual(len(reports["deep_ancestor_paths"]), 1)
        self.assertEqual(len(reports["associated_function_calls"]), 1)
        self.assertEqual(len(reports["constructor_calls"]), 1)
        self.assertEqual(len(reports["receiver_dependency_parameters"]), 1)
        self.assertEqual(len(reports["field_bundle_calls"]), 1)
        self.assertEqual(
            reports["receiverless_stateful_callables"][0]["symbol"],
            target["symbol"],
        )

    def test_receiver_dependency_parameter_calls_exclude_constructor_injection(self):
        operation = self.graph_record(
            "sample::rows::<StoreRows>::write",
            kind="method",
            receiver_type="StoreRows",
            parameters=[
                {"name": "self", "type": "&self"},
                {"name": "database", "type": "StoreDatabase"},
            ],
            callers=[
                {
                    "symbol": "sample::sync::<StoreSync>::run",
                    "sites": [
                        {
                            "range": {
                                "start": {"line": 8, "character": 4},
                                "end": {"line": 8, "character": 30},
                            },
                            "arguments": [{"text": "self.database"}],
                            "expression": "self.rows.write(self.database)",
                        }
                    ],
                },
                {
                    "symbol": "sample::rows::<StoreRows>::new",
                    "sites": [
                        {
                            "range": {
                                "start": {"line": 3, "character": 4},
                                "end": {"line": 3, "character": 25},
                            },
                            "arguments": [{"text": "database"}],
                            "expression": "rows.write(database)",
                        }
                    ],
                },
            ],
        )
        runtime_caller = self.graph_record(
            "sample::sync::<StoreSync>::run",
            kind="method",
            receiver_type="StoreSync",
            parameters=[{"name": "self", "type": "&self"}],
        )
        constructor = self.graph_record(
            "sample::rows::<StoreRows>::new",
            kind="associated",
            receiver_type="StoreRows",
            parameters=[{"name": "database", "type": "StoreDatabase"}],
            return_type="Self",
            receiver_constructor=True,
        )

        reports = ownership_audit.build_reach_through_reports(
            [operation, runtime_caller, constructor],
            {},
        )

        self.assertEqual(
            reports["receiver_dependency_parameter_calls"],
            [
                {
                    "argument": "self.database",
                    "caller": runtime_caller["symbol"],
                    "caller_owner": "StoreSync",
                    "categories": ["database"],
                    "parameter": "database",
                    "parameter_type": "StoreDatabase",
                    "range": {
                        "start": {"line": 8, "character": 4},
                        "end": {"line": 8, "character": 30},
                    },
                    "symbol": operation["symbol"],
                }
            ],
        )

    def test_callable_argument_reference_ignores_borrow_wrappers(self):
        self.assertEqual(
            ownership_audit.callable_argument_reference("&mut build_request"),
            "build_request",
        )
        self.assertEqual(
            ownership_audit.callable_argument_reference(
                "ffi::sqlite3changeset_new"
            ),
            "ffi::sqlite3changeset_new",
        )

    def test_parameter_candidates_follow_forwarded_callback_arguments(self):
        leaf = {
            "symbol": "sample::leaf",
            "name": "leaf",
            "visibility": "private",
            "parameters": [{"name": "callback", "type": "&F"}],
            "path": "sample.rs",
            "callers": [
                {
                    "symbol": "sample::middle",
                    "sites": [
                        {
                            "arguments": [
                                {
                                    "text": "&callback",
                                    "range": {
                                        "start": {"line": 2, "character": 9},
                                        "end": {"line": 2, "character": 18},
                                    },
                                }
                            ]
                        }
                    ],
                }
            ],
        }
        middle = {
            "symbol": "sample::middle",
            "name": "middle",
            "crate": "sample",
            "visibility": "private",
            "parameters": [{"name": "callback", "type": "F"}],
            "path": "sample.rs",
            "callers": [
                {
                    "symbol": "sample::root",
                    "sites": [
                        {
                            "arguments": [
                                {
                                    "text": "actual_callback",
                                    "range": {
                                        "start": {"line": 4, "character": 11},
                                        "end": {"line": 4, "character": 26},
                                    },
                                }
                            ]
                        }
                    ],
                }
            ],
        }
        root = {
            "symbol": "sample::root",
            "name": "root",
            "crate": "sample",
            "visibility": "private",
            "parameters": [],
            "path": "sample.rs",
        }
        actual = {
            "symbol": "sample::actual_callback",
            "name": "actual_callback",
            "path": "sample.rs",
        }
        records = [leaf, middle, root, actual]
        candidates = ownership_audit.parameter_call_candidates(
            leaf,
            0,
            {record["symbol"]: record for record in records},
            {ownership_audit.ROOT / "sample.rs": []},
            records,
        )
        self.assertEqual(candidates, ["sample::actual_callback"])

    def test_parameter_candidates_preserve_external_function_paths(self):
        leaf = {
            "symbol": "sample::leaf",
            "name": "leaf",
            "visibility": "private",
            "parameters": [{"name": "callback", "type": "Callback"}],
            "path": "sample.rs",
            "callers": [
                {
                    "symbol": "sample::root",
                    "sites": [
                        {
                            "arguments": [
                                {
                                    "text": "ffi::external_callback",
                                    "range": {
                                        "start": {"line": 2, "character": 9},
                                        "end": {"line": 2, "character": 31},
                                    },
                                }
                            ]
                        }
                    ],
                }
            ],
        }
        root = {
            "symbol": "sample::root",
            "name": "root",
            "crate": "sample",
            "visibility": "private",
            "parameters": [],
            "path": "sample.rs",
        }
        candidates = ownership_audit.parameter_call_candidates(
            leaf,
            0,
            {
                leaf["symbol"]: leaf,
                root["symbol"]: root,
            },
            {ownership_audit.ROOT / "sample.rs": []},
            [leaf, root],
        )
        self.assertEqual(
            candidates,
            ["external-callable::ffi::external_callback"],
        )

    def test_parameter_site_argument_accounts_for_explicit_self(self):
        record = {
            "parameters": [
                {"name": "self", "type": "&self"},
                {"name": "body", "type": "Body"},
                {"name": "callback", "type": "Callback"},
            ]
        }
        explicit = {
            "arguments": [
                {"text": "self"},
                {"text": "body"},
                {"text": "callback"},
            ]
        }
        method = {
            "arguments": [
                {"text": "body"},
                {"text": "callback"},
            ]
        }
        self.assertEqual(
            ownership_audit.parameter_site_argument(record, explicit, 1),
            {"text": "callback"},
        )
        self.assertEqual(
            ownership_audit.parameter_site_argument(record, method, 1),
            {"text": "callback"},
        )

    def test_trait_callback_without_static_callers_names_trait_dispatch(self):
        record = {
            "symbol": "sample::<trait Storage>::write",
            "name": "write",
            "kind": "trait-method",
            "visibility": "private",
            "receiver_type": "trait Storage",
            "parameters": [
                {"name": "self", "type": "&self"},
                {"name": "callback", "type": "Callback"},
            ],
            "callers": [],
            "path": "sample.rs",
        }
        self.assertEqual(
            ownership_audit.parameter_call_candidates(
                record,
                0,
                {record["symbol"]: record},
                {ownership_audit.ROOT / "sample.rs": []},
                [record],
            ),
            ["trait-dispatch-supplied"],
        )

    def test_callback_argument_resolves_a_lexically_bound_closure(self):
        caller = {
            "symbol": "sample::root",
            "name": "root",
            "crate": "sample",
            "kind": "free",
            "semantic_parent": None,
            "parameters": [],
            "path": "sample.rs",
        }
        closure = {
            "symbol": "sample::root::<closure>",
            "name": "closure",
            "kind": "closure",
            "binding": "sink",
            "semantic_parent": "sample::root",
            "path": "sample.rs",
            "range": {
                "start": {"line": 1, "character": 15},
                "end": {"line": 1, "character": 24},
            },
        }
        candidates = ownership_audit.argument_callable_candidates(
            caller,
            {
                "text": "&sink",
                "range": {
                    "start": {"line": 2, "character": 8},
                    "end": {"line": 2, "character": 13},
                },
            },
            {
                caller["symbol"]: caller,
                closure["symbol"]: closure,
            },
            {ownership_audit.ROOT / "sample.rs": [caller, closure]},
            [caller, closure],
            set(),
        )
        self.assertEqual(candidates, [closure["symbol"]])

    def test_callable_argument_is_an_ownership_dependency(self):
        argument_range = {
            "start": {"line": 2, "character": 31},
            "end": {"line": 2, "character": 44},
        }
        caller = {
            "symbol": "sample::make_clock",
            "name": "make_clock",
            "crate": "sample",
            "kind": "free",
            "semantic_parent": None,
            "parameters": [],
            "path": "sample.rs",
            "callees": [
                {
                    "symbol": "sample::<Clock>::with_source",
                    "sites": [
                        {
                            "range": {
                                "start": {"line": 2, "character": 4},
                                "end": {"line": 2, "character": 45},
                            },
                            "views": ["library-default"],
                            "expression": "Clock::with_source(wall_clock_ms)",
                            "arguments": [
                                {
                                    "text": "wall_clock_ms",
                                    "range": argument_range,
                                }
                            ],
                            "callee_text": "Clock::with_source",
                        }
                    ],
                }
            ],
            "callers": [],
        }
        constructor = {
            "symbol": "sample::<Clock>::with_source",
            "name": "with_source",
            "crate": "sample",
            "kind": "associated",
            "semantic_parent": None,
            "parameters": [
                {
                    "name": "source",
                    "type": "impl Fn() -> u64 + Send + Sync + 'static",
                }
            ],
            "signature": "fn with_source(source: impl Fn() -> u64 + Send + Sync + 'static) -> Self",
            "path": "sample.rs",
            "callees": [],
            "callers": [
                {
                    "symbol": caller["symbol"],
                    "sites": caller["callees"][0]["sites"],
                }
            ],
        }
        source = {
            "symbol": "sample::wall_clock_ms",
            "name": "wall_clock_ms",
            "crate": "sample",
            "kind": "free",
            "semantic_parent": None,
            "parameters": [],
            "signature": "fn wall_clock_ms() -> u64",
            "path": "sample.rs",
            "callees": [],
            "callers": [],
        }
        records = [caller, constructor, source]

        ownership_audit.attach_callable_argument_edges(records)

        self.assertEqual(
            [edge["symbol"] for edge in caller["callees"]],
            [constructor["symbol"], source["symbol"]],
        )
        self.assertEqual(source["callers"][0]["symbol"], caller["symbol"])
        self.assertEqual(source["callers"][0]["sites"][0]["range"], argument_range)

    def test_callback_factory_resolves_its_returned_closure(self):
        caller = {
            "symbol": "sample::root",
            "name": "root",
            "crate": "sample",
            "kind": "free",
            "semantic_parent": None,
            "parameters": [],
            "path": "sample.rs",
        }
        factory = {
            "symbol": "sample::no_progress",
            "name": "no_progress",
            "kind": "free",
            "return_type": "impl Fn(u64)",
            "path": "sample.rs",
        }
        closure = {
            "symbol": "sample::no_progress::<closure>",
            "name": "closure",
            "kind": "closure",
            "enclosing_callable": factory["symbol"],
            "path": "sample.rs",
            "range": {
                "start": {"line": 5, "character": 4},
                "end": {"line": 5, "character": 10},
            },
        }
        records = [caller, factory, closure]
        candidates = ownership_audit.argument_callable_candidates(
            caller,
            {
                "text": "&no_progress()",
                "range": {
                    "start": {"line": 8, "character": 8},
                    "end": {"line": 8, "character": 22},
                },
            },
            {record["symbol"]: record for record in records},
            {ownership_audit.ROOT / "sample.rs": records},
            records,
            set(),
        )
        self.assertEqual(candidates, [closure["symbol"]])

    def test_lexical_closure_call_resolves_to_its_binding(self):
        callable_record = {
            "symbol": "sample::run",
            "name": "run",
            "kind": "free",
            "binding": None,
            "semantic_parent": None,
            "path": "sample.rs",
        }
        closure = {
            "symbol": "sample::run::<closure>",
            "name": "closure",
            "kind": "closure",
            "binding": "load",
            "semantic_parent": "sample::run",
            "path": "sample.rs",
            "range": {
                "start": {"line": 2, "character": 15},
                "end": {"line": 2, "character": 32},
            },
        }
        caller = {
            "symbol": "sample::run::<closure-2>",
            "name": "closure",
            "kind": "closure",
            "binding": None,
            "semantic_parent": "sample::run",
            "path": "sample.rs",
            "range": {
                "start": {"line": 4, "character": 0},
                "end": {"line": 5, "character": 1},
            },
        }
        candidates = ownership_audit.lexical_call_candidates(
            caller,
            {
                "text": "load()",
                "callee_text": "load",
                "range": {
                    "start": {"line": 4, "character": 4},
                    "end": {"line": 4, "character": 10},
                },
                "callee_range": {
                    "start": {"line": 4, "character": 4},
                    "end": {"line": 4, "character": 8},
                },
            },
            [callable_record, closure, caller],
        )
        self.assertEqual(
            [candidate["symbol"] for candidate in candidates],
            ["sample::run::<closure>"],
        )

    def test_closure_argument_is_not_the_called_callable(self):
        closure = {
            "symbol": "sample::run::<closure>",
            "name": "closure",
            "kind": "closure",
            "binding": None,
            "semantic_parent": "sample::run",
            "path": "sample.rs",
            "range": {
                "start": {"line": 2, "character": 12},
                "end": {"line": 2, "character": 20},
            },
        }
        caller = {
            "symbol": "sample::run",
            "name": "run",
            "kind": "free",
            "binding": None,
            "semantic_parent": None,
            "path": "sample.rs",
            "range": {
                "start": {"line": 1, "character": 0},
                "end": {"line": 3, "character": 1},
            },
        }
        candidates = ownership_audit.lexical_call_candidates(
            caller,
            {
                "text": "apply(|| value)",
                "callee_text": "apply",
                "range": {
                    "start": {"line": 2, "character": 4},
                    "end": {"line": 2, "character": 21},
                },
                "callee_range": {
                    "start": {"line": 2, "character": 4},
                    "end": {"line": 2, "character": 9},
                },
            },
            [caller, closure],
        )
        self.assertEqual(candidates, [])

    def test_qualified_candidate_search_uses_the_receiver_type(self):
        record = {
            "crate": "coven",
        }
        records = [
            {"name": "new", "symbol": "coven::keys::<StoreKeys>::new"},
            {"name": "new", "symbol": "coven::other::<Other>::new"},
        ]
        self.assertEqual(
            ownership_audit.named_call_candidates(
                record,
                "StoreKeys::new",
                records,
            ),
            ["coven::keys::<StoreKeys>::new"],
        )
        self.assertEqual(
            ownership_audit.named_call_candidates(
                record,
                "keyring_core::Entry::new",
                records,
            ),
            [],
        )
        self.assertTrue(
            ownership_audit.is_data_constructor(
                "CloudHomeError::Configuration"
            )
        )
        self.assertFalse(ownership_audit.is_data_constructor("StoreKeys::new"))

    def test_recursive_call_groups_are_one_bottom_up_component(self):
        records = [
            {
                "symbol": "entry",
                "callees": [{"symbol": "left", "sites": []}],
            },
            {
                "symbol": "left",
                "callees": [{"symbol": "right", "sites": []}],
            },
            {
                "symbol": "right",
                "callees": [{"symbol": "left", "sites": []}],
            },
            {
                "symbol": "leaf",
                "callees": [],
            },
        ]
        components = ownership_audit.call_components(records)
        recursive = next(component for component in components if component["recursive"])
        self.assertEqual(recursive["members"], ["left", "right"])
        self.assertEqual(records[0]["bottom_up_rank"], 1)
        self.assertEqual(records[3]["bottom_up_rank"], 0)

    def test_collapsed_callbacks_do_not_make_the_executor_and_caller_recursive(self):
        records = [
            {
                "symbol": "caller",
                "kind": "free",
                "callees": [
                    {"symbol": "executor", "sites": []},
                    {"symbol": "caller::<closure>", "sites": []},
                ],
            },
            {
                "symbol": "caller::<closure>",
                "kind": "closure",
                "enclosing_callable": "caller",
                "callees": [],
            },
            {
                "symbol": "executor",
                "kind": "free",
                "callees": [
                    {
                        "symbol": "caller::<closure>",
                        "sites": [{"role": "callable-argument"}],
                    }
                ],
            },
        ]

        components = ownership_audit.call_components(records, collapse_nested=True)

        self.assertFalse(any(component["recursive"] for component in components))
        self.assertEqual(records[0]["bottom_up_rank"], 1)
        self.assertEqual(records[2]["bottom_up_rank"], 0)

    def test_bottom_up_order_handles_deep_call_graphs_iteratively(self):
        records = [
            {
                "symbol": f"callable_{index:04}",
                "callees": (
                    [{"symbol": f"callable_{index + 1:04}", "sites": []}]
                    if index < 1999
                    else []
                ),
            }
            for index in range(2000)
        ]
        ownership_audit.call_components(records)
        self.assertEqual(records[0]["bottom_up_rank"], 1999)
        self.assertEqual(records[-1]["bottom_up_rank"], 0)

    def test_source_discovery_follows_modules_and_reports_orphans(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            crate = root / "crates" / "sample"
            source = crate / "src"
            tests = crate / "tests"
            (source / "nested").mkdir(parents=True)
            tests.mkdir()
            (source / "lib.rs").write_text(
                """
mod direct;
mod nested { mod child; }
#[path = "alternate.rs"]
mod renamed;
#[cfg(test)]
mod gated;
"""
            )
            (source / "direct.rs").write_text("fn direct() {}")
            (source / "nested" / "child.rs").write_text("fn child() {}")
            (source / "alternate.rs").write_text("fn alternate() {}")
            (source / "gated.rs").write_text("fn gated() {}")
            (source / "orphan.rs").write_text("fn orphan() {}")
            (tests / "integration.rs").write_text("fn integration() {}")
            metadata = {
                "packages": [
                    {
                        "name": "sample",
                        "manifest_path": str(crate / "Cargo.toml"),
                        "targets": [
                            {
                                "name": "sample",
                                "kind": ["lib"],
                                "src_path": str(source / "lib.rs"),
                            },
                            {
                                "name": "integration",
                                "kind": ["test"],
                                "src_path": str(tests / "integration.rs"),
                            },
                        ],
                    }
                ]
            }
            original_root = ownership_audit.ROOT
            ownership_audit.ROOT = root.resolve()
            try:
                discovery = ownership_audit.discover_workspace_sources(metadata)
            finally:
                ownership_audit.ROOT = original_root
            self.assertEqual(
                {path.relative_to(root.resolve()) for path in discovery.unreachable},
                {Path("crates/sample/src/orphan.rs")},
            )
            self.assertEqual(len(discovery.reachable), 6)
            self.assertEqual(
                discovery.conditions[(source / "gated.rs").resolve()],
                (("#[cfg(test)]",),),
            )
            self.assertIn(
                "crates/sample/src/gated.rs",
                discovery.targets[0]["sources"],
            )

    def test_rust_analyzer_does_not_inherit_compiler_wrappers(self):
        original_wrapper = ownership_audit.os.environ.get("RUSTC_WRAPPER")
        original_workspace_wrapper = ownership_audit.os.environ.get(
            "RUSTC_WORKSPACE_WRAPPER"
        )
        ownership_audit.os.environ["RUSTC_WRAPPER"] = "sccache"
        ownership_audit.os.environ["RUSTC_WORKSPACE_WRAPPER"] = "wrapper"
        try:
            environment = ownership_audit.rust_analyzer_environment()
        finally:
            if original_wrapper is None:
                ownership_audit.os.environ.pop("RUSTC_WRAPPER", None)
            else:
                ownership_audit.os.environ["RUSTC_WRAPPER"] = original_wrapper
            if original_workspace_wrapper is None:
                ownership_audit.os.environ.pop("RUSTC_WORKSPACE_WRAPPER", None)
            else:
                ownership_audit.os.environ["RUSTC_WORKSPACE_WRAPPER"] = (
                    original_workspace_wrapper
                )
        self.assertNotIn("RUSTC_WRAPPER", environment)
        self.assertNotIn("RUSTC_WORKSPACE_WRAPPER", environment)

    def test_ownership_analyzer_is_pinned_independently(self):
        self.assertEqual(
            ownership_audit.rust_analyzer_command("parse"),
            [
                "rustup",
                "run",
                "1.97.1",
                "rust-analyzer",
                "parse",
            ],
        )

    def test_rust_analyzer_internal_errors_are_rejected(self):
        log = """
2026-07-29T10:00:00Z WARN configuration file absent
2026-07-29T10:00:01Z ERROR pattern has unexpected type
"""
        self.assertEqual(
            ownership_audit.analyzer_error_lines(log),
            ["2026-07-29T10:00:01Z ERROR pattern has unexpected type"],
        )

    def test_rust_analyzer_views_distinguish_features_and_tests(self):
        self.assertEqual(
            [
                (view.name, view.configuration())
                for view in ownership_audit.SEMANTIC_VIEWS
            ],
            [
                (
                    "library-default",
                    {
                        "cargo": {
                            "allTargets": False,
                            "features": [],
                        },
                        "cfg": {"setTest": False},
                        "procMacro": {"enable": True},
                    },
                ),
                (
                    "library-all-features",
                    {
                        "cargo": {
                            "allTargets": False,
                            "features": "all",
                        },
                        "cfg": {"setTest": False},
                        "procMacro": {"enable": True},
                    },
                ),
                (
                    "tests-all-features",
                    {
                        "cargo": {
                            "allTargets": True,
                            "features": "all",
                        },
                        "cfg": {"setTest": True},
                        "procMacro": {"enable": True},
                    },
                ),
            ],
        )

    def test_semantic_edge_merges_configuration_views_at_one_site(self):
        record = {"callees": []}
        site = {
            "range": {
                "start": {"line": 2, "character": 4},
                "end": {"line": 2, "character": 10},
            },
            "expression": "target()",
            "views": ["library-default"],
        }
        ownership_audit.append_semantic_edge(record, "sample::target", site)
        ownership_audit.append_semantic_edge(
            record,
            "sample::target",
            {
                **site,
                "views": ["library-all-features"],
            },
        )
        self.assertEqual(
            record["callees"][0]["sites"][0]["views"],
            ["library-all-features", "library-default"],
        )

    def test_resolved_name_inside_call_range_marks_the_call_resolved(self):
        call_range = {
            "start": {"line": 4, "character": 8},
            "end": {"line": 4, "character": 28},
        }
        callee_name = {"line": 4, "character": 17}
        self.assertTrue(ownership_audit.range_contains(call_range, callee_name))

    def test_semantic_call_site_belongs_to_innermost_callable(self):
        outer = {
            "symbol": "outer",
            "range": {
                "start": {"line": 1, "character": 0},
                "end": {"line": 8, "character": 1},
            },
        }
        closure = {
            "symbol": "closure",
            "range": {
                "start": {"line": 3, "character": 12},
                "end": {"line": 5, "character": 5},
            },
        }
        selected = ownership_audit.record_containing_position(
            [outer, closure],
            {"line": 4, "character": 2},
        )
        self.assertEqual(selected["symbol"], "closure")


if __name__ == "__main__":
    unittest.main()
