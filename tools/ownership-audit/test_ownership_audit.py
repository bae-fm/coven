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

    def test_inventory_records_closures_under_their_enclosing_callable(self):
        records = self.inventory(
            "fn run(database: &Database) { let load = || database.load(); load(); }"
        )
        closure = next(record for record in records if record["kind"] == "closure")
        self.assertIn("::run::<closure@", closure["symbol"])
        self.assertEqual(closure["binding"], "load")
        self.assertTrue(closure["calls"])

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


class SemanticIndexTests(unittest.TestCase):
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
"""
            )
            (source / "direct.rs").write_text("fn direct() {}")
            (source / "nested" / "child.rs").write_text("fn child() {}")
            (source / "alternate.rs").write_text("fn alternate() {}")
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
            self.assertEqual(len(discovery.reachable), 5)

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
