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

    def test_rust_analyzer_configuration_is_shared_by_every_lsp_path(self):
        self.assertEqual(
            ownership_audit.RustAnalyzer.configuration(),
            {
                "cargo": {"allTargets": True, "features": "all"},
                "procMacro": {"enable": True},
            },
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
