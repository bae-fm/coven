#!/usr/bin/env python3
"""Build and query Coven's Rust callable-ownership index."""

from __future__ import annotations

import argparse
import bisect
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from typing import Any, Iterable
from urllib.parse import unquote, urlparse


ROOT = Path(__file__).resolve().parents[2]
INDEX_PATH = ROOT / "target" / "ownership-audit" / "index.json"
GRAPH_PATH = ROOT / "target" / "ownership-audit" / "graph.html"
ANALYZER_LOG_PATH = ROOT / "target" / "ownership-audit" / "rust-analyzer.log"
DECISIONS_PATH = Path(__file__).resolve().parent / "decisions.toml"
CACHE_DIR = INDEX_PATH.parent / "cache"
CACHE_SCHEMA = 2
CACHE_CHECKPOINT_INTERVAL = 100

NODE_PATTERN = re.compile(r"^(?P<indent> *)(?P<kind>[A-Z_]+)@(?P<start>\d+)\.\.(?P<end>\d+)")
STATEFUL_TYPE_PATTERNS = {
    "database": re.compile(
        r"\b(?:Database|StoreDatabase|Connection|Transaction|WriteTransaction|DbHandle)\b"
    ),
    "storage": re.compile(
        r"\b(?:SyncStorage|CloudSyncStorage|ExactSlotStorage|StoreDir|Storage|CloudHome)\b"
    ),
    "identity": re.compile(
        r"\b(?:UserKeypair|Identity|DeviceSigner|MasterKeyCustody|Keyring|Keypair)\b"
    ),
    "authority": re.compile(
        r"\b(?:Authorized\w+|MergeHistoryVerifier|StoreCommitVerifier|MembershipChain|StoreRootRef)\b"
    ),
    "encryption": re.compile(r"\b(?:EncryptionService|CloudCipher|RoutingKey|EpochKey)\b"),
    "runtime": re.compile(
        r"\b(?:Runtime|Handle|Cancellation|Cancel|Sender|Receiver|Mutex|RwLock|Semaphore)\b"
    ),
    "configuration": re.compile(
        r"\b(?:Config|Configuration|TransferLimits|Migration|SyncedTable|RetryPolicy)\b"
    ),
    "time_or_id": re.compile(r"\b(?:Clock|Hlc|IdProvider|Uuid|Instant|SystemTime)\b"),
    "client": re.compile(r"\b(?:Client|HttpClient|OAuth|ProviderBinding)\b"),
}
SERVICE_TYPE_PATTERNS = {
    "database": re.compile(
        r"\b(?:Database|StoreDatabase|Connection|Transaction|WriteTransaction|DbHandle)\b"
    ),
    "storage": re.compile(
        r"\b(?:SyncStorage|CloudSyncStorage|ExactSlotStorage|Storage|CloudHome)\b"
    ),
    "identity": re.compile(
        r"\b(?:UserKeypair|Identity|DeviceSigner|MasterKeyCustody|Keypair)\b"
    ),
    "verification": re.compile(r"\b(?:[A-Za-z0-9_]*Verifier|Authorized[A-Za-z0-9_]*)\b"),
    "encryption": re.compile(r"\b(?:EncryptionService|CloudCipherAccess)\b"),
    "runtime": re.compile(
        r"\b(?:Runtime|Handle|Cancellation|Cancel|Sender|Receiver|Mutex|RwLock|Semaphore)\b"
    ),
    "client": re.compile(r"\b(?:Client|HttpClient|OAuth)\b"),
}
SERVICE_FIELD_PATTERNS = {
    "database": re.compile(r"^(?:database|db|connection|transaction)$"),
    "storage": re.compile(r"^(?:storage|cloud_home)$"),
    "identity": re.compile(
        r"^(?:identity|keypair|signer|device_signer|custody|keyring)$"
    ),
    "verification": re.compile(r"(?:^|_)verifier$"),
    "encryption": re.compile(r"^(?:encryption|cipher)$"),
    "runtime": re.compile(r"^(?:runtime|clock|cancel|cancellation)$"),
    "client": re.compile(r"(?:^|_)client$"),
}
AMBIENT_PATTERNS = {
    "clock": re.compile(r"\b(?:Utc|Local|SystemTime|Instant)::now\s*\("),
    "randomness": re.compile(
        r"\b(?:rand::|thread_rng\s*\(|rng\s*\(|Uuid::new_v[47]\s*\(|UserKeypair::generate\s*\()"
    ),
    "environment": re.compile(r"\b(?:std::env|env::(?:var|vars|args|current_dir))\b"),
    "runtime": re.compile(
        r"\b(?:tokio::spawn|spawn_blocking|Handle::current|Runtime::new)\s*\("
    ),
    "filesystem": re.compile(
        r"\b(?:std::fs|tokio::fs)::(?:read|write|copy|rename|remove|create|open|metadata)"
    ),
    "process": re.compile(r"\b(?:std::process|Command::new)\b"),
}
EFFECT_PATTERNS = {
    "database-read": re.compile(
        r"\b(?:query_row|query_map|prepare|load_|get_|select_|read_)\w*\s*\("
    ),
    "database-write": re.compile(
        r"\b(?:execute|execute_batch|insert_|update_|delete_|persist_|mark_|complete_|install_|stage_)\w*\s*\("
    ),
    "storage-read": re.compile(
        r"\b(?:read_|load_|open_|download_)\w*(?:object|blob|file|slot|snapshot|root)\w*\s*\("
    ),
    "storage-write": re.compile(
        r"\b(?:write_|create_|upload_|publish_|delete_|remove_|rename_|copy_)\w*(?:object|blob|file|slot|snapshot|root)?\w*\s*\("
    ),
    "cryptography": re.compile(
        r"\b(?:sign|verify|encrypt|decrypt|derive|seal|open_containing)\w*\s*\("
    ),
    "task": re.compile(r"\b(?:spawn|spawn_blocking|send|broadcast|cancel)\w*\s*\("),
    "mutation": re.compile(r"\b(?:lock|write|transaction|commit|rollback)\w*\s*\("),
}
CALL_NODE_KINDS = {"CALL_EXPR", "METHOD_CALL_EXPR", "MACRO_CALL"}
CALLABLE_NODE_KINDS = {"FN", "CLOSURE_EXPR"}
CONTENT_MODIFIED = -32801
ANALYZER_TOOLCHAIN = "1.97.1"


class RustAnalyzerRequestError(RuntimeError):
    def __init__(self, method: str, error: dict[str, Any]):
        super().__init__(
            f"rust-analyzer {method} failed: {json.dumps(error)}"
        )
        self.code = error.get("code")


@dataclass
class SyntaxNode:
    kind: str
    start: int
    end: int
    indent: int
    parent: int | None
    children: list[int] = field(default_factory=list)


@dataclass
class SourceFile:
    path: Path
    source: str
    data: bytes
    line_starts: list[int]
    nodes: list[SyntaxNode]
    call_nodes_by_line: dict[int, list[tuple[int, SyntaxNode]]] = field(
        default_factory=dict
    )

    def position(self, offset: int) -> dict[str, int]:
        line = bisect.bisect_right(self.line_starts, offset) - 1
        return {"line": line, "character": offset - self.line_starts[line]}

    def offset(self, position: dict[str, int]) -> int:
        line = position["line"]
        if line >= len(self.line_starts):
            raise ValueError(f"line {line} lies outside {self.path}")
        return self.line_starts[line] + position["character"]

    def text(self, node: SyntaxNode) -> str:
        return self.slice(node.start, node.end)

    def slice(self, start: int, end: int) -> str:
        return self.data[start:end].decode()


@dataclass(frozen=True)
class SourceDiscovery:
    reachable: tuple[Path, ...]
    unreachable: tuple[Path, ...]
    targets: tuple[dict[str, Any], ...]
    conditions: dict[Path, tuple[tuple[str, ...], ...]]
    source_targets: dict[Path, tuple[str, ...]]


@dataclass(frozen=True)
class SemanticView:
    name: str
    all_targets: bool
    features: tuple[str, ...] | str
    set_test: bool

    def configuration(self) -> dict[str, Any]:
        features: list[str] | str = (
            list(self.features)
            if isinstance(self.features, tuple)
            else self.features
        )
        return {
            "cargo": {
                "allTargets": self.all_targets,
                "features": features,
            },
            "cfg": {"setTest": self.set_test},
            "procMacro": {"enable": True},
        }


SEMANTIC_VIEWS = (
    SemanticView("library-default", False, (), False),
    SemanticView("library-all-features", False, "all", False),
    SemanticView("tests-all-features", True, "all", True),
)
MACRO_EXPANSION_VIEW = SemanticView(
    "macro-expansions-all-features",
    True,
    "all",
    True,
)


def rust_analyzer_environment() -> dict[str, str]:
    environment = os.environ.copy()
    environment.pop("RUSTC_WRAPPER", None)
    environment.pop("RUSTC_WORKSPACE_WRAPPER", None)
    return environment


def rust_analyzer_command(*arguments: str) -> list[str]:
    return [
        "rustup",
        "run",
        ANALYZER_TOOLCHAIN,
        "rust-analyzer",
        *arguments,
    ]


def analyzer_error_lines(log: str) -> list[str]:
    return [
        line
        for line in log.splitlines()
        if " ERROR " in line
    ]


def cargo_metadata(root: Path = ROOT) -> dict[str, Any]:
    result = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=root,
        env=rust_analyzer_environment(),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"cargo metadata failed:\n{result.stderr}")
    return json.loads(result.stdout)


def analyzer_identity() -> str:
    commands = (
        rust_analyzer_command("--version"),
        ["rustup", "run", ANALYZER_TOOLCHAIN, "rustc", "--version", "--verbose"],
    )
    output = []
    for command in commands:
        result = subprocess.run(
            command,
            cwd=ROOT,
            env=rust_analyzer_environment(),
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"tool identity command failed: {' '.join(command)}\n{result.stderr}"
            )
        output.append(result.stdout.strip())
    return "\n".join(output)


def workspace_cache_inputs(metadata: dict[str, Any]) -> list[Path]:
    paths = {
        path.resolve()
        for package in metadata["packages"]
        for path in Path(package["manifest_path"]).resolve().parent.glob("**/*.rs")
    }
    paths.update(
        Path(package["manifest_path"]).resolve()
        for package in metadata["packages"]
    )
    paths.update(
        path.resolve()
        for name in (
            "Cargo.lock",
            "Cargo.toml",
            "rust-toolchain",
            "rust-toolchain.toml",
            ".cargo/config",
            ".cargo/config.toml",
        )
        if (path := ROOT / name).is_file()
    )
    return sorted(paths)


def hash_cache_inputs(
    paths: Iterable[Path],
    metadata: dict[str, Any],
    tool_identity: str,
) -> str:
    digest = hashlib.sha256()
    digest.update(f"ownership-audit-cache-v{CACHE_SCHEMA}\0".encode())
    digest.update(
        json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode()
    )
    digest.update(b"\0")
    digest.update(tool_identity.encode())
    for path in paths:
        relative = path.relative_to(ROOT)
        digest.update(b"\0path\0")
        digest.update(relative.as_posix().encode())
        digest.update(b"\0content\0")
        digest.update(path.read_bytes())
    return digest.hexdigest()


def workspace_cache_fingerprint(
    metadata: dict[str, Any],
    tool_identity: str | None = None,
) -> str:
    return hash_cache_inputs(
        workspace_cache_inputs(metadata),
        metadata,
        analyzer_identity() if tool_identity is None else tool_identity,
    )


def index_cache_fingerprint(workspace_fingerprint: str) -> str:
    digest = hashlib.sha256()
    digest.update(f"ownership-audit-index-v{CACHE_SCHEMA}\0".encode())
    digest.update(workspace_fingerprint.encode())
    digest.update(b"\0tool\0")
    digest.update(Path(__file__).read_bytes())
    return digest.hexdigest()


def view_cache_fingerprint(workspace_fingerprint: str, view: SemanticView) -> str:
    digest = hashlib.sha256()
    digest.update(workspace_fingerprint.encode())
    digest.update(b"\0")
    digest.update(
        json.dumps(
            view.configuration(),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    )
    return digest.hexdigest()


def trait_callable(source_file: SourceFile, index: int) -> bool:
    for ancestor_index, ancestor in ancestors(source_file, index):
        if is_callable_node(source_file, ancestor_index):
            return False
        if ancestor.kind == "TRAIT":
            return True
        if ancestor.kind == "IMPL":
            return any(
                source_file.nodes[child].kind == "FOR_KW"
                for child in ancestor.children
            )
    return False


def callable_erasure_spans(source_file: SourceFile) -> list[tuple[int, int, bytes]]:
    spans: list[tuple[int, int, bytes]] = []
    for index, node in enumerate(source_file.nodes):
        if node.kind != "FN":
            continue
        body = direct_child(source_file, index, "BLOCK_EXPR")
        if trait_callable(source_file, index):
            if body is not None:
                spans.append(
                    (body.start, body.end, b"{/* callable body */}")
                )
        else:
            spans.append((node.start, node.end, b""))
    outermost: list[tuple[int, int, bytes]] = []
    for start, end, replacement in sorted(spans):
        if outermost and start < outermost[-1][1]:
            continue
        outermost.append((start, end, replacement))
    return outermost


def semantic_source_surface(source_file: SourceFile) -> bytes:
    output = bytearray()
    cursor = 0
    for start, end, replacement in callable_erasure_spans(source_file):
        output.extend(source_file.data[cursor:start])
        output.extend(replacement)
        cursor = end
    output.extend(source_file.data[cursor:])
    return re.sub(rb"\s+", b" ", bytes(output)).strip()


def full_semantic_source_roots(metadata: dict[str, Any]) -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                Path(package["manifest_path"]).resolve().parent
                for package in metadata["packages"]
                if any(
                    set(target["kind"]).intersection(
                        {"proc-macro", "custom-build"}
                    )
                    for target in package["targets"]
                )
            }
        )
    )


def semantic_workspace_cache_fingerprint(
    metadata: dict[str, Any],
    source_files: dict[Path, SourceFile],
    tool_identity: str | None = None,
) -> str:
    digest = hashlib.sha256()
    digest.update(f"ownership-audit-semantic-surface-v{CACHE_SCHEMA}\0".encode())
    digest.update(
        json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode()
    )
    digest.update(b"\0analyzer\0")
    digest.update(
        (analyzer_identity() if tool_identity is None else tool_identity).encode()
    )
    digest.update(b"\0tool\0")
    digest.update(Path(__file__).read_bytes())
    full_source_roots = full_semantic_source_roots(metadata)
    for path, source_file in sorted(source_files.items()):
        digest.update(b"\0path\0")
        digest.update(path.relative_to(ROOT).as_posix().encode())
        digest.update(b"\0surface\0")
        digest.update(
            source_file.data
            if any(path.is_relative_to(root) for root in full_source_roots)
            else semantic_source_surface(source_file)
        )
    return digest.hexdigest()


def semantic_entry_cache_fingerprint(
    view_fingerprint: str,
    record: dict[str, Any],
    source_file: SourceFile,
    candidate_surfaces: dict[str, list[dict[str, Any]]] | None = None,
) -> str:
    start = source_file.offset(record["range"]["start"])
    end = source_file.offset(record["range"]["end"])
    digest = hashlib.sha256()
    digest.update(f"ownership-audit-semantic-entry-v{CACHE_SCHEMA}\0".encode())
    digest.update(view_fingerprint.encode())
    digest.update(b"\0symbol\0")
    digest.update(record["symbol"].encode())
    digest.update(b"\0callable\0")
    digest.update(source_file.data[start:end])
    candidate_surfaces = candidate_surfaces or {}
    referenced_names = sorted(
        {
            name
            for call in record.get("calls", [])
            if (name := call_name(call.get("callee_text", ""))) is not None
        }
    )
    digest.update(b"\0candidates\0")
    digest.update(
        json.dumps(
            {
                name: candidate_surfaces.get(name, [])
                for name in referenced_names
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    )
    return digest.hexdigest()


def semantic_callable_candidate_surfaces(
    records: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    surfaces: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        if record["kind"] in {"closure", "async-block"}:
            continue
        surfaces.setdefault(record["name"], []).append(
            {
                "symbol": record["symbol"],
                "signature": record["signature"],
                "receiver": record.get("receiver_type"),
                "cfg": record.get("cfg", []),
            }
        )
    for candidates in surfaces.values():
        candidates.sort(key=lambda candidate: candidate["symbol"])
    return surfaces


def write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        dir=path.parent,
        prefix=f"{path.stem}.",
        suffix=path.suffix,
        delete=False,
    ) as temporary:
        json.dump(value, temporary, indent=2, sort_keys=True)
        temporary.write("\n")
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, path)


def semantic_view_cache_path(view: SemanticView) -> Path:
    return CACHE_DIR / f"call-hierarchy-{view.name}.sqlite3"


def open_semantic_view_cache(
    view: SemanticView,
    fingerprint: str,
) -> sqlite3.Connection:
    path = semantic_view_cache_path(view)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30)
    connection.execute(
        "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    metadata = dict(connection.execute("SELECT key, value FROM metadata"))
    expected = {
        "schema": str(CACHE_SCHEMA),
        "fingerprint": fingerprint,
    }
    if metadata != expected:
        connection.execute("DROP TABLE IF EXISTS entries")
        connection.execute("DELETE FROM metadata")
        connection.executemany(
            "INSERT INTO metadata (key, value) VALUES (?, ?)",
            expected.items(),
        )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS entries ("
        "symbol TEXT PRIMARY KEY, "
        "fingerprint TEXT NOT NULL, "
        "calls TEXT"
        ")"
    )
    connection.commit()
    return connection


def read_semantic_view_cache(
    view: SemanticView,
    fingerprint: str,
    entry_fingerprints: dict[str, str],
) -> dict[str, Any]:
    connection = open_semantic_view_cache(view, fingerprint)
    try:
        return {
            symbol: {
                "fingerprint": entry_fingerprint,
                "calls": json.loads(calls) if calls is not None else None,
            }
            for symbol, entry_fingerprint, calls in connection.execute(
                "SELECT symbol, fingerprint, calls FROM entries"
            )
            if entry_fingerprints.get(symbol) == entry_fingerprint
        }
    finally:
        connection.close()


def write_semantic_view_cache(
    view: SemanticView,
    fingerprint: str,
    entries: dict[str, Any],
) -> None:
    if not entries:
        return
    connection = open_semantic_view_cache(view, fingerprint)
    try:
        connection.executemany(
            "INSERT OR REPLACE INTO entries (symbol, fingerprint, calls) "
            "VALUES (?, ?, ?)",
            (
                (
                    symbol,
                    entry["fingerprint"],
                    json.dumps(entry["calls"], separators=(",", ":"))
                    if entry["calls"] is not None
                    else None,
                )
                for symbol, entry in entries.items()
            ),
        )
        connection.commit()
    finally:
        connection.close()


def read_cached_index(fingerprint: str) -> dict[str, Any] | None:
    if not INDEX_PATH.exists():
        return None
    index = json.loads(INDEX_PATH.read_text())
    cache = index.get("cache")
    if not isinstance(cache, dict):
        return None
    if cache.get("schema") != CACHE_SCHEMA or cache.get("fingerprint") != fingerprint:
        return None
    return index


def run_parse(source: str) -> str:
    result = subprocess.run(
        rust_analyzer_command("parse"),
        input=source,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"rust-analyzer parse failed:\n{result.stderr}")
    if "error:" in result.stderr.lower():
        raise RuntimeError(f"rust-analyzer parse reported an error:\n{result.stderr}")
    return result.stdout


def parse_nodes(tree: str) -> list[SyntaxNode]:
    nodes: list[SyntaxNode] = []
    stack: list[int] = []
    for line in tree.splitlines():
        match = NODE_PATTERN.match(line)
        if match is None:
            continue
        indent = len(match.group("indent"))
        while stack and nodes[stack[-1]].indent >= indent:
            stack.pop()
        parent = stack[-1] if stack else None
        node = SyntaxNode(
            kind=match.group("kind"),
            start=int(match.group("start")),
            end=int(match.group("end")),
            indent=indent,
            parent=parent,
        )
        index = len(nodes)
        nodes.append(node)
        if parent is not None:
            nodes[parent].children.append(index)
        stack.append(index)
    return nodes


def parse_source(path: Path, source: str | None = None) -> SourceFile:
    source = path.read_text() if source is None else source
    data = source.encode()
    line_starts = [0]
    line_starts.extend(match.end() for match in re.finditer(b"\n", data))
    nodes = parse_nodes(run_parse(source))
    call_nodes_by_line: dict[int, list[tuple[int, SyntaxNode]]] = {}
    for index, node in enumerate(nodes):
        if node.kind not in CALL_NODE_KINDS:
            continue
        start_line = bisect.bisect_right(line_starts, node.start) - 1
        end_line = bisect.bisect_right(line_starts, node.end) - 1
        for line in range(start_line, end_line + 1):
            call_nodes_by_line.setdefault(line, []).append((index, node))
    return SourceFile(
        path,
        source,
        data,
        line_starts,
        nodes,
        call_nodes_by_line,
    )


def direct_child(source_file: SourceFile, index: int, kind: str) -> SyntaxNode | None:
    for child in source_file.nodes[index].children:
        node = source_file.nodes[child]
        if node.kind == kind:
            return node
    return None


def is_callable_node(source_file: SourceFile, index: int) -> bool:
    node = source_file.nodes[index]
    return node.kind in CALLABLE_NODE_KINDS or (
        node.kind == "BLOCK_EXPR"
        and direct_child(source_file, index, "ASYNC_KW") is not None
    )


def descendants(
    source_file: SourceFile,
    index: int,
) -> Iterable[tuple[int, SyntaxNode]]:
    pending = list(reversed(source_file.nodes[index].children))
    while pending:
        child = pending.pop()
        node = source_file.nodes[child]
        yield child, node
        pending.extend(reversed(node.children))


def ancestors(source_file: SourceFile, index: int) -> Iterable[tuple[int, SyntaxNode]]:
    parent = source_file.nodes[index].parent
    while parent is not None:
        yield parent, source_file.nodes[parent]
        parent = source_file.nodes[parent].parent


def record_field_type(source_file: SourceFile, index: int) -> str:
    return next(
        (
            source_file.text(source_file.nodes[child]).strip()
            for child in source_file.nodes[index].children
            if source_file.nodes[child].kind.endswith("_TYPE")
        ),
        "",
    )


def enclosing_record_name(source_file: SourceFile, index: int) -> str:
    for ancestor_index, ancestor in ancestors(source_file, index):
        if ancestor.kind not in {"STRUCT", "ENUM_VARIANT"}:
            continue
        name = direct_name(source_file, ancestor_index)
        if name is not None:
            return name[0]
    return "<record>"


def field_expression_chain(
    source_file: SourceFile,
    index: int,
) -> list[str]:
    node = source_file.nodes[index]
    if node.kind != "FIELD_EXPR":
        return []
    receiver_index = next(
        (
            child
            for child in node.children
            if source_file.nodes[child].kind.endswith("_EXPR")
        ),
        None,
    )
    field = next(
        (
            source_file.text(source_file.nodes[child]).strip()
            for child in reversed(node.children)
            if source_file.nodes[child].kind == "NAME_REF"
        ),
        None,
    )
    if receiver_index is None or field is None:
        return []
    receiver = source_file.nodes[receiver_index]
    if receiver.kind == "FIELD_EXPR":
        return [*field_expression_chain(source_file, receiver_index), field]
    return [source_file.text(receiver).strip(), field]


def direct_name(source_file: SourceFile, index: int) -> tuple[str, int, int] | None:
    name = direct_child(source_file, index, "NAME")
    if name is None:
        return None
    text = source_file.text(name)
    match = re.search(r"[A-Za-z_][A-Za-z0-9_]*", text)
    if match is None:
        return None
    start = name.start + match.start()
    return match.group(), start, name.start + match.end()


def module_base(path: Path) -> Path:
    if path.name in {"lib.rs", "main.rs", "mod.rs"}:
        return path.parent
    return path.parent / path.stem


def path_attribute(source_file: SourceFile, index: int) -> str | None:
    for child in source_file.nodes[index].children:
        node = source_file.nodes[child]
        if node.kind != "ATTR":
            continue
        match = re.fullmatch(
            r'#\s*\[\s*path\s*=\s*"([^"]+)"\s*\]',
            source_file.text(node).strip(),
        )
        if match is not None:
            return match.group(1)
    return None


def external_modules(
    source_file: SourceFile,
) -> list[tuple[Path, tuple[str, ...]]]:
    modules: list[tuple[Path, tuple[str, ...]]] = []
    for index, node in enumerate(source_file.nodes):
        if node.kind != "MODULE" or direct_child(source_file, index, "ITEM_LIST"):
            continue
        name = direct_name(source_file, index)
        if name is None:
            raise RuntimeError(
                f"module without a name in {source_file.path}:{node.start}"
            )
        explicit_path = path_attribute(source_file, index)
        if explicit_path is not None:
            modules.append(
                (
                    (source_file.path.parent / explicit_path).resolve(),
                    tuple(cfg_attributes(source_file, index)),
                )
            )
            continue
        inline_modules = [
            ancestor_name[0]
            for ancestor_index, ancestor in reversed(
                list(ancestors(source_file, index))
            )
            if ancestor.kind == "MODULE"
            and direct_child(source_file, ancestor_index, "ITEM_LIST")
            and (ancestor_name := direct_name(source_file, ancestor_index))
            is not None
        ]
        base = module_base(source_file.path).joinpath(*inline_modules)
        candidates = [
            base / f"{name[0]}.rs",
            base / name[0] / "mod.rs",
        ]
        existing = [candidate.resolve() for candidate in candidates if candidate.is_file()]
        if len(existing) != 1:
            rendered = ", ".join(str(candidate) for candidate in candidates)
            raise RuntimeError(
                f"module {name[0]} in {source_file.path} resolves to "
                f"{len(existing)} files; checked {rendered}"
            )
        modules.append(
            (
                existing[0],
                tuple(cfg_attributes(source_file, index)),
            )
        )
    return modules


def external_module_paths(source_file: SourceFile) -> list[Path]:
    return [path for path, _ in external_modules(source_file)]


def normalize_conditions(conditions: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(set(conditions)))


def source_closure(
    root: Path,
    parsed: dict[Path, SourceFile],
) -> dict[Path, set[tuple[str, ...]]]:
    root = root.resolve()
    pending = [(root, ())]
    visited: set[tuple[Path, tuple[str, ...]]] = set()
    conditions: dict[Path, set[tuple[str, ...]]] = {}
    while pending:
        path, inherited = pending.pop()
        state = (path, inherited)
        if state in visited:
            continue
        visited.add(state)
        if not path.is_file():
            raise RuntimeError(f"Cargo source root or module is absent: {path}")
        conditions.setdefault(path, set()).add(inherited)
        source_file = parsed.get(path)
        if source_file is None:
            source_file = parse_source(path)
            parsed[path] = source_file
        for child, child_conditions in external_modules(source_file):
            pending.append(
                (
                    child,
                    normalize_conditions((*inherited, *child_conditions)),
                )
            )
    return conditions


def reachable_sources(roots: Iterable[Path]) -> tuple[Path, ...]:
    parsed: dict[Path, SourceFile] = {}
    reachable: set[Path] = set()
    for root in roots:
        reachable.update(source_closure(root, parsed))
    return tuple(sorted(reachable))


def discover_workspace_sources(
    metadata: dict[str, Any] | None = None,
) -> SourceDiscovery:
    metadata = cargo_metadata() if metadata is None else metadata
    targets: list[dict[str, Any]] = []
    all_sources: set[Path] = set()
    parsed: dict[Path, SourceFile] = {}
    conditions: dict[Path, set[tuple[str, ...]]] = {}
    source_targets: dict[Path, set[str]] = {}
    for package in metadata["packages"]:
        package_root = Path(package["manifest_path"]).resolve().parent
        all_sources.update(path.resolve() for path in package_root.glob("**/*.rs"))
        for target in package["targets"]:
            source = Path(target["src_path"]).resolve()
            kinds = target["kind"]
            target_id = (
                f"{package['name']}:{','.join(kinds)}:{target['name']}"
            )
            target_conditions = source_closure(source, parsed)
            for path, paths in target_conditions.items():
                conditions.setdefault(path, set()).update(paths)
                source_targets.setdefault(path, set()).add(target_id)
            targets.append(
                {
                    "id": target_id,
                    "package": package["name"],
                    "name": target["name"],
                    "kind": kinds,
                    "source": str(source.relative_to(ROOT)),
                    "required_features": target.get("required-features", []),
                    "sources": sorted(
                        str(path.relative_to(ROOT))
                        for path in target_conditions
                    ),
                }
            )
    reachable = tuple(sorted(conditions))
    return SourceDiscovery(
        reachable=reachable,
        unreachable=tuple(sorted(all_sources.difference(reachable))),
        targets=tuple(targets),
        conditions={
            path: tuple(sorted(paths))
            for path, paths in conditions.items()
        },
        source_targets={
            path: tuple(sorted(targets))
            for path, targets in source_targets.items()
        },
    )


def crate_and_modules(path: Path) -> tuple[str, list[str]]:
    relative = path.relative_to(ROOT)
    crate_dir = relative.parts[1]
    crate = crate_dir.replace("-", "_")
    within = Path(*relative.parts[2:])
    if within.parts[0] == "src":
        module_parts = list(within.parts[1:])
        filename = module_parts.pop()
        stem = Path(filename).stem
        if stem not in {"lib", "main", "mod"}:
            module_parts.append(stem)
    else:
        module_parts = ["tests", *within.parts[1:]]
        module_parts[-1] = Path(module_parts[-1]).stem
    return crate, module_parts


def enclosing_modules(source_file: SourceFile, index: int) -> list[str]:
    modules: list[str] = []
    for ancestor_index, ancestor in reversed(list(ancestors(source_file, index))):
        if ancestor.kind != "MODULE":
            continue
        name = direct_name(source_file, ancestor_index)
        if name is not None:
            modules.append(name[0])
    return modules


def impl_name(source_file: SourceFile, index: int) -> str | None:
    for ancestor_index, ancestor in ancestors(source_file, index):
        if is_callable_node(source_file, ancestor_index):
            return None
        if ancestor.kind not in {"IMPL", "TRAIT"}:
            continue
        if ancestor.kind == "TRAIT":
            name = direct_name(source_file, ancestor_index)
            return f"trait {name[0]}" if name else "trait"
        implemented_types = [
            source_file.text(source_file.nodes[child]).strip()
            for child in ancestor.children
            if source_file.nodes[child].kind.endswith("_TYPE")
        ]
        if not implemented_types:
            return None
        has_trait = any(
            source_file.nodes[child].kind == "FOR_KW"
            for child in ancestor.children
        )
        return (
            f"{implemented_types[-2]} for {implemented_types[-1]}"
            if has_trait and len(implemented_types) >= 2
            else implemented_types[-1]
        )
    return None


def cfg_attributes(source_file: SourceFile, index: int) -> list[str]:
    return [
        re.sub(r"\s+", " ", source_file.text(source_file.nodes[child])).strip()
        for child in source_file.nodes[index].children
        if source_file.nodes[child].kind == "ATTR"
        and re.match(r"#\s*\[\s*cfg(?:_attr)?\b", source_file.text(source_file.nodes[child]))
    ]


def effective_cfg_attributes(
    source_file: SourceFile,
    index: int,
) -> list[str]:
    conditions = cfg_attributes(source_file, index)
    for ancestor_index, _ in ancestors(source_file, index):
        conditions.extend(cfg_attributes(source_file, ancestor_index))
    return list(normalize_conditions(conditions))


def identity_cfg_attributes(
    source_file: SourceFile,
    index: int,
) -> list[str]:
    conditions = cfg_attributes(source_file, index)
    for ancestor_index, _ in ancestors(source_file, index):
        if is_callable_node(source_file, ancestor_index):
            break
        conditions.extend(cfg_attributes(source_file, ancestor_index))
    return list(normalize_conditions(conditions))


def nested_cfg_attributes(
    source_file: SourceFile,
    index: int,
    callable_index: int,
) -> list[str]:
    conditions = cfg_attributes(source_file, index)
    for ancestor_index, _ in ancestors(source_file, index):
        if ancestor_index == callable_index:
            break
        conditions.extend(cfg_attributes(source_file, ancestor_index))
    return list(normalize_conditions(conditions))


def is_test_context(
    source_file: SourceFile,
    index: int,
) -> bool:
    candidates = [index, *(ancestor for ancestor, _ in ancestors(source_file, index))]
    return any(
        re.search(
            r"#\s*\[\s*(?:[A-Za-z_][A-Za-z0-9_]*::)*"
            r"(?:test|test_case|rstest)\b",
            source_file.text(source_file.nodes[child]),
        )
        for candidate in candidates
        for child in source_file.nodes[candidate].children
        if source_file.nodes[child].kind == "ATTR"
    )


def visibility(signature: str) -> str:
    match = re.match(r"\s*(pub(?:\([^)]*\))?)\b", signature)
    return match.group(1) if match else "private"


def signature_for(source_file: SourceFile, index: int) -> str:
    node = source_file.nodes[index]
    block = direct_child(source_file, index, "BLOCK_EXPR")
    end = block.start if block is not None else node.end
    start = next(
        (
            source_file.nodes[child].start
            for child in node.children
            if source_file.nodes[child].kind not in {"COMMENT", "WHITESPACE", "ATTR"}
        ),
        node.start,
    )
    return re.sub(r"\s+", " ", source_file.slice(start, end)).strip()


def body_for(source_file: SourceFile, index: int) -> str:
    block = direct_child(source_file, index, "BLOCK_EXPR")
    return source_file.text(block) if block is not None else ""


def callable_parameters(
    source_file: SourceFile,
    index: int,
) -> list[dict[str, str]]:
    parameter_list = direct_child(source_file, index, "PARAM_LIST")
    if parameter_list is None:
        return []
    parameters = []
    for child in parameter_list.children:
        parameter = source_file.nodes[child]
        if parameter.kind == "SELF_PARAM":
            parameters.append(
                {
                    "name": "self",
                    "type": source_file.text(parameter).strip(),
                }
            )
            continue
        if parameter.kind != "PARAM":
            continue
        name = next(
            (
                direct_name(source_file, candidate_index)
                for candidate_index, candidate in descendants(source_file, child)
                if candidate.kind == "IDENT_PAT"
            ),
            None,
        )
        type_node = next(
            (
                candidate
                for candidate_index in parameter.children
                if (
                    candidate := source_file.nodes[candidate_index]
                ).kind.endswith("_TYPE")
            ),
            None,
        )
        parameters.append(
            {
                "name": name[0] if name is not None else source_file.text(parameter),
                "type": (
                    source_file.text(type_node).strip()
                    if type_node is not None
                    else ""
                ),
            }
        )
    return parameters


def callable_return_type(source_file: SourceFile, index: int) -> str:
    return_type = direct_child(source_file, index, "RET_TYPE")
    if return_type is None:
        return ""
    return source_file.text(return_type).removeprefix("->").strip()


def is_receiver_constructor(
    owner: str | None,
    parameters: list[dict[str, str]],
    return_type: str,
) -> bool:
    concrete_owner = concrete_receiver_owner(owner)
    owner_name = (
        concrete_owner.rsplit("::", 1)[-1]
        if concrete_owner is not None
        else None
    )
    return bool(
        owner is not None
        and not any(parameter["name"] == "self" for parameter in parameters)
        and (
            re.search(r"\bSelf\b", return_type)
            or (
                owner_name is not None
                and re.search(rf"\b{re.escape(owner_name)}\b", return_type)
            )
        )
    )


def nearest_callable_ancestor(
    source_file: SourceFile,
    index: int,
) -> int | None:
    return next(
        (
            ancestor_index
            for ancestor_index, _ in ancestors(source_file, index)
            if is_callable_node(source_file, ancestor_index)
        ),
        None,
    )


def binding_scope(
    source_file: SourceFile,
    index: int,
) -> SyntaxNode:
    for ancestor_index, ancestor in ancestors(source_file, index):
        if ancestor.kind in {"STMT_LIST", "MATCH_ARM"} or is_callable_node(
            source_file, ancestor_index
        ):
            return ancestor
    return source_file.nodes[index]


def lexical_bindings(
    source_file: SourceFile,
    records_by_index: dict[int, dict[str, Any]],
) -> list[dict[str, Any]]:
    bindings: list[dict[str, Any]] = []
    for index, node in enumerate(source_file.nodes):
        if node.kind not in {"IDENT_PAT", "SELF_PARAM"}:
            continue
        callable_index = nearest_callable_ancestor(source_file, index)
        if callable_index not in records_by_index:
            continue
        if node.kind == "SELF_PARAM":
            name = "self"
        else:
            name_info = direct_name(source_file, index)
            if name_info is None:
                continue
            name = name_info[0]
        is_parameter = any(
            ancestor.kind == "PARAM"
            for _, ancestor in ancestors(source_file, index)
            if ancestor.start >= source_file.nodes[callable_index].start
        ) or node.kind == "SELF_PARAM"
        record = records_by_index[callable_index]
        parameter = next(
            (
                candidate
                for candidate in record["parameters"]
                if candidate["name"] == name
            ),
            None,
        )
        scope = binding_scope(source_file, index)
        bindings.append(
            {
                "name": name,
                "kind": "parameter" if is_parameter else "local",
                "type": (
                    parameter["type"]
                    if is_parameter and parameter is not None
                    else ""
                ),
                "callable_index": callable_index,
                "start": node.start,
                "scope_start": scope.start,
                "scope_end": scope.end,
            }
        )
    return bindings


def captured_values(
    source_file: SourceFile,
    callable_index: int,
    bindings: list[dict[str, Any]],
    records_by_index: dict[int, dict[str, Any]],
) -> list[dict[str, str]]:
    node = source_file.nodes[callable_index]
    outer_callables = [
        ancestor_index
        for ancestor_index, _ in ancestors(source_file, callable_index)
        if is_callable_node(source_file, ancestor_index)
    ]
    captures: dict[tuple[str, str], dict[str, str]] = {}
    for reference_index, reference in descendants(source_file, callable_index):
        if reference.kind != "NAME_REF":
            continue
        if nearest_callable_ancestor(source_file, reference_index) != callable_index:
            continue
        name = source_file.text(reference)
        local = [
            binding
            for binding in bindings
            if binding["callable_index"] == callable_index
            and binding["name"] == name
            and binding["scope_start"] <= reference.start < binding["scope_end"]
            and (
                binding["kind"] == "parameter"
                or binding["start"] < reference.start
            )
        ]
        if local:
            continue
        outer = [
            binding
            for binding in bindings
            if binding["callable_index"] in outer_callables
            and binding["name"] == name
            and binding["scope_start"] <= node.start < binding["scope_end"]
            and (
                binding["kind"] == "parameter"
                or binding["start"] < node.start
            )
        ]
        if not outer:
            continue
        selected = min(
            outer,
            key=lambda binding: (
                outer_callables.index(binding["callable_index"]),
                -binding["scope_start"],
                -binding["start"],
            ),
        )
        declared_by = records_by_index[selected["callable_index"]]["symbol"]
        captures[(declared_by, name)] = {
            "declared_by": declared_by,
            "kind": selected["kind"],
            "name": name,
            "type": selected["type"],
        }
    return sorted(
        captures.values(),
        key=lambda capture: (
            capture["declared_by"],
            capture["name"],
        ),
    )


def callable_paths(
    source_file: SourceFile,
    callable_index: int,
) -> list[dict[str, Any]]:
    paths = []
    for path_index, path in descendants(source_file, callable_index):
        if path.kind != "PATH":
            continue
        if nearest_callable_ancestor(source_file, path_index) != callable_index:
            continue
        parent = path.parent
        if parent is not None and source_file.nodes[parent].kind == "PATH":
            continue
        paths.append(
            {
                "text": re.sub(
                    r"\s+",
                    "",
                    source_file.text(path),
                ),
                "range": {
                    "start": source_file.position(path.start),
                    "end": source_file.position(path.end),
                },
            }
        )
    return paths


def closure_binding(source_file: SourceFile, index: int) -> str | None:
    for ancestor_index, ancestor in ancestors(source_file, index):
        if is_callable_node(source_file, ancestor_index):
            return None
        if ancestor.kind != "LET_STMT":
            continue
        for pattern_index, pattern in descendants(source_file, ancestor_index):
            if pattern.kind != "IDENT_PAT":
                continue
            name = direct_name(source_file, pattern_index)
            return name[0] if name is not None else None
    return None


def retained_dependencies(signature: str) -> list[str]:
    return [
        category
        for category, pattern in STATEFUL_TYPE_PATTERNS.items()
        if pattern.search(signature)
    ]


def service_dependencies(type_text: str) -> list[str]:
    return [
        category
        for category, pattern in SERVICE_TYPE_PATTERNS.items()
        if pattern.search(type_text)
    ]


def service_field_dependencies(fields: Iterable[str]) -> list[str]:
    return sorted(
        {
            category
            for field in fields
            for category, pattern in SERVICE_FIELD_PATTERNS.items()
            if pattern.search(field)
        }
    )


def service_exposures_for_source(
    source_file: SourceFile,
) -> list[dict[str, Any]]:
    path = str(source_file.path.relative_to(ROOT))
    findings: list[dict[str, Any]] = []
    records = inventory_file(source_file)
    records_by_start = {
        (
            record["range"]["start"]["line"],
            record["range"]["start"]["character"],
        ): record
        for record in records
    }

    for index, node in enumerate(source_file.nodes):
        if node.kind == "RECORD_FIELD":
            visibility_node = direct_child(source_file, index, "VISIBILITY")
            field_visibility = (
                source_file.text(visibility_node).strip()
                if visibility_node is not None
                else "private"
            )
            if field_visibility == "private":
                continue
            name = direct_name(source_file, index)
            if name is None:
                continue
            dependencies = service_dependencies(record_field_type(source_file, index))
            if not dependencies:
                continue
            findings.append(
                {
                    "kind": "service-field",
                    "name": f"{enclosing_record_name(source_file, index)}::{name[0]}",
                    "dependencies": dependencies,
                    "visibility": field_visibility,
                    "path": path,
                    "line": source_file.position(node.start)["line"] + 1,
                    "expression": re.sub(
                        r"\s+", " ", source_file.text(node)
                    ).strip(),
                }
            )
            continue

        if node.kind != "FIELD_EXPR":
            continue
        parent = node.parent
        if parent is not None and source_file.nodes[parent].kind == "FIELD_EXPR":
            continue
        chain = field_expression_chain(source_file, index)
        if len(chain) < 3:
            continue
        dependencies = service_field_dependencies(chain[1:])
        if not dependencies:
            continue
        callable_index = nearest_callable_ancestor(source_file, index)
        if callable_index is None:
            continue
        callable_start = source_file.position(
            source_file.nodes[callable_index].start
        )
        record = records_by_start.get(
            (callable_start["line"], callable_start["character"])
        )
        if record is None:
            continue
        findings.append(
            {
                "kind": "nested-service-reach",
                "name": record["symbol"],
                "dependencies": dependencies,
                "visibility": record["visibility"],
                "path": path,
                "line": source_file.position(node.start)["line"] + 1,
                "expression": re.sub(
                    r"\s+", " ", source_file.text(node)
                ).strip(),
            }
        )

    for record in records:
        if record["visibility"] == "private" or record["kind"] != "method":
            continue
        dependencies = service_dependencies(record["return_type"])
        returns_shared_service = (
            "&" in record["return_type"]
            or "Arc<" in record["return_type"]
            or "Box<dyn " in record["return_type"]
        )
        if not dependencies or not returns_shared_service:
            continue
        findings.append(
            {
                "kind": "service-getter",
                "name": record["symbol"],
                "dependencies": dependencies,
                "visibility": record["visibility"],
                "path": path,
                "line": record["range"]["start"]["line"] + 1,
                "expression": record["signature"],
            }
        )

    return sorted(
        findings,
        key=lambda finding: (
            finding["path"],
            finding["line"],
            finding["kind"],
            finding["name"],
        ),
    )


def matches(body: str, patterns: dict[str, re.Pattern[str]]) -> list[str]:
    return [name for name, pattern in patterns.items() if pattern.search(body)]


def call_text(source_file: SourceFile, node: SyntaxNode) -> str:
    text = re.sub(r"\s+", " ", source_file.text(node)).strip()
    return text[:240]


def call_callee(
    source_file: SourceFile,
    index: int,
) -> tuple[dict[str, dict[str, int]], str]:
    node = source_file.nodes[index]
    arguments = direct_child(source_file, index, "ARG_LIST")
    end = arguments.start if arguments is not None else node.end
    if node.kind == "METHOD_CALL_EXPR":
        names = [
            candidate
            for _, candidate in descendants(source_file, index)
            if candidate.kind == "NAME_REF"
            and candidate.start < end
        ]
        if names:
            name = max(names, key=lambda candidate: candidate.start)
            return (
                {
                    "start": source_file.position(name.start),
                    "end": source_file.position(name.end),
                },
                source_file.text(name),
            )
    callee_expression = next(
        (
            source_file.nodes[child]
            for child in node.children
            if source_file.nodes[child].kind.endswith("_EXPR")
            and source_file.nodes[child].end <= end
        ),
        None,
    )
    start = (
        callee_expression.start
        if callee_expression is not None
        else node.start
    )
    end = (
        callee_expression.end
        if callee_expression is not None
        else end
    )
    return (
        {
            "start": source_file.position(start),
            "end": source_file.position(end),
        },
        source_file.slice(start, end).strip(),
    )


def call_arguments(
    source_file: SourceFile,
    index: int,
) -> list[dict[str, Any]]:
    arguments = direct_child(source_file, index, "ARG_LIST")
    if arguments is None:
        return []
    values = []
    for child in arguments.children:
        node = source_file.nodes[child]
        if not (node.kind.endswith("_EXPR") or node.kind == "LITERAL"):
            continue
        values.append(
            {
                "text": re.sub(r"\s+", " ", source_file.text(node)).strip(),
                "range": {
                    "start": source_file.position(node.start),
                    "end": source_file.position(node.end),
                },
            }
        )
    return values


def inventory_file(
    source_file: SourceFile,
    module_prefix: list[str] | None = None,
) -> list[dict[str, Any]]:
    crate, discovered_modules = crate_and_modules(source_file.path)
    file_modules = (
        discovered_modules
        if module_prefix is None
        else module_prefix
    )
    callable_indices = [
        index
        for index, _ in enumerate(source_file.nodes)
        if is_callable_node(source_file, index)
    ]
    records: list[dict[str, Any]] = []
    index_to_symbol: dict[int, str] = {}
    records_by_index: dict[int, dict[str, Any]] = {}

    for index in callable_indices:
        node = source_file.nodes[index]
        named = node.kind == "FN"
        name_info = direct_name(source_file, index) if named else None
        parent_callable = next(
            (
                ancestor_index
                for ancestor_index, ancestor in ancestors(source_file, index)
                if is_callable_node(source_file, ancestor_index)
            ),
            None,
        )
        semantic_parent = next(
            (
                ancestor_index
                for ancestor_index, ancestor in ancestors(source_file, index)
                if ancestor.kind == "FN"
            ),
            None,
        )
        owner = impl_name(source_file, index) if named else None
        modules = [*file_modules, *enclosing_modules(source_file, index)]
        if named and name_info is not None:
            name, name_start, name_end = name_info
            params = direct_child(source_file, index, "PARAM_LIST")
            params_text = source_file.text(params) if params is not None else "()"
            if owner is None:
                kind = "free"
            elif owner.startswith("trait "):
                kind = "trait-method" if "self" in params_text else "trait-associated"
            else:
                kind = "method" if "self" in params_text else "associated"
            symbol_parts = [crate, *modules]
            if parent_callable is not None:
                symbol = f"{index_to_symbol[parent_callable]}::<local>::{name}"
            else:
                if owner is not None:
                    symbol_parts.append(f"<{owner}>")
                symbol_parts.append(name)
                symbol = "::".join(part for part in symbol_parts if part)
            signature = signature_for(source_file, index)
            display_name = name
        else:
            position = source_file.position(node.start)
            anonymous_kind = "closure" if node.kind == "CLOSURE_EXPR" else "async-block"
            parent_symbol = index_to_symbol.get(parent_callable, "::".join([crate, *modules]))
            symbol = (
                f"{parent_symbol}::<{anonymous_kind}@"
                f"{position['line'] + 1}:{position['character'] + 1}>"
            )
            name_start = node.start
            name_end = node.start
            kind = anonymous_kind
            signature = source_file.slice(node.start, min(node.end, node.start + 160))
            display_name = anonymous_kind
        binding = closure_binding(source_file, index) if not named else None
        identity_cfg = identity_cfg_attributes(source_file, index)
        cfg = effective_cfg_attributes(source_file, index)
        if identity_cfg:
            symbol = f"{symbol}@{' & '.join(identity_cfg)}"
        index_to_symbol[index] = symbol
        body = body_for(source_file, index) if named else source_file.text(node)
        parameters = callable_parameters(source_file, index)
        return_type = callable_return_type(source_file, index)
        record = {
                "symbol": symbol,
                "name": display_name,
                "kind": kind,
                "crate": crate,
                "module": "::".join([crate, *modules]),
                "receiver_type": owner,
                "enclosing_callable": index_to_symbol.get(parent_callable),
                "semantic_parent": index_to_symbol.get(semantic_parent),
                "binding": binding,
                "path": str(source_file.path.relative_to(ROOT)),
                "range": {
                    "start": source_file.position(node.start),
                    "end": source_file.position(node.end),
                },
                "name_range": {
                    "start": source_file.position(name_start),
                    "end": source_file.position(name_end),
                },
                "signature": signature,
                "parameters": parameters,
                "parameter_dependencies": sorted(
                    {
                        dependency
                        for parameter in parameters
                        for dependency in retained_dependencies(
                            parameter["type"]
                        )
                    }
                ),
                "return_type": return_type,
                "return_dependencies": retained_dependencies(return_type),
                "receiver_constructor": is_receiver_constructor(
                    owner,
                    parameters,
                    return_type,
                ),
                "paths": callable_paths(source_file, index),
                "visibility": visibility(signature),
                "cfg": cfg,
                "test_context": is_test_context(source_file, index),
                "retained_dependencies": retained_dependencies(signature),
                "ambient_dependencies": matches(body, AMBIENT_PATTERNS),
                "effects": matches(body, EFFECT_PATTERNS),
                "calls": [],
                "callees": [],
                "callers": [],
                "unresolved_calls": [],
            }
        records.append(record)
        records_by_index[index] = record

    bindings = lexical_bindings(
        source_file,
        records_by_index,
    )
    for index in callable_indices:
        record = records_by_index[index]
        record["captured_values"] = (
            captured_values(
                source_file,
                index,
                bindings,
                records_by_index,
            )
            if record["kind"] in {"closure", "async-block"}
            else []
        )

    for node_index, node in enumerate(source_file.nodes):
        if node.kind not in CALL_NODE_KINDS:
            continue
        containing = [
            (record_index, callable_index)
            for record_index, callable_index in enumerate(callable_indices)
            if source_file.nodes[callable_index].start <= node.start
            and node.end <= source_file.nodes[callable_index].end
        ]
        if not containing:
            continue
        record_index, callable_index = min(
            containing,
            key=lambda pair: source_file.nodes[pair[1]].end
            - source_file.nodes[pair[1]].start,
        )
        callee_range, callee_text = call_callee(source_file, node_index)
        records[record_index]["calls"].append(
            {
                "kind": node.kind,
                "range": {
                    "start": source_file.position(node.start),
                    "end": source_file.position(node.end),
                },
                "text": call_text(source_file, node),
                "callee_range": callee_range,
                "callee_text": callee_text,
                "arguments": call_arguments(source_file, node_index),
                "cfg": nested_cfg_attributes(
                    source_file,
                    node_index,
                    callable_index,
                ),
            }
        )
    return records


def item_macro_invocations(
    source_file: SourceFile,
) -> list[dict[str, Any]]:
    crate, file_modules = crate_and_modules(source_file.path)
    invocations = []
    for index, node in enumerate(source_file.nodes):
        if node.kind != "MACRO_CALL":
            continue
        if nearest_callable_ancestor(source_file, index) is not None:
            continue
        path = direct_child(source_file, index, "PATH")
        if path is None:
            continue
        path_text = source_file.text(path)
        name = path_text.rsplit("::", 1)[-1]
        invocations.append(
            {
                "crate": crate,
                "modules": [
                    *file_modules,
                    *enclosing_modules(source_file, index),
                ],
                "name": name,
                "path": str(source_file.path.relative_to(ROOT)),
                "range": {
                    "start": source_file.position(node.start),
                    "end": source_file.position(node.end),
                },
                "position": source_file.position(path.start),
                "cfg": effective_cfg_attributes(source_file, index),
                "text": re.sub(
                    r"\s+",
                    " ",
                    source_file.text(node),
                ).strip(),
            }
        )
    return invocations


def inventory_macro_expansion(
    invocation: dict[str, Any],
    expansion: str,
) -> list[dict[str, Any]]:
    path = (ROOT / invocation["path"]).resolve()
    source_file = parse_source(path, expansion)
    records = inventory_file(source_file, invocation["modules"])
    origin = {
        "name": invocation["name"],
        "path": invocation["path"],
        "range": invocation["range"],
        "text": invocation["text"],
    }
    for record in records:
        record["generated_range"] = record["range"]
        record["macro_origin"] = origin
        record["cfg"] = list(
            normalize_conditions(
                (*invocation["cfg"], *record["cfg"])
            )
        )
    return records


def attach_source_context(
    records: list[dict[str, Any]],
    path: Path,
    discovery: SourceDiscovery,
) -> None:
    inherited_paths = discovery.conditions[path]
    cargo_targets = list(discovery.source_targets[path])
    for record in records:
        record["cfg_paths"] = [
            list(
                normalize_conditions(
                    (*inherited, *record["cfg"])
                )
            )
            for inherited in inherited_paths
        ]
        record["cargo_targets"] = cargo_targets
        for call in record["calls"]:
            call["cfg_paths"] = [
                list(
                    normalize_conditions(
                        (*conditions, *call["cfg"])
                    )
                )
                for conditions in record["cfg_paths"]
            ]


def expand_workspace_item_macros(
    source_files: dict[Path, SourceFile],
    discovery: SourceDiscovery,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    invocations = [
        invocation
        for source_file in source_files.values()
        for invocation in item_macro_invocations(source_file)
    ]
    if not invocations:
        return [], []
    analyzer = RustAnalyzer(ROOT, MACRO_EXPANSION_VIEW)
    analyzer.initialize()
    records: list[dict[str, Any]] = []
    expansions: list[dict[str, Any]] = []
    try:
        analyzer.wait_until_ready()
        opened_paths: set[Path] = set()
        for invocation in invocations:
            path = (ROOT / invocation["path"]).resolve()
            if path not in opened_paths:
                analyzer.open_document(path, source_files[path].source)
                opened_paths.add(path)
            result = analyzer.expand_macro(path, invocation["position"])
            expansion_record = {
                "name": invocation["name"],
                "path": invocation["path"],
                "range": invocation["range"],
                "cfg": invocation["cfg"],
                "cargo_targets": list(discovery.source_targets[path]),
            }
            if result is None:
                expansion_record["status"] = "unavailable"
                expansion_record["callables"] = []
                expansions.append(expansion_record)
                continue
            generated = inventory_macro_expansion(
                invocation,
                result["expansion"],
            )
            attach_source_context(generated, path, discovery)
            records.extend(generated)
            expansion_record["status"] = "expanded"
            expansion_record["expanded_name"] = result["name"]
            expansion_record["callables"] = [
                record["symbol"]
                for record in generated
            ]
            expansions.append(expansion_record)
    finally:
        analyzer.close()
    return records, expansions


class RustAnalyzer:
    def __init__(self, root: Path, view: SemanticView):
        self.root = root
        self.view = view
        self.next_id = 1
        ANALYZER_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        log_path = ANALYZER_LOG_PATH.with_name(
            f"{ANALYZER_LOG_PATH.stem}-{view.name}{ANALYZER_LOG_PATH.suffix}"
        )
        self.log_path = log_path
        self.stderr = log_path.open("wb")
        self.process = subprocess.Popen(
            rust_analyzer_command(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.stderr,
            env=rust_analyzer_environment(),
        )
        if self.process.stdin is None or self.process.stdout is None:
            raise RuntimeError("failed to open rust-analyzer pipes")
        self.stdin = self.process.stdin
        self.stdout = self.process.stdout

    def configuration(self) -> dict[str, Any]:
        return self.view.configuration()

    def send(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.stdin.write(f"Content-Length: {len(body)}\r\n\r\n".encode())
        self.stdin.write(body)
        self.stdin.flush()

    def read(self) -> dict[str, Any]:
        headers: dict[str, str] = {}
        while True:
            line = self.stdout.readline()
            if not line:
                raise RuntimeError("rust-analyzer terminated while awaiting a response")
            if line == b"\r\n":
                break
            key, value = line.decode().split(":", 1)
            headers[key.lower()] = value.strip()
        length = int(headers["content-length"])
        return json.loads(self.stdout.read(length))

    def respond_to_server(self, message: dict[str, Any]) -> None:
        method = message.get("method")
        params = message.get("params", {})
        if method == "workspace/configuration":
            result = [
                self.configuration()
                for _ in params.get("items", [])
            ]
        elif method == "workspace/workspaceFolders":
            result = [
                {
                    "uri": self.root.as_uri(),
                    "name": self.root.name,
                }
            ]
        else:
            result = None
        self.send({"jsonrpc": "2.0", "id": message["id"], "result": result})

    def request(self, method: str, params: Any) -> Any:
        request_id = self.next_id
        self.next_id += 1
        self.send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            }
        )
        while True:
            message = self.read()
            if "method" in message and "id" in message:
                self.respond_to_server(message)
                continue
            if message.get("id") != request_id:
                continue
            if "error" in message:
                raise RustAnalyzerRequestError(method, message["error"])
            return message.get("result")

    def notify(self, method: str, params: Any) -> None:
        self.send({"jsonrpc": "2.0", "method": method, "params": params})

    def initialize(self) -> None:
        result = self.request(
            "initialize",
            {
                "processId": os.getpid(),
                "rootUri": self.root.as_uri(),
                "workspaceFolders": [
                    {
                        "uri": self.root.as_uri(),
                        "name": self.root.name,
                    }
                ],
                "capabilities": {
                    "textDocument": {
                        "callHierarchy": {"dynamicRegistration": False},
                        "documentSymbol": {"hierarchicalDocumentSymbolSupport": True},
                    },
                    "window": {"workDoneProgress": True},
                    "workspace": {"configuration": True, "workspaceFolders": True},
                    "general": {"positionEncodings": ["utf-8"]},
                    "experimental": {"serverStatusNotification": True},
                },
                "initializationOptions": {
                    "rust-analyzer": self.configuration(),
                },
            },
        )
        if result.get("capabilities", {}).get("positionEncoding") != "utf-8":
            raise RuntimeError("rust-analyzer did not negotiate UTF-8 positions")
        self.notify("initialized", {})
        self.notify(
            "workspace/didChangeConfiguration",
            {
                "settings": {
                    "rust-analyzer": self.configuration(),
                }
            },
        )

    def wait_until_ready(self) -> None:
        while True:
            message = self.read()
            if "method" in message and "id" in message:
                self.respond_to_server(message)
                continue
            if message.get("method") != "experimental/serverStatus":
                continue
            status = message.get("params", {})
            if not status.get("quiescent", False):
                continue
            health = status.get("health")
            if health != "ok":
                detail = status.get("message") or "no diagnostic supplied"
                raise RuntimeError(
                    f"rust-analyzer became quiescent with health {health}: {detail}"
                )
            return

    def open_document(self, path: Path, source: str) -> None:
        self.notify(
            "textDocument/didOpen",
            {
                "textDocument": {
                    "uri": path.as_uri(),
                    "languageId": "rust",
                    "version": 1,
                    "text": source,
                }
            },
        )

    def outgoing(
        self,
        path: Path,
        position: dict[str, int],
    ) -> list[dict[str, Any]] | None:
        for attempt in range(3):
            try:
                prepared = self.request(
                    "textDocument/prepareCallHierarchy",
                    {
                        "textDocument": {"uri": path.as_uri()},
                        "position": position,
                    },
                )
                break
            except RustAnalyzerRequestError as error:
                if error.code != CONTENT_MODIFIED or attempt == 2:
                    raise
        if not prepared:
            return None
        outgoing: list[dict[str, Any]] = []
        for item in prepared:
            calls = self.request("callHierarchy/outgoingCalls", {"item": item}) or []
            outgoing.extend(calls)
        return outgoing

    def expand_macro(
        self,
        path: Path,
        position: dict[str, int],
    ) -> dict[str, str] | None:
        return self.request(
            "rust-analyzer/expandMacro",
            {
                "textDocument": {"uri": path.as_uri()},
                "position": position,
            },
        )

    def close(self) -> None:
        try:
            self.request("shutdown", None)
            self.notify("exit", None)
            self.process.wait(timeout=10)
        finally:
            self.stderr.close()
        errors = analyzer_error_lines(self.log_path.read_text())
        if errors:
            examples = "\n".join(errors[:5])
            raise RuntimeError(
                f"rust-analyzer logged {len(errors)} internal errors "
                f"for {self.view.name}:\n{examples}"
            )


def path_from_uri(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise ValueError(f"unsupported URI: {uri}")
    return Path(unquote(parsed.path)).resolve()


def range_contains(range_value: dict[str, Any], position: dict[str, int]) -> bool:
    start = (range_value["start"]["line"], range_value["start"]["character"])
    end = (range_value["end"]["line"], range_value["end"]["character"])
    value = (position["line"], position["character"])
    return start <= value <= end


def record_containing_position(
    records: list[dict[str, Any]],
    position: dict[str, int],
) -> dict[str, Any] | None:
    candidates = [
        record
        for record in records
        if "macro_origin" not in record
        and range_contains(record["range"], position)
    ]
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda record: (
            record["range"]["end"]["line"] - record["range"]["start"]["line"],
            record["range"]["end"]["character"]
            - record["range"]["start"]["character"],
        ),
    )


def append_semantic_edge(
    record: dict[str, Any],
    target: str,
    site: dict[str, Any],
) -> None:
    edge = next(
        (
            candidate
            for candidate in record["callees"]
            if candidate["symbol"] == target
        ),
        None,
    )
    if edge is None:
        edge = {"symbol": target, "sites": []}
        record["callees"].append(edge)
    existing_site = next(
        (
            existing
            for existing in edge["sites"]
            if existing["range"] == site["range"]
        ),
        None,
    )
    if existing_site is None:
        edge["sites"].append(site)
        return
    existing_site["views"] = sorted(
        set(existing_site.get("views", [])).union(site.get("views", []))
    )


def attach_nested_callable_edges(
    records: list[dict[str, Any]],
    attach_callers: bool,
) -> None:
    records_by_symbol = {
        record["symbol"]: record
        for record in records
    }
    for nested in records:
        if nested["kind"] not in {"closure", "async-block"}:
            continue
        parent = records_by_symbol.get(nested.get("enclosing_callable"))
        if parent is None or any(
            edge["symbol"] == nested["symbol"]
            for edge in parent["callees"]
        ):
            continue
        site = {
            "range": nested["range"],
            "views": sorted(
                set(parent.get("semantic_views", [])).intersection(
                    nested.get("semantic_views", [])
                )
            ),
            "expression": f"nested {nested['kind']}",
            "arguments": [],
            "callee_text": nested["kind"],
        }
        append_semantic_edge(parent, nested["symbol"], site)
        if attach_callers:
            nested["callers"].append(
                {
                    "symbol": parent["symbol"],
                    "sites": [site],
                }
            )


def call_name(callee: str) -> str | None:
    match = re.fullmatch(
        r"\s*(?:(?:r#)?[A-Za-z_][A-Za-z0-9_]*::)*"
        r"((?:r#)?[A-Za-z_][A-Za-z0-9_]*)\s*",
        callee,
    )
    return match.group(1) if match is not None else None


def is_data_constructor(callee: str) -> bool:
    name = call_name(callee)
    return name is not None and name[:1].isupper()


def named_call_candidates(
    record: dict[str, Any],
    callee: str,
    records: list[dict[str, Any]],
) -> list[str]:
    name = call_name(callee)
    if name is None:
        return []
    segments = [
        segment
        for segment in callee.strip().split("::")
        if segment
    ]
    candidates = [
        candidate
        for candidate in records
        if candidate["name"] == name
    ]
    if len(segments) == 1:
        return sorted(candidate["symbol"] for candidate in candidates)
    first = segments[0]
    if (
        first[0].islower()
        and first
        not in {
            "crate",
            "self",
            "super",
            record["crate"],
        }
    ):
        return []
    qualifier = segments[-2]
    if qualifier not in {"crate", "self", "super", "Self"}:
        candidates = [
            candidate
            for candidate in candidates
            if f"<{qualifier}>" in candidate["symbol"]
            or f"<impl {qualifier}>" in candidate["symbol"]
            or f" {qualifier}<" in candidate["symbol"]
        ]
    return sorted(candidate["symbol"] for candidate in candidates)


def parameter_position(
    record: dict[str, Any],
    name: str,
) -> int | None:
    parameters = [
        parameter
        for parameter in record["parameters"]
        if parameter["name"] != "self"
    ]
    return next(
        (
            index
            for index, parameter in enumerate(parameters)
            if parameter["name"] == name
        ),
        None,
    )


def unborrowed_expression(expression: str) -> str:
    value = expression.strip()
    while True:
        borrowed = re.fullmatch(r"&\s*(?:mut\s+)?(.+)", value, re.DOTALL)
        if borrowed is None:
            break
        value = borrowed.group(1).strip()
    while value.startswith("(") and value.endswith(")"):
        value = value[1:-1].strip()
    return value


def callable_argument_reference(expression: str) -> str | None:
    value = unborrowed_expression(expression)
    if not re.fullmatch(
        r"(?:(?:r#)?[A-Za-z_][A-Za-z0-9_]*::)*"
        r"(?:r#)?[A-Za-z_][A-Za-z0-9_]*",
        value,
    ):
        return None
    return value


def callable_factory_reference(expression: str) -> str | None:
    value = unborrowed_expression(expression)
    match = re.fullmatch(
        r"((?:(?:r#)?[A-Za-z_][A-Za-z0-9_]*::)*"
        r"(?:r#)?[A-Za-z_][A-Za-z0-9_]*)\s*\(.*\)",
        value,
        re.DOTALL,
    )
    return match.group(1) if match is not None else None


def bound_closure_candidates(
    caller: dict[str, Any],
    binding: str,
    records: list[dict[str, Any]],
) -> list[str]:
    semantic_scope = (
        caller["semantic_parent"]
        if caller.get("kind") in {"closure", "async-block"}
        else caller["symbol"]
    )
    return sorted(
        candidate["symbol"]
        for candidate in records
        if candidate.get("path") == caller["path"]
        and candidate.get("kind") == "closure"
        and candidate.get("binding") == binding
        and candidate.get("semantic_parent") == semantic_scope
    )


def returned_callable_candidates(
    factory_symbols: list[str],
    records: list[dict[str, Any]],
) -> list[str]:
    factories = set(factory_symbols)
    return sorted(
        record["symbol"]
        for record in records
        if record["kind"] in {"closure", "async-block"}
        and record.get("enclosing_callable") in factories
    )


def argument_callable_candidates(
    caller: dict[str, Any],
    argument: dict[str, Any],
    records_by_symbol: dict[str, dict[str, Any]],
    records_by_path: dict[Path, list[dict[str, Any]]],
    records: list[dict[str, Any]],
    visited_parameters: set[tuple[str, int]],
) -> list[str]:
    path = (ROOT / caller["path"]).resolve()
    anonymous = [
        candidate
        for candidate in records_by_path.get(path, [])
        if candidate["kind"] in {"closure", "async-block"}
        and range_contains(argument["range"], candidate["range"]["start"])
        and range_contains(argument["range"], candidate["range"]["end"])
    ]
    if anonymous:
        return [
            max(
                anonymous,
                key=lambda candidate: (
                    candidate["range"]["end"]["line"]
                    - candidate["range"]["start"]["line"],
                    candidate["range"]["end"]["character"]
                    - candidate["range"]["start"]["character"],
                ),
            )["symbol"]
        ]
    reference = callable_argument_reference(argument["text"])
    if reference is not None:
        name = reference.rsplit("::", 1)[-1]
        caller_parameter = (
            parameter_position(caller, name)
            if "::" not in reference
            else None
        )
        if caller_parameter is not None:
            return parameter_call_candidates(
                caller,
                caller_parameter,
                records_by_symbol,
                records_by_path,
                records,
                visited_parameters,
            )
        bound = bound_closure_candidates(caller, name, records)
        if bound:
            return bound
        candidates = named_call_candidates(caller, reference, records)
        if candidates:
            return candidates
        if "::" in reference:
            return [f"external-callable::{reference}"]
    factory = callable_factory_reference(argument["text"])
    if factory is not None:
        factories = named_call_candidates(caller, factory, records)
        returned = returned_callable_candidates(factories, records)
        if returned:
            return returned
        if factories:
            return [
                f"returned-callable::{factory_symbol}"
                for factory_symbol in factories
            ]
        if "::" in factory:
            return [f"external-callable-factory::{factory}"]
    return []


def parameter_site_argument(
    record: dict[str, Any],
    site: dict[str, Any],
    position: int,
) -> dict[str, Any] | None:
    arguments = site.get("arguments", [])
    parameters = record["parameters"]
    has_self = bool(parameters) and parameters[0]["name"] == "self"
    explicit_self = has_self and len(arguments) == len(parameters)
    argument_position = position + int(explicit_self)
    return (
        arguments[argument_position]
        if argument_position < len(arguments)
        else None
    )


def parameter_call_candidates(
    record: dict[str, Any],
    position: int,
    records_by_symbol: dict[str, dict[str, Any]],
    records_by_path: dict[Path, list[dict[str, Any]]],
    records: list[dict[str, Any]],
    visited_parameters: set[tuple[str, int]] | None = None,
) -> list[str]:
    visited_parameters = (
        set()
        if visited_parameters is None
        else set(visited_parameters)
    )
    parameter = (record["symbol"], position)
    if parameter in visited_parameters:
        return []
    visited_parameters.add(parameter)
    candidates: set[str] = set()
    for caller_edge in record["callers"]:
        caller = records_by_symbol[caller_edge["symbol"]]
        for site in caller_edge["sites"]:
            argument = parameter_site_argument(record, site, position)
            if argument is None:
                continue
            candidates.update(
                argument_callable_candidates(
                    caller,
                    argument,
                    records_by_symbol,
                    records_by_path,
                    records,
                    visited_parameters,
                )
            )
    if record["visibility"] != "private":
        candidates.add("external-caller-supplied")
    if not record["callers"] and (
        record.get("kind") in {"trait-method", "trait-associated"}
        or " for " in (record.get("receiver_type") or "")
    ):
        candidates.add("trait-dispatch-supplied")
    return sorted(candidates)


def callable_parameter_positions(record: dict[str, Any]) -> list[int]:
    parameters = [
        parameter
        for parameter in record["parameters"]
        if parameter["name"] != "self"
    ]
    return [
        position
        for position, parameter in enumerate(parameters)
        if re.search(r"\b(?:FnOnce|FnMut|Fn)\s*\(", parameter["type"])
        or re.search(r"\bfn\s*\(", parameter["type"])
    ]


def append_caller_edge(
    record: dict[str, Any],
    caller: str,
    site: dict[str, Any],
) -> None:
    edge = next(
        (
            candidate
            for candidate in record["callers"]
            if candidate["symbol"] == caller
        ),
        None,
    )
    if edge is None:
        edge = {"symbol": caller, "sites": []}
        record["callers"].append(edge)
    existing_site = next(
        (
            existing
            for existing in edge["sites"]
            if existing["range"] == site["range"]
        ),
        None,
    )
    if existing_site is None:
        edge["sites"].append(site)
        return
    existing_site["views"] = sorted(
        set(existing_site.get("views", [])).union(site.get("views", []))
    )


def attach_callable_argument_edges(records: list[dict[str, Any]]) -> None:
    records_by_symbol = {
        record["symbol"]: record
        for record in records
    }
    records_by_path: dict[Path, list[dict[str, Any]]] = {}
    for record in records:
        path = (ROOT / record["path"]).resolve()
        records_by_path.setdefault(path, []).append(record)

    for caller in records:
        for edge in list(caller["callees"]):
            callee = records_by_symbol.get(edge["symbol"])
            if callee is None:
                continue
            for position in callable_parameter_positions(callee):
                for call_site in edge["sites"]:
                    argument = parameter_site_argument(
                        callee,
                        call_site,
                        position,
                    )
                    if argument is None:
                        continue
                    for target_symbol in argument_callable_candidates(
                        caller,
                        argument,
                        records_by_symbol,
                        records_by_path,
                        records,
                        set(),
                    ):
                        target = records_by_symbol.get(target_symbol)
                        if target is None:
                            continue
                        site = {
                            "range": argument["range"],
                            "views": call_site.get("views", []),
                            "expression": argument["text"],
                            "arguments": [],
                            "callee_text": argument["text"],
                            "role": "callable-argument",
                        }
                        append_semantic_edge(caller, target_symbol, site)
                        append_caller_edge(target, caller["symbol"], site)


def lexical_call_candidates(
    record: dict[str, Any],
    call: dict[str, Any],
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    immediate = [
        candidate
        for candidate in records
        if candidate["path"] == record["path"]
        and candidate["kind"] in {"closure", "async-block"}
        and range_contains(call["callee_range"], candidate["range"]["start"])
        and range_contains(call["callee_range"], candidate["range"]["end"])
    ]
    if immediate:
        return [
            max(
                immediate,
                key=lambda candidate: (
                    candidate["range"]["end"]["line"]
                    - candidate["range"]["start"]["line"],
                    candidate["range"]["end"]["character"]
                    - candidate["range"]["start"]["character"],
                ),
            )
        ]
    name = call_name(call["callee_text"])
    if name is None:
        return []
    semantic_scope = (
        record["semantic_parent"]
        if record["kind"] in {"closure", "async-block"}
        else record["symbol"]
    )
    return [
        candidate
        for candidate in records
        if candidate["path"] == record["path"]
        and candidate["kind"] == "closure"
        and candidate["binding"] == name
        and candidate["semantic_parent"] == semantic_scope
    ]


def call_components(
    records: list[dict[str, Any]],
    *,
    collapse_nested: bool = False,
) -> list[dict[str, Any]]:
    symbols = {record["symbol"] for record in records}
    records_by_symbol = {
        record["symbol"]: record
        for record in records
    }
    anchor_by_symbol: dict[str, str] = {}
    for record in records:
        current = record
        visited: set[str] = set()
        while (
            collapse_nested
            and current.get("kind") in {"closure", "async-block"}
            and current["symbol"] not in visited
        ):
            visited.add(current["symbol"])
            parent = records_by_symbol.get(current.get("enclosing_callable"))
            if parent is None:
                break
            current = parent
        anchor_by_symbol[record["symbol"]] = current["symbol"]

    anchors = set(anchor_by_symbol.values())
    graph = {
        anchor: set()
        for anchor in anchors
    }
    directly_recursive: set[str] = set()
    for record in records:
        source = anchor_by_symbol[record["symbol"]]
        for edge in record["callees"]:
            if edge["symbol"] not in symbols:
                continue
            target = anchor_by_symbol[edge["symbol"]]
            if source == target:
                if record["symbol"] == edge["symbol"]:
                    directly_recursive.add(source)
                continue
            graph[source].add(target)
    graph = {
        symbol: sorted(callees)
        for symbol, callees in graph.items()
    }
    reversed_graph = {
        symbol: []
        for symbol in graph
    }
    for caller, callees in graph.items():
        for callee in callees:
            reversed_graph[callee].append(caller)

    visited: set[str] = set()
    finish_order: list[str] = []
    for root in sorted(graph):
        if root in visited:
            continue
        pending = [(root, False)]
        while pending:
            symbol, expanded = pending.pop()
            if expanded:
                finish_order.append(symbol)
                continue
            if symbol in visited:
                continue
            visited.add(symbol)
            pending.append((symbol, True))
            pending.extend(
                (callee, False)
                for callee in reversed(graph[symbol])
                if callee not in visited
            )

    assigned: set[str] = set()
    anchor_groups: list[list[str]] = []
    for root in reversed(finish_order):
        if root in assigned:
            continue
        members: list[str] = []
        pending = [root]
        assigned.add(root)
        while pending:
            symbol = pending.pop()
            members.append(symbol)
            for caller in reversed_graph[symbol]:
                if caller not in assigned:
                    assigned.add(caller)
                    pending.append(caller)
        anchor_groups.append(sorted(members))

    lexical_members: dict[str, list[str]] = {
        anchor: []
        for anchor in anchors
    }
    for symbol, anchor in anchor_by_symbol.items():
        lexical_members[anchor].append(symbol)
    member_groups = [
        sorted(
            symbol
            for anchor in anchor_group
            for symbol in lexical_members[anchor]
        )
        for anchor_group in anchor_groups
    ]

    symbol_to_component: dict[str, str] = {}
    anchor_to_component: dict[str, str] = {}
    for anchor_group, members in zip(anchor_groups, member_groups):
        component = members[0]
        for member in members:
            symbol_to_component[member] = component
        for anchor in anchor_group:
            anchor_to_component[anchor] = component

    outgoing: dict[str, set[str]] = {
        members[0]: set()
        for members in member_groups
    }
    incoming: dict[str, set[str]] = {
        members[0]: set()
        for members in member_groups
    }
    for caller, callees in graph.items():
        caller_component = anchor_to_component[caller]
        for callee in callees:
            callee_component = anchor_to_component[callee]
            if caller_component == callee_component:
                continue
            outgoing[caller_component].add(callee_component)
            incoming[callee_component].add(caller_component)

    ranks = {
        component: 0
        for component in outgoing
    }
    remaining_callees = {
        component: len(callees)
        for component, callees in outgoing.items()
    }
    pending_components = sorted(
        component
        for component, count in remaining_callees.items()
        if count == 0
    )
    processed = 0
    while pending_components:
        component = pending_components.pop()
        processed += 1
        for caller in incoming[component]:
            ranks[caller] = max(ranks[caller], ranks[component] + 1)
            remaining_callees[caller] -= 1
            if remaining_callees[caller] == 0:
                pending_components.append(caller)
    if processed != len(outgoing):
        raise RuntimeError("collapsed call graph still contains a cycle")

    components = []
    for anchor_group, members in zip(anchor_groups, member_groups):
        component = members[0]
        recursive = (
            len(anchor_group) > 1
            or any(anchor in directly_recursive for anchor in anchor_group)
        )
        for member in members:
            records_by_symbol[member]["component"] = component
            records_by_symbol[member]["bottom_up_rank"] = ranks[component]
        components.append(
            {
                "id": component,
                "members": members,
                "recursive": recursive,
                "callees": sorted(outgoing[component]),
                "callers": sorted(incoming[component]),
                "bottom_up_rank": ranks[component],
            }
        )
    return sorted(
        components,
        key=lambda component: (
            component["bottom_up_rank"],
            component["id"],
        ),
    )


def internal_symbol(
    records_by_path: dict[Path, list[dict[str, Any]]],
    item: dict[str, Any],
) -> str | None:
    try:
        path = path_from_uri(item["uri"])
    except ValueError:
        return None
    position = item["selectionRange"]["start"]
    candidates = [
        record
        for record in records_by_path.get(path, [])
        if "macro_origin" not in record
        and record["kind"] not in {"closure", "async-block"}
        and range_contains(record["name_range"], position)
    ]
    if not candidates:
        candidates = [
            record
            for record in records_by_path.get(path, [])
            if "macro_origin" not in record
            and range_contains(record["range"], position)
        ]
    return candidates[0]["symbol"] if candidates else None


def source_call_site(
    source_files: dict[Path, SourceFile],
    path: Path,
    range_value: dict[str, Any],
) -> dict[str, Any]:
    source_file = source_files.get(path)
    if source_file is None:
        return {"expression": "", "arguments": []}
    start = source_file.offset(range_value["start"])
    line = range_value["start"]["line"]
    containing_calls = [
        (index, node)
        for index, node in source_file.call_nodes_by_line.get(line, [])
        if node.start <= start <= node.end
    ]
    if not containing_calls:
        begin = source_file.line_starts[line]
        end = (
            source_file.line_starts[line + 1]
            if line + 1 < len(source_file.line_starts)
            else len(source_file.data)
        )
        return {
            "expression": source_file.slice(begin, end).strip(),
            "arguments": [],
            "callee_text": "",
        }
    index, node = min(
        containing_calls,
        key=lambda value: value[1].end - value[1].start,
    )
    _, callee_text = call_callee(source_file, index)
    return {
        "expression": call_text(source_file, node),
        "arguments": call_arguments(source_file, index),
        "callee_text": callee_text,
    }


def receiver_matches_qualifier(
    receiver_type: str | None,
    qualifier: str,
) -> bool:
    if receiver_type is None:
        return False
    concrete = receiver_type.rsplit(" for ", 1)[-1]
    concrete = concrete.rsplit("::", 1)[-1]
    concrete = concrete.split("<", 1)[0].strip()
    return concrete == qualifier


def macro_generated_target(
    records: list[dict[str, Any]],
    target_item: dict[str, Any],
    site: dict[str, Any],
) -> str | None:
    name = target_item.get("name")
    if not name:
        return None
    candidates = [
        record
        for record in records
        if "macro_origin" in record
        and record["name"] == name
    ]
    callee = site.get("callee_text", "")
    segments = [
        segment
        for segment in callee.strip().split("::")
        if segment
    ]
    if len(segments) > 1:
        qualifier = segments[-2]
        candidates = [
            record
            for record in candidates
            if receiver_matches_qualifier(
                record.get("receiver_type"),
                qualifier,
            )
        ]
    detail = re.sub(
        r"\s+",
        " ",
        target_item.get("detail", ""),
    ).strip()
    if detail and len(candidates) > 1:
        matching_detail = [
            record
            for record in candidates
            if re.sub(r"\s+", " ", record["signature"]).strip() == detail
        ]
        if matching_detail:
            candidates = matching_detail
    return candidates[0]["symbol"] if len(candidates) == 1 else None


def trait_implementation_candidates(
    target: dict[str, Any],
    records: list[dict[str, Any]],
    callee: str,
) -> list[str]:
    receiver = target.get("receiver_type") or ""
    if not receiver.startswith("trait "):
        return []
    trait_name = receiver.removeprefix("trait ").strip()
    candidates = [
        record
        for record in records
        if record["name"] == target["name"]
        and (record.get("receiver_type") or "").startswith(
            f"{trait_name} for "
        )
    ]
    segments = [
        segment
        for segment in callee.strip().split("::")
        if segment
    ]
    if len(segments) > 1 and segments[-2] not in {"Self", "T"}:
        qualifier = segments[-2]
        matching_receiver = [
            record
            for record in candidates
            if receiver_matches_qualifier(
                record.get("receiver_type"),
                qualifier,
            )
        ]
        if matching_receiver:
            candidates = matching_receiver
    return sorted(record["symbol"] for record in candidates)


def macro_syntactic_target(
    record: dict[str, Any],
    call: dict[str, Any],
    records: list[dict[str, Any]],
) -> str | None:
    if "macro_origin" not in record or "::" in call["callee_text"]:
        return None
    candidates = [
        candidate
        for candidate in records
        if candidate["module"] == record["module"]
        and candidate["kind"] == "free"
        and candidate["name"] == call["callee_text"]
    ]
    return candidates[0]["symbol"] if len(candidates) == 1 else None


def external_path_candidate(callee: str) -> str | None:
    if "::" not in callee:
        return None
    if not re.fullmatch(
        r"(?:(?:r#)?[A-Za-z_][A-Za-z0-9_]*::)+"
        r"(?:r#)?[A-Za-z_][A-Za-z0-9_]*",
        callee,
    ):
        return None
    return f"external-callable::{callee}"


def source_use_paths(
    source_files: dict[Path, SourceFile],
) -> list[dict[str, Any]]:
    uses = []
    for path, source_file in source_files.items():
        crate, file_modules = crate_and_modules(path)
        for index, node in enumerate(source_file.nodes):
            if node.kind != "USE":
                continue
            visibility_node = direct_child(
                source_file,
                index,
                "VISIBILITY",
            )
            text = re.sub(
                r"\s+",
                " ",
                source_file.text(node),
            ).strip()
            uses.append(
                {
                    "module": "::".join(
                        [
                            crate,
                            *file_modules,
                            *enclosing_modules(source_file, index),
                        ]
                    ),
                    "path": str(path.relative_to(ROOT)),
                    "range": {
                        "start": source_file.position(node.start),
                        "end": source_file.position(node.end),
                    },
                    "text": text,
                    "visibility": (
                        source_file.text(visibility_node).strip()
                        if visibility_node is not None
                        else "private"
                    ),
                }
            )
    return uses


def crate_path_relationship(module: str, path: str) -> str | None:
    match = re.search(
        r"\bcrate::((?:[A-Za-z_][A-Za-z0-9_]*::)*"
        r"[A-Za-z_][A-Za-z0-9_]*)",
        path,
    )
    if match is None:
        return None
    current = module.split("::")[1:]
    target = match.group(1).split("::")[:-1]
    if not target:
        return "ancestor"
    if current[: len(target)] == target:
        return "ancestor"
    common = 0
    for left, right in zip(current, target):
        if left != right:
            break
        common += 1
    return "sibling" if common else "cross-root"


def retained_parameter_categories(parameter_type: str) -> list[str]:
    return [
        category
        for category, pattern in STATEFUL_TYPE_PATTERNS.items()
        if pattern.search(parameter_type)
    ]


def field_arguments(call: dict[str, Any]) -> dict[str, set[str]]:
    fields: dict[str, set[str]] = {}
    for argument in call.get("arguments", []):
        match = re.match(
            r"\s*&?\s*(?:mut\s+)?"
            r"([A-Za-z_][A-Za-z0-9_]*)\."
            r"([A-Za-z_][A-Za-z0-9_]*)",
            argument["text"],
        )
        if match is not None:
            fields.setdefault(match.group(1), set()).add(match.group(2))
    return fields


def build_reach_through_reports(
    records: list[dict[str, Any]],
    source_files: dict[Path, SourceFile],
) -> dict[str, list[dict[str, Any]]]:
    reports: dict[str, list[dict[str, Any]]] = {
        "deep_ancestor_paths": [],
        "absolute_ancestor_paths": [],
        "sibling_imports": [],
        "implementation_reexports": [],
        "associated_function_calls": [],
        "constructor_calls": [],
        "receiver_dependency_parameters": [],
        "receiverless_stateful_callables": [],
        "field_bundle_calls": [],
    }
    records_by_symbol = {
        record["symbol"]: record
        for record in records
    }
    for record in records:
        for path in record.get("paths", []):
            entry = {
                "symbol": record["symbol"],
                "module": record.get("module", ""),
                **path,
            }
            if re.search(r"\bsuper::super::", path["text"]):
                reports["deep_ancestor_paths"].append(entry)
            relationship = crate_path_relationship(
                record.get("module", ""),
                path["text"],
            )
            if relationship == "ancestor":
                reports["absolute_ancestor_paths"].append(entry)
        if record.get("kind") in {"method", "trait-method"}:
            parameters = [
                {
                    **parameter,
                    "categories": retained_parameter_categories(
                        parameter["type"]
                    ),
                }
                for parameter in record.get("parameters", [])
                if parameter["name"] != "self"
                and retained_parameter_categories(parameter["type"])
            ]
            if parameters:
                reports["receiver_dependency_parameters"].append(
                    {
                        "symbol": record["symbol"],
                        "parameters": parameters,
                    }
                )
        if (
            record.get("kind") in {"associated", "trait-associated"}
            and (
                record.get("retained_dependencies")
                or record.get("ambient_dependencies")
                or record.get("effects")
            )
        ):
            reports["receiverless_stateful_callables"].append(
                {
                    "symbol": record["symbol"],
                    "retained_dependencies": record.get(
                        "retained_dependencies", []
                    ),
                    "ambient_dependencies": record.get(
                        "ambient_dependencies", []
                    ),
                    "effects": record.get("effects", []),
                }
            )
        for call in record.get("calls", []):
            for root, fields in field_arguments(call).items():
                if len(fields) < 2:
                    continue
                reports["field_bundle_calls"].append(
                    {
                        "symbol": record["symbol"],
                        "expression": call["text"],
                        "root": root,
                        "fields": sorted(fields),
                    }
                )
        for edge in record.get("callees", []):
            target = records_by_symbol.get(edge["symbol"])
            if target is None or target.get("kind") not in {
                "associated",
                "trait-associated",
            }:
                continue
            for site in edge["sites"]:
                entry = {
                    "caller": record["symbol"],
                    "target": target["symbol"],
                    "expression": site.get("expression", ""),
                    "range": site["range"],
                }
                reports["associated_function_calls"].append(entry)
                if target.get("name") in {
                    "build",
                    "create",
                    "from_parts",
                    "initialize",
                    "load",
                    "new",
                    "open",
                }:
                    reports["constructor_calls"].append(entry)

    for use in source_use_paths(source_files):
        if re.search(r"\bsuper::super::", use["text"]):
            reports["deep_ancestor_paths"].append(use)
        relationship = crate_path_relationship(
            use["module"],
            use["text"],
        )
        if relationship == "ancestor":
            reports["absolute_ancestor_paths"].append(use)
        elif relationship == "sibling":
            reports["sibling_imports"].append(use)
        if use["visibility"] != "private":
            reports["implementation_reexports"].append(use)
    return {
        name: sorted(
            entries,
            key=lambda entry: json.dumps(entry, sort_keys=True),
        )
        for name, entries in reports.items()
    }


def normalize_semantic_calls(
    record: dict[str, Any],
    calls: list[dict[str, Any]],
    records: list[dict[str, Any]],
    records_by_path: dict[Path, list[dict[str, Any]]],
    source_files: dict[Path, SourceFile],
) -> list[dict[str, Any]]:
    path = (ROOT / record["path"]).resolve()
    callable_start = source_files[path].offset(record["range"]["start"])
    normalized = []
    for call in calls:
        target_item = call["to"]
        internal_target = internal_symbol(records_by_path, target_item)
        for range_value in call.get("fromRanges", []):
            site = {
                "range": range_value,
                **source_call_site(source_files, path, range_value),
            }
            target = internal_target or macro_generated_target(
                records,
                target_item,
                site,
            )
            if target is None:
                target = (
                    f"external::{target_item.get('detail', '')}::"
                    f"{target_item.get('name', '<unknown>')}"
                )
            normalized.append(
                {
                    "target": target,
                    "relativeRanges": [
                        {
                            "start": source_files[path].offset(
                                range_value["start"]
                            )
                            - callable_start,
                            "end": source_files[path].offset(
                                range_value["end"]
                            )
                            - callable_start,
                        }
                    ],
                }
            )
    return normalized


def collect_semantic_view_calls(
    view: SemanticView,
    named: list[dict[str, Any]],
    records: list[dict[str, Any]],
    records_by_path: dict[Path, list[dict[str, Any]]],
    source_files: dict[Path, SourceFile],
    semantic_workspace_fingerprint: str,
) -> dict[str, Any]:
    fingerprint = view_cache_fingerprint(semantic_workspace_fingerprint, view)
    candidate_surfaces = semantic_callable_candidate_surfaces(records)
    entry_fingerprints = {
        record["symbol"]: semantic_entry_cache_fingerprint(
            fingerprint,
            record,
            source_files[(ROOT / record["path"]).resolve()],
            candidate_surfaces,
        )
        for record in named
    }
    entries = read_semantic_view_cache(
        view,
        fingerprint,
        entry_fingerprints,
    )
    missing = [record for record in named if record["symbol"] not in entries]
    if not missing:
        print(
            f"reused {view.name} call hierarchy for {len(named)} callables",
            flush=True,
        )
        return entries

    print(
        f"indexing {view.name} call hierarchy for {len(missing)} uncached "
        f"of {len(named)} callables",
        flush=True,
    )
    analyzer = RustAnalyzer(ROOT, view)
    analyzer.initialize()
    queried = 0
    pending: dict[str, Any] = {}
    try:
        analyzer.wait_until_ready()
        opened_paths: set[Path] = set()
        for record in missing:
            path = (ROOT / record["path"]).resolve()
            if path not in opened_paths:
                analyzer.open_document(path, source_files[path].source)
                opened_paths.add(path)
            calls = analyzer.outgoing(path, record["name_range"]["start"])
            entry = {
                "fingerprint": entry_fingerprints[record["symbol"]],
                "calls": (
                    normalize_semantic_calls(
                        record,
                        calls,
                        records,
                        records_by_path,
                        source_files,
                    )
                    if calls is not None
                    else None
                ),
            }
            entries[record["symbol"]] = entry
            pending[record["symbol"]] = entry
            queried += 1
            if queried % CACHE_CHECKPOINT_INTERVAL == 0:
                write_semantic_view_cache(view, fingerprint, pending)
                pending.clear()
            if queried % 250 == 0:
                print(
                    f"indexed {view.name} call hierarchy for "
                    f"{queried}/{len(missing)} uncached callables",
                    flush=True,
                )
    finally:
        try:
            write_semantic_view_cache(view, fingerprint, pending)
        finally:
            analyzer.close()
    return entries


def attach_semantic_view_calls(
    view: SemanticView,
    named: list[dict[str, Any]],
    entries: dict[str, Any],
    records: list[dict[str, Any]],
    records_by_path: dict[Path, list[dict[str, Any]]],
    source_files: dict[Path, SourceFile],
    matched_sites: dict[str, list[dict[str, Any]]],
) -> None:
    for record in named:
        path = (ROOT / record["path"]).resolve()
        entry = entries.get(record["symbol"])
        if not isinstance(entry, dict) or "calls" not in entry:
            raise RuntimeError(
                f"semantic view {view.name} omitted {record['symbol']}"
            )
        calls = entry["calls"]
        status = "unavailable" if calls is None else "resolved"
        record.setdefault("call_hierarchy_views", {})[view.name] = status
        if calls is None:
            continue
        if not isinstance(calls, list):
            raise RuntimeError(
                f"invalid calls for {record['symbol']} in {view.name} cache"
            )
        for call in calls:
            resolved_target = call["target"]
            callable_start = source_files[path].offset(record["range"]["start"])
            for relative_range in call.get("relativeRanges", []):
                range_value = {
                    "start": source_files[path].position(
                        callable_start + relative_range["start"]
                    ),
                    "end": source_files[path].position(
                        callable_start + relative_range["end"]
                    ),
                }
                source_record = record_containing_position(
                    records_by_path[path],
                    range_value["start"],
                )
                if source_record is None:
                    raise RuntimeError(
                        f"semantic call site lies outside a callable: "
                        f"{record['path']}:{range_value['start']}"
                    )
                site = {
                    "range": range_value,
                    "views": [view.name],
                    **source_call_site(source_files, path, range_value),
                }
                matched_sites[source_record["symbol"]].append(
                    {
                        "position": range_value["start"],
                        "view": view.name,
                        "target": resolved_target,
                    }
                )
                append_semantic_edge(source_record, resolved_target, site)


def build_index(
    metadata: dict[str, Any] | None = None,
    workspace_fingerprint: str | None = None,
    index_fingerprint: str | None = None,
    analyzer_tool_identity: str | None = None,
) -> dict[str, Any]:
    metadata = cargo_metadata() if metadata is None else metadata
    analyzer_tool_identity = (
        analyzer_identity()
        if analyzer_tool_identity is None
        else analyzer_tool_identity
    )
    workspace_fingerprint = (
        workspace_cache_fingerprint(metadata, analyzer_tool_identity)
        if workspace_fingerprint is None
        else workspace_fingerprint
    )
    index_fingerprint = (
        index_cache_fingerprint(workspace_fingerprint)
        if index_fingerprint is None
        else index_fingerprint
    )
    discovery = discover_workspace_sources(metadata)
    source_files: dict[Path, SourceFile] = {}
    records: list[dict[str, Any]] = []
    for path in discovery.reachable:
        source_file = parse_source(path)
        source_files[path] = source_file
        file_records = inventory_file(source_file)
        attach_source_context(file_records, path, discovery)
        records.extend(file_records)
    macro_records, macro_expansions = expand_workspace_item_macros(
        source_files,
        discovery,
    )
    records.extend(macro_records)

    records_by_path: dict[Path, list[dict[str, Any]]] = {}
    records_by_symbol: dict[str, dict[str, Any]] = {}
    for record in records:
        path = (ROOT / record["path"]).resolve()
        records_by_path.setdefault(path, []).append(record)
        if record["symbol"] in records_by_symbol:
            raise RuntimeError(f"duplicate callable identity: {record['symbol']}")
        records_by_symbol[record["symbol"]] = record

    semantic_fingerprint = semantic_workspace_cache_fingerprint(
        metadata,
        source_files,
        analyzer_tool_identity,
    )

    matched_sites: dict[str, list[dict[str, Any]]] = {
        record["symbol"]: []
        for record in records
    }
    named = [
        record
        for record in records
        if record["kind"] not in {"closure", "async-block"}
        and "macro_origin" not in record
    ]
    with ThreadPoolExecutor(max_workers=len(SEMANTIC_VIEWS)) as executor:
        futures = {
            view: executor.submit(
                collect_semantic_view_calls,
                view,
                named,
                records,
                records_by_path,
                source_files,
                semantic_fingerprint,
            )
            for view in SEMANTIC_VIEWS
        }
        view_entries = {
            view: futures[view].result()
            for view in SEMANTIC_VIEWS
        }
    for view in SEMANTIC_VIEWS:
        attach_semantic_view_calls(
            view,
            named,
            view_entries[view],
            records,
            records_by_path,
            source_files,
            matched_sites,
        )

    for record in records:
        if "macro_origin" in record:
            record["call_hierarchy_views"] = {
                MACRO_EXPANSION_VIEW.name: "macro-expanded"
            }
            record["semantic_views"] = [MACRO_EXPANSION_VIEW.name]
            record["call_hierarchy"] = "macro-expanded"
            continue
        record["semantic_views"] = sorted(
            view
            for view, status in record.get(
                "call_hierarchy_views", {}
            ).items()
            if status == "resolved"
        )
        record["call_hierarchy"] = (
            "resolved"
            if record["semantic_views"]
            else "unavailable"
        )

    for record in records:
        if (
            record["kind"] in {"closure", "async-block"}
            and "macro_origin" not in record
        ):
            parent = records_by_symbol.get(record["semantic_parent"])
            record["call_hierarchy_views"] = (
                {
                    view: (
                        "enclosing-resolved"
                        if status == "resolved"
                        else "unavailable"
                    )
                    for view, status in parent.get(
                        "call_hierarchy_views", {}
                    ).items()
                }
                if parent is not None
                else {}
            )
            record["semantic_views"] = sorted(
                view
                for view, status in record["call_hierarchy_views"].items()
                if status == "enclosing-resolved"
            )
            record["call_hierarchy"] = (
                "enclosing-resolved"
                if record["semantic_views"]
                else "unavailable"
            )
        for call in record["calls"]:
            matched = [
                site
                for site in matched_sites[record["symbol"]]
                if range_contains(call["range"], site["position"])
            ]
            resolved = bool(matched)
            call["semantic_views"] = sorted(
                {site["view"] for site in matched}
            )
            call["unresolved_in_views"] = sorted(
                set(record["semantic_views"]).difference(
                    call["semantic_views"]
                )
            )
            if resolved:
                resolved_targets = {
                    site["target"]
                    for site in matched
                }
                dynamic_candidates = {
                    candidate
                    for target in resolved_targets
                    if (target_record := records_by_symbol.get(target))
                    for candidate in trait_implementation_candidates(
                        target_record,
                        records,
                        call["callee_text"],
                    )
                }
                if dynamic_candidates:
                    call["dynamic_dispatch_candidates"] = sorted(
                        dynamic_candidates
                    )
                continue
            if call["kind"] == "MACRO_CALL":
                continue
            if is_data_constructor(call["callee_text"]):
                continue
            candidates = lexical_call_candidates(record, call, records)
            if len(candidates) == 1:
                candidate = candidates[0]
                append_semantic_edge(
                    record,
                    candidate["symbol"],
                    {
                        "range": call["range"],
                        "views": record["semantic_views"],
                        "expression": call["text"],
                    },
                )
                continue
            named_candidates = named_call_candidates(
                record, call["callee_text"], records
            )
            syntactic_target = macro_syntactic_target(
                record,
                call,
                records,
            )
            if syntactic_target is not None:
                append_semantic_edge(
                    record,
                    syntactic_target,
                    {
                        "range": call["range"],
                        "views": record["semantic_views"],
                        "expression": call["text"],
                        "arguments": call["arguments"],
                        "callee_text": call["callee_text"],
                    },
                )
                continue
            call["dynamic_dispatch_candidates"] = sorted(
                {
                    candidate["symbol"]
                    for candidate in candidates
                }
                .union(named_candidates)
                .union(
                    [external_candidate]
                    if (
                        external_candidate := external_path_candidate(
                            call["callee_text"]
                        )
                    )
                    else []
                )
            )
            call["resolution"] = (
                "inactive-configuration"
                if record.get("call_hierarchy") == "unavailable"
                else "dynamic-or-unresolved"
            )
            record["unresolved_calls"].append(call)

    attach_nested_callable_edges(records, attach_callers=False)

    for record in records:
        for edge in record["callees"]:
            target = records_by_symbol.get(edge["symbol"])
            if target is not None:
                target["callers"].append(
                    {
                        "symbol": record["symbol"],
                        "sites": edge["sites"],
                    }
                )

    attach_callable_argument_edges(records)

    for record in records:
        for call in record["unresolved_calls"]:
            name = call_name(call["callee_text"])
            position = (
                parameter_position(record, name)
                if name is not None
                else None
            )
            if position is None:
                continue
            call["resolution"] = "callable-parameter"
            call["dynamic_dispatch_candidates"] = parameter_call_candidates(
                record,
                position,
                records_by_symbol,
                records_by_path,
                records,
            )

    components = call_components(records)
    reach_throughs = build_reach_through_reports(
        records,
        source_files,
    )
    return {
        "schema": 1,
        "cache": {
            "schema": CACHE_SCHEMA,
            "fingerprint": index_fingerprint,
        },
        "root": str(ROOT),
        "semantic_views": [
            {
                "name": view.name,
                "configuration": view.configuration(),
            }
            for view in (*SEMANTIC_VIEWS, MACRO_EXPANSION_VIEW)
        ],
        "macro_expansions": macro_expansions,
        "reach_throughs": reach_throughs,
        "sources": {
            "reachable": [
                str(path.relative_to(ROOT))
                for path in discovery.reachable
            ],
            "unreachable": [
                str(path.relative_to(ROOT))
                for path in discovery.unreachable
            ],
            "targets": discovery.targets,
        },
        "components": components,
        "callables": sorted(records, key=lambda value: value["symbol"]),
    }


def write_index(index: dict[str, Any]) -> None:
    write_json_atomic(INDEX_PATH, index)


def read_index() -> dict[str, Any]:
    if not INDEX_PATH.exists():
        raise RuntimeError("ownership index is absent; run `ownership_audit.py inventory`")
    return json.loads(INDEX_PATH.read_text())


def record_realm(record: dict[str, Any]) -> str:
    if "macro_origin" in record:
        return "macro"
    path = record["path"]
    module_parts = record["module"].split("::")
    test_module = any(
        part == "tests"
        or part.endswith("_tests")
        or part
        in {
            "test_helpers",
            "test_support",
            "test_utils",
        }
        for part in module_parts
    )
    test_path = (
        "/tests/" in path
        or path.endswith("_tests.rs")
        or "/test_" in path
    )
    test_configuration = bool(record.get("cfg_paths")) and all(
        any(
            "test" in condition
            or 'feature = "test-utils"' in condition
            for condition in conditions
        )
        for conditions in record["cfg_paths"]
    )
    return (
        "test"
        if (
            record.get("test_context", False)
            or test_module
            or test_path
            or test_configuration
        )
        else "production"
    )


def without_generic_arguments(value: str) -> str:
    output = []
    depth = 0
    for character in value:
        if character == "<":
            depth += 1
            continue
        if character == ">":
            depth = max(0, depth - 1)
            continue
        if depth == 0:
            output.append(character)
    return re.sub(r"\s+", " ", "".join(output)).strip()


def concrete_receiver_owner(receiver: str | None) -> str | None:
    if receiver is None or receiver.startswith("trait "):
        return None
    concrete = receiver.rsplit(" for ", 1)[-1]
    return without_generic_arguments(concrete)


def has_receiver(record: dict[str, Any]) -> bool:
    return any(
        parameter.get("name") == "self"
        for parameter in record.get("parameters", [])
    )


def receiver_interface(receiver: str | None) -> str | None:
    if receiver is None:
        return None
    if receiver.startswith("trait "):
        return receiver.removeprefix("trait ").strip()
    if " for " in receiver:
        return without_generic_arguments(receiver.rsplit(" for ", 1)[0])
    return None


def stateful_record(record: dict[str, Any]) -> bool:
    return bool(
        record.get("retained_dependencies")
        or record.get("ambient_dependencies")
        or record.get("effects")
    )


def record_is_receiver_constructor(record: dict[str, Any]) -> bool:
    return record.get(
        "receiver_constructor",
        is_receiver_constructor(
            record.get("receiver_type"),
            record.get("parameters", []),
            record.get("return_type", ""),
        ),
    )


def record_return_dependencies(record: dict[str, Any]) -> list[str]:
    return record.get(
        "return_dependencies",
        retained_dependencies(record.get("return_type", "")),
    )


def record_parameter_dependencies(record: dict[str, Any]) -> list[str]:
    return record.get(
        "parameter_dependencies",
        sorted(
            {
                dependency
                for parameter in record.get("parameters", [])
                for dependency in retained_dependencies(
                    parameter.get("type", "")
                )
            }
        ),
    )


def construction_boundary_roots(
    records: list[dict[str, Any]],
) -> set[str]:
    produced_types = {
        record["symbol"]: concrete_receiver_owner(record.get("receiver_type"))
        for record in records
        if record_is_receiver_constructor(record)
    }
    boundaries = set(produced_types)
    boundaries.update(
        record["symbol"]
        for record in records
        if (
            not has_receiver(record)
            and record.get("kind") not in {"closure", "async-block"}
            and record_return_dependencies(record)
            and not record_parameter_dependencies(record)
        )
    )
    changed = True
    while changed:
        changed = False
        for record in records:
            if (
                record["symbol"] in boundaries
                or has_receiver(record)
                or record.get("kind") in {"closure", "async-block"}
                or record.get("ambient_dependencies")
                or record.get("effects")
                or record.get("unresolved_calls")
            ):
                continue
            callees = record.get("callees", [])
            if (
                not callees
                or any(edge["symbol"] not in produced_types for edge in callees)
            ):
                continue
            return_type = record.get("return_type", "")
            for edge in callees:
                produced_type = produced_types.get(edge["symbol"])
                if produced_type is None:
                    continue
                type_name = produced_type.rsplit("::", 1)[-1]
                if re.search(rf"\b{re.escape(type_name)}\b", return_type):
                    produced_types[record["symbol"]] = produced_type
                    boundaries.add(record["symbol"])
                    changed = True
                    break
    return boundaries


def construction_boundary_members(
    records: list[dict[str, Any]],
) -> tuple[set[str], dict[str, str]]:
    roots = construction_boundary_roots(records)
    root_by_symbol = {
        symbol: symbol
        for symbol in roots
    }
    changed = True
    while changed:
        changed = False
        for record in records:
            if record["symbol"] in root_by_symbol:
                continue
            parent_root = root_by_symbol.get(record.get("enclosing_callable"))
            if parent_root is None:
                continue
            root_by_symbol[record["symbol"]] = parent_root
            changed = True
    return roots, root_by_symbol


def construction_boundary_symbols(
    records: list[dict[str, Any]],
) -> set[str]:
    return set(construction_boundary_members(records)[1])


def bound_owner(
    record: dict[str, Any],
    records_by_symbol: dict[str, dict[str, Any]],
) -> str | None:
    current: dict[str, Any] | None = record
    visited: set[str] = set()
    while current is not None:
        symbol = current["symbol"]
        if symbol in visited:
            return None
        visited.add(symbol)
        if has_receiver(current):
            receiver = current.get("receiver_type")
            concrete = concrete_receiver_owner(receiver)
            if concrete is not None:
                return concrete
            interface = receiver_interface(receiver)
            if interface is not None:
                return f"trait {interface}"
        parent_symbol = current.get("enclosing_callable")
        current = (
            records_by_symbol.get(parent_symbol)
            if parent_symbol is not None
            else None
        )
    return None


def anchored_owner(
    record: dict[str, Any],
    records_by_symbol: dict[str, dict[str, Any]],
) -> str | None:
    owner = bound_owner(record, records_by_symbol)
    return owner if owner is not None and not owner.startswith("trait ") else None


def adjacent_owner_scores(
    record: dict[str, Any],
    records_by_symbol: dict[str, dict[str, Any]],
) -> dict[str, int]:
    scores: dict[str, int] = {}
    for edge in record.get("callers", []):
        caller = records_by_symbol.get(edge["symbol"])
        if caller is None or record_realm(caller) != record_realm(record):
            continue
        owner = anchored_owner(caller, records_by_symbol)
        if owner is not None:
            scores[owner] = scores.get(owner, 0) + 2 * len(
                edge.get("sites", [])
            )
    for edge in record.get("callees", []):
        callee = records_by_symbol.get(edge["symbol"])
        if callee is None or record_realm(callee) != record_realm(record):
            continue
        owner = anchored_owner(callee, records_by_symbol)
        if owner is not None:
            scores[owner] = scores.get(owner, 0) + len(
                edge.get("sites", [])
            )
    return scores


def dependency_bundle(record: dict[str, Any]) -> list[str]:
    return sorted(
        set(
            record.get("retained_dependencies", [])
            + record.get("ambient_dependencies", [])
        )
    )


def component_realm(records: list[dict[str, Any]]) -> str:
    realms = {record_realm(record) for record in records}
    if "production" in realms:
        return "production"
    if "test" in realms:
        return "test"
    return "macro"


def component_owner(
    records: list[dict[str, Any]],
    records_by_symbol: dict[str, dict[str, Any]],
) -> str | None:
    owners = {
        bound_owner(record, records_by_symbol)
        for record in records
    }
    return next(iter(owners)) if len(owners) == 1 and None not in owners else None


def component_candidate_owner(
    records: list[dict[str, Any]],
    records_by_symbol: dict[str, dict[str, Any]],
) -> tuple[str, int] | None:
    scores: dict[str, int] = {}
    for record in records:
        for owner, score in adjacent_owner_scores(
            record,
            records_by_symbol,
        ).items():
            scores[owner] = scores.get(owner, 0) + score
    ranked = sorted(
        scores.items(),
        key=lambda candidate: (-candidate[1], candidate[0]),
    )
    if not ranked:
        return None
    owner, score = ranked[0]
    next_score = ranked[1][1] if len(ranked) > 1 else 0
    return (owner, score) if score >= 2 and score > next_score else None


def verified_free_workflow(
    records: list[dict[str, Any]],
    decisions: dict[tuple[str, str], dict[str, Any]],
) -> tuple[bool, list[str]]:
    accepted = {
        "boundary",
        "callback-adapter",
        "dependency-primitive",
        "transformation",
    }
    classifications = []
    for record in records:
        decision = decisions.get((record["symbol"], record["signature"]))
        if (
            decision is None
            or decision.get("status") != "verified"
            or decision.get("classification") not in accepted
        ):
            return False, []
        classifications.append(decision["classification"])
    return True, sorted(set(classifications))


def callable_graph_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": record["symbol"],
        "signature": record["signature"],
        "kind": record["kind"],
        "visibility": record.get("visibility", "private"),
        "module": record["module"],
        "path": record["path"],
        "receiver": record.get("receiver_type"),
        "retained": record.get("retained_dependencies", []),
        "ambient": record.get("ambient_dependencies", []),
        "effects": record.get("effects", []),
        "captures": record.get("captured_values", []),
        "unresolved": record.get("unresolved_calls", []),
        "views": record.get("semantic_views", []),
        "rank": record.get("bottom_up_rank", 0),
    }


def workflow_label(
    component: dict[str, Any],
    records: list[dict[str, Any]],
) -> str:
    module = records[0]["module"].rsplit("::", 1)[-1]
    tail = component["id"].rsplit("::", 1)[-1]
    label = f"{module}::{tail}"
    return (
        f"Recursive · {label}"
        if component.get("recursive", False)
        else label
    )


def build_graph_data(
    index: dict[str, Any],
    decisions: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    decisions = decisions or {}
    records = index["callables"]
    attach_nested_callable_edges(records, attach_callers=True)
    records_by_symbol = {
        record["symbol"]: record
        for record in records
    }
    construction_roots, construction_root_by_symbol = (
        construction_boundary_members(records)
    )
    construction_boundaries = set(construction_root_by_symbol)
    workflow_records = [
        record
        for record in records
        if record["symbol"] not in construction_boundaries
    ]
    components = call_components(workflow_records, collapse_nested=True)
    component_records = {
        component["id"]: [
            records_by_symbol[symbol]
            for symbol in component["members"]
        ]
        for component in components
    }
    directly_stateful = {
        component["id"]: any(
            stateful_record(record)
            for record in component_records[component["id"]]
        )
        for component in components
    }
    behaviorally_stateful: dict[str, bool] = {}
    for component in sorted(
        components,
        key=lambda value: (value["bottom_up_rank"], value["id"]),
    ):
        behaviorally_stateful[component["id"]] = (
            directly_stateful[component["id"]]
            or any(
                behaviorally_stateful.get(callee, False)
                for callee in component["callees"]
            )
        )

    findings_by_symbol: dict[str, int] = {}
    for entries in index.get("reach_throughs", {}).values():
        for entry in entries:
            symbol = entry.get("symbol") or entry.get("caller")
            if symbol is not None:
                findings_by_symbol[symbol] = (
                    findings_by_symbol.get(symbol, 0) + 1
                )

    construction_nodes = []
    construction_node_id_by_root = {
        root: f"construction|{root}"
        for root in construction_roots
    }
    construction_members_by_root = {
        root: []
        for root in construction_roots
    }
    for record in records:
        root = construction_root_by_symbol.get(record["symbol"])
        if root is not None:
            construction_members_by_root[root].append(record)
    for root in sorted(construction_roots):
        members = sorted(
            construction_members_by_root[root],
            key=lambda record: record["symbol"],
        )
        root_record = records_by_symbol[root]
        caller_symbols = sorted(
            {
                caller["symbol"]
                for member in members
                for caller in member.get("callers", [])
                if construction_root_by_symbol.get(caller["symbol"]) != root
                and caller["symbol"] in records_by_symbol
            }
        )
        module = root_record["module"].rsplit("::", 1)[-1]
        receiver = root_record.get("receiver_type")
        label = (
            f"{module}::<{receiver}>::{root_record['name']}"
            if receiver is not None
            else f"{module}::{root_record['name']}"
        )
        construction_nodes.append(
            {
                "id": construction_node_id_by_root[root],
                "kind": "construction",
                "label": label,
                "realm": component_realm(members),
                "crate": root_record["crate"],
                "components": [],
                "callables": [
                    callable_graph_record(record)
                    for record in members
                ],
                "construction_callers": [
                    callable_graph_record(records_by_symbol[symbol])
                    for symbol in caller_symbols
                ],
                "requirements": sorted(
                    {
                        dependency
                        for record in members
                        for dependency in dependency_bundle(record)
                    }
                ),
                "effects": sorted(
                    {
                        effect
                        for record in members
                        for effect in record.get("effects", [])
                    }
                ),
                "modules": sorted(
                    {record["module"] for record in members}
                ),
                "findings": sum(
                    findings_by_symbol.get(record["symbol"], 0)
                    for record in members
                ),
                "rank": root_record.get("bottom_up_rank", 0),
                "directly_stateful": int(
                    any(stateful_record(record) for record in members)
                ),
                "blockers": [],
                "blocked_callers": [],
                "ready": False,
                "candidate_owner": None,
                "classifications": ["construction-boundary"],
            }
        )

    owner_nodes: dict[str, dict[str, Any]] = {}
    component_to_node: dict[str, str] = {}
    workflow_nodes: list[dict[str, Any]] = []
    owner_ids_by_type: dict[tuple[str, str, str], str] = {}

    for component in components:
        component_id = component["id"]
        if not behaviorally_stateful[component_id]:
            continue
        members = component_records[component_id]
        realm = component_realm(members)
        crate = members[0]["crate"]
        owner = component_owner(members, records_by_symbol)
        verified, classifications = verified_free_workflow(
            members,
            decisions,
        )
        callables = [
            callable_graph_record(record)
            for record in members
        ]
        requirements = sorted(
            {
                dependency
                for record in members
                for dependency in dependency_bundle(record)
            }
        )
        effects = sorted(
            {
                effect
                for record in members
                for effect in record.get("effects", [])
            }
        )
        findings = sum(
            findings_by_symbol.get(record["symbol"], 0)
            for record in members
        )
        if owner is not None:
            owner_id = f"owner|{realm}|{crate}|{owner}"
            owner_ids_by_type[(realm, crate, owner)] = owner_id
            node = owner_nodes.setdefault(
                owner_id,
                {
                    "id": owner_id,
                    "kind": "owner",
                    "label": owner,
                    "realm": realm,
                    "crate": crate,
                    "components": [],
                    "callables": [],
                    "requirements": set(),
                    "effects": set(),
                    "modules": set(),
                    "findings": 0,
                    "rank": 0,
                    "directly_stateful": 0,
                    "blockers": [],
                    "blocked_callers": [],
                    "ready": False,
                    "candidate_owner": None,
                    "classifications": [],
                },
            )
            node["components"].append(component_id)
            node["callables"].extend(callables)
            node["requirements"].update(requirements)
            node["effects"].update(effects)
            node["modules"].update(record["module"] for record in members)
            node["findings"] += findings
            node["rank"] = max(node["rank"], component["bottom_up_rank"])
            node["directly_stateful"] += int(directly_stateful[component_id])
            component_to_node[component_id] = owner_id
            continue

        node_id = (
            f"resolved|{realm}|{component_id}"
            if verified
            else f"workflow|{realm}|{component_id}"
        )
        candidate = component_candidate_owner(members, records_by_symbol)
        workflow_nodes.append(
            {
                "id": node_id,
                "kind": "resolved" if verified else "unbound",
                "label": workflow_label(component, members),
                "realm": realm,
                "crate": crate,
                "component": component_id,
                "components": [component_id],
                "callables": callables,
                "requirements": requirements,
                "effects": effects,
                "modules": sorted(
                    {record["module"] for record in members}
                ),
                "findings": findings,
                "rank": component["bottom_up_rank"],
                "directly_stateful": int(directly_stateful[component_id]),
                "blockers": [],
                "blocked_callers": [],
                "ready": False,
                "candidate_owner": (
                    {
                        "label": candidate[0],
                        "score": candidate[1],
                        "id": None,
                    }
                    if candidate is not None
                    else None
                ),
                "classifications": classifications,
            }
        )
        component_to_node[component_id] = node_id

    for owner in owner_nodes.values():
        owner["components"].sort()
        owner["callables"].sort(key=lambda record: record["symbol"])
        owner["requirements"] = sorted(owner["requirements"])
        owner["effects"] = sorted(owner["effects"])
        owner["modules"] = sorted(owner["modules"])

    nodes = [*owner_nodes.values(), *workflow_nodes]
    nodes_by_id = {node["id"]: node for node in nodes}
    for node in workflow_nodes:
        candidate = node["candidate_owner"]
        if candidate is not None:
            candidate["id"] = owner_ids_by_type.get(
                (node["realm"], node["crate"], candidate["label"])
            )

    edge_counts: dict[tuple[str, str], int] = {}
    for record in workflow_records:
        source = component_to_node.get(record.get("component"))
        if source is None:
            continue
        for edge in record.get("callees", []):
            target_record = records_by_symbol.get(edge["symbol"])
            target = (
                component_to_node.get(target_record.get("component"))
                if target_record is not None
                else None
            )
            if target is None or source == target:
                continue
            key = (source, target)
            edge_counts[key] = edge_counts.get(key, 0) + len(
                edge.get("sites", [])
            )

    call_edges = [
        {
            "source": source,
            "target": target,
            "kind": "call",
            "sites": sites,
        }
        for (source, target), sites in sorted(edge_counts.items())
    ]
    construction_edge_counts: dict[tuple[str, str], int] = {}
    for record in records:
        source_root = construction_root_by_symbol.get(record["symbol"])
        source = (
            construction_node_id_by_root[source_root]
            if source_root is not None
            else component_to_node.get(record.get("component"))
        )
        if source is None:
            continue
        for edge in record.get("callees", []):
            target_root = construction_root_by_symbol.get(edge["symbol"])
            if target_root is None:
                continue
            target = construction_node_id_by_root[target_root]
            if source == target:
                continue
            key = (source, target)
            construction_edge_counts[key] = (
                construction_edge_counts.get(key, 0)
                + len(edge.get("sites", []))
            )
    construction_edges = [
        {
            "source": source,
            "target": target,
            "kind": "constructs",
            "sites": sites,
        }
        for (source, target), sites in sorted(
            construction_edge_counts.items()
        )
    ]
    for node in workflow_nodes:
        if node["kind"] != "unbound":
            continue
        blockers = sorted(
            {
                edge["target"]
                for edge in call_edges
                if edge["source"] == node["id"]
                and nodes_by_id[edge["target"]]["kind"] == "unbound"
            }
        )
        node["blockers"] = blockers
        node["ready"] = not blockers
        for blocker in blockers:
            nodes_by_id[blocker]["blocked_callers"].append(node["id"])

    capability_nodes: dict[str, dict[str, Any]] = {}
    supporting_edges: list[dict[str, Any]] = []
    for node in [*nodes, *construction_nodes]:
        for requirement in node["requirements"]:
            capability_id = f"capability|{requirement}"
            capability_nodes.setdefault(
                capability_id,
                {
                    "id": capability_id,
                    "kind": "capability",
                    "label": requirement,
                    "realm": "shared",
                    "crate": "",
                    "components": [],
                    "callables": [],
                    "requirements": [],
                    "effects": [],
                    "modules": [],
                    "findings": 0,
                    "rank": 0,
                    "directly_stateful": 0,
                    "blockers": [],
                    "blocked_callers": [],
                    "ready": False,
                    "candidate_owner": None,
                    "classifications": [],
                },
            )
            supporting_edges.append(
                {
                    "source": node["id"],
                    "target": capability_id,
                    "kind": "requires",
                    "sites": 1,
                }
            )
        candidate = node.get("candidate_owner")
        if candidate is not None and candidate["id"] is not None:
            supporting_edges.append(
                {
                    "source": node["id"],
                    "target": candidate["id"],
                    "kind": "candidate",
                    "sites": candidate["score"],
                }
            )

    return {
        "summary": {
            "callables": len(records),
            "stateful_components": sum(behaviorally_stateful.values()),
            "owners": len(owner_nodes),
            "unbound": sum(
                node["kind"] == "unbound"
                for node in workflow_nodes
            ),
            "ready": sum(node["ready"] for node in workflow_nodes),
            "resolved": sum(
                node["kind"] == "resolved"
                for node in workflow_nodes
            ),
            "call_edges": len(call_edges),
            "construction_boundaries": len(construction_nodes),
            "construction_edges": len(construction_edges),
            "semantic_views": [
                view["name"]
                for view in index.get("semantic_views", [])
            ],
            "macro_expansions": len(
                index.get("macro_expansions", [])
            ),
            "reach_through_findings": sum(
                len(entries)
                for entries in index.get(
                    "reach_throughs", {}
                ).values()
            ),
        },
        "nodes": sorted(
            [*nodes, *capability_nodes.values()],
            key=lambda node: node["id"],
        ),
        "construction_boundaries": construction_nodes,
        "call_edges": call_edges,
        "construction_edges": construction_edges,
        "supporting_edges": supporting_edges,
    }


def render_graph_html(graph: dict[str, Any]) -> str:
    data = json.dumps(graph, sort_keys=True).replace("</", "<\\/")
    template = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Coven ownership work queue</title>
<style>
:root {
  color-scheme: dark;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  background: #101418;
  color: #e5edf2;
}
* { box-sizing: border-box; }
[hidden] { display: none !important; }
body { margin: 0; min-height: 100vh; }
header {
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 16px;
  padding: 14px 18px;
  border-bottom: 1px solid #33404a;
  background: #151b20;
}
h1 { font-size: 17px; margin: 0 0 7px; }
#summary { color: #9fb0bc; font-size: 12px; }
.controls { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
button, input {
  border: 1px solid #455562;
  border-radius: 4px;
  background: #202a31;
  color: inherit;
}
button { padding: 7px 9px; cursor: pointer; }
button:hover, button.active { background: #344550; }
input[type="search"] {
  width: 300px;
  padding: 8px 10px;
  background: #0c1013;
}
main {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 500px;
  height: calc(100vh - 74px);
}
#canvas { position: relative; overflow: auto; }
#ownership-list { padding: 12px; }
.node-list-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 14px;
  width: 100%;
  margin: 0 0 7px;
  padding: 10px 12px;
  text-align: left;
}
.node-list-row.selected { border-color: #fff4c2; background: #344550; }
.node-list-row .status { text-align: right; }
#graph { display: block; min-width: 100%; min-height: 100%; background: #0d1115; }
#details {
  overflow: auto;
  padding: 16px;
  border-left: 1px solid #33404a;
  background: #141a1f;
}
#details h2 { font-size: 14px; overflow-wrap: anywhere; }
#details h3 { margin-top: 20px; font-size: 12px; color: #9fb0bc; }
.meta { color: #91a5b2; margin-top: 4px; }
.badge {
  display: inline-block;
  margin: 3px 4px 0 0;
  padding: 2px 5px;
  border-radius: 3px;
  background: #26343e;
  color: #c5d4dc;
}
.queue-row, .relation-row {
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 8px;
  width: 100%;
  margin: 5px 0;
  text-align: left;
}
.callable {
  padding: 9px 0;
  border-top: 1px solid #29343c;
  font-size: 11px;
  overflow-wrap: anywhere;
}
.edge { stroke: #657985; stroke-opacity: .25; fill: none; }
.edge.candidate { stroke: #efc56c; stroke-dasharray: 7 5; }
.edge.constructs { stroke: #74b6a9; stroke-dasharray: 7 5; }
.edge.requires { stroke: #a98bc4; stroke-dasharray: 2 5; }
.edge.active { stroke: #fff0a6; stroke-opacity: .9; }
.node rect { stroke-width: 2; cursor: pointer; }
.node text { fill: #f0f5f8; font-size: 11px; pointer-events: none; }
.node .count { fill: #c0cbd1; font-size: 9px; }
.node.selected rect { stroke: #fff4c2; stroke-width: 4; }
.legend {
  position: absolute;
  left: 14px;
  bottom: 12px;
  padding: 8px 10px;
  background: #10161bdd;
  border: 1px solid #33404a;
  font-size: 10px;
  color: #a9bac4;
}
</style>
</head>
<body>
<header>
  <div>
    <h1>Coven ownership work queue</h1>
    <div id="summary"></div>
  </div>
  <div class="controls">
    <button data-mode="ready" class="active">Ready leaves</button>
    <button data-mode="unbound">All unowned</button>
    <button data-mode="owners">Owner graph</button>
    <button data-mode="construction">Construction boundaries</button>
    <input id="search" type="search" placeholder="Filter owner, workflow, module, callable">
    <label><input id="tests" type="checkbox"> tests</label>
    <label><input id="macros" type="checkbox"> macro expansions</label>
    <label><input id="capabilities" type="checkbox"> capability requirements</label>
  </div>
</header>
<main>
  <section id="canvas">
    <div id="ownership-list"></div>
    <svg id="graph" role="img" aria-label="Callable ownership graph"></svg>
    <div id="legend" class="legend">Arrow: caller → callee · amber: ready · red: blocked · blue: owner · teal: construction · green: reviewed boundary · purple: required capability</div>
  </section>
  <aside id="details"></aside>
</main>
<script>
const DATA = __GRAPH_DATA__;
const svg = document.getElementById("graph");
const ownershipList = document.getElementById("ownership-list");
const details = document.getElementById("details");
const summary = document.getElementById("summary");
const search = document.getElementById("search");
const includeTests = document.getElementById("tests");
const includeMacros = document.getElementById("macros");
const includeCapabilities = document.getElementById("capabilities");
const legend = document.getElementById("legend");
const CONSTRUCTION_NODES = DATA.construction_boundaries || [];
const ALL_NODES = DATA.nodes.concat(CONSTRUCTION_NODES);
const nodesById = new Map(ALL_NODES.map(function(node) { return [node.id, node]; }));
let mode = "ready";
let selected = null;

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function realmVisible(node) {
  if (node.kind === "capability") return includeCapabilities.checked;
  if (node.realm === "test") return includeTests.checked;
  if (node.realm === "macro") return includeMacros.checked;
  return true;
}

function matches(node, query) {
  if (!query) return true;
  return node.label.toLowerCase().includes(query) ||
    node.modules.some(function(module) { return module.toLowerCase().includes(query); }) ||
    node.callables.some(function(item) {
      return item.symbol.toLowerCase().includes(query);
    });
}

function nodeColor(node) {
  if (node.kind === "owner") return "#43849a";
  if (node.kind === "resolved") return "#4f8a68";
  if (node.kind === "construction") return "#477f78";
  if (node.kind === "capability") return "#80639b";
  if (node.ready) return "#b98737";
  return "#a64f4f";
}

function kindLabel(node) {
  if (node.kind === "owner") return "retained owner";
  if (node.kind === "resolved") {
    return "verified " + (node.classifications.join(", ") || "workflow");
  }
  if (node.kind === "construction") return "construction boundary";
  if (node.kind === "capability") return "required capability";
  if (node.ready) return "ready unowned stateful call group";
  return "blocked unowned stateful call group";
}

function badges(values) {
  return values.map(function(value) {
    return '<span class="badge">' + escapeHtml(value) + '</span>';
  }).join("");
}

function relatedRows(ids, title) {
  if (!ids.length) return "";
  const rows = ids.map(function(id) {
    const node = nodesById.get(id);
    return '<button class="relation-row" data-node="' + escapeHtml(id) + '">' +
      '<span>' + escapeHtml(node.label) + '</span>' +
      '<span class="meta">' + escapeHtml(kindLabel(node)) + '</span></button>';
  }).join("");
  return "<h3>" + escapeHtml(title) + "</h3>" + rows;
}

function callableMarkup(items) {
  return items.map(function(item) {
    const facts = []
      .concat(item.retained.map(function(value) { return "retains:" + value; }))
      .concat(item.ambient.map(function(value) { return "ambient:" + value; }))
      .concat(item.effects.map(function(value) { return "effect:" + value; }))
      .concat(item.captures.length ? ["captures:" + item.captures.length] : [])
      .concat(item.unresolved.length ? ["unknown calls:" + item.unresolved.length] : []);
    return '<div class="callable"><div>' + escapeHtml(item.symbol) + '</div>' +
      '<div class="meta">' + escapeHtml(item.kind) + " · " +
      escapeHtml(item.visibility) + " · " +
      escapeHtml(item.path) + " · raw rank " + item.rank + "</div>" +
      "<div>" + badges(facts) + "</div></div>";
  }).join("");
}

function attachRelationHandlers() {
  details.querySelectorAll("[data-node]").forEach(function(button) {
    button.addEventListener("click", function() {
      const node = nodesById.get(button.dataset.node);
      if (node) showDetails(node);
    });
  });
}

function showQueue() {
  const ready = DATA.nodes
    .filter(function(node) {
      return node.kind === "unbound" && node.ready && realmVisible(node);
    })
    .sort(function(left, right) {
      return left.rank - right.rank ||
        right.findings - left.findings ||
        left.id.localeCompare(right.id);
    });
  details.innerHTML =
    "<h2>Ready ownership queue</h2>" +
    '<div class="meta">These call groups do not call another unowned stateful call group.</div>' +
    "<h3>" + ready.length + " ready groups</h3>" +
    '<div class="meta">Readiness applies to the selected group only. It may be owned, classified, deleted, or internalized into a caller; a blocked caller remains in place until its own blockers are resolved.</div>' +
    '<div class="meta">Select a row from the list to inspect its owner, callers, and dependencies.</div>';
}

function showConstructionQueue() {
  const boundaries = CONSTRUCTION_NODES
    .filter(realmVisible)
    .sort(function(left, right) {
      return left.rank - right.rank ||
        right.findings - left.findings ||
        left.id.localeCompare(right.id);
    });
  details.innerHTML =
    "<h2>Construction boundary queue</h2>" +
    '<div class="meta">Review which parent boundary may call each constructor or opener; repository callers and current visibility are shown.</div>' +
    "<h3>" + boundaries.length + " construction boundaries</h3>" +
    '<div class="meta">Select a row from the list to inspect its callers and visibility.</div>';
}

function graphEdges() {
  return callEdges()
    .concat(DATA.supporting_edges);
}

function callEdges() {
  return DATA.call_edges.concat(DATA.construction_edges || []);
}

function showDetails(node) {
  selected = node.id;
  const incoming = callEdges()
    .filter(function(edge) { return edge.target === node.id; })
    .map(function(edge) { return edge.source; });
  const outgoing = callEdges()
    .filter(function(edge) { return edge.source === node.id; })
    .map(function(edge) { return edge.target; });
  const candidate = node.kind === "unbound"
    ? '<h3>Owner decision</h3><div>' +
      (node.candidate_owner
        ? (node.candidate_owner.id
          ? '<button class="relation-row" data-node="' +
            escapeHtml(node.candidate_owner.id) + '"><span>' +
            escapeHtml(node.candidate_owner.label) + '</span><span class="meta">' +
            node.candidate_owner.score + " adjacent call points</span></button>"
          : escapeHtml(node.candidate_owner.label))
        : '<span class="meta">No existing owner detected.</span>') +
      '<div class="meta">The adjacent type is evidence, not a constraint. Create an owner when this workflow has a distinct lifetime or invariant.</div></div>'
    : "";
  const readiness = node.kind === "unbound"
    ? '<div class="meta">' +
      (node.ready
        ? "Ready: no unowned stateful callees. This does not make its callers ready."
        : "Blocked by " + node.blockers.length + " unowned stateful callees.") +
      "</div>"
    : "";
  details.innerHTML =
    "<h2>" + escapeHtml(node.label) + "</h2>" +
    '<div class="meta">' + escapeHtml(kindLabel(node)) + " · raw rank " +
      node.rank + " · " + node.findings + " findings</div>" +
    readiness +
    '<p><button id="neighborhood">Show dependency neighborhood</button></p>' +
    candidate +
    "<h3>Required capabilities</h3><div>" +
      (badges(node.requirements) || '<span class="meta">none detected directly</span>') +
      "</div>" +
    "<h3>Effects</h3><div>" +
      (badges(node.effects) || '<span class="meta">' +
        (node.directly_stateful || node.kind === "construction"
          ? "none detected directly"
          : "stateful behavior inherited through callees") +
        "</span>") +
      "</div>" +
    relatedRows(node.blockers, "Unowned stateful blockers") +
    relatedRows(node.blocked_callers, "Callers this group unblocks") +
    relatedRows(outgoing, "Calls") +
    relatedRows(incoming, "Called by") +
    ((node.construction_callers || []).length
      ? "<h3>Direct construction callers</h3>" +
        callableMarkup(node.construction_callers)
      : "") +
    "<h3>Modules</h3><div>" + badges(node.modules) + "</div>" +
    "<h3>Callables</h3>" + callableMarkup(node.callables);
  document.getElementById("neighborhood").addEventListener("click", function() {
    mode = "neighborhood";
    setModeButtons();
    render();
  });
  attachRelationHandlers();
  render();
}

function setModeButtons() {
  document.querySelectorAll("[data-mode]").forEach(function(button) {
    button.classList.toggle("active", button.dataset.mode === mode);
  });
}

function selectedBaseNodes(query) {
  let nodes = ALL_NODES.filter(function(node) {
    if (!realmVisible(node) || !matches(node, query)) return false;
    if (mode === "ready") return node.kind === "unbound" && node.ready;
    if (mode === "unbound") return node.kind === "unbound";
    if (mode === "owners") return node.kind === "owner" || node.kind === "resolved";
    if (mode === "construction") return node.kind === "construction";
    return false;
  });
  nodes.sort(function(left, right) {
    return left.rank - right.rank ||
      right.findings - left.findings ||
      left.id.localeCompare(right.id);
  });
  if (mode === "neighborhood" && selected) {
    const ids = new Set([selected]);
    graphEdges().forEach(function(edge) {
      if (edge.source === selected) ids.add(edge.target);
      if (edge.target === selected) ids.add(edge.source);
    });
    nodes = ALL_NODES.filter(function(node) {
      return ids.has(node.id) && realmVisible(node) && matches(node, query);
    });
  }
  return nodes;
}

function nodeStatus(node) {
  if (node.kind === "unbound") {
    return node.ready ? "ready" : node.blockers.length + " blockers";
  }
  if (node.kind === "construction") {
    return node.construction_callers.length + " repository callers";
  }
  return node.callables.length + " callables";
}

function updateSummary(visibleNodes) {
  const ready = DATA.nodes.filter(function(node) {
    return node.kind === "unbound" && node.ready && realmVisible(node);
  }).length;
  const unbound = DATA.nodes.filter(function(node) {
    return node.kind === "unbound" && realmVisible(node);
  }).length;
  const owners = DATA.nodes.filter(function(node) {
    return node.kind === "owner" && realmVisible(node);
  }).length;
  const boundaries = CONSTRUCTION_NODES.filter(realmVisible).length;
  summary.textContent =
    ready.toLocaleString() + " ready · " +
    unbound.toLocaleString() + " unowned stateful groups · " +
    owners.toLocaleString() + " retained owners · " +
    boundaries.toLocaleString() + " construction boundaries · " +
    visibleNodes.toLocaleString() + " visible rows";
}

function renderList(nodes) {
  ownershipList.innerHTML = nodes.map(function(node) {
    const label = node.realm === "test" ? "Test · " + node.label :
      node.realm === "macro" ? "Macro · " + node.label : node.label;
    return '<button class="node-list-row ' +
      (selected === node.id ? "selected" : "") +
      '" data-node="' + escapeHtml(node.id) + '"><span>' +
      escapeHtml(label) + '<span class="meta"><br>' +
      escapeHtml(node.modules.join(", ")) + '</span></span>' +
      '<span class="status"><span>' + escapeHtml(kindLabel(node)) +
      '</span><span class="meta"><br>' + escapeHtml(nodeStatus(node)) +
      " · " + node.findings + " findings</span></span></button>";
  }).join("");
  ownershipList.querySelectorAll("[data-node]").forEach(function(element) {
    element.addEventListener("click", function() {
      const node = nodesById.get(element.dataset.node);
      if (node) showDetails(node);
    });
  });
}

function render() {
  const query = search.value.trim().toLowerCase();
  let nodes = selectedBaseNodes(query);
  updateSummary(nodes.length);
  if (mode !== "neighborhood") {
    svg.hidden = true;
    legend.hidden = true;
    ownershipList.hidden = false;
    renderList(nodes);
    return;
  }
  ownershipList.hidden = true;
  svg.hidden = false;
  legend.hidden = false;

  const visibleIds = new Set(nodes.map(function(node) { return node.id; }));
  let edges = graphEdges().filter(function(edge) {
    if (!visibleIds.has(edge.source) || !visibleIds.has(edge.target)) return false;
    if (edge.kind === "requires" && !includeCapabilities.checked) return false;
    return true;
  });
  nodes.sort(function(left, right) {
    return Number(right.ready) - Number(left.ready) ||
      left.rank - right.rank ||
      right.findings - left.findings ||
      left.label.localeCompare(right.label);
  });
  const nodeWidth = 230;
  const nodeHeight = 58;
  const gapX = 90;
  const gapY = 40;
  const columns = Math.min(4, Math.max(1, Math.ceil(Math.sqrt(nodes.length * 1.6))));
  const rows = Math.max(1, Math.ceil(nodes.length / columns));
  const width = Math.max(900, 70 + columns * (nodeWidth + gapX));
  const height = Math.max(650, 70 + rows * (nodeHeight + gapY));
  svg.setAttribute("viewBox", "0 0 " + width + " " + height);
  svg.setAttribute("width", width);
  svg.setAttribute("height", height);

  const positions = new Map();
  nodes.forEach(function(node, index) {
    positions.set(node.id, {
      x: 40 + (index % columns) * (nodeWidth + gapX),
      y: 40 + Math.floor(index / columns) * (nodeHeight + gapY)
    });
  });

  const marker = '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" ' +
    'markerWidth="5" markerHeight="5" orient="auto-start-reverse">' +
    '<path d="M 0 0 L 10 5 L 0 10 z" fill="#768994"/></marker></defs>';
  const edgeMarkup = edges.map(function(edge) {
    const source = positions.get(edge.source);
    const target = positions.get(edge.target);
    const sourceCenter = {x: source.x + nodeWidth / 2, y: source.y + nodeHeight / 2};
    const targetCenter = {x: target.x + nodeWidth / 2, y: target.y + nodeHeight / 2};
    const dx = targetCenter.x - sourceCenter.x;
    const dy = targetCenter.y - sourceCenter.y;
    const scale = 0.5 / Math.max(
      Math.abs(dx) / nodeWidth,
      Math.abs(dy) / nodeHeight,
      0.001
    );
    const start = {x: sourceCenter.x + dx * scale, y: sourceCenter.y + dy * scale};
    const end = {x: targetCenter.x - dx * scale, y: targetCenter.y - dy * scale};
    const active = selected && (edge.source === selected || edge.target === selected);
    const width = Math.min(5, 0.7 + Math.log2(edge.sites + 1) * 0.42);
    return '<line class="edge ' + edge.kind + (active ? " active" : "") +
      '" x1="' + start.x + '" y1="' + start.y +
      '" x2="' + end.x + '" y2="' + end.y +
      '" stroke-width="' + width + '" marker-end="url(#arrow)"><title>' +
      escapeHtml(edge.kind + ": " + edge.source + " → " + edge.target) +
      "</title></line>";
  }).join("");

  const nodeMarkup = nodes.map(function(node) {
    const point = positions.get(node.id);
    const label = node.realm === "test" ? "Test · " + node.label :
      node.realm === "macro" ? "Macro · " + node.label : node.label;
    return '<g class="node ' + (selected === node.id ? "selected" : "") +
      '" data-node="' + escapeHtml(node.id) + '" transform="translate(' +
      point.x + " " + point.y + ')"><rect width="' + nodeWidth +
      '" height="' + nodeHeight + '" rx="7" fill="' + nodeColor(node) +
      '" fill-opacity=".84" stroke="' + (node.findings ? "#e09a70" : "#52636e") +
      '"><title>' + escapeHtml(label + " · " + kindLabel(node)) +
      '</title></rect><text x="10" y="23">' +
      escapeHtml(label.length > 34 ? label.slice(0, 32) + "…" : label) +
      '</text><text class="count" x="10" y="43">' +
      escapeHtml(nodeStatus(node) + " · " + node.findings + " findings") +
      "</text></g>";
  }).join("");

  svg.innerHTML = marker + edgeMarkup + nodeMarkup;
  svg.querySelectorAll(".node").forEach(function(element) {
    element.addEventListener("click", function() {
      const node = nodesById.get(element.dataset.node);
      if (node) showDetails(node);
    });
  });
}

document.querySelectorAll("[data-mode]").forEach(function(button) {
  button.addEventListener("click", function() {
    mode = button.dataset.mode;
    selected = null;
    setModeButtons();
    render();
    if (mode === "ready") showQueue();
    else if (mode === "construction") showConstructionQueue();
    else details.innerHTML = "<h2>Select a node</h2>";
  });
});
[search, includeTests, includeMacros, includeCapabilities]
  .forEach(function(control) {
    control.addEventListener(control === search ? "input" : "change", function() {
      render();
      if (mode === "ready" && !selected) showQueue();
      if (mode === "construction" && !selected) showConstructionQueue();
    });
  });

showQueue();
render();
</script>
</body>
</html>
"""
    return template.replace("__GRAPH_DATA__", data)


def write_graph(
    index: dict[str, Any],
    decisions: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> None:
    GRAPH_PATH.parent.mkdir(parents=True, exist_ok=True)
    graph = build_graph_data(
        index,
        read_decisions() if decisions is None else decisions,
    )
    with tempfile.NamedTemporaryFile(
        "w",
        dir=GRAPH_PATH.parent,
        prefix="graph.",
        suffix=".html",
        delete=False,
    ) as temporary:
        temporary.write(render_graph_html(graph))
        temporary.write("\n")
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, GRAPH_PATH)


def read_decisions() -> dict[tuple[str, str], dict[str, Any]]:
    if not DECISIONS_PATH.exists():
        return {}
    data = parse_decisions(DECISIONS_PATH.read_text())
    decisions: dict[tuple[str, str], dict[str, Any]] = {}
    for decision in data.get("decision", []):
        key = (decision["symbol"], decision["signature"])
        if key in decisions:
            raise RuntimeError(f"duplicate decision: {decision['symbol']}")
        decisions[key] = decision
    return decisions


def parse_decisions(source: str) -> dict[str, list[dict[str, str]]]:
    decisions: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for line_number, raw_line in enumerate(source.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line == "[[decision]]":
            current = {}
            decisions.append(current)
            continue
        if current is None or "=" not in line:
            raise ValueError(f"invalid decision ledger line {line_number}: {raw_line}")
        key, raw_value = (part.strip() for part in line.split("=", 1))
        if not re.fullmatch(r"[a-z_]+", key):
            raise ValueError(f"invalid decision key on line {line_number}: {key}")
        try:
            value = json.loads(raw_value)
        except json.JSONDecodeError as error:
            raise ValueError(
                f"invalid decision value on line {line_number}: {raw_value}"
            ) from error
        if not isinstance(value, str):
            raise ValueError(f"decision value on line {line_number} must be a string")
        current[key] = value
    return {"decision": decisions}


def select_symbols(index: dict[str, Any], query: str) -> list[dict[str, Any]]:
    exact = [record for record in index["callables"] if record["symbol"] == query]
    if exact:
        return exact
    matches = [record for record in index["callables"] if query in record["symbol"]]
    if not matches:
        raise RuntimeError(f"no callable matches {query!r}")
    return matches


def print_record(record: dict[str, Any]) -> None:
    print(record["symbol"])
    print(f"  {record['kind']} {record['visibility']} {record['path']}")
    print(f"  signature: {record['signature']}")
    print(
        "  retained dependencies: "
        + (", ".join(record["retained_dependencies"]) or "none")
    )
    print(
        "  ambient dependencies: "
        + (", ".join(record["ambient_dependencies"]) or "none")
    )
    print("  effects: " + (", ".join(record["effects"]) or "none"))
    print(f"  callers: {len(record['callers'])}")
    print(f"  callees: {len(record['callees'])}")
    print(f"  unresolved calls: {len(record['unresolved_calls'])}")
    for call in record["unresolved_calls"]:
        print(
            f"    {call['resolution']}: {call['text']} "
            f"({len(call['dynamic_dispatch_candidates'])} candidates)"
        )
    print(
        f"  call group: {record['component']} "
        f"(bottom-up rank {record['bottom_up_rank']})"
    )


def command_inventory(_: argparse.Namespace) -> None:
    metadata = cargo_metadata()
    tool_identity = analyzer_identity()
    workspace_fingerprint = workspace_cache_fingerprint(metadata, tool_identity)
    index_fingerprint = index_cache_fingerprint(workspace_fingerprint)
    index = read_cached_index(index_fingerprint)
    if index is None:
        index = build_index(
            metadata,
            workspace_fingerprint,
            index_fingerprint,
            tool_identity,
        )
        write_index(index)
        disposition = "wrote"
    else:
        disposition = "reused"
    counts: dict[str, int] = {}
    for record in index["callables"]:
        counts[record["kind"]] = counts.get(record["kind"], 0) + 1
    print(f"{disposition} {INDEX_PATH}")
    print(json.dumps(counts, indent=2, sort_keys=True))


def command_graph(_: argparse.Namespace) -> None:
    write_graph(read_index())
    print(f"wrote {GRAPH_PATH}")


def command_reach_throughs(args: argparse.Namespace) -> None:
    reports = read_index().get("reach_throughs", {})
    selected = (
        {args.report: reports.get(args.report, [])}
        if args.report
        else reports
    )
    for name, entries in selected.items():
        print(f"{name}: {len(entries)}")
        for entry in entries:
            print(f"  {json.dumps(entry, sort_keys=True)}")


def selected_rust_paths(values: list[str]) -> list[Path]:
    selected = values or ["crates"]
    paths: set[Path] = set()
    for value in selected:
        candidate = (ROOT / value).resolve()
        if not candidate.is_relative_to(ROOT):
            raise ValueError(f"path lies outside the repository: {value}")
        if candidate.is_dir():
            paths.update(path.resolve() for path in candidate.rglob("*.rs"))
        elif candidate.is_file() and candidate.suffix == ".rs":
            paths.add(candidate)
        else:
            raise ValueError(f"path is not a Rust file or directory: {value}")
    return sorted(paths)


def command_service_exposures(args: argparse.Namespace) -> None:
    findings = [
        finding
        for path in selected_rust_paths(args.paths)
        for finding in service_exposures_for_source(parse_source(path))
    ]
    for finding in findings:
        dependencies = ",".join(finding["dependencies"])
        print(
            f"{finding['path']}:{finding['line']}: "
            f"{finding['kind']} {finding['visibility']} "
            f"[{dependencies}] {finding['name']}: "
            f"{finding['expression']}"
        )
    print(f"{len(findings)} service exposures")


def command_show(args: argparse.Namespace) -> None:
    index = read_index()
    for record in select_symbols(index, args.symbol):
        print_record(record)


def print_edges(records: list[dict[str, Any]], edge: str) -> None:
    for record in records:
        print(record["symbol"])
        for value in record[edge]:
            print(f"  {value['symbol']}")
            for site in value.get("sites", []):
                expression = site.get("expression", "")
                if expression:
                    print(f"    {expression}")


def command_callers(args: argparse.Namespace) -> None:
    print_edges(select_symbols(read_index(), args.symbol), "callers")


def command_callees(args: argparse.Namespace) -> None:
    print_edges(select_symbols(read_index(), args.symbol), "callees")


def command_retained(args: argparse.Namespace) -> None:
    records = read_index()["callables"]
    if args.symbol:
        records = select_symbols({"callables": records}, args.symbol)
    for record in records:
        if (
            record["retained_dependencies"]
            or record["ambient_dependencies"]
            or record["effects"]
        ):
            print_record(record)


def command_stack(_: argparse.Namespace) -> None:
    index = read_index()
    for component in index["components"]:
        marker = "recursive" if component["recursive"] else "single"
        print(
            f"{component['bottom_up_rank']}\t{marker}\t"
            f"{component['id']}\t{len(component['members'])}"
        )


def unclassified(
    index: dict[str, Any],
    decisions: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        record
        for record in index["callables"]
        if (record["symbol"], record["signature"]) not in decisions
    ]


def command_unclassified(_: argparse.Namespace) -> None:
    missing = unclassified(read_index(), read_decisions())
    for record in missing:
        print(record["symbol"])
    print(f"{len(missing)} unclassified callables")


def command_check(_: argparse.Namespace) -> None:
    index = read_index()
    unreachable_sources = index.get("sources", {}).get("unreachable", [])
    if unreachable_sources:
        raise RuntimeError(
            f"{len(unreachable_sources)} Rust source files are outside every Cargo target"
        )
    decisions = read_decisions()
    missing = unclassified(index, decisions)
    if missing:
        raise RuntimeError(f"{len(missing)} callables have no durable disposition")
    unresolved = [
        record
        for record in index["callables"]
        if record.get("call_hierarchy")
        not in {"resolved", "enclosing-resolved"}
        or record["unresolved_calls"]
    ]
    if unresolved:
        raise RuntimeError(f"{len(unresolved)} callables have unresolved call edges")
    stale = [
        decision
        for key, decision in decisions.items()
        if not any(
            record["symbol"] == key[0] and record["signature"] == key[1]
            for record in index["callables"]
        )
        and decision.get("classification") != "delete"
        and "replaced_by" not in decision
    ]
    if stale:
        raise RuntimeError(f"{len(stale)} ledger decisions refer to absent callables")
    print(f"verified {len(index['callables'])} callable dispositions")


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser()
    subcommands = value.add_subparsers(dest="command", required=True)
    subcommands.add_parser("inventory").set_defaults(run=command_inventory)
    subcommands.add_parser("graph").set_defaults(run=command_graph)
    for name, run in (
        ("show", command_show),
        ("callers", command_callers),
        ("callees", command_callees),
    ):
        command = subcommands.add_parser(name)
        command.add_argument("symbol")
        command.set_defaults(run=run)
    retained = subcommands.add_parser("retained-dependencies")
    retained.add_argument("symbol", nargs="?")
    retained.set_defaults(run=command_retained)
    subcommands.add_parser("stack").set_defaults(run=command_stack)
    reach_throughs = subcommands.add_parser("reach-throughs")
    reach_throughs.add_argument("report", nargs="?")
    reach_throughs.set_defaults(run=command_reach_throughs)
    service_exposures = subcommands.add_parser("service-exposures")
    service_exposures.add_argument("paths", nargs="*")
    service_exposures.set_defaults(run=command_service_exposures)
    subcommands.add_parser("unclassified").set_defaults(run=command_unclassified)
    subcommands.add_parser("check").set_defaults(run=command_check)
    return value


def main() -> int:
    try:
        args = parser().parse_args()
        args.run(args)
        return 0
    except BrokenPipeError:
        descriptor = os.open(os.devnull, os.O_WRONLY)
        os.dup2(descriptor, sys.stdout.fileno())
        os.close(descriptor)
        return 0
    except (OSError, RuntimeError, ValueError) as error:
        print(f"ownership audit failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
