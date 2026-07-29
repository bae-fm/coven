#!/usr/bin/env python3
"""Build and query Coven's Rust callable-ownership index."""

from __future__ import annotations

import argparse
import bisect
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from typing import Any, Iterable
from urllib.parse import unquote, urlparse


ROOT = Path(__file__).resolve().parents[2]
INDEX_PATH = ROOT / "target" / "ownership-audit" / "index.json"
ANALYZER_LOG_PATH = ROOT / "target" / "ownership-audit" / "rust-analyzer.log"
DECISIONS_PATH = Path(__file__).resolve().parent / "decisions.toml"

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
CALLABLE_NODE_KINDS = {"FN", "CLOSURE_EXPR", "ASYNC_BLOCK_EXPR"}
CONTENT_MODIFIED = -32801


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


def rust_sources() -> list[Path]:
    paths: list[Path] = []
    for crate in ("coven-core", "coven"):
        root = ROOT / "crates" / crate
        paths.extend(root.glob("src/**/*.rs"))
        paths.extend(root.glob("tests/**/*.rs"))
    return sorted(path.resolve() for path in paths)


def rust_analyzer_environment() -> dict[str, str]:
    environment = os.environ.copy()
    environment.pop("RUSTC_WRAPPER", None)
    environment.pop("RUSTC_WORKSPACE_WRAPPER", None)
    return environment


def run_parse(source: str) -> str:
    result = subprocess.run(
        ["rust-analyzer", "parse"],
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
    return SourceFile(path, source, data, line_starts, parse_nodes(run_parse(source)))


def direct_child(source_file: SourceFile, index: int, kind: str) -> SyntaxNode | None:
    for child in source_file.nodes[index].children:
        node = source_file.nodes[child]
        if node.kind == kind:
            return node
    return None


def ancestors(source_file: SourceFile, index: int) -> Iterable[tuple[int, SyntaxNode]]:
    parent = source_file.nodes[index].parent
    while parent is not None:
        yield parent, source_file.nodes[parent]
        parent = source_file.nodes[parent].parent


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
        if ancestor.kind in CALLABLE_NODE_KINDS:
            return None
        if ancestor.kind not in {"IMPL", "TRAIT"}:
            continue
        if ancestor.kind == "TRAIT":
            name = direct_name(source_file, ancestor_index)
            return f"trait {name[0]}" if name else "trait"
        body = direct_child(source_file, ancestor_index, "ASSOC_ITEM_LIST")
        header_end = body.start if body is not None else ancestor.end
        header = source_file.slice(ancestor.start, header_end)
        header = re.sub(r"\s+", " ", header).strip()
        return header.removeprefix("impl ").strip()
    return None


def cfg_attributes(source_file: SourceFile, index: int) -> list[str]:
    return [
        re.sub(r"\s+", " ", source_file.text(source_file.nodes[child])).strip()
        for child in source_file.nodes[index].children
        if source_file.nodes[child].kind == "ATTR"
        and re.match(r"#\s*\[\s*cfg(?:_attr)?\b", source_file.text(source_file.nodes[child]))
    ]


def visibility(signature: str) -> str:
    match = re.match(r"\s*(pub(?:\([^)]*\))?)\b", signature)
    return match.group(1) if match else "private"


def signature_for(source_file: SourceFile, index: int) -> str:
    node = source_file.nodes[index]
    block = direct_child(source_file, index, "BLOCK_EXPR")
    end = block.start if block is not None else node.end
    return re.sub(r"\s+", " ", source_file.slice(node.start, end)).strip()


def body_for(source_file: SourceFile, index: int) -> str:
    block = direct_child(source_file, index, "BLOCK_EXPR")
    return source_file.text(block) if block is not None else ""


def retained_dependencies(signature: str) -> list[str]:
    return [
        category
        for category, pattern in STATEFUL_TYPE_PATTERNS.items()
        if pattern.search(signature)
    ]


def matches(body: str, patterns: dict[str, re.Pattern[str]]) -> list[str]:
    return [name for name, pattern in patterns.items() if pattern.search(body)]


def call_text(source_file: SourceFile, node: SyntaxNode) -> str:
    text = re.sub(r"\s+", " ", source_file.text(node)).strip()
    return text[:240]


def inventory_file(source_file: SourceFile) -> list[dict[str, Any]]:
    crate, file_modules = crate_and_modules(source_file.path)
    callable_indices = [
        index
        for index, node in enumerate(source_file.nodes)
        if node.kind in CALLABLE_NODE_KINDS
    ]
    records: list[dict[str, Any]] = []
    index_to_symbol: dict[int, str] = {}

    for index in callable_indices:
        node = source_file.nodes[index]
        named = node.kind == "FN"
        name_info = direct_name(source_file, index) if named else None
        parent_callable = next(
            (
                ancestor_index
                for ancestor_index, ancestor in ancestors(source_file, index)
                if ancestor.kind in CALLABLE_NODE_KINDS
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
        cfg = cfg_attributes(source_file, index)
        if cfg:
            symbol = f"{symbol}@{' & '.join(cfg)}"
        index_to_symbol[index] = symbol
        body = body_for(source_file, index) if named else source_file.text(node)
        records.append(
            {
                "symbol": symbol,
                "name": display_name,
                "kind": kind,
                "crate": crate,
                "module": "::".join([crate, *modules]),
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
                "visibility": visibility(signature),
                "cfg": cfg,
                "retained_dependencies": retained_dependencies(signature),
                "ambient_dependencies": matches(body, AMBIENT_PATTERNS),
                "effects": matches(body, EFFECT_PATTERNS),
                "calls": [],
                "callees": [],
                "callers": [],
                "unresolved_calls": [],
            }
        )

    for node in source_file.nodes:
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
        record_index, _ = min(
            containing,
            key=lambda pair: source_file.nodes[pair[1]].end
            - source_file.nodes[pair[1]].start,
        )
        records[record_index]["calls"].append(
            {
                "kind": node.kind,
                "range": {
                    "start": source_file.position(node.start),
                    "end": source_file.position(node.end),
                },
                "text": call_text(source_file, node),
            }
        )
    return records


class RustAnalyzer:
    def __init__(self, root: Path):
        self.root = root
        self.next_id = 1
        ANALYZER_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        self.stderr = ANALYZER_LOG_PATH.open("wb")
        self.process = subprocess.Popen(
            ["rust-analyzer"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.stderr,
            env=rust_analyzer_environment(),
        )
        if self.process.stdin is None or self.process.stdout is None:
            raise RuntimeError("failed to open rust-analyzer pipes")
        self.stdin = self.process.stdin
        self.stdout = self.process.stdout

    @staticmethod
    def configuration() -> dict[str, Any]:
        return {
            "cargo": {"allTargets": True, "features": "all"},
            "procMacro": {"enable": True},
        }

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

    def close(self) -> None:
        try:
            self.request("shutdown", None)
            self.notify("exit", None)
            self.process.wait(timeout=10)
        finally:
            self.stderr.close()


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
        if record["kind"] not in {"closure", "async-block"}
        and range_contains(record["name_range"], position)
    ]
    if not candidates:
        candidates = [
            record
            for record in records_by_path.get(path, [])
            if range_contains(record["range"], position)
        ]
    return candidates[0]["symbol"] if candidates else None


def source_expression(
    source_files: dict[Path, SourceFile],
    path: Path,
    range_value: dict[str, Any],
) -> str:
    source_file = source_files.get(path)
    if source_file is None:
        return ""
    start = source_file.offset(range_value["start"])
    containing_calls = [
        node
        for node in source_file.nodes
        if node.kind in CALL_NODE_KINDS and node.start <= start <= node.end
    ]
    if not containing_calls:
        line = range_value["start"]["line"]
        begin = source_file.line_starts[line]
        end = (
            source_file.line_starts[line + 1]
            if line + 1 < len(source_file.line_starts)
            else len(source_file.data)
        )
        return source_file.slice(begin, end).strip()
    node = min(containing_calls, key=lambda value: value.end - value.start)
    return call_text(source_file, node)


def build_index() -> dict[str, Any]:
    source_files: dict[Path, SourceFile] = {}
    records: list[dict[str, Any]] = []
    for path in rust_sources():
        source_file = parse_source(path)
        source_files[path] = source_file
        records.extend(inventory_file(source_file))

    records_by_path: dict[Path, list[dict[str, Any]]] = {}
    records_by_symbol: dict[str, dict[str, Any]] = {}
    for record in records:
        path = (ROOT / record["path"]).resolve()
        records_by_path.setdefault(path, []).append(record)
        if record["symbol"] in records_by_symbol:
            raise RuntimeError(f"duplicate callable identity: {record['symbol']}")
        records_by_symbol[record["symbol"]] = record

    analyzer = RustAnalyzer(ROOT)
    analyzer.initialize()
    try:
        analyzer.wait_until_ready()
        opened_paths: set[Path] = set()
        named = [
            record
            for record in records
            if record["kind"] not in {"closure", "async-block"}
        ]
        for number, record in enumerate(named, start=1):
            path = (ROOT / record["path"]).resolve()
            if path not in opened_paths:
                analyzer.open_document(path, source_files[path].source)
                opened_paths.add(path)
            calls = analyzer.outgoing(path, record["name_range"]["start"])
            if calls is None:
                record["call_hierarchy"] = "unavailable"
                continue
            record["call_hierarchy"] = "resolved"
            matched_sites: list[dict[str, int]] = []
            for call in calls:
                target_item = call["to"]
                target = internal_symbol(records_by_path, target_item)
                if target is None:
                    target = (
                        f"external::{target_item.get('detail', '')}::"
                        f"{target_item.get('name', '<unknown>')}"
                    )
                sites = []
                for range_value in call.get("fromRanges", []):
                    matched_sites.append(range_value["start"])
                    sites.append(
                        {
                            "range": range_value,
                            "expression": source_expression(
                                source_files,
                                path,
                                range_value,
                            ),
                        }
                    )
                record["callees"].append({"symbol": target, "sites": sites})
            for call in record["calls"]:
                resolved = any(
                    range_contains(call["range"], site)
                    for site in matched_sites
                )
                if call["kind"] != "MACRO_CALL" and not resolved:
                    record["unresolved_calls"].append(call)
            if number % 250 == 0:
                print(f"indexed call hierarchy for {number}/{len(named)} callables")
    finally:
        analyzer.close()

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

    return {
        "schema": 1,
        "root": str(ROOT),
        "callables": sorted(records, key=lambda value: value["symbol"]),
    }


def write_index(index: dict[str, Any]) -> None:
    INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        dir=INDEX_PATH.parent,
        prefix="index.",
        suffix=".json",
        delete=False,
    ) as temporary:
        json.dump(index, temporary, indent=2, sort_keys=True)
        temporary.write("\n")
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, INDEX_PATH)


def read_index() -> dict[str, Any]:
    if not INDEX_PATH.exists():
        raise RuntimeError("ownership index is absent; run `ownership_audit.py inventory`")
    return json.loads(INDEX_PATH.read_text())


def read_decisions() -> dict[tuple[str, str], dict[str, Any]]:
    if not DECISIONS_PATH.exists():
        raise RuntimeError(f"decision ledger is absent: {DECISIONS_PATH}")
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


def command_inventory(_: argparse.Namespace) -> None:
    index = build_index()
    write_index(index)
    counts: dict[str, int] = {}
    for record in index["callables"]:
        counts[record["kind"]] = counts.get(record["kind"], 0) + 1
    print(f"wrote {INDEX_PATH}")
    print(json.dumps(counts, indent=2, sort_keys=True))


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
    decisions = read_decisions()
    missing = unclassified(index, decisions)
    if missing:
        raise RuntimeError(f"{len(missing)} callables have no durable disposition")
    unresolved = [
        record
        for record in index["callables"]
        if record["kind"] not in {"closure", "async-block"}
        and (
            record.get("call_hierarchy") != "resolved"
            or record["unresolved_calls"]
        )
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
    subcommands.add_parser("unclassified").set_defaults(run=command_unclassified)
    subcommands.add_parser("check").set_defaults(run=command_check)
    return value


def main() -> int:
    try:
        args = parser().parse_args()
        args.run(args)
        return 0
    except (OSError, RuntimeError, ValueError) as error:
        print(f"ownership audit failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
