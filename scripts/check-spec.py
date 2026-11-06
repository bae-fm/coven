#!/usr/bin/env python3
"""Keep the spec readable: check every markdown file directly under spec/.

    scripts/check-spec.py [spec directory]

The writing standard is spec/WRITING.md. This script checks the parts of it
a machine can see:

- every section reference (§12, §12.3), appendix reference (Appendix E.6, or
  E.6 alone), format.md section (D5.6) and api.md section (E13) names a
  heading that exists, and every link to a heading or a file resolves, using
  GitHub's heading anchors;
- every link into proofs/ before coven.md's Appendix G names theorems listed
  in a row of Appendix G marked *Body.*, so body citations are deliberate;
- every theorem a row of Appendix G cites is declared in a Lean file that row
  names, and every theorem Appendix G or a proof write-up (proofs/<package>.md)
  cites has its own `#print axioms` line in its package's Axioms.lean, so the
  proof check audits it by name;
- no prose line is longer than 100 characters (tables, code blocks and lines
  that hold only links are exempt);
- no paragraph or list is broken by two blank lines in a row.

Exit status 0 when every file passes, 1 with one line per finding otherwise.
"""

from pathlib import Path
import re
import sys

# The files the other documents refer into, which must exist.
REQUIRED = ("coven.md", "format.md", "api.md")
MAX_WIDTH = 100

FENCE = re.compile(r"^\s*(```|~~~)")
HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
EXPLICIT_ANCHOR = re.compile(r'<a\s+(?:id|name)="([^"]+)"')
INLINE_CODE = re.compile(r"`[^`\n]*`")
# A link's text may wrap onto the next line; its target may not.
LINK = re.compile(r"\[((?:[^\[\]]|\[[^\[\]]*\])*)\]\(([^()\s]+)\)")
SECTION_REF = re.compile(r"§(\d+(?:\.\d+)?)")
APPENDIX_REF = re.compile(r"\bAppendix ([A-Z])(?:\.(\d+))?")
DOTTED_REF = re.compile(r"(?<![\w.])([A-H])\.(\d+)\b")
FORMAT_REF = re.compile(r"(?<![\w.§])D(\d+)(?:\.(\d+))?\b")
API_REF = re.compile(r"(?<![\w.§])E(\d+)\b")


def slug(text):
    """GitHub's anchor for a heading: lowercase, punctuation dropped, spaces to hyphens."""
    text = INLINE_CODE.sub(lambda m: m.group(0)[1:-1], text)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"<[^>]+>", "", text).strip().lower()
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


class Document:
    """One markdown file: its lines, which are code, and its anchors."""

    def __init__(self, path):
        self.path = path
        self.text = path.read_text(encoding="utf-8")
        self.lines = self.text.split("\n")
        self.code = []
        inside = False
        for line in self.lines:
            fence = bool(FENCE.match(line))
            self.code.append(inside or fence)
            if fence:
                inside = not inside
        self.anchors = set()
        self.headings = []
        seen = {}
        for number, line in enumerate(self.lines, 1):
            if self.code[number - 1]:
                continue
            self.anchors.update(EXPLICIT_ANCHOR.findall(line))
            match = HEADING.match(line)
            if match:
                base = slug(match.group(2))
                count = seen.get(base, 0)
                seen[base] = count + 1
                self.anchors.add(base if count == 0 else f"{base}-{count}")
                self.headings.append((number, match.group(2)))

    def prose(self):
        """The text outside code blocks, with inline code blanked, line numbers kept."""
        kept = []
        for number, line in enumerate(self.lines, 1):
            if self.code[number - 1]:
                kept.append("")
            else:
                kept.append(INLINE_CODE.sub(lambda m: " " * len(m.group(0)), line))
        return "\n".join(kept)

    def line_of(self, offset, text=None):
        return (self.text if text is None else text).count("\n", 0, offset) + 1


def coven_sections(coven):
    """The section and appendix names coven.md defines: §5, §5.1, Appendix E, Appendix E.6."""
    names = set()
    for _, title in coven.headings:
        match = re.match(r"(\d+)\.\s", title) or re.match(r"(\d+\.\d+)\s", title)
        if match:
            names.add("§" + match.group(1))
        match = re.match(r"Appendix ([A-Z])\.\s", title)
        if match:
            names.add("Appendix " + match.group(1))
        match = re.match(r"([A-Z])\.(\d+)\s", title)
        if match:
            names.add(f"Appendix {match.group(1)}.{match.group(2)}")
    return names


def numbered(document, letter):
    """format.md's D sections (D5, D5.6) or api.md's E sections (E13)."""
    names = set()
    for _, title in document.headings:
        match = re.match(rf"{letter}(\d+(?:\.\d+)?)\s", title)
        if match:
            names.add(letter + match.group(1))
    return names


def check_references(document, sections, format_sections, api_sections, findings):
    prose = document.prose()
    # Link targets are checked below; their text is prose like any other.
    scan = LINK.sub(lambda m: "[" + m.group(1) + "]" + " " * (len(m.group(2)) + 2), prose)
    for match in SECTION_REF.finditer(scan):
        if "§" + match.group(1) not in sections:
            findings.append((document, document.line_of(match.start(), scan),
                             f"§{match.group(1)} names no section of coven.md"))
    for match in APPENDIX_REF.finditer(scan):
        name = "Appendix " + match.group(1) + (f".{match.group(2)}" if match.group(2) else "")
        if name not in sections:
            findings.append((document, document.line_of(match.start(), scan),
                             f"{name} names no appendix of coven.md"))
    for match in DOTTED_REF.finditer(scan):
        name = f"Appendix {match.group(1)}.{match.group(2)}"
        if name not in sections:
            findings.append((document, document.line_of(match.start(), scan),
                             f"{match.group(0)} names no appendix part of coven.md"))
    for match in FORMAT_REF.finditer(scan):
        name = "D" + match.group(1) + (f".{match.group(2)}" if match.group(2) else "")
        if name not in format_sections:
            findings.append((document, document.line_of(match.start(), scan),
                             f"{name} names no section of format.md"))
    for match in API_REF.finditer(scan):
        if "E" + match.group(1) not in api_sections:
            findings.append((document, document.line_of(match.start(), scan),
                             f"E{match.group(1)} names no section of api.md"))


def check_links(document, documents, findings):
    prose = document.prose()
    for match in LINK.finditer(prose):
        target = match.group(2)
        if re.match(r"[a-z][a-z0-9+.-]*:", target):
            continue
        line = document.line_of(match.start(2), prose)
        path, _, anchor = target.partition("#")
        file = (document.path.parent / path).resolve() if path else document.path.resolve()
        if not file.exists():
            findings.append((document, line, f"link to {target}: no such file"))
            continue
        if not anchor:
            continue
        if file.suffix != ".md" or file.is_dir():
            findings.append((document, line, f"link to {target}: only markdown has anchors"))
            continue
        linked = documents.get(file)
        if linked is None:
            linked = documents[file] = Document(file)
        if anchor not in linked.anchors:
            findings.append((document, line, f"link to {target}: no such heading or anchor"))


def body_citations(coven):
    """Appendix G's *Body.* rows: theorem name -> the files that row names."""
    listed = {}
    start = next((n for n, title in coven.headings if title.startswith("Appendix G.")), None)
    if start is None:
        return None, None
    end = next((n for n, title in coven.headings
                if n > start and title.startswith("Appendix ") and not title.startswith("Appendix G")),
               len(coven.lines) + 1)
    for line in coven.lines[start:end - 1]:
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if not line.lstrip().startswith("|") or len(cells) < 3 or not cells[0].startswith("*Body.*"):
            continue
        files = set(re.findall(r"proofs/[\w./-]+", cells[2]))
        for theorem in re.findall(r"`([^`]+)`", cells[1]):
            listed.setdefault(theorem, set()).update(files)
    return start, listed


def check_body_citations(coven, findings):
    start, listed = body_citations(coven)
    if start is None:
        findings.append((coven, 1, "no Appendix G heading to hold body proof citations"))
        return
    body = "\n".join(coven.lines[: start - 1])
    code = coven.code[: start - 1]
    for match in LINK.finditer(body):
        target = match.group(2)
        if not target.startswith("proofs/"):
            continue
        line = coven.line_of(match.start(), body)
        if code[line - 1]:
            continue
        theorems = re.findall(r"`([^`]+)`", match.group(1))
        path = target.partition("#")[0]
        if path.endswith(".lean") and not theorems:
            findings.append((coven, line, f"link to {target} names no theorem"))
        for theorem in theorems:
            if theorem not in listed:
                findings.append((coven, line,
                                 f"body cites `{theorem}`, which no *Body.* row of Appendix G lists"))
            elif path.endswith(".lean") and f"{path}" not in listed[theorem]:
                findings.append((coven, line,
                                 f"body cites `{theorem}` in {path}; its *Body.* row names "
                                 + ", ".join(sorted(listed[theorem]))))


LEAN_SCOPE = re.compile(r"^(namespace|section|mutual|end)\b[ \t]*(\S*)")
LEAN_DECLARATION = re.compile(
    r"^\s*(?:@\[[^\]]*\]\s*)?(?:noncomputable\s+)?(?:protected\s+)?(theorem|def|abbrev)\s+([^\s(:{\[]+)")


def lean_code(text):
    """Lean source with its comments blanked, line breaks kept; string literals are left alone."""
    out, depth, i = [], 0, 0
    while i < len(text):
        if depth == 0 and text[i] == '"':
            end = i + 1
            while end < len(text) and text[end] != '"':
                end += 2 if text[end] == "\\" else 1
            out.append(text[i:end + 1])
            i = end + 1
        elif text.startswith("/-", i):
            depth += 1
            i += 2
        elif depth and text.startswith("-/", i):
            depth -= 1
            i += 2
        elif depth == 0 and text.startswith("--", i):
            while i < len(text) and text[i] != "\n":
                i += 1
        else:
            out.append(text[i] if depth == 0 or text[i] == "\n" else " ")
            i += 1
    return "".join(out)


def lean_declarations(path):
    """A Lean file's public theorems and definitions: full name -> (kind, line)."""
    declared, scopes = {}, []
    for number, line in enumerate(lean_code(path.read_text(encoding="utf-8")).split("\n"), 1):
        scope = LEAN_SCOPE.match(line)
        if scope:
            if scope.group(1) == "end":
                if scopes:
                    scopes.pop()
            else:
                scopes.append(scope.group(2) if scope.group(1) == "namespace" else "")
            continue
        match = LEAN_DECLARATION.match(line)
        if match:
            name = ".".join([part for part in scopes if part] + [match.group(2)])
            declared[name] = ("theorem" if match.group(1) == "theorem" else "def", number)
    return declared


class ProofPackage:
    """One Lean package under proofs/: what its files declare and which theorems Axioms.lean audits."""

    def __init__(self, directory, axioms):
        self.directory = directory
        self.axioms = axioms.resolve()
        self.files = {}
        for path in sorted(directory.rglob("*.lean")):
            if ".lake" not in path.relative_to(directory).parts:
                self.files[path.resolve()] = lean_declarations(path)
        names = {name for declared in self.files.values() for name in declared}
        text = lean_code(axioms.read_text(encoding="utf-8"))
        opened = [name for line in re.findall(r"^open\s+(.+)$", text, re.M) for name in line.split()]
        self.audited = set()
        for printed in re.findall(r"^#print\s+axioms\s+(\S+)", text, re.M):
            for candidate in [printed] + [f"{namespace}.{printed}" for namespace in opened]:
                if candidate in names:
                    self.audited.add(candidate)
                    break

    def theorems(self, cited, files=None):
        """The full names of the theorems `cited` can name, in `files` or anywhere in the package."""
        return sorted(name for path, declared in self.files.items()
                      if files is None or path in files
                      for name, (kind, _) in declared.items()
                      if kind == "theorem" and (name == cited or name.endswith("." + cited)))


def proof_packages(spec):
    packages = {}
    proofs = spec / "proofs"
    for directory in sorted(proofs.iterdir()) if proofs.is_dir() else []:
        axioms = sorted(directory.glob("*/Axioms.lean")) if directory.is_dir() else []
        if axioms:
            packages[directory.name] = ProofPackage(directory, axioms[0])
    return packages


def check_audits(coven, writeups, packages, findings):
    """Each theorem Appendix G or a write-up cites is declared where cited and audited by name."""
    proofs = (coven.path.parent / "proofs").resolve()

    def unaudited(document, line, package, names):
        for name in names:
            if name not in package.audited:
                findings.append((document, line, f"cites `{name}`, which "
                                 f"{package.axioms.relative_to(proofs.parent)} has no "
                                 "`#print axioms` line for"))

    start = next((n for n, title in coven.headings if title.startswith("Appendix G.")), None)
    end = next((n for n, title in coven.headings
                if start and n > start and title.startswith("Appendix ")
                and not title.startswith("Appendix G")), len(coven.lines) + 1)
    for number in range(start or len(coven.lines), end - 1):
        line = coven.lines[number]
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if not line.lstrip().startswith("|") or len(cells) < 3:
            continue
        files = {(coven.path.parent / path).resolve()
                 for path in re.findall(r"proofs/[\w./-]+\.lean", cells[2])}
        if not files:
            continue
        package = packages.get(next(iter(files)).relative_to(proofs).parts[0])
        if package is None:
            findings.append((coven, number + 1, "cites a proof package with no Axioms.lean"))
            continue
        for cited in re.findall(r"`([^`]+)`", cells[1]):
            names = package.theorems(cited, files)
            if not names:
                findings.append((coven, number + 1, f"cites `{cited}`, which "
                                 + ", ".join(sorted(str(path.relative_to(proofs.parent))
                                                    for path in files))
                                 + " declares no theorem of that name"))
            unaudited(coven, number + 1, package, names)
    for name, document in writeups.items():
        package = packages.get(name)
        if package is None:
            continue
        for match in re.finditer(r"`([A-Za-z_][\w.']*)`", document.text):
            unaudited(document, document.line_of(match.start()), package,
                      package.theorems(match.group(1)))


def only_links(line):
    """Whether a line holds nothing but links (whole, or the ends of one wrapped from a neighbour)."""
    rest = LINK.sub("", line)
    rest = re.sub(r"^[^\[]*\]\([^()\s]+\)", "", rest)
    rest = re.sub(r"\[[^\]]*$", "", rest)
    return line.strip() != "" and re.fullmatch(r"[\s\-*+().,;:\d]*", rest) is not None


def check_layout(document, findings):
    blank_run = 0
    for number, line in enumerate(document.lines, 1):
        code = document.code[number - 1]
        if not code and len(line) > MAX_WIDTH and not line.lstrip().startswith("|") \
                and not only_links(line):
            findings.append((document, number, f"line is {len(line)} characters; wrap prose at "
                             f"{MAX_WIDTH}"))
        if not code and line.strip() == "":
            blank_run += 1
            if blank_run == 2:
                findings.append((document, number, "two blank lines in a row"))
        else:
            blank_run = 0


def main(argv):
    if len(argv) > 2:
        print("usage: scripts/check-spec.py [spec directory]", file=sys.stderr)
        return 2
    spec = Path(argv[1]) if len(argv) == 2 else Path(__file__).resolve().parent.parent / "spec"
    documents = {}
    for name in REQUIRED:
        if not (spec / name).exists():
            print(f"{spec / name}: missing", file=sys.stderr)
            return 1
    names = sorted(path.name for path in spec.glob("*.md"))
    for name in names:
        path = (spec / name).resolve()
        documents[path] = Document(path)
    coven = documents[(spec / "coven.md").resolve()]
    sections = coven_sections(coven)
    format_sections = numbered(documents[(spec / "format.md").resolve()], "D")
    api_sections = numbered(documents[(spec / "api.md").resolve()], "E")
    findings = []
    for name in names:
        document = documents[(spec / name).resolve()]
        check_references(document, sections, format_sections, api_sections, findings)
        check_links(document, documents, findings)
        check_layout(document, findings)
    check_body_citations(coven, findings)
    writeups = {path.stem: Document(path.resolve())
                for path in sorted((spec / "proofs").glob("*.md"))}
    check_audits(coven, writeups, proof_packages(spec), findings)
    for document, line, message in sorted(findings, key=lambda f: (str(f[0].path), f[1])):
        print(f"{document.path.relative_to(spec.resolve().parent)}:{line}: {message}")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
