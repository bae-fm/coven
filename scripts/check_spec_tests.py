"""Exercise scripts/check-spec.py on small spec directories."""

from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest

SCRIPT = Path(__file__).with_name("check-spec.py")

COVEN = """\
# coven

See §1, §1.1, Appendix G, Appendix G.1, G.1, format.md D1 and api.md E1.
Links: [the problem](#1-the-problem), [rules](format.md#d1-rules),
[opening](api.md#e1-opening) and [§1.1](#11-a-part).

### 1. The problem

A body citation: [`kept_once`](proofs/model/Claim.lean).

#### 1.1 A part

Text.

## Appendix G. What's proved

### G.1 Rows

| Claim | Theorems | File | Section |
|---|---|---|---|
| *Body.* A claim lands once | `kept_once` | `proofs/model/Claim.lean` | §1 |
| A listed claim | `listed_only` | `proofs/model/Claim.lean` | §1.1 |
"""

FORMAT = """\
# Format

## D1 Rules

See §1 and [§1.1](coven.md#11-a-part).
"""

API = """\
## Appendix E. API

### E1 Opening
"""


def run(coven=COVEN, format_md=FORMAT):
    with tempfile.TemporaryDirectory() as tmp:
        spec = Path(tmp) / "spec"
        (spec / "proofs" / "model").mkdir(parents=True)
        (spec / "proofs" / "model" / "Claim.lean").write_text("theorem kept_once : True := trivial\n")
        (spec / "coven.md").write_text(coven)
        (spec / "format.md").write_text(format_md)
        (spec / "api.md").write_text(API)
        result = subprocess.run(
            [sys.executable, str(SCRIPT), str(spec)], capture_output=True, text=True, check=False
        )
        return result.returncode, result.stdout


class CheckSpec(unittest.TestCase):
    def test_a_consistent_spec_passes(self):
        self.assertEqual(run(), (0, ""))

    def test_unresolved_references_and_links_are_reported(self):
        coven = COVEN.replace("Text.", textwrap.dedent("""\
            See §2, §1.2, Appendix H, G.2, D9 and E7.
            [gone](#nowhere), [other](format.md#d2-types), [file](proofs/none.lean)."""))
        status, out = run(coven)
        self.assertEqual(status, 1)
        for expected in ("§2 names no section", "§1.2 names no section", "Appendix H names no appendix",
                         "G.2 names no appendix part", "D9 names no section of format.md",
                         "E7 names no section of api.md", "link to #nowhere: no such heading",
                         "link to format.md#d2-types: no such heading",
                         "link to proofs/none.lean: no such file"):
            self.assertIn(expected, out)

    def test_body_proof_links_must_be_listed_as_body_rows(self):
        status, out = run(COVEN.replace("Text.", "[`listed_only`](proofs/model/Claim.lean)."))
        self.assertEqual(status, 1)
        self.assertIn("body cites `listed_only`, which no *Body.* row of Appendix G lists", out)
        status, out = run(COVEN.replace("Text.", "[the model](proofs/model/Claim.lean)."))
        self.assertIn("names no theorem", out)

    def test_long_prose_lines_fail_but_tables_code_and_links_do_not(self):
        long_prose = "word " * 21
        exempt = "\n".join([
            "| " + "cell " * 30 + "|",
            "```",
            "code " * 30,
            "```",
            "  ([`kept_once`, `also_kept_once_with_a_name_long_enough_to_pass_the_limit_by_itself`]"
            "(proofs/model/Claim.lean)).",
        ])
        status, out = run(COVEN.replace("Text.", exempt))
        self.assertEqual(out.count("characters; wrap prose"), 0, out)
        status, out = run(COVEN.replace("Text.", long_prose))
        self.assertEqual(status, 1)
        self.assertIn("line is 105 characters", out)

    def test_two_blank_lines_break_a_paragraph(self):
        status, out = run(format_md=FORMAT.replace("## D1 Rules\n\n", "## D1 Rules\n\n\n"))
        self.assertEqual(status, 1)
        self.assertIn("format.md:5: two blank lines in a row", out)


if __name__ == "__main__":
    unittest.main()
