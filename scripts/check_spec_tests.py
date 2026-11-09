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

A body citation: [`kept_once`](proofs/model/Model/Claim.lean).

#### 1.1 A part

Text.

## Appendix G. What's proved

### G.1 Rows

| Claim | Theorems | File | Section |
|---|---|---|---|
| *Body.* A claim lands once | `kept_once` | `proofs/model/Model/Claim.lean` | §1 |
| A listed claim | `listed_only` | `proofs/model/Model/Claim.lean` | §1.1 |
"""

FORMAT = """\
# Format

## D1 Rules

See §1 and [§1.1](coven.md#11-a-part).
"""

API = """\
# API

### E1 Opening
"""


CLAIM = """\
/- A comment naming `namespace Hidden`, which opens nothing.
   /- nested -/ theorem hidden_in_a_comment : True := trivial -/
namespace Model

theorem kept_once : True := trivial -- the body's citation

namespace Claim
theorem listed_only : True := trivial
end Claim

section
theorem read_through : True := trivial
end

end Model
"""

AXIOMS = """\
import Model
open Model

#print axioms kept_once
#print axioms Model.Claim.listed_only
#print axioms read_through
"""

WRITEUP = """\
# Model

[Claim.lean](model/Model/Claim.lean) proves `read_through`; `not_a_theorem` is code.
"""


def run(coven=COVEN, format_md=FORMAT, claim=CLAIM, axioms=AXIOMS, writeup=WRITEUP):
    with tempfile.TemporaryDirectory() as tmp:
        spec = Path(tmp) / "spec"
        (spec / "proofs" / "model" / "Model").mkdir(parents=True)
        # The spec is UTF-8 on every platform, whatever the locale's encoding.
        for path, text in [
            (spec / "proofs" / "model" / "Model" / "Claim.lean", claim),
            (spec / "proofs" / "model" / "Model" / "Axioms.lean", axioms),
            (spec / "proofs" / "model.md", writeup),
            (spec / "coven.md", coven),
            (spec / "format.md", format_md),
            (spec / "api.md", API),
        ]:
            path.write_text(text, encoding="utf-8")
        result = subprocess.run(
            [sys.executable, str(SCRIPT), str(spec)],
            capture_output=True, encoding="utf-8", check=False,
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
        status, out = run(COVEN.replace("Text.", "[`listed_only`](proofs/model/Model/Claim.lean)."))
        self.assertEqual(status, 1)
        self.assertIn("body cites `listed_only`, which no *Body.* row of Appendix G lists", out)
        status, out = run(COVEN.replace("Text.", "[the model](proofs/model/Model/Claim.lean)."))
        self.assertIn("names no theorem", out)

    def test_cited_theorems_are_declared_where_cited_and_audited(self):
        status, out = run(axioms=AXIOMS.replace("#print axioms Model.Claim.listed_only\n", ""))
        self.assertEqual(status, 1)
        self.assertIn("spec/coven.md:22: cites `Model.Claim.listed_only`, which "
                      "proofs/model/Model/Axioms.lean has no `#print axioms` line for", out)
        status, out = run(axioms=AXIOMS.replace("#print axioms read_through\n", ""))
        self.assertEqual(status, 1)
        self.assertIn("spec/proofs/model.md:3: cites `Model.read_through`", out)
        status, out = run(COVEN.replace("`listed_only`", "`hidden_in_a_comment`"))
        self.assertEqual(status, 1)
        self.assertIn("cites `hidden_in_a_comment`, which proofs/model/Model/Claim.lean "
                      "declares no theorem of that name", out)
        status, out = run(claim=CLAIM.replace("namespace Claim\n", "").replace("end Claim\n", ""))
        self.assertEqual(status, 1)
        self.assertIn("cites `Model.listed_only`", out)

    def test_long_prose_lines_fail_but_tables_code_and_links_do_not(self):
        long_prose = "word " * 21
        exempt = "\n".join([
            "| " + "cell " * 30 + "|",
            "```",
            "code " * 30,
            "```",
            "  ([`kept_once`, `also_kept_once_with_a_name_long_enough_to_pass_the_limit_by_itself`]"
            "(proofs/model/Model/Claim.lean)).",
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
