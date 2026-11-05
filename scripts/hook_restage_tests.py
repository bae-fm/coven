"""Exercise the pre-commit hook's restage step in a temporary git repo."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

LIB = Path(__file__).with_name("hooks") / "restage-formatted.sh"
CLEAN = "fn main() {}\n"
UGLY = "fn  main( ) {}\n"


# The hook runs with GIT_DIR and GIT_INDEX_FILE set; without scrubbing them
# these tests would act on the real repository.
CLEAN_ENV = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


def git(repo, *args):
    return subprocess.run(["git", *args], cwd=repo, env=CLEAN_ENV, check=True, capture_output=True, text=True).stdout


def run_hook_step(repo, mutate):
    """Snapshot, apply `mutate` (standing in for cargo fmt), then restage."""
    script = f'source "{LIB}"; snapshot_staged_files; {mutate}; restage_and_fail_if_formatted "cargo fmt"'
    return subprocess.run(["bash", "-c", script], cwd=repo, env=CLEAN_ENV, capture_output=True, text=True)


class Restage(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.repo = Path(self._tmp.name)
        git(self.repo, "init", "-q")
        git(self.repo, "config", "user.email", "t@example.com")
        git(self.repo, "config", "user.name", "t")
        (self.repo / "a b.rs").write_text(CLEAN)
        (self.repo / "a.rs").write_text(CLEAN)
        git(self.repo, "add", ".")
        git(self.repo, "commit", "-qm", "base")

    def staged(self):
        return git(self.repo, "diff", "--cached", "--name-only").splitlines()

    def test_partially_staged_clean_file_is_left_alone(self):
        path = self.repo / "a b.rs"
        path.write_text("fn main() { let _x = 1; }\n")
        git(self.repo, "add", "a b.rs")
        path.write_text("fn main() { let _x = 1; }\n// unstaged\n")
        result = run_hook_step(self.repo, "true")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(git(self.repo, "show", ":a b.rs"), "fn main() { let _x = 1; }\n")

    def test_fully_staged_misformatted_file_is_restaged_and_fails(self):
        path = self.repo / "a b.rs"
        path.write_text(UGLY)
        git(self.repo, "add", "a b.rs")
        result = run_hook_step(self.repo, f'printf %s "{CLEAN}" > "a b.rs"')
        self.assertEqual(result.returncode, 1)
        self.assertEqual(git(self.repo, "show", ":a b.rs"), CLEAN)
        self.assertIn("a b.rs", result.stdout)

    def test_partially_staged_misformatted_file_is_not_added_and_fails_naming_it(self):
        path = self.repo / "a b.rs"
        path.write_text(UGLY)
        git(self.repo, "add", "a b.rs")
        path.write_text(UGLY + "// unstaged\n")
        result = run_hook_step(self.repo, f'printf %s "{CLEAN}// unstaged\n" > "a b.rs"')
        self.assertEqual(result.returncode, 1)
        self.assertEqual(git(self.repo, "show", ":a b.rs"), UGLY)
        self.assertIn("by hand", result.stdout)
        self.assertIn("a b.rs", result.stdout)

    def test_unstaged_file_the_formatter_touches_stays_unstaged(self):
        (self.repo / "a b.rs").write_text("fn main() { let _x = 1; }\n")
        git(self.repo, "add", "a b.rs")
        (self.repo / "a.rs").write_text(UGLY)
        result = run_hook_step(self.repo, 'printf "fn main() {}\n// f\n" > a.rs')
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(self.staged(), ["a b.rs"])


if __name__ == "__main__":
    unittest.main()
