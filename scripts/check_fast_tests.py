"""Exercise the fast-check entry point with controlled Cargo commands."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class FastChecks(unittest.TestCase):
    def test_checker_precedes_compilation_and_stops_on_failure(self):
        script = Path(__file__).with_name("check-fast.sh")
        for checker_status in (0, 1):
            with self.subTest(checker_status=checker_status), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp)
                cargo = directory / "cargo"
                cargo.write_text(
                    '#!/bin/bash\n'
                    'echo "$*" >> "$CARGO_CALLS"\n'
                    'if [ "$1" = run ]; then exit "$CHECKER_STATUS"; fi\n'
                )
                cargo.chmod(0o755)
                calls = directory / "calls"
                result = subprocess.run(
                    ["bash", str(script)],
                    env={
                        **os.environ,
                        "HOME": str(directory),
                        "PATH": os.pathsep.join([str(directory), os.environ["PATH"]]),
                        "CARGO_CALLS": str(calls),
                        "CHECKER_STATUS": str(checker_status),
                        "COVEN_CHECK_PACKAGES": "coven-storage",
                        "HOOK_MODE": "strict",
                    },
                    capture_output=True,
                    text=True,
                    check=False,
                )
                commands = calls.read_text().splitlines()
                self.assertEqual(result.returncode, checker_status, result.stderr)
                expected = [
                    "fmt --all --check",
                    "run --quiet -p owner-construction-check -- . coven-storage",
                ]
                if checker_status == 0:
                    expected += [
                        "deny check advisories bans licenses",
                        "clippy -p coven-storage --all-targets --all-features"
                        " -- -D warnings",
                        "check -p coven-storage",
                    ]
                self.assertEqual(commands, expected)

    def test_quick_mode_checks_instead_of_linting_and_a_bad_mode_fails(self):
        script = Path(__file__).with_name("check-fast.sh")
        for mode, expected_status, expected_tail in (
            ("quick", 0, ["check -p coven-storage --all-targets --all-features"]),
            ("fast", 1, None),
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp)
                cargo = directory / "cargo"
                cargo.write_text('#!/bin/bash\necho "$*" >> "$CARGO_CALLS"\n')
                cargo.chmod(0o755)
                calls = directory / "calls"
                result = subprocess.run(
                    ["bash", str(script)],
                    env={
                        **os.environ,
                        "HOME": str(directory),
                        "PATH": os.pathsep.join([str(directory), os.environ["PATH"]]),
                        "CARGO_CALLS": str(calls),
                        "COVEN_CHECK_PACKAGES": "coven-storage",
                        "HOOK_MODE": mode,
                    },
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(result.returncode, expected_status, result.stderr)
                if expected_tail is None:
                    self.assertFalse(calls.exists())
                    self.assertIn("HOOK_MODE must be", result.stderr)
                else:
                    commands = calls.read_text().splitlines()
                    self.assertEqual(commands[-1:], expected_tail)
                    self.assertFalse([c for c in commands if c.startswith("clippy")])
                    self.assertNotIn("check -p coven-storage", commands)

    def test_machine_wide_mode_comes_from_builds_and_defaults_to_strict(self):
        script = Path(__file__).with_name("check-fast.sh")
        for builds_body, expected_first in (
            ("echo quick", "check -p coven-storage --all-targets --all-features"),
            ("echo strict", "clippy -p coven-storage --all-targets --all-features -- -D warnings"),
            ("exit 1", "clippy -p coven-storage --all-targets --all-features -- -D warnings"),
        ):
            with self.subTest(builds=builds_body), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp)
                for name, body in (
                    ("cargo", 'echo "$*" >> "$CARGO_CALLS"\n'),
                    ("builds", builds_body + "\n"),
                ):
                    tool = directory / name
                    tool.write_text("#!/bin/bash\n" + body)
                    tool.chmod(0o755)
                calls = directory / "calls"
                env = {k: v for k, v in os.environ.items() if k != "HOOK_MODE"}
                result = subprocess.run(
                    ["bash", str(script)],
                    env={
                        **env,
                        "HOME": str(directory),
                        "PATH": os.pathsep.join([str(directory), os.environ["PATH"]]),
                        "CARGO_CALLS": str(calls),
                        "COVEN_CHECK_PACKAGES": "coven-storage",
                    },
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(expected_first, calls.read_text().splitlines())


if __name__ == "__main__":
    unittest.main()
