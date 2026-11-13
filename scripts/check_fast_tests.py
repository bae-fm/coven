"""Exercise the fast-check entry point with controlled Cargo commands."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

# In forward slashes, which the script's `dirname "$0"` splits on every platform.
SCRIPT = Path(__file__).with_name("check-fast.sh").as_posix()
# The bash on PATH, the one running these checks. Named bare, Windows would
# start System32's bash.exe, which runs WSL: CreateProcess looks there first.
BASH = shutil.which("bash")
if BASH is None:
    raise RuntimeError("no bash on PATH")


def tool(directory, name, body):
    """A stand-in for the command `name` in `directory`, run by bash. Its lines
    end in LF on every platform: bash would read a CR as part of each line."""
    path = directory / name
    path.write_text("#!/bin/bash\n" + body, newline="\n")
    path.chmod(0o755)


class FastChecks(unittest.TestCase):
    def test_checker_precedes_compilation_and_stops_on_failure(self):
        for checker_status in (0, 1):
            with self.subTest(checker_status=checker_status), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp)
                tool(
                    directory,
                    "cargo",
                    'echo "$*" >> "$CARGO_CALLS"\n'
                    'if [ "$1" = run ]; then exit "$CHECKER_STATUS"; fi\n',
                )
                calls = directory / "calls"
                result = subprocess.run(
                    [BASH, SCRIPT],
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
                self.assertEqual(result.returncode, checker_status, result.stderr)
                commands = calls.read_text().splitlines()
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

    def test_quick_mode_lints_what_the_landing_lints_and_a_bad_mode_fails(self):
        for mode, expected_status, expected_tail in (
            ("quick", 0, ["clippy -p coven-storage --all-targets --all-features -- -D warnings", "check -p coven-storage"]),
            ("fast", 1, None),
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp)
                tool(directory, "cargo", 'echo "$*" >> "$CARGO_CALLS"\n')
                calls = directory / "calls"
                result = subprocess.run(
                    [BASH, SCRIPT],
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
                    self.assertEqual(commands[-2:], expected_tail)

    def test_machine_wide_mode_comes_from_builds_and_defaults_to_strict(self):
        for builds_body, expected_first in (
            ("echo quick", "clippy -p coven-storage --all-targets --all-features -- -D warnings"),
            ("echo strict", "clippy -p coven-storage --all-targets --all-features -- -D warnings"),
            ("exit 1", "clippy -p coven-storage --all-targets --all-features -- -D warnings"),
        ):
            with self.subTest(builds=builds_body), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp)
                tool(directory, "cargo", 'echo "$*" >> "$CARGO_CALLS"\n')
                tool(directory, "builds", builds_body + "\n")
                calls = directory / "calls"
                env = {k: v for k, v in os.environ.items() if k != "HOOK_MODE"}
                result = subprocess.run(
                    [BASH, SCRIPT],
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
