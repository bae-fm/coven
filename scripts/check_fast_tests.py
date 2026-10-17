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
                        "PATH": f"{directory}:{os.environ['PATH']}",
                        "CARGO_CALLS": str(calls),
                        "CHECKER_STATUS": str(checker_status),
                        "COVEN_CHECK_PACKAGES": "coven-storage",
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
                    expected.append("clippy -p coven-storage --all-targets --all-features -- -D warnings")
                self.assertEqual(commands, expected)


if __name__ == "__main__":
    unittest.main()
