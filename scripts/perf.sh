#!/bin/sh
# The timed scenarios, on demand: scripts/perf.sh list | run | accept.
# Explained in scripts/perf.py.
exec python3 "$(dirname "$0")/perf.py" "$@"
