#!/usr/bin/env python3
"""The performance suite: coven's timed measurements, compared with baselines.

    scripts/perf.sh list
    scripts/perf.sh run [--repeat N] [scenario...]
    scripts/perf.sh accept [scenario...]

A scenario is one of coven-database's ignored measurements (the `SCENARIOS` table:
its cargo command and how its numbers are read). They run on a person's
machine on demand, in the release profile, on an otherwise idle machine; no
runner holds them and GitHub never does.

`run` runs each scenario's command `--repeat` times (3 by default). A number's
value is the median over those runs, its spread is (largest - smallest) /
median. Every number is a cost, so lower is better. It is compared with the
baseline in `perf/baselines/<machine>.json`, committed per machine, so
accepting a slower number takes a commit:

  better        below the baseline by more than the tolerance
  within        inside the tolerance
  slower        above the baseline by more than the tolerance (exit status 1)
  no baseline   the scenario's number has none yet (exit status 0)
  not compared  the machine's one-minute load was over half its cores when the
                scenario started; the numbers are shown and recorded, and
                `accept` refuses them

The tolerance is 25%, widened by the baseline's own spread. The last run
is kept in `target/perf/last-run.json` (build output, not the repo) with each
scenario's raw output beside it. `accept` copies the last run's medians and
spreads into the machine's baseline file for the person to commit.

The numbers are read from the lines the measurements print through
`coven_database::test_utils::report_cost`:
`COST metric=<name> value=<n> unit=<ms|ratio|count>`.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import statistics
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASELINES = ROOT / "perf" / "baselines"
LAST_RUN = ROOT / "target" / "perf" / "last-run.json"

TOLERANCE = 0.25
LOAD_SHARE = 0.5
DEFAULT_REPEAT = 3

BETTER, WITHIN, SLOWER, NO_BASELINE, NOT_COMPARED = (
    "better",
    "within",
    "slower",
    "no baseline",
    "not compared",
)

COST = re.compile(r"^COST metric=(\S+) value=(\S+) unit=(\S+)$")


@dataclass(frozen=True)
class Scenario:
    name: str
    # The arguments to scripts/cargo.sh.
    cargo: tuple[str, ...]
    summary: str


def test_command(target: tuple[str, ...], filter_: str) -> tuple[str, ...]:
    return (
        "test", "--release", "-p", "coven-database", "--features", "test-utils", *target,
        "--", *([filter_] if filter_ else []), "--ignored", "--nocapture", "--test-threads", "1",
    )  # fmt: skip


LIB = ("--lib",)

SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        "app-statements",
        test_command(LIB, "a_triggered_statements_cost"),
        "an app statement whose triggers reach a view, against plain SQLite",
    ),
    Scenario(
        "local-table-writes",
        test_command(LIB, "local_table_writes_against_plain_sqlite"),
        "filling and clearing a device-local table, against plain SQLite",
    ),
    Scenario(
        "joined-live-query",
        test_command(LIB, "joined_runs_against_one_shot_reads"),
        "a live run of a joined query, against a one-shot read",
    ),
    Scenario(
        "statistics",
        test_command(LIB, "gathering_statistics_on_a_library_sized_store"),
        "gathering statistics on a library-sized store",
    ),
    Scenario(
        "commits-under-live-query",
        test_command(LIB, "commits_under_a_rerunning_live_query_cost"),
        "a writer's commits while a live query reruns or a reader reads",
    ),
    Scenario(
        "sending-state",
        test_command(LIB, "reading_what_is_being_sent_costs"),
        "reading the upload queue's sending state",
    ),
    Scenario(
        "queue-by-release",
        test_command(LIB, "reading_the_queue_by_release_costs"),
        "reading the upload queue by release",
    ),
    Scenario(
        "unpinned-releases",
        test_command(LIB, "counting_unpinned_releases_costs"),
        "counting unpinned releases, and keeping the count",
    ),
    Scenario(
        "store-size",
        test_command(LIB, "ten_thousand_row_store_size"),
        "the size in bytes of a 10,000-row store (unit count)",
    ),
    Scenario(
        "write-cost-per-row",
        test_command(LIB, "write_cost_per_row_as_the_table_grows"),
        "a write's cost as a synced table grows",
    ),
    Scenario(
        "insert-cost",
        test_command(("--test", "insert_cost"), ""),
        "a synced insert, a release import and joined live queries",
    ),
    Scenario(
        "bulk-write-cost",
        test_command(("--test", "bulk_write_cost"), ""),
        "filling and clearing a device-local table through a write and a bulk write",
    ),
)


def parse_numbers(output: str) -> dict[str, tuple[float, str]]:
    """Each number a measurement printed: metric -> (value, unit).

    A metric printed twice keeps its last value."""
    numbers: dict[str, tuple[float, str]] = {}
    for line in output.splitlines():
        line = line.strip()
        if match := COST.match(line):
            numbers[match.group(1)] = (float(match.group(2)), match.group(3))
    return numbers


def summarize(runs: list[dict[str, tuple[float, str]]]) -> dict[str, dict]:
    """Median and spread of each metric over the repeated runs of a scenario."""
    summary: dict[str, dict] = {}
    for metric in sorted(set().union(*runs)):
        values = [run[metric][0] for run in runs if metric in run]
        unit = next(run[metric][1] for run in runs if metric in run)
        median = statistics.median(values)
        spread = (max(values) - min(values)) / median if median else 0.0
        summary[metric] = {"median": median, "spread": spread, "unit": unit}
    return summary


def compare(
    value: float, baseline: dict | None, load: float, cores: int
) -> str:
    """How a number stands against its baseline entry (median, spread)."""
    if load > cores * LOAD_SHARE:
        return NOT_COMPARED
    if baseline is None:
        return NO_BASELINE
    tolerance = TOLERANCE + baseline["spread"]
    if value > baseline["median"] * (1 + tolerance):
        return SLOWER
    if value < baseline["median"] * (1 - tolerance):
        return BETTER
    return WITHIN


def machine_name() -> str:
    """A stable name for this kind of machine: its model and core count."""
    model = ""
    if sys.platform == "darwin":
        model = subprocess.run(
            ["sysctl", "-n", "hw.model"], capture_output=True, text=True, check=False
        ).stdout.strip()
    model = model or f"{platform.system()}-{platform.machine()}"
    return re.sub(r"[^A-Za-z0-9._]+", "-", f"{model}-{os.cpu_count()}cores")


def load_json(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def chosen(names: list[str]) -> list[Scenario]:
    known = {scenario.name: scenario for scenario in SCENARIOS}
    unknown = [name for name in names if name not in known]
    if unknown:
        sys.exit(f"unknown scenario {', '.join(unknown)}; `scripts/perf.sh list` names them")
    return [known[name] for name in names] if names else list(SCENARIOS)


def run_once(scenario: Scenario) -> tuple[int, str]:
    process = subprocess.run(
        [str(ROOT / "scripts" / "cargo.sh"), *scenario.cargo],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
        check=False,
    )
    return process.returncode, process.stdout


def command_list(_: argparse.Namespace) -> int:
    for scenario in SCENARIOS:
        print(f"{scenario.name:26} {scenario.summary}")
        print(f"{'':26} scripts/cargo.sh {' '.join(scenario.cargo)}")
    return 0


def command_run(args: argparse.Namespace) -> int:
    machine = machine_name()
    baselines = load_json(BASELINES / f"{machine}.json").get("scenarios", {})
    cores = os.cpu_count() or 1
    last = {"machine": machine, "scenarios": {}}
    slower = 0
    print(f"machine {machine}")
    for scenario in chosen(args.scenarios):
        load = os.getloadavg()[0]
        print(f"\n{scenario.name} (load {load:.2f} of {cores} cores)")
        runs, logs = [], []
        for _ in range(args.repeat):
            status, output = run_once(scenario)
            logs.append(output)
            if status != 0:
                sys.stderr.write(output[-4000:])
                sys.exit(f"{scenario.name}: cargo exited {status}")
            runs.append(parse_numbers(output))
        LAST_RUN.parent.mkdir(parents=True, exist_ok=True)
        (LAST_RUN.parent / f"{scenario.name}.log").write_text("\n".join(logs))
        if not any(runs):
            sys.exit(f"{scenario.name}: the run printed no numbers")
        numbers = summarize(runs)
        recorded = {}
        for metric, number in numbers.items():
            baseline = baselines.get(scenario.name, {}).get(metric)
            verdict = compare(number["median"], baseline, load, cores)
            slower += verdict == SLOWER
            against = (
                f" (baseline {baseline['median']:.4g})" if baseline else ""
            )
            print(f"  {verdict:12} {metric}: {number['median']:.4g} {number['unit']}{against}")
            recorded[metric] = number
        last["scenarios"][scenario.name] = {
            "load": load,
            "cores": cores,
            "compared": load <= cores * LOAD_SHARE,
            "numbers": recorded,
        }
        LAST_RUN.write_text(json.dumps(last, indent=2) + "\n")
    return 1 if slower else 0


def command_accept(args: argparse.Namespace) -> int:
    last = load_json(LAST_RUN)
    if not last:
        sys.exit("no last run; `scripts/perf.sh run` first")
    if last["machine"] != machine_name():
        sys.exit(f"the last run was on {last['machine']}, not this machine")
    names = [scenario.name for scenario in chosen(args.scenarios)] if args.scenarios else list(
        last["scenarios"]
    )
    path = BASELINES / f"{last['machine']}.json"
    baseline = load_json(path) or {"machine": last["machine"], "scenarios": {}}
    for name in names:
        ran = last["scenarios"].get(name)
        if ran is None:
            sys.exit(f"{name}: not in the last run")
        if not ran["compared"]:
            sys.exit(
                f"{name}: the last run was recorded as not compared "
                f"(load {ran['load']:.2f} of {ran['cores']} cores); run it again on a quiet machine"
            )
    for name in names:
        baseline["scenarios"][name] = last["scenarios"][name]["numbers"]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n")
    print(f"wrote {', '.join(names)} to {path.relative_to(ROOT)}; commit it")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="the scenarios").set_defaults(run=command_list)
    run = commands.add_parser("run", help="run scenarios and compare with the baseline")
    run.add_argument("--repeat", type=int, default=DEFAULT_REPEAT, help="runs per scenario")
    run.add_argument("scenarios", nargs="*")
    run.set_defaults(run=command_run)
    accept = commands.add_parser("accept", help="write the last run's numbers as the baseline")
    accept.add_argument("scenarios", nargs="*")
    accept.set_defaults(run=command_accept)
    args = parser.parse_args()
    return args.run(args)


if __name__ == "__main__":
    sys.exit(main())
