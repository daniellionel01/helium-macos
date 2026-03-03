#!/usr/bin/env python3

"""Compare deterministic recording manifests from repeated runs.

Each manifest must be a JSONL file with at least these fields per row:
  - step_id
  - virtual_time_us
  - frame_hash

The script compares one baseline manifest against one or more candidate manifests
and prints a compact report with hash match rates and structural divergences.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


REQUIRED_FIELDS = ("step_id", "virtual_time_us", "frame_hash")


@dataclass
class FrameRow:
    step_id: int
    virtual_time_us: int
    frame_hash: str


@dataclass
class ManifestData:
    path: Path
    rows: List[FrameRow]

    @property
    def by_step(self) -> Dict[int, FrameRow]:
        return {row.step_id: row for row in self.rows}


def load_manifest(path: Path) -> ManifestData:
    rows: List[FrameRow] = []
    seen_steps = set()

    with path.open("r", encoding="utf-8") as handle:
        for index, line in enumerate(handle, start=1):
            raw = line.strip()
            if not raw:
                continue

            try:
                item = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{index}: invalid JSON: {exc}") from exc

            missing = [field for field in REQUIRED_FIELDS if field not in item]
            if missing:
                raise ValueError(
                    f"{path}:{index}: missing required fields: {', '.join(missing)}"
                )

            step_id = int(item["step_id"])
            virtual_time_us = int(item["virtual_time_us"])
            frame_hash = str(item["frame_hash"])

            if step_id in seen_steps:
                raise ValueError(f"{path}:{index}: duplicate step_id: {step_id}")
            seen_steps.add(step_id)

            rows.append(
                FrameRow(
                    step_id=step_id,
                    virtual_time_us=virtual_time_us,
                    frame_hash=frame_hash,
                )
            )

    rows.sort(key=lambda row: row.step_id)
    return ManifestData(path=path, rows=rows)


def validate_monotonic(manifest: ManifestData) -> List[str]:
    issues: List[str] = []
    for prev, curr in zip(manifest.rows, manifest.rows[1:]):
        if curr.step_id <= prev.step_id:
            issues.append(
                f"step_id is not strictly increasing at {prev.step_id} -> {curr.step_id}"
            )
        if curr.virtual_time_us <= prev.virtual_time_us:
            issues.append(
                "virtual_time_us is not strictly increasing "
                f"at step {prev.step_id} -> {curr.step_id}"
            )
    return issues


def compare_manifests(
    baseline: ManifestData,
    candidate: ManifestData,
) -> Tuple[Dict[str, Any], bool]:
    base_map = baseline.by_step
    cand_map = candidate.by_step

    base_steps = set(base_map.keys())
    cand_steps = set(cand_map.keys())

    missing_steps = sorted(base_steps - cand_steps)
    extra_steps = sorted(cand_steps - base_steps)
    common_steps = sorted(base_steps & cand_steps)

    compared = len(common_steps)
    hash_matches = 0
    timestamp_matches = 0
    mismatched_hash_steps: List[int] = []
    mismatched_time_steps: List[int] = []

    for step_id in common_steps:
        base_row = base_map[step_id]
        cand_row = cand_map[step_id]
        if base_row.frame_hash == cand_row.frame_hash:
            hash_matches += 1
        else:
            mismatched_hash_steps.append(step_id)

        if base_row.virtual_time_us == cand_row.virtual_time_us:
            timestamp_matches += 1
        else:
            mismatched_time_steps.append(step_id)

    baseline_total = len(base_steps)
    hash_match_rate = (hash_matches / baseline_total * 100.0) if baseline_total else 0.0
    timestamp_match_rate = (
        timestamp_matches / baseline_total * 100.0 if baseline_total else 0.0
    )

    result = {
        "baseline": str(baseline.path),
        "candidate": str(candidate.path),
        "baseline_frames": len(baseline.rows),
        "candidate_frames": len(candidate.rows),
        "compared_steps": compared,
        "missing_steps": len(missing_steps),
        "extra_steps": len(extra_steps),
        "hash_matches": hash_matches,
        "hash_match_rate": hash_match_rate,
        "timestamp_matches": timestamp_matches,
        "timestamp_match_rate": timestamp_match_rate,
        "sample_mismatched_hash_steps": mismatched_hash_steps[:10],
        "sample_mismatched_time_steps": mismatched_time_steps[:10],
        "sample_missing_steps": missing_steps[:10],
        "sample_extra_steps": extra_steps[:10],
    }

    is_structurally_identical = not missing_steps and not extra_steps
    return result, is_structurally_identical


def _print_human_report(report: Dict[str, Any]) -> None:
    print(f"baseline:          {report['baseline']}")
    print(f"candidate:         {report['candidate']}")
    print(
        "frames:            "
        f"baseline={report['baseline_frames']} candidate={report['candidate_frames']}"
    )
    print(
        "step set delta:    "
        f"missing={report['missing_steps']} extra={report['extra_steps']}"
    )
    print(
        "hash match:        "
        f"{report['hash_matches']}/{report['baseline_frames']} "
        f"({report['hash_match_rate']:.4f}%)"
    )
    print(
        "timestamp match:   "
        f"{report['timestamp_matches']}/{report['baseline_frames']} "
        f"({report['timestamp_match_rate']:.4f}%)"
    )

    if report["sample_mismatched_hash_steps"]:
        print("sample hash mismatches:", report["sample_mismatched_hash_steps"])
    if report["sample_missing_steps"]:
        print("sample missing steps: ", report["sample_missing_steps"])
    if report["sample_extra_steps"]:
        print("sample extra steps:   ", report["sample_extra_steps"])


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare deterministic recording manifests (JSONL)."
    )
    parser.add_argument("baseline", type=Path, help="Baseline manifest path")
    parser.add_argument(
        "candidates",
        nargs="+",
        type=Path,
        help="Candidate manifest path(s)",
    )
    parser.add_argument(
        "--min-hash-match-rate",
        type=float,
        default=99.9,
        help="Minimum required frame hash match rate in percent (default: 99.9)",
    )
    parser.add_argument(
        "--require-identical-step-set",
        action="store_true",
        help="Fail if candidate has missing or extra step_ids",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON report",
    )
    return parser


def iter_reports(
    baseline: ManifestData,
    candidates: Iterable[ManifestData],
    min_hash_match_rate: float,
    require_identical_step_set: bool,
) -> Tuple[List[Dict[str, Any]], bool]:
    reports: List[Dict[str, Any]] = []
    ok = True

    baseline_issues = validate_monotonic(baseline)
    if baseline_issues:
        ok = False
        reports.append(
            {
                "baseline": str(baseline.path),
                "candidate": None,
                "error": "baseline monotonicity validation failed",
                "issues": baseline_issues,
            }
        )
        return reports, ok

    for candidate in candidates:
        candidate_issues = validate_monotonic(candidate)
        if candidate_issues:
            ok = False
            reports.append(
                {
                    "baseline": str(baseline.path),
                    "candidate": str(candidate.path),
                    "error": "candidate monotonicity validation failed",
                    "issues": candidate_issues,
                }
            )
            continue

        report, identical_steps = compare_manifests(baseline, candidate)
        pass_hash = report["hash_match_rate"] >= min_hash_match_rate
        pass_steps = identical_steps or not require_identical_step_set
        report["pass_hash_rate"] = pass_hash
        report["pass_step_set"] = pass_steps
        report["pass"] = pass_hash and pass_steps

        if not report["pass"]:
            ok = False
        reports.append(report)

    return reports, ok


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    try:
        baseline = load_manifest(args.baseline)
        candidates = [load_manifest(path) for path in args.candidates]
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    except OSError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    reports, ok = iter_reports(
        baseline,
        candidates,
        min_hash_match_rate=args.min_hash_match_rate,
        require_identical_step_set=args.require_identical_step_set,
    )

    if args.json:
        print(json.dumps(reports, indent=2, sort_keys=True))
    else:
        for index, report in enumerate(reports):
            if index:
                print("-" * 72)

            if "error" in report:
                print(f"baseline:  {report['baseline']}")
                if report["candidate"]:
                    print(f"candidate: {report['candidate']}")
                print(f"error:     {report['error']}")
                print("issues:")
                for issue in report["issues"]:
                    print(f"  - {issue}")
                continue

            _print_human_report(report)
            print(f"pass:              {report['pass']}")

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
