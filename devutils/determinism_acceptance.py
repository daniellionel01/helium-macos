#!/usr/bin/env python3

"""Run deterministic acceptance checks across repeated manifest runs.

This utility wraps determinism_report to evaluate an entire corpus of run manifests
against a baseline and enforce RFC-style thresholds (e.g. >=99.9% hash matches).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

from determinism_report import iter_reports, load_manifest


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate deterministic acceptance across repeated run manifests."
    )
    parser.add_argument(
        "manifests",
        nargs="*",
        type=Path,
        help=(
            "Manifest paths (JSONL). First path is baseline unless --baseline-index is set."
        ),
    )
    parser.add_argument(
        "--manifest-dir",
        type=Path,
        help="Directory containing run manifests",
    )
    parser.add_argument(
        "--manifest-pattern",
        default="*.jsonl",
        help="Glob pattern for --manifest-dir (default: *.jsonl)",
    )
    parser.add_argument(
        "--baseline-index",
        type=int,
        default=0,
        help="Index in manifests list to use as baseline (default: 0)",
    )
    parser.add_argument(
        "--min-hash-match-rate",
        type=float,
        default=99.9,
        help="Minimum hash match rate in percent for each candidate (default: 99.9)",
    )
    parser.add_argument(
        "--require-identical-step-set",
        action="store_true",
        help="Require candidates to have no missing/extra step IDs",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON output",
    )
    return parser


def _aggregate(reports: List[Dict[str, Any]]) -> Dict[str, Any]:
    evaluated = [report for report in reports if "error" not in report]
    failures = [report for report in evaluated if not report.get("pass", False)]
    errors = [report for report in reports if "error" in report]

    min_hash_match_rate = (
        min((float(report["hash_match_rate"]) for report in evaluated), default=0.0)
        if evaluated
        else 0.0
    )
    avg_hash_match_rate = (
        sum(float(report["hash_match_rate"]) for report in evaluated) / len(evaluated)
        if evaluated
        else 0.0
    )

    return {
        "evaluated_candidates": len(evaluated),
        "failing_candidates": len(failures),
        "error_candidates": len(errors),
        "min_hash_match_rate": min_hash_match_rate,
        "avg_hash_match_rate": avg_hash_match_rate,
        "pass": not failures and not errors,
    }


def _natural_key(path: Path) -> List[Any]:
    parts = re.split(r"(\d+)", path.name)
    key: List[Any] = []
    for part in parts:
        if part.isdigit():
            key.append(int(part))
        else:
            key.append(part.lower())
    return key


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    manifest_paths = list(args.manifests)
    if args.manifest_dir:
        if not args.manifest_dir.is_dir():
            print(f"error: manifest dir does not exist: {args.manifest_dir}", file=sys.stderr)
            return 2
        matched = sorted(
            args.manifest_dir.glob(args.manifest_pattern),
            key=_natural_key,
        )
        manifest_paths.extend(matched)

    seen = set()
    deduped_paths: List[Path] = []
    for path in manifest_paths:
        normalized = str(path.resolve())
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped_paths.append(path)

    if len(deduped_paths) < 2:
        print(
            "error: provide at least 2 manifest paths (directly or via --manifest-dir)",
            file=sys.stderr,
        )
        return 2

    if args.baseline_index < 0 or args.baseline_index >= len(deduped_paths):
        print(
            f"error: baseline index {args.baseline_index} out of range "
            f"for {len(deduped_paths)} manifests",
            file=sys.stderr,
        )
        return 2

    baseline_path = deduped_paths[args.baseline_index]
    candidate_paths = [
        path for index, path in enumerate(deduped_paths) if index != args.baseline_index
    ]

    try:
        baseline = load_manifest(baseline_path)
        candidates = [load_manifest(path) for path in candidate_paths]
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
    aggregate = _aggregate(reports)
    overall_ok = ok and aggregate["pass"]

    if args.json:
        payload = {
            "baseline": str(baseline_path),
            "candidates": [str(path) for path in candidate_paths],
            "aggregate": aggregate,
            "reports": reports,
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"baseline:            {baseline_path}")
        print(f"candidate count:     {len(candidate_paths)}")
        print(f"evaluated:           {aggregate['evaluated_candidates']}")
        print(f"failing candidates:  {aggregate['failing_candidates']}")
        print(f"error candidates:    {aggregate['error_candidates']}")
        print(f"min hash match rate: {aggregate['min_hash_match_rate']:.4f}%")
        print(f"avg hash match rate: {aggregate['avg_hash_match_rate']:.4f}%")
        print(f"pass:                {overall_ok}")

        failing_files = [
            report.get("candidate")
            for report in reports
            if report.get("candidate") and not report.get("pass", False)
        ]
        if failing_files:
            print("failing manifests:")
            for path in failing_files[:20]:
                print(f"  - {path}")

    return 0 if overall_ok else 1


if __name__ == "__main__":
    sys.exit(main())
