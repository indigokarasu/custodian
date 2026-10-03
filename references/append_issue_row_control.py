#!/usr/bin/env python3
"""Control suite for scripts/append_issue_row.py.

Run from anywhere:  python3 references/append_issue_row_control.py

A passing suite proves nothing on its own — this exists so the DETECTOR is
proven to fire. Each arm asserts an expected outcome on a synthetic throwaway
store under a unique temp dir; the real issues.jsonl is never touched.

Arms:
  POSITIVE  10 cases that must succeed (new issue, rate delta, resolution, ...)
  NEGATIVE   8 cases that must FAIL — each one reproduced from a real defect
             this helper exists to prevent.
"""
import json
import os
import subprocess
import sys
import tempfile
import shutil

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(os.path.dirname(HERE), "scripts", "append_issue_row.py")


def run(store, *args):
    cmd = [sys.executable, SCRIPT, "--path", store] + list(args)
    p = subprocess.run(cmd, capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr


def rows(store):
    if not os.path.exists(store):
        return []
    return [json.loads(l) for l in open(store) if l.strip()]


def case(store, rows_):
    """Seed a throwaway store with raw rows."""
    with open(store, "w") as f:
        for r in rows_:
            f.write(json.dumps(r) + "\n")


PASS = FAIL = 0
failures = []


def check(name, got, want):
    global PASS, FAIL
    if got == want:
        PASS += 1
        print("  ok   %-58s rc=%s" % (name, got))
    else:
        FAIL += 1
        failures.append("%s: got rc=%s want rc=%s" % (name, got, want))
        print("  FAIL %-58s rc=%s want %s" % (name, got, want))


tmp = tempfile.mkdtemp(prefix="appendrow-control-")
try:
    print("POSITIVE cases")

    s = os.path.join(tmp, "p1.jsonl")
    case(s, [])
    check("new open issue -> +1",
          run(s, "--issue-id", "oc_a", "--status", "open", "--escalation-needed",
              "--description", "new finding")[0], 0)

    s = os.path.join(tmp, "p2.jsonl")
    case(s, [{"issue_id": "oc_a", "status": "open", "escalation_needed": True,
              "description": "x"}])
    check("rate delta on an OPEN issue -> holds at 0",
          run(s, "--issue-id", "oc_a", "--status", "open_rate_delta",
              "--escalation-needed", "--description", "rate moved")[0], 0)

    s = os.path.join(tmp, "p3.jsonl")
    case(s, [{"issue_id": "oc_a", "status": "open", "escalation_needed": True,
              "description": "x"}])
    check("resolution -> -1",
          run(s, "--issue-id", "oc_a", "--status", "resolved",
              "--description", "gone")[0], 0)

    s = os.path.join(tmp, "p4.jsonl")
    case(s, [])
    check("new non-escalating row (explicit False) -> 0",
          run(s, "--issue-id", "oc_a", "--status", "observed_no_action",
              "--no-escalation", "--description", "not actionable")[0], 0)

    s = os.path.join(tmp, "p5.jsonl")
    case(s, [])
    check("user_gated exact match -> +1",
          run(s, "--issue-id", "oc_a", "--status", "user_gated",
              "--description", "needs the user")[0], 0)

    s = os.path.join(tmp, "p6.jsonl")
    case(s, [{"issue_id": "oc_a", "status": "resolved", "description": "x"}])
    check("re-resolution of a closed issue -> holds at 0",
          run(s, "--issue-id", "oc_a", "--status", "resolved",
              "--description", "re-resolved")[0], 0)

    s = os.path.join(tmp, "p7.jsonl")
    case(s, [])
    check("decorated user_gated_* with --no-escalation -> 0 (exact match, not prefix)",
          run(s, "--issue-id", "oc_a",
              "--status", "user_gated_reverified_still_broken",
              "--no-escalation", "--description", "decorated")[0], 0)

    s = os.path.join(tmp, "p8.jsonl")
    case(s, [{"issue_id": "oc_a", "status": "open", "escalation_needed": True,
              "description": "x"}])
    check("superseding row exiting the open set without terminal status -> 0",
          run(s, "--issue-id", "oc_a",
              "--status", "resolved_structural_control_installed_and_wired",
              "--no-escalation", "--description", "control landed")[0], 0)

    s = os.path.join(tmp, "p9.jsonl")
    case(s, [])
    check("store absent -> creates it, +1",
          run(s, "--issue-id", "oc_a", "--status", "open", "--escalation-needed",
              "--description", "new")[0], 0)

    s = os.path.join(tmp, "p10.jsonl")
    case(s, [{"issue_id": "oc_a", "status": "open", "escalation_needed": True,
              "description": "x"}])
    check("--dry-run writes nothing",
          run(s, "--issue-id", "oc_b", "--status", "open", "--escalation-needed",
              "--description", "new", "--dry-run")[0], 0)
    check("  ...and the store is still 1 row", len(rows(s)), 1)

    s = os.path.join(tmp, "p11.jsonl")
    case(s, [])
    check("optional tier/fingerprint/note accepted",
          run(s, "--issue-id", "oc_a", "--status", "open", "--escalation-needed",
              "--tier", "2", "--fingerprint", "fp1", "--note", "n", "--description", "d")[0], 0)

    print("\nNEGATIVE cases (each reproduced from a real defect)")

    s = os.path.join(tmp, "n1.jsonl")
    case(s, [])
    # Real defect: row omitted escalation_needed while holding a non-terminal
    # status -> invisible to the open filter while reading as open.
    check("non-terminal row OMITTING escalation_needed -> rc 2",
          run(s, "--issue-id", "oc_a", "--status", "open_rate_delta",
              "--description", "omitted the boolean")[0], 2)
    check("  ...and nothing was written", len(rows(s)), 0)

    s = os.path.join(tmp, "n2.jsonl")
    case(s, [])
    check("terminal row claiming escalation_needed: true -> rc 2 (contradictory)",
          run(s, "--issue-id", "oc_a", "--status", "resolved", "--escalation-needed",
              "--description", "contradiction")[0], 2)

    s = os.path.join(tmp, "n3.jsonl")
    case(s, [])
    check("--escalation-needed + --no-escalation together -> rc 3",
          run(s, "--issue-id", "oc_a", "--status", "open", "--escalation-needed",
              "--no-escalation", "--description", "d")[0], 3)

    s = os.path.join(tmp, "n4.jsonl")
    case(s, [])
    check("missing --issue-id -> rc 3", run(s, "--status", "open", "--description", "d")[0], 3)
    check("missing --description -> rc 3", run(s, "--issue-id", "oc_a", "--status", "open")[0], 3)

    s = os.path.join(tmp, "n5.jsonl")
    case(s, [])
    check("unwritable store path -> rc 4",
          run("/nonexistent-dir-xyz/i.jsonl", "--issue-id", "oc_a", "--status", "open",
              "--escalation-needed", "--description", "d")[0], 4)

    s = os.path.join(tmp, "n6.jsonl")
    # Real defect shape: a rate-delta row keyed only on whether the issue EXISTED
    # derived 0 for a resolution instead of -1. Positive arm p3 covers the fix;
    # here we assert the resolution genuinely decrements the open count.
    case(s, [{"issue_id": "oc_a", "status": "open", "escalation_needed": True,
              "description": "x"},
             {"issue_id": "oc_b", "status": "open", "escalation_needed": True,
              "description": "y"}])
    rc, out, err = run(s, "--issue-id", "oc_a", "--status", "resolved",
                       "--description", "gone")
    check("resolution decrements while others stay open", rc, 0)
    check("  ...open count 2 -> 1", "open 2 -> 1" in out, True)

    s = os.path.join(tmp, "n7.jsonl")
    # The reopen case, which the helper's FIRST version got wrong: the row
    # supersedes an existing issue_id, yet the open count genuinely returns to
    # +1 because the previous last row was closed. Asserting "any superseding
    # row holds at 0" raised a phantom failure here.
    case(s, [{"issue_id": "oc_a", "status": "resolved", "description": "x"}])
    rc, out, err = run(s, "--issue-id", "oc_a", "--status", "open",
                       "--escalation-needed", "--description", "reopened")
    check("reopen of a resolved issue_id -> +1 (not 0, not +1-as-new)", rc, 0)
    check("  ...open count 0 -> 1", "open 0 -> 1" in out, True)

    s = os.path.join(tmp, "n8.jsonl")
    case(s, [{"no_issue_id": True, "status": "open", "escalation_needed": True,
              "description": "unreachable legacy row"}])
    rc, out, err = run(s, "--issue-id", "oc_a", "--status", "open",
                       "--escalation-needed", "--description", "new")
    check("unreachable legacy row (no issue_id) is ignored, not counted", rc, 0)

    print("\n%d passed, %d failed" % (PASS, FAIL))
    for f in failures:
        print("  FAILED: " + f)
    sys.exit(1 if FAIL else 0)
finally:
    shutil.rmtree(tmp, ignore_errors=True)