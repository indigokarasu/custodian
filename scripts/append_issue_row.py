#!/usr/bin/env python3
"""Shared append helper for the append-only issues.jsonl store.

WHY THIS EXISTS
---------------
Two defects in production came from hand-rolled appends:

  1. A row that OMITS `issue_id` is unreachable by every sanctioned tool
     (`parse_issues_jsonl`, `race_safe_issue_patch`, `verify_escalation_state`,
     `reopen_false_resolutions`, `confirm_provider_recovery` all key on it).
  2. A row that OMITS `escalation_needed` while holding a non-terminal,
     non-`user_gated` status parses cleanly, raises nothing, and is invisible
     to the open filter while still reading as `open` to a human. Measured
     2026-10-02: the open count fell 80 -> 79 on a single append with zero
     resolutions.

Both are silent by construction. This helper asserts both keys per row and
verifies the open-count delta, so the failure is loud at write time instead.

The delta expectation is derived from the open filter AS WRITTEN, not from a
hand-maintained kind table -- a kind table drifts, and a guard that rejects the
fix it exists to enable is worse than no guard. Under last-row-wins the rule is
simply the filter applied twice:

    expected = is_open(this_row) - is_open(previous_last_row_for_this_issue_id)

  +1  new issue_id whose row matches the filter (or a REOPEN: the previous last
      row was closed and this one matches — issue existence alone is not the
      discriminator, filter membership of the two rows is)
   0  a superseding row that is open before and after, or closed before and after
  -1  the previous last row matched the filter and this one does not

Note that `escalation_needed: False` on a still-open status also exits the open
set without being terminal, so it contributes -1 — while a decorated
`user_gated_*` status never matches at all, because the filter compares that
status EXACTLY.

CONTROL SUITE: references/append_issue_row_control.py runs 11 positive and 9
negative arms on a throwaway store under a unique temp dir and never touches
the real issues.jsonl. Run it after any change here. It exists because this
helper's own first version shipped "green" while mishandling exactly the
reopen case above.

Store layout: append-only, one JSON object per line, resolved by LAST-ROW-WINS
keyed on `issue_id`. Rows missing `issue_id` are never reachable.

Usage:
    python3 append_issue_row.py --issue-id oc_x --status open --description "..."
    python3 append_issue_row.py --issue-id oc_x --status open_rate_delta \\
        --escalation-needed --description "..." --note "rate 1/162 -> 7/162"
    python3 append_issue_row.py --issue-id oc_x --status resolved \\
        --description "..."          # delta -1, no escalation_needed needed

Options:
    --issue-id ID          required; written as `issue_id`
    --status STATUS        required; any string. TERMINAL_STATUSES are treated
                           as closed by the open filter; `user_gated` is matched
                           EXACTLY, so `user_gated_reverified_...` does NOT match
    --description TEXT     required; the row's human-readable summary
    --escalation-needed    set escalation_needed: true on the row
    --no-escalation        set escalation_needed: false explicitly (a recorded
                           deliberate non-escalation, not an omission)
    --tier N               tier 1-4
    --fingerprint FP       fingerprint string for dedupe
    --note TEXT            free-text note field
    --path FILE            store path (default: the authoritative data-path)
    --dry-run              print the row and the expected delta, write nothing

Exit codes:
    0  row appended (or validated under --dry-run) and delta matched
    2  assertion failure: missing issue_id/escalation_needed, terminal row that
       still claims escalation, or an open-count delta that does not match
    3  bad usage (missing --issue-id/--status/--description)
    4  store unreadable or unwritable

The store is append-only: this helper NEVER rewrites, patches, or resolves
existing rows. Use scripts/race_safe_issue_patch.py for that.
"""
import argparse
import json
import os
import sys

_HELP_ARGS = {"--help", "-h"}
if set(sys.argv[1:]) & _HELP_ARGS:
    print((__doc__ or "").strip())
    sys.exit(0)

DEFAULT_PATH = os.path.expanduser(
    "~/.hermes/profiles/indigo/commons/data/ocas-custodian/issues.jsonl")
# Kept in sync with scripts/parse_issues_jsonl.py — the sanctioned open filter.
TERMINAL_STATUSES = ("resolved", "duplicate", "closed")
USER_GATED = "user_gated"


def is_open(row):
    """The sanctioned open filter, verbatim from parse_issues_jsonl.py."""
    if not isinstance(row, dict):
        return False
    if row.get("status") in TERMINAL_STATUSES:
        return False
    return row.get("escalation_needed") is True or row.get("status") == USER_GATED


def load(path):
    if not os.path.exists(path):
        return []
    try:
        from custodian_common import parse_issues
    except ImportError:
        parse_issues = None
    with open(path) as f:
        raw = f.read()
    if parse_issues is not None:
        try:
            return parse_issues(raw)
        except Exception:
            pass
    # Fallback: tolerate concatenated objects on one line.
    out = []
    dec = json.JSONDecoder()
    idx = 0
    while idx < len(raw):
        while idx < len(raw) and raw[idx] in " \t\r\n":
            idx += 1
        if idx >= len(raw):
            break
        try:
            obj, end = dec.raw_decode(raw, idx)
        except ValueError:
            break
        out.append(obj)
        idx = end
    return out


def last_rows(entries):
    best = {}
    for e in entries:
        key = e.get("issue_id") or e.get("id")
        if not key:
            continue
        best[key] = e
    return best


def main(argv=None):
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--issue-id", required=True)
    ap.add_argument("--status", required=True)
    ap.add_argument("--description", required=True)
    ap.add_argument("--escalation-needed", action="store_true")
    ap.add_argument("--no-escalation", action="store_true")
    ap.add_argument("--tier")
    ap.add_argument("--fingerprint")
    ap.add_argument("--note")
    ap.add_argument("--path", default=DEFAULT_PATH)
    ap.add_argument("--dry-run", action="store_true")
    try:
        args = ap.parse_args(argv)
    except SystemExit:
        sys.stderr.write("bad usage: --issue-id, --status and --description are required\n")
        return 3

    if args.escalation_needed and args.no_escalation:
        sys.stderr.write("--escalation-needed and --no-escalation are mutually exclusive\n")
        return 3

    row = {
        "issue_id": args.issue_id,
        "status": args.status,
        "description": args.description,
    }
    if args.escalation_needed:
        row["escalation_needed"] = True
    elif args.no_escalation:
        row["escalation_needed"] = False
    if args.tier:
        row["tier"] = int(args.tier) if str(args.tier).isdigit() else args.tier
    if args.fingerprint:
        row["fingerprint"] = args.fingerprint
    if args.note:
        row["note"] = args.note

    # Control: every appended row must set BOTH keys the store's tools key on.
    # A terminal row that still claims escalation_needed: true is contradictory.
    problems = []
    if not row.get("issue_id"):
        problems.append("row has no issue_id — unreachable by every sanctioned tool")
    if "escalation_needed" not in row and row.get("status") not in TERMINAL_STATUSES:
        if row.get("status") != USER_GATED:
            problems.append(
                "row omits escalation_needed while holding non-terminal status "
                f"{row.get('status')!r} — invisible to the open filter")
    if row.get("status") in TERMINAL_STATUSES and row.get("escalation_needed") is True:
        problems.append("terminal row declares escalation_needed: true — contradictory")
    if problems:
        for p in problems:
            sys.stderr.write("ASSERT: " + p + "\n")
        return 2

    try:
        entries = load(args.path)
    except OSError as e:
        sys.stderr.write("cannot read %s: %s\n" % (args.path, e))
        return 4

    before = last_rows(entries)
    open_before = sum(1 for r in before.values() if is_open(r))

    key = row["issue_id"]
    prev = before.get(key)
    prev_open = is_open(prev) if isinstance(prev, dict) else False
    this_open = is_open(row)

    # Delta is keyed on the open FILTER, applied to this row and to the previous
    # last row for the same issue_id — not on whether this row "supersedes".
    # Those differ for exactly one case: reopening a CLOSED issue. The row
    # supersedes (issue already exists) yet the open count genuinely returns to
    # +1, because the previous last row did not match the filter and this one
    # does. A rule stated as "0 for any superseding row" raises a phantom
    # failure there; found by the control suite in references/, not by reading
    # the code.
    if prev is None:
        expected = 1 if this_open else 0
    else:
        expected = (1 if this_open else 0) - (1 if prev_open else 0)

    payload = json.dumps(row, ensure_ascii=False)
    if args.dry_run:
        print("dry-run: would append %s" % payload)
        print("expected open-count delta: %+d (open_before=%d open_after=%d)"
              % (expected, open_before, open_before + expected))
        return 0

    try:
        with open(args.path, "a") as f:
            f.write(payload + "\n")
    except OSError as e:
        sys.stderr.write("cannot append to %s: %s\n" % (args.path, e))
        return 4

    try:
        entries_after = load(args.path)
    except OSError as e:
        sys.stderr.write("appended but cannot re-read %s: %s\n" % (args.path, e))
        return 4
    open_after = sum(1 for r in last_rows(entries_after).values() if is_open(r))
    observed = open_after - open_before

    if observed != expected:
        sys.stderr.write(
            "ASSERT: open-count delta %+d, expected %+d (open_before=%d open_after=%d).\n"
            "        The row WAS appended — inspect it before trusting the store.\n"
            % (observed, expected, open_before, open_after))
        return 2

    print("appended %s | open %d -> %d (delta %+d, as expected)"
          % (key, open_before, open_after, observed))
    return 0


if __name__ == "__main__":
    sys.exit(main())