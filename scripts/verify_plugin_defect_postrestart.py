#!/usr/bin/env python3
"""
verify_plugin_defect_postrestart.py — deterministic probe for the Custodian
gateway-log traceback-gap detection (Step 2 of light scan, see
references/gateway-log-traceback-gap-detection.md).

WHY THIS EXISTS
---------------
A jobs.json-only scan can report "clean" while the gateway is throwing recurring
plugin-code tracebacks that NEVER surface as a jobs.json `last_error`. These come
from gateway-internal paths (chronicle/engine/store.py, conversation_compression.py)
not from cron job scripts. The key question for escalation: is the signature STILL
recurring POST the most-recent gateway restart (-> live defect, escalate) or did it
stop after the last restart (-> dormant, do not re-escalate)?

This script answers that by scanning the gateway log, locating the last restart
marker, and counting signature hits pre- vs post-restart.

USAGE
-----
  python3 verify_plugin_defect_postrestart.py [--log PATH ...] [--pattern NAME=REGEX ...]

Defaults (if no --pattern given): the three known Chronicle plugin signatures
confirmed 2026-07-22:
  actor_check   = CHECK constraint failed: actor
  compress_force= compress() got an unexpected keyword argument 'force'
  seq_unique    = UNIQUE constraint failed

If no --log given, scans both profile and system gateway logs:
  <hermes-home>/logs/gateway.log
  <hermes-root>/logs/gateway.log

Restart markers detected: 'Received SIGTERM', 'Starting Hermes Gateway',
'Gateway start*' (case-insensitive).

OUTPUT (stdout, deterministic, parse-friendly):
  ### FILE <path>
  last_restart: <ISO or None>
  <sig_name>: pre=<N> post=<N> last=<ISO or None>
  ...
  SUMMARY lines: for each signature across all logs, post-restart total and verdict
  (LIVE if post>0 else DORMANT).

Exit code 0 always (probe, not a pass/fail gate).
"""
import argparse
import os
import re
import sys

DEFAULT_LOGS = [
    os.path.expanduser("~/.hermes/profiles/indigo/logs/gateway.log"),
    os.path.expanduser("~/.hermes/logs/gateway.log"),
]

DEFAULT_PATTERNS = {
    "actor_check": r"CHECK constraint failed: actor",
    "compress_force": r"compress\(\) got an unexpected keyword argument 'force'",
    "seq_unique": r"UNIQUE constraint failed",
}

RESTART_RE = re.compile(r"Received SIGTERM|Starting Hermes Gateway|Gateway start", re.IGNORECASE)
TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})")


def scan_log(path, patterns):
    if not os.path.exists(path):
        return None
    # Pass 1: find the MOST RECENT restart across the whole file. Bucketting each
    # signature hit against the *nearest preceding* restart (the old behavior)
    # is wrong: a log with many restarts puts every historical error after SOME
    # earlier restart, so post_restart_total was always >0 and the verdict was
    # always LIVE (false positive). See references/verify-plugin-defect-
    # postrestart-false-live-bug.md.
    overall_last_restart = None
    with open(path, errors="replace") as f:
        for line in f:
            if RESTART_RE.search(line):
                m = TS_RE.match(line)
                if m:
                    t = m.group(1).replace(" ", "T")
                    if overall_last_restart is None or t > overall_last_restart:
                        overall_last_restart = t
    # Pass 2: bucket each hit as 'post' only if it follows the MOST RECENT restart.
    counts = {k: {"pre": 0, "post": 0} for k in patterns}
    last_ts = {k: None for k in patterns}
    with open(path, errors="replace") as f:
        for line in f:
            m = TS_RE.match(line)
            t = m.group(1).replace(" ", "T") if m else None
            for k, rx in patterns.items():
                if re.search(rx, line):
                    bucket = "post" if (overall_last_restart and t and t >= overall_last_restart) else "pre"
                    counts[k][bucket] += 1
                    last_ts[k] = t
    return overall_last_restart, counts, last_ts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", action="append", default=[])
    ap.add_argument("--pattern", action="append", default=[],
                    help="NAME=REGEX pairs, repeatable")
    args = ap.parse_args()

    patterns = dict(DEFAULT_PATTERNS)
    for p in args.pattern:
        if "=" not in p:
            print(f"# bad --pattern (need NAME=REGEX): {p}", file=sys.stderr)
            continue
        name, rx = p.split("=", 1)
        patterns[name.strip()] = rx

    logs = args.log or DEFAULT_LOGS

    print("=== verify_plugin_defect_postrestart ===")
    per_sig_post = {k: 0 for k in patterns}
    for p in logs:
        res = scan_log(p, patterns)
        if res is None:
            print(f"### FILE {p}  [MISSING]")
            continue
        last_restart, counts, last_ts = res
        print(f"### FILE {p}  size={os.path.getsize(p)}")
        print(f"  last_restart: {last_restart}")
        for k in patterns:
            c = counts[k]
            print(f"  {k}: pre={c['pre']} post={c['post']} last={last_ts[k]}")
            per_sig_post[k] += c["post"]

    print("=== SUMMARY (post-restart recurrence across all logs) ===")
    for k in patterns:
        post = per_sig_post[k]
        verdict = "LIVE (escalate / keep open)" if post > 0 else "DORMANT (do not re-escalate)"
        print(f"  {k}: post_restart_total={post} -> {verdict}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
