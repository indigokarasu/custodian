#!/usr/bin/env python3
"""verify_fixes_cron_run.py — Post-fix verification loop for Custodian Tier 1 fixes.

After applying a Tier 1 auto-fix to a cron job, the registry (jobs.json) keeps
showing last_status=='error' until the job's NEXT scheduled run — which can be a
day away. The scheduler only rewrites last_status when the job actually executes.
To CLOSE THE LOOP and prove the fix held NOW, re-run the job on demand with
`hermes cron run <id>`; a no_agent job returns in ~2s and the registry flips to
'ok' on success.

Usage:
    python3 verify_fixes_cron_run.py ID1 [ID2 ...]
    python3 verify_fixes_cron_run.py --file /tmp/fixed_ids.txt

For each ID it runs `hermes cron run <id>` (foreground, serial) and parses the
output for 'succeeded' / 'failed'. Prints a per-job line + summary.

Safety: explicit IDs only. It never auto-discovers LLM jobs (which would burn
tokens) — you pass exactly the job IDs you just fixed.
"""
import subprocess
import sys

TIMEOUT = 120  # generous; no_agent jobs finish in ~2s, LLM jobs ~60s


def run_one(jid):
    try:
        out = subprocess.run(
            ["hermes", "cron", "run", jid],
            capture_output=True,
            text=True,
            timeout=TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        return jid, False, "timeout (>%ss)" % TIMEOUT
    text = (out.stdout or "") + (out.stderr or "")
    ok = "succeeded" in text.lower()
    last = text.strip().splitlines()[-1] if text.strip() else "(no output)"
    return jid, ok, last


def main():
    ids = []
    if "--file" in sys.argv:
        i = sys.argv.index("--file")
        with open(sys.argv[i + 1]) as f:
            ids = [l.strip() for l in f if l.strip()]
    else:
        ids = [a for a in sys.argv[1:] if not a.startswith("-")]
    if not ids:
        print("usage: verify_fixes_cron_run.py ID1 [ID2 ...] [--file ids.txt]")
        sys.exit(2)
    ok = fail = 0
    for jid in ids:
        j, success, last = run_one(jid)
        mark = "OK  " if success else "FAIL"
        print(f"{mark} {j} :: {last}")
        ok += success
        fail += (not success)
    print(f"\nRESULT ok={ok} fail={fail}")


if __name__ == "__main__":
    main()
