#!/usr/bin/env python3
"""
verify_recovery_by_runtime.py — Decisive recovery check for provider-token /
API-key / credits issues.

WHY THIS EXISTS (2026-07-13 false-resolution lesson):
A prior escalation loop resolved oc_provider_auth_token_expired citing
"hermes chat -q returned PONG on the free default model (tencent/hy3:free)"
and "OpenRouter /models returned HTTP 200". NEITHER validates the session
token or API key the failing jobs actually use. The fingerprint
oc_provider_auth_token_expired is documented non-self-healing, yet 19 jobs
remained erroring — all with last_run_at on/before the claimed recovery,
never re-running successfully afterward.

The ONLY valid recovery evidence is TIME-BASED: for each affected job,
check whether it actually re-ran with last_status=ok AND last_error cleared
AFTER the claimed recovery timestamp. If the job's last_run_at predates the
recovery and its last_error is still intact, the issue is NOT recovered.

USAGE:
  # Check by issue id (reads jobs_paused + resolved_at from profile issues.jsonl):
  python3 scripts/verify_recovery_by_runtime.py --issue oc_provider_auth_token_expired_20260712T040120

  # Check explicit job ids against an explicit recovery timestamp:
  python3 scripts/verify_recovery_by_runtime.py --jobs aaa,bbb,ccc --recovery 2026-07-13T16:10:00Z

  # If --recovery omitted with --issue, uses the issue's resolved_at field.

EXIT CODE: 0 if ALL affected jobs have re-run OK since recovery; 1 if ANY
still failing (i.e. the "resolved" claim is false and the issue must stay
open / be re-opened).

Run via terminal(python3 /path/to/script.py) in cron — never execute_code,
never pipe-to-interpreter.
"""
import json
import sys
import argparse
from datetime import datetime, timezone

JOBS = "~/.hermes/profiles/indigo/cron/jobs.json"
ISSUES = "~/.hermes/profiles/indigo/commons/data/ocas-custodian/issues.jsonl"


def parse_ts(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def load_jobs():
    with open(JOBS) as f:
        data = json.load(f)
    jobs = data.get("jobs", []) if isinstance(data, dict) else data
    return {j.get("id"): j for j in jobs}


def parse_issues_brace_depth(text):
    records = []
    buf = []
    depth = 0
    in_str = False
    esc = False
    started = False
    for ch in text:
        if in_str:
            buf.append(ch)
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            buf.append(ch)
            continue
        if ch == "{":
            depth += 1
            started = True
            buf.append(ch)
            continue
        if ch == "}":
            depth -= 1
            buf.append(ch)
            if depth == 0 and started:
                try:
                    records.append(json.loads("".join(buf)))
                except Exception:
                    pass
                buf = []
                started = False
            continue
        if started:
            buf.append(ch)
    return records


def load_issue(iid):
    with open(ISSUES) as f:
        recs = parse_issues_brace_depth(f.read())
    for r in recs:
        if (r.get("issue_id") or r.get("id")) == iid:
            return r
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--issue", help="issue id to read jobs_paused + resolved_at from")
    ap.add_argument("--jobs", help="comma-separated job ids")
    ap.add_argument("--recovery", help="claimed recovery UTC timestamp (ISO 8601)")
    args = ap.parse_args()

    jobs = load_jobs()

    if args.issue:
        iss = load_issue(args.issue)
        if not iss:
            print(f"ERROR: issue {args.issue} not found in {ISSUES}")
            return 2
        job_ids = iss.get("jobs_paused") or []
        recovery = parse_ts(args.recovery) or parse_ts(iss.get("resolved_at"))
        print(f"Issue: {args.issue}")
        print(f"  jobs_paused enrolled: {len(job_ids)}")
        print(f"  claimed recovery: {recovery.isoformat() if recovery else None}")
    elif args.jobs:
        job_ids = [j.strip() for j in args.jobs.split(",") if j.strip()]
        recovery = parse_ts(args.recovery)
        if not recovery:
            print("ERROR: --recovery required when not using --issue")
            return 2
        print(f"Explicit job set: {len(job_ids)} jobs")
        print(f"  claimed recovery: {recovery.isoformat()}")
    else:
        print("ERROR: pass --issue <id> or --jobs a,b,c [--recovery TS]")
        return 2

    if not recovery:
        print("ERROR: no recovery timestamp resolved")
        return 2

    recovered = []
    still_failing = []
    for jid in job_ids:
        j = jobs.get(jid)
        if not j:
            print(f"  [MISSING] {jid} not in jobs.json")
            still_failing.append(jid)
            continue
        lr = parse_ts(j.get("last_run_at"))
        last_err = (j.get("last_error") or "").strip()
        name = j.get("name", jid)
        if lr and lr > recovery:
            if not last_err and j.get("last_status") == "ok":
                recovered.append(jid)
                print(f"  [RECOVERED] {name} ({jid}) last_run={lr.isoformat()} status=ok")
            else:
                still_failing.append(jid)
                print(f"  [FAILING]    {name} ({jid}) re-ran {lr.isoformat()} but still erroring: {last_err[:60]!r}")
        else:
            still_failing.append(jid)
            lr_s = lr.isoformat() if lr else "never"
            print(f"  [FAILING]    {name} ({jid}) last_run={lr_s} (pre-recovery) error={last_err[:60]!r}")

    print(f"\nSUMMARY: recovered={len(recovered)} still_failing={len(still_failing)}")
    if still_failing:
        print("VERDICT: RECOVERY NOT CONFIRMED — issue must stay OPEN / be RE-OPENED.")
        print("  Generic model/endpoint probes do NOT validate the failing jobs' token/key.")
        return 1
    print("VERDICT: all affected jobs re-ran OK since recovery — resolution valid.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
