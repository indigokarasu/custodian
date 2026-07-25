#!/usr/bin/env python3
"""reopen_false_resolutions.py -- light-scan inverse-gotcha guard.

Reopens issues.jsonl entries marked `resolved` whose provider/auth/credit
fingerprint still has >=1 live erroring job in jobs.json. Read-only by
default; pass --write to persist reopens.

Cron-safe: no pipe-to-interpreter, no execute_code, hardcoded ~/.hermes
paths (Path.home() breaks in cron). Run via:
  python3 ~/.hermes/profiles/indigo/skills/ocas-custodian/scripts/reopen_false_resolutions.py
  python3 ~/.hermes/profiles/indigo/skills/ocas-custodian/scripts/reopen_false_resolutions.py --write

Usage:
  python3 reopen_false_resolutions.py          # dry-run
  python3 reopen_false_resolutions.py --write  # persist reopens
"""
import json
import os
import datetime
import argparse

PROFILE = "~/.hermes/profiles/indigo"
JOBS = os.path.join(PROFILE, "cron", "jobs.json")
ISSUES = os.path.join(PROFILE, "commons", "data", "ocas-custodian", "issues.jsonl")

# fingerprint -> substring(s) that still prove the outage is live in last_error
# (ALL substrings must be present for a job to count as still-failing)
OUTAGE_MATCH = {
    "oc_nous_api_key_invalid": ["token_expired"],
    "oc_provider_auth_token_expired": ["token_expired"],
    "oc_openrouter_402_credits_exhausted": ["402", "credits"],
    "oc_http_404_model_deprecated": ["No endpoints found for", "owl-alpha"],
}


def brace_depth_parse(text):
    depth = 0
    buf = ""
    out = []
    for ch in text:
        buf += ch
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and buf.strip():
                try:
                    out.append(json.loads(buf))
                except Exception:
                    pass
                buf = ""
    return out


def live_error_count(fp):
    jobs = json.load(open(JOBS)).get("jobs", [])
    subs = OUTAGE_MATCH.get(fp, [])
    if not subs:
        return 0
    n = 0
    for j in jobs:
        if j.get("last_status") != "error":
            continue
        le = j.get("last_error") or ""
        if all(s in le for s in subs):
            n += 1
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true",
                    help="persist reopens (default: dry-run)")
    args = ap.parse_args()

    if not os.path.isfile(ISSUES):
        print("No issues.jsonl at", ISSUES)
        return

    entries = brace_depth_parse(open(ISSUES).read())
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    reopened = []
    for e in entries:
        fp = e.get("fingerprint")
        if e.get("status") != "resolved":
            continue
        if fp not in OUTAGE_MATCH:
            continue
        cnt = live_error_count(fp)
        if cnt >= 1:
            e["status"] = "user_gated"
            e["escalation_needed"] = True
            if "resolved_at" in e:
                e["resolved_at"] = None
            e["reopened_at"] = now
            e["reopen_note"] = (
                f"False-resolution guard (reopen_false_resolutions.py): "
                f"{cnt} live job(s) still error with fingerprint {fp}."
            )
            reopened.append((e.get("issue_id") or e.get("id"), fp, cnt))
            print(f"[REOPEN] {e.get('issue_id') or e.get('id')} fp={fp} live_jobs={cnt}")
        else:
            print(f"[ok]     {e.get('issue_id') or e.get('id')} fp={fp} live_jobs=0")

    if not reopened:
        print("No false resolutions found.")
        return
    if args.write:
        with open(ISSUES, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")
        print(f"WROTE {ISSUES} ({len(reopened)} reopened)")
    else:
        print("DRY-RUN: rerun with --write to persist.")


if __name__ == "__main__":
    main()
