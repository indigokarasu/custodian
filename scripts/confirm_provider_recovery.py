#!/usr/bin/env python3
"""Confirm whether a provider-auth error cluster is forward-stale.

Positive-confirmation method (NOT a lone `hermes chat -q` probe or a
`/v1/models` curl — both mislead on non-standard base_urls):

  1. Count jobs that ran OK TODAY (last_run_at starts today's UTC date).
  2. For each provider-auth issue, check how many of its affected_job_ids
     are now live-OK with a today timestamp.

Run:
  python3 ~/.hermes/profiles/indigo/skills/ocas-custodian/scripts/confirm_provider_recovery.py
Override profile:
  HERMES_PROFILE_HOME=/path/to/profile python3 confirm_provider_recovery.py
"""
import json
import os
import datetime
from collections import Counter

PROFILE = os.environ.get("HERMES_PROFILE_HOME", "~/.hermes/profiles/indigo")
JOBS = os.path.join(PROFILE, "cron", "jobs.json")
ISSUES = os.path.join(PROFILE, "commons", "data", "ocas-custodian", "issues.jsonl")

PROVIDER_FPS = {"token_expired", "openrouter_402", "nous_401", "owl_404"}


def fp_of(le):
    le = le or ""
    if "token is expired" in le or "token_expired" in le:
        return "token_expired"
    if "402" in le and "credits" in le:
        return "openrouter_402"
    if "portal.nousresearch.com" in le or "Your API key is invalid" in le:
        return "nous_401"
    if "owl-alpha" in le or "No endpoints found" in le:
        return "owl_404"
    return "other"


def parse_issues(path):
    recs = []
    depth = 0
    cur = ""
    started = False
    for ch in open(path, encoding="utf-8", errors="replace").read():
        if ch == "{":
            depth += 1
            started = True
            cur += ch
        elif ch == "}":
            depth -= 1
            cur += ch
            if depth == 0 and started:
                recs.append(cur)
                cur = ""
                started = False
        else:
            if started:
                cur += ch
    out = []
    for r in recs:
        try:
            out.append(json.loads(r))
        except Exception:
            pass
    return out


def main():
    today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    d = json.load(open(JOBS))
    jobs = {j.get("id"): j for j in d.get("jobs", [])}
    today_ok = [
        j
        for j in jobs.values()
        if (j.get("last_run_at") or "").startswith(today)
        and j.get("last_status") == "ok"
    ]
    enabled_err = {
        jid: j
        for jid, j in jobs.items()
        if j.get("enabled", True) and j.get("last_status") == "error"
    }
    live_fp = Counter(fp_of(j.get("last_error")) for j in enabled_err.values())
    print(f"Jobs total={len(jobs)} today_OK={len(today_ok)} enabled_error={len(enabled_err)}")
    print("Live enabled-error fingerprints:", dict(live_fp))
    verdict = "LIVE" if len(today_ok) > len(jobs) * 0.4 else "LOW - possible real outage"
    print(f"Today-OK share: {len(today_ok)}/{len(jobs)} ({verdict})")
    print()
    issues = parse_issues(ISSUES)
    prov_issues = [
        i
        for i in issues
        if (i.get("issue_id") or i.get("id", "")).startswith(
            ("oc_provider", "oc_nous", "oc_openrouter")
        )
    ]
    for i in prov_issues:
        iid = i.get("issue_id") or i.get("id")
        affected = i.get("affected_job_ids") or i.get("affected_jobs") or []
        if isinstance(affected, dict):
            affected = list(affected.keys())
        still_err = [
            a
            for a in affected
            if a in enabled_err and fp_of(enabled_err[a].get("last_error")) in PROVIDER_FPS
        ]
        recovered = [
            a
            for a in affected
            if jobs.get(a, {}).get("last_status") == "ok"
            and (jobs[a].get("last_run_at") or "").startswith(today)
        ]
        print(
            f"{iid}: status={i.get('status')} esc={i.get('escalation_needed')} "
            f"affected={len(affected)} still_err={len(still_err)} recovered_today={len(recovered)}"
        )
        if len(still_err) == 0 and len(recovered) > 0:
            print("   -> FORWARD-STALE: resolve (status=resolved, escalation_needed=false)")
        else:
            print(f"   -> STILL ACTIVE: {still_err[:5]}")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Custodian probe: confirm whether a provider-auth error cluster is forward-stale (resolved in practice) by comparing live enabled-error fingerprints against issue last_error signatures.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n  python3 confirm_provider_recovery.py\n  python3 confirm_provider_recovery.py --profile koda",
    )
    ap.add_argument("--profile", default=PROFILE, help="Profile HOME dir or name")
    args = ap.parse_args()
    PROFILE = args.profile if os.path.sep in args.profile else f"~/.hermes/profiles/{args.profile}"
    JOBS = os.path.join(PROFILE, "cron", "jobs.json")
    ISSUES = os.path.join(PROFILE, "commons", "data", "ocas-custodian", "issues.jsonl")
    main()
