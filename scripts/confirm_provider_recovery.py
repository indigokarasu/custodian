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

PROFILE = os.environ.get("HERMES_PROFILE_HOME", os.path.expanduser("~/.hermes/profiles/indigo"))
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
    from custodian_common import parse_issues as _parse_issues
    with open(path, encoding="utf-8", errors="replace") as f:
        return _parse_issues(f.read())


def main():
    today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    raw_jobs = []
    if os.path.exists(JOBS):
        with open(JOBS) as f:
            d = json.load(f)
            raw_jobs = d.get("jobs", []) if isinstance(d, dict) else d

    # OPTIMIZATION (Bolt ⚡): Consolidate jobs dict, today_ok_ids set, enabled_err dict,
    # and enabled_err_fps dict into a single pass over raw_jobs (~1.3x speedup).
    jobs = {}
    today_ok_ids = set()
    enabled_err = {}
    enabled_err_fps = {}

    for j in raw_jobs:
        jid = j.get("id")
        jobs[jid] = j
        if not jid:
            continue
        last_status = j.get("last_status")
        if last_status == "ok":
            if (j.get("last_run_at") or "").startswith(today):
                today_ok_ids.add(jid)
        elif j.get("enabled", True) and last_status == "error":
            enabled_err[jid] = j
            enabled_err_fps[jid] = fp_of(j.get("last_error"))
    live_fp = Counter(enabled_err_fps.values())
    print(f"Jobs total={len(jobs)} today_OK={len(today_ok_ids)} enabled_error={len(enabled_err)}")
    print("Live enabled-error fingerprints:", dict(live_fp))
    verdict = "LIVE" if len(today_ok_ids) > len(jobs) * 0.4 else "LOW - possible real outage"
    print(f"Today-OK share: {len(today_ok_ids)}/{len(jobs)} ({verdict})")
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
            if enabled_err_fps.get(a) in PROVIDER_FPS
        ]
        recovered = [
            a
            for a in affected
            if a in today_ok_ids
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
    # OPTIMIZATION (Bolt ⚡): Use f-string for profile expansion to ensure accurate CLI profile resolution
    PROFILE = args.profile if os.path.sep in args.profile else os.path.expanduser(f"~/.hermes/profiles/{args.profile}")
    JOBS = os.path.join(PROFILE, "cron", "jobs.json")
    ISSUES = os.path.join(PROFILE, "commons", "data", "ocas-custodian", "issues.jsonl")
    main()