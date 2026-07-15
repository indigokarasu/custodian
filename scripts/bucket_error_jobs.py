#!/usr/bin/env python3
"""bucket_error_jobs.py — Custodian light/deep scan error-job bucketizer.

Reads the profile jobs.json, classifies every ENABLED error job into a known
fingerprint bucket, prints bucket counts, and surfaces any UNKNOWN job whose
last_error does not match a known pattern. Replaces ad-hoc per-scan bucketing so
the dominant 'other' collapse (e.g. 48 token_expired jobs) does not hide the
real root cause.

Key distinction this enforces:
  - `token_expired` 401  ("Provided authentication token is expired")  -> USER-GATED, re-auth, RECURS
  - first-occurrence generic 401 / "Provider returned error"           -> TRANSIENT (self-resolves)
Do NOT conflate them: the former persists and recurs; the latter self-heals.

Run:
  python3 <hermes-home>/skills/ocas-custodian/scripts/bucket_error_jobs.py
"""
import json, datetime, os, sys
from collections import defaultdict

JOBS = "<hermes-home>/cron/jobs.json"


def parse_ts(ts):
    if not ts:
        return None
    try:
        return datetime.datetime.fromisoformat(ts)
    except Exception:
        return None


def classify(le):
    le = le or ""
    if "token_expired" in le or "authentication token is expired" in le:
        return "P_provider_auth_token_expired"
    if "portal.nousresearch.com" in le:
        return "P_nous_api_key_invalid"
    if "requires more credits" in le or "can only afford" in le:
        return "P_openrouter_402_credits"
    if "owl-alpha" in le or ("No endpoints found" in le and "openrouter" in le):
        return "P_openrouter_404_owl_alpha"
    if "ResourceExhausted" in le or "Worker local total request limit" in le:
        return "T_nvidia_resource_exhausted"
    if "Rate limit exceeded" in le or "429" in le:
        return "T_http_429"
    if "Script not found" in le:
        return "T1_script_not_found"
    if "Script exited with code 1" in le:
        return "T1_exit1_aggregate"
    if "ModuleNotFoundError" in le:
        return "T1_missing_dep"
    if "cannot schedule new futures" in le:
        return "T_futures_shutdown"
    if "Provider returned error" in le:
        return "T_provider_error"
    if "No such file" in le:
        return "T1_path"
    if "invalid_grant" in le:
        return "P_google_oauth_token_revoked"
    if "deleted_client" in le:
        return "P_google_oauth_client_deleted"
    if "403" in le and "google" in le.lower():
        return "P_google_tasks_403"
    return "UNKNOWN"


DESC = {
    "P_provider_auth_token_expired": "Provider 401 token_expired — session token expired, needs re-auth (USER-GATED, RECURS)",
    "P_nous_api_key_invalid": "Nous 401 API key invalid/blocked/out of funds — key rotation/credits (USER-GATED)",
    "P_openrouter_402_credits": "OpenRouter 402 credits exhausted — add credits (USER-GATED)",
    "P_openrouter_404_owl_alpha": "OpenRouter owl-alpha model 404 — likely skill-internal hardcoded model (USER-GATED)",
    "T_nvidia_resource_exhausted": "Nvidia upstream ResourceExhausted — transient overload",
    "T_http_429": "HTTP 429 rate limit — transient",
    "T1_script_not_found": "Script not found — Tier 1 symlink fix candidate",
    "T1_exit1_aggregate": "Script exited code 1 — de-aggregate (read stderr)",
    "T1_missing_dep": "ModuleNotFoundError — Tier 1 dependency install",
    "T_futures_shutdown": "cannot schedule new futures — transient interpreter shutdown",
    "T_provider_error": "Generic Provider returned error — transient first-occurrence",
    "T1_path": "No such file — Tier 1 path fix candidate",
    "P_google_oauth_token_revoked": "Google OAuth invalid_grant — token revoked (USER-GATED)",
    "P_google_oauth_client_deleted": "Google OAuth deleted_client (USER-GATED)",
    "P_google_tasks_403": "Google 403 (Tasks API) — USER-GATED",
}


def main():
    if not os.path.exists(JOBS):
        print("jobs.json not found:", JOBS, file=sys.stderr)
        sys.exit(2)
    d = json.load(open(JOBS))
    jobs = d.get("jobs", []) if isinstance(d, dict) else d
    now = datetime.datetime.now(datetime.timezone.utc)
    errs = [j for j in jobs if j.get("last_status") == "error" and j.get("enabled", True)]
    buckets = defaultdict(list)
    for j in errs:
        buckets[classify(j.get("last_error"))].append(j)

    print("TOTAL_JOBS", len(jobs), "| ENABLED_ERROR_JOBS", len(errs))
    print("\nBUCKET COUNTS:")
    for k in sorted(buckets, key=lambda x: -len(buckets[x])):
        print(f"  {k}: {len(buckets[k])}  — {DESC.get(k, '')}")

    print("\nUNKNOWN JOBS (need inspection):")
    any_unknown = False
    for j in errs:
        if classify(j.get("last_error")) == "UNKNOWN":
            any_unknown = True
            lr = parse_ts(j.get("last_run_at"))
            lru = lr.astimezone(datetime.timezone.utc) if lr else None
            age = round((now - lru).total_seconds() / 60, 1) if lru else "?"
            print(f"  name: {j.get('name')} | id: {j.get('id')} | no_agent: {j.get('no_agent')} | cf: {j.get('consecutive_failures')} | mins_since_run: {age}")
            print(f"    script: {j.get('script')}")
            print(f"    last_error: {repr((j.get('last_error') or '')[:400])}")
    if not any_unknown:
        print("  (none — all error jobs match known fingerprints)")

    print("\nUSER-GATED JOB LISTS (for issues.jsonl reconciliation):")
    for k in ["P_provider_auth_token_expired", "P_nous_api_key_invalid",
              "P_openrouter_402_credits", "P_openrouter_404_owl_alpha",
              "P_google_oauth_token_revoked", "P_google_oauth_client_deleted",
              "P_google_tasks_403"]:
        if buckets.get(k):
            print(f"  {k}: {[j.get('name') for j in buckets[k]]}")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Custodian light/deep-scan error-job bucketizer: bucket enabled error jobs by fingerprint, show counts, and surface UNKNOWN jobs needing inspection.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n  python3 bucket_error_jobs.py\n  python3 bucket_error_jobs.py --jobs <hermes-root>/profiles/koda/cron/jobs.json",
    )
    ap.add_argument("--jobs", default=JOBS, help="Path to jobs.json (default: indigo profile)")
    ap.add_argument("--profile", default="indigo", help="Profile name, used only if --jobs is omitted")
    args = ap.parse_args()
    JOBS = args.jobs or f"<hermes-root>/profiles/{args.profile}/cron/jobs.json"
    main()
