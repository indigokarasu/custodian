#!/usr/bin/env python3
"""classify_error_jobs.py — Custodian light-scan probe.

Reads the profile-scoped jobs.json, filters enabled + not-paused error jobs,
buckets them by last_error fingerprint, and — critically — lists every job whose
last_error is the bare 'Script exited with code 1' wrapper together with its
`script` name, so each can be inspected individually (DE-AGGREGATION rule).

Run from cron/scheduled context via terminal():
    python3 ~/.hermes/profiles/indigo/skills/ocas-custodian/scripts/classify_error_jobs.py

Avoids pipe-to-interpreter: this is a standalone file, not `cat | python3`.
"""
import json
import os
from collections import Counter

PROFILE = os.environ.get("HERMES_PROFILE", "indigo")
CANDIDATES = [
    os.path.expanduser("~/.hermes/profiles/{PROFILE}/cron/jobs.json"),
    os.path.expanduser("~/.hermes/profiles/indigo/cron/jobs.json"),
    os.path.expanduser("~/.hermes/cron/jobs.json"),
]


def load_jobs():
    for p in CANDIDATES:
        if os.path.exists(p):
            with open(p) as f:
                d = json.load(f)
            return d.get("jobs", []) if isinstance(d, dict) else d
    raise SystemExit("jobs.json not found in candidates")


def classify(j):
    e = j.get("last_error") or ""
    if "requires more credits" in e or "code': 402" in e:
        return "openrouter_402_credits"
    if "portal.nousresearch.com" in e or ("401" in e and "nousresearch" in e):
        return "nous_401_api_key"
    if "owl-alpha" in e:
        return "openrouter_404_owl_alpha_model"
    if "ResourceExhausted" in e or "Nvidia" in e:
        return "nvidia_resource_exhausted_transient"
    if "429" in e or "Rate limit" in e:
        return "http_429_rate_limit_transient"
    if "cannot schedule new futures" in e:
        return "interpreter_shutdown_transient"
    if "Script exited with code 1" in e:
        return "no_agent_script_exit_1"   # AMBIGUOUS — must de-aggregate
    if "404" in e and "No endpoints" in e:
        return "openrouter_404_other_model"
    return "other"


def main():
    jobs = load_jobs()
    errs = [j for j in jobs
            if j.get("last_status") == "error"
            and j.get("enabled", True) is not False
            and j.get("state") != "paused"]
    print(f"ENABLED_ERROR_JOBS {len(errs)}")
    c = Counter(classify(j) for j in errs)
    for k, v in c.most_common():
        print(f"  {v:3d}  {k}")
    # De-aggregation: list every ambiguous wrapper job with its script name
    amb = [j for j in errs if classify(j) == "no_agent_script_exit_1"]
    if amb:
        print("\n=== DE-AGGREGATE: 'Script exited with code 1' jobs (inspect EACH) ===")
        for j in amb:
            has_stderr = "stderr:" in (j.get("last_error") or "")
            print(f"  name={j.get('name')} | script={j.get('script')} | stderr_in_error={has_stderr}")
            print(f"      id={j.get('id')} last_run={j.get('last_run_at')}")
        print("Rule: run each script directly; inspect sys.exit paths. "
              "No-op-by-design (no stderr) = oc_cron_no_agent_exit_1_noop (Tier2). "
              "Traceback = real failure.")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Custodian light-scan probe: bucket enabled error jobs by last_error fingerprint and list every 'Script exited with code 1' job for de-aggregation.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n  python3 classify_error_jobs.py\n  python3 classify_error_jobs.py --profile koda",
    )
    ap.add_argument("--profile", default=PROFILE, help="Profile name or HOME dir")
    args = ap.parse_args()
    PROFILE = args.profile
    CANDIDATES[:] = [
        os.path.expanduser("~/.hermes/profiles/{PROFILE}/cron/jobs.json"),
        os.path.expanduser("~/.hermes/profiles/indigo/cron/jobs.json"),
        os.path.expanduser("~/.hermes/cron/jobs.json"),
    ]
    main()