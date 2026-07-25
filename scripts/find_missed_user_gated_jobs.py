#!/usr/bin/env python3
"""Escalation-loop missed-enrollment probe.

Loads the profile jobs.json, finds every ENABLED job currently in error state
whose last_error is NOT already covered by any open issue's jobs_paused list,
and classifies it against known user-gated fingerprints.

Outputs three buckets:
  - MISSED (user-gated): jobs matching a user-gated fingerprint that should be
    PAUSED and ENROLLED into the matching issue. These failed in the inter-scan
    window *after* the last esc pass and were never added to jobs_paused.
  - TRANSIENT: jobs matching known transient patterns (429, interpreter
    shutdown, Nvidia ResourceExhausted, first-occurrence provider error) --
    leave running.
  - UNKNOWN: anything else -- inspect manually.

This catches the exact failure mode from 2026-07-07T23:34Z: 3 Nous-401 jobs
(bones:research, bones:position-tracker, finch:work) were enabled+erroring with
the Nous message but absent from oc_nous_401_key_invalid_20260707's jobs_paused
because they failed after the 21:04 esc pass. Run this probe AFTER
verify_escalation_state.py in every escalation loop.

Run: python3 ~/.hermes/profiles/indigo/skills/ocas-custodian/scripts/find_missed_user_gated_jobs.py
"""
import json

JOBS_PATH = "~/.hermes/profiles/indigo/cron/jobs.json"
ISSUES_PATHS = [
    "~/.hermes/profiles/indigo/commons/data/ocas-custodian/issues.jsonl",
]

# (fingerprint, recommended_issue_id, match-substrings in last_error)
USER_GATED = [
    ("oc_nous_api_key_invalid", "oc_nous_401_key_invalid_20260707",
     ["portal.nousresearch.com", "your api key is invalid, blocked or out of funds"]),
    ("oc_openrouter_402_credits_exhausted", "oc_openrouter_402_credits_exhausted_20260706",
     ["402", "insufficient", "credits", "openrouter"]),
    ("oc_http_404_model_deprecated", "oc_owl_alpha_model_404_20260701",
     ["404", "no endpoints found", "owl-alpha"]),
    ("oc_google_tasks_api_403", "oc_google_tasks_api_403_forbidden",
     ["403", "forbidden", "tasks api", "tasks"]),
    ("oc_google_oauth_token_revoked", "oc_google_oauth_token_revoked",
     ["invalid_grant", "token has been expired or revoked"]),
]

TRANSIENT = [
    "cannot schedule new futures after interpreter shutdown",
    "resourceexhausted",
    "rate limit exceeded",
    "429",
    "provider returned error",
    "futures shutdown",
]


def brace_depth_parse(text):
    objs = []
    depth = 0
    in_str = False
    esc = False
    buf = []
    for ch in text:
        if in_str:
            buf.append(ch)
            if esc:
                esc = False
            elif ch == '\\':
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            buf.append(ch)
            continue
        if ch == '{':
            depth += 1
            buf.append(ch)
            continue
        if ch == '}':
            depth -= 1
            buf.append(ch)
            if depth == 0:
                s = ''.join(buf).strip()
                if s:
                    try:
                        objs.append(json.loads(s))
                    except Exception:
                        pass
                buf = []
            continue
        if depth > 0:
            buf.append(ch)
    return objs


def collect_paused():
    paused = set()
    for p in ISSUES_PATHS:
        try:
            with open(p) as f:
                text = f.read()
        except Exception:
            continue
        for o in brace_depth_parse(text):
            for jid in (o.get("jobs_paused") or []):
                if isinstance(jid, str):
                    paused.add(jid)
    return paused


def classify(err):
    if not err:
        return ("UNKNOWN", None, None)
    low = err.lower()
    for fp, iid, subs in USER_GATED:
        for sub in subs:
            if sub in low:
                return ("MISSED", fp, iid)
    for t in TRANSIENT:
        if t in low:
            return ("TRANSIENT", None, None)
    return ("UNKNOWN", None, None)


def main():
    with open(JOBS_PATH) as f:
        data = json.load(f)
    jobs = data.get("jobs", []) if isinstance(data, dict) else data
    paused = collect_paused()

    missed = []
    transient = []
    unknown = []
    for j in jobs:
        if j.get("enabled") is not True:
            continue
        if j.get("last_status") != "error":
            continue
        jid = j.get("id")
        if jid in paused:
            continue
        err = j.get("last_error") or ""
        kind, fp, iid = classify(err)
        rec = {"id": jid, "name": j.get("name"), "skill": j.get("skill"),
               "last_error": err[:200]}
        if kind == "MISSED":
            rec["fingerprint"] = fp
            rec["recommended_issue"] = iid
            missed.append(rec)
        elif kind == "TRANSIENT":
            transient.append(rec)
        else:
            unknown.append(rec)

    print(f"Enabled+erroring jobs NOT in any jobs_paused: "
          f"{len(missed)+len(transient)+len(unknown)}")
    print(f"\n=== MISSED (user-gated -> PAUSE + ENROLL into recommended_issue) ===")
    for r in missed:
        print(f"  [{r['id']}] {r['name']} -> fp={r['fingerprint']} "
              f"issue={r['recommended_issue']}")
        print(f"      {r['last_error']}")
    print(f"\n=== TRANSIENT (leave running) ===")
    for r in transient:
        print(f"  [{r['id']}] {r['name']}: {r['last_error'][:120]}")
    print(f"\n=== UNKNOWN (inspect manually) ===")
    for r in unknown:
        print(f"  [{r['id']}] {r['name']}: {r['last_error'][:120]}")
    print(f"\nSUMMARY: missed={len(missed)} transient={len(transient)} "
          f"unknown={len(unknown)}")
    if missed:
        print("ACTION: pause each MISSED job (enabled=false, state=paused) and "
              "append its id to the recommended_issue's jobs_paused. Keep issue "
              "user_gated+escalation_needed.")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Custodian escalation-loop missed-enrollment probe: find ENABLED+erroring jobs not covered by any open issue's jobs_paused, classify against user-gated fingerprints.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n  python3 find_missed_user_gated_jobs.py\n  python3 find_missed_user_gated_jobs.py --profile koda",
    )
    ap.add_argument("--profile", default="indigo", help="Profile name")
    args = ap.parse_args()
    HOME = f"~/.hermes/profiles/{args.profile}"
    JOBS_PATH = f"{HOME}/cron/jobs.json"
    ISSUES_PATHS[:] = [f"{HOME}/commons/data/ocas-custodian/issues.jsonl"]
    main()
