#!/usr/bin/env python3
"""
verify_escalation_state.py — Deterministic bidirectional verification for the
ocas-custodian escalation execution loop.

Reads the PROFILE issues.jsonl (brace-depth parsed) and the PROFILE jobs.json, then
checks the escalation state BOTH directions:
  (a) INVERSE staleness: issue says resolved/paused but a job is still
      enabled=true and last_status=error -> re-pause + re-open.
  (b) FORWARD staleness: an open/user_gated issue whose listed jobs have ALL
      recovered (last_status=ok, last_error empty) -> candidate to resolve.
AND reports whether each issue's jobs_paused metadata matches the live paused set,
so the loop can skip a no-delta issues.jsonl rewrite (concurrency-safe).

Confirmed pattern from the 2026-07-07 escalation loop run: when verification shows
all user-gated jobs already paused AND metadata already matches live state, the
correct action is verify-and-document (write action journal), NOT a forced rewrite.

Run: python3 scripts/verify_escalation_state.py
"""
import json
import os

PROFILE = "~/.hermes/profiles/indigo"
ISSUES = f"{PROFILE}/commons/data/ocas-custodian/issues.jsonl"
JOBS = f"{PROFILE}/cron/jobs.json"

# issue fingerprint -> live job-error fingerprint
LIVE_FP = {
    "oc_openrouter_402_credits_exhausted": "openrouter_402",
    "oc_nous_api_key_invalid": "nous_401",
    "oc_http_404_model_deprecated": "owl_404",
}
MONITOR_LIST_ID = "39b7edc44b35"  # monitor:list (Google Tasks 403)


def parse_brace_depth(text):
    objs = []; depth = 0; in_str = False; esc = False; buf = []
    for ch in text:
        if in_str:
            buf.append(ch)
            if esc: esc = False
            elif ch == '\\': esc = True
            elif ch == '"': in_str = False
            continue
        if ch == '"':
            in_str = True; buf.append(ch); continue
        if ch == '{': depth += 1
        elif ch == '}': depth -= 1
        buf.append(ch)
        if depth == 0 and ch == '}':
            s = ''.join(buf).strip()
            if s:
                try: objs.append(json.loads(s))
                except Exception: pass
            buf = []
    return objs


def fp_of(j):
    le = (j.get("last_error") or "")
    if "402" in le and ("credits" in le or "openrouter" in le or "insufficient" in le):
        return "openrouter_402"
    if "401" in le and ("API key is invalid" in le or "nous" in le.lower()):
        return "nous_401"
    if "404" in le and "owl-alpha" in le:
        return "owl_404"
    return None


def main():
    with open(JOBS) as f:
        data = json.load(f)
    jobs = data.get("jobs", []) if isinstance(data, dict) else data
    by_id = {j.get("id"): j for j in jobs}

    paused_by_fp = {}
    for j in jobs:
        if (not j.get("enabled", True)) and j.get("state") == "paused":
            fp = fp_of(j)
            if fp: paused_by_fp.setdefault(fp, set()).add(j.get("id"))

    enabled_err = [j.get("id") for j in jobs
                   if j.get("enabled", True) and j.get("last_status") == "error"]

    with open(ISSUES) as f:
        issues = []
        for line in f:
            if line.strip():
                issues.extend(parse_brace_depth(line))

    print(f"Jobs total={len(jobs)} enabled+erroring={len(enabled_err)}")
    print("Live paused by fp: " + ", ".join(f"{k}={len(v)}" for k, v in paused_by_fp.items()))
    print("=" * 72)

    inverse = 0
    reconcile_needed = False
    forward_candidates = []
    for e in issues:
        iid = e.get("issue_id") or e.get("id")
        status = e.get("status")
        esc = e.get("escalation_needed")
        if status in ("resolved", "closed", "legacy") and not esc:
            continue
        listed = set(e.get("jobs_paused") or [])
        fp = e.get("fingerprint")
        actual = set()
        if fp == "oc_google_tasks_api_403":
            j = by_id.get(MONITOR_LIST_ID)
            if j and (not j.get("enabled", True)) and j.get("state") == "paused":
                actual.add(MONITOR_LIST_ID)
        elif fp in LIVE_FP:
            actual = paused_by_fp.get(LIVE_FP[fp], set())
        missing = actual - listed
        extra = listed - actual
        if missing or extra:
            reconcile_needed = True

        # (a) inverse: listed/paused id that is actually enabled+erroring
        for jid in listed:
            j = by_id.get(jid)
            if j and j.get("enabled", True) and j.get("last_status") == "error":
                inverse += 1
                print(f"  [INVERSE] {iid}: job {jid} enabled+erroring but claimed paused/resolved")

        # (b) forward: open/escalated issue whose listed jobs ALL recovered
        if listed:
            all_recovered = all(
                (lambda x: x and x.get("last_status") == "ok" and not (x.get("last_error") or "").strip())(by_id.get(jid))
                for jid in listed
            )
            if all_recovered:
                forward_candidates.append(iid)

        print(f"issue={iid} status={status} fp={fp} "
              f"listed={len(listed)} actual_paused={len(actual)} "
              f"missing={len(missing)} extra={len(extra)}")

    print("=" * 72)
    print(f"INVERSE-GOTCHA count: {inverse}")
    print(f"FORWARD-staleness candidates (all jobs recovered, resolve?): {forward_candidates}")
    print(f"Reconcile write needed: {reconcile_needed}")
    print(f"Live enabled+erroring job ids: {enabled_err}")

    all_paused = set()
    for e in issues:
        all_paused.update(e.get("jobs_paused") or [])
    unaccounted = [j for j in enabled_err if j not in all_paused]
    print(f"Enabled+erroring NOT in any jobs_paused (expected=transient, leave running): {unaccounted}")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Custodian escalation-loop bidirectional verification probe: parse the profile issues.jsonl and jobs.json, check both staleness directions, report per-issue jobs_paused deltas vs the live paused set.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n  python3 verify_escalation_state.py\n  python3 verify_escalation_state.py --profile koda",
    )
    ap.add_argument("--profile", default=PROFILE, help="Profile name or HOME dir")
    args = ap.parse_args()
    PROFILE = args.profile if os.path.sep in args.profile else f"~/.hermes/profiles/{args.profile}"
    JOBS = f"{PROFILE}/cron/jobs.json"
    ISSUES = f"{PROFILE}/commons/data/ocas-custodian/issues.jsonl"
    main()