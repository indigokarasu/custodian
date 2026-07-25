#!/usr/bin/env python3
"""Escalation execution loop - actually PAUSE user-gated burning jobs and reconcile issues.jsonl.

Generic + re-runnable. Reads jobs.json + the PROFILE issues.jsonl, classifies enabled+erroring
jobs by LIVE last_error, pauses user-gated ones (leaving transient running), reconciles each
open issue's jobs_paused to the ACTUAL paused set, and flags duplicate issues.

WHY THIS EXISTS: prior escalation passes populated issues.jsonl jobs_paused lists but NEVER
actually paused the jobs in jobs.json (inverse-gotcha: issue claims paused, job still enabled+
erroring). The fix is to pause from the LIVE jobs.json state, not trust the issue metadata.

Safe: backs up jobs.json before editing. Use --dry-run to preview with zero writes.
"""
import json, os, argparse
from datetime import datetime, timezone

PROFILE = "~/.hermes/profiles/indigo"
JOBS = os.path.join(PROFILE, "cron/jobs.json")
ISSUES = os.path.join(PROFILE, "commons/data/ocas-custodian/issues.jsonl")

# issue fingerprint field -> live last_error classifier bucket
FP_MAP = {
    "oc_nous_api_key_invalid": "nous",
    "oc_default_provider_token_expired": "nous",   # same Nous provider
    "oc_openrouter_402_credits_exhausted": "openrouter",
    "oc_google_tasks_api_403": "google403",
    "oc_http_404_model_deprecated": "owl",
}
# bucket -> keywords in last_error (lowercased)
BUCKET_KW = {
    "nous": ["portal.nousresearch.com", "your api key is invalid", "token_expired", "token is expired"],
    "openrouter": ["402", "credits", "openrouter"],
    "google403": ["403", "forbidden"],
    "owl": ["404", "owl", "no endpoints found"],
    "transient": ["resourceexhausted", "worker local total request limit", "nvidia"],
}


def classify(le, name):
    s = (le or "").lower()
    n = (name or "").lower()
    # masked subprocess wrapper: monitor:list exits 1 but is really a Google 403 OAuth failure
    if n.startswith("monitor:list"):
        return "google403"
    for kw in BUCKET_KW["transient"]:
        if kw in s:
            return "transient"
    for kw in BUCKET_KW["nous"]:
        if kw in s:
            return "nous"
    for kw in BUCKET_KW["openrouter"]:
        if kw in s:
            return "openrouter"
    for kw in BUCKET_KW["google403"]:
        if kw in s:
            return "google403"
    for kw in BUCKET_KW["owl"]:
        if kw in s:
            return "owl"
    return "unknown"


def parse_issues(path):
    recs = []
    text = open(path).read()
    depth = 0
    buf = []
    ins = False
    esc = False
    for ch in text:
        if ins:
            buf.append(ch)
            if esc:
                esc = False
            elif ch == '\\':
                esc = True
            elif ch == '"':
                ins = False
            continue
        if ch == '"':
            ins = True
            buf.append(ch)
            continue
        if ch == '{':
            if depth == 0:
                buf = ['{']
            else:
                buf.append(ch)
            depth += 1
            continue
        if ch == '}':
            if depth > 0:
                buf.append('}')
                depth -= 1
                if depth == 0:
                    try:
                        recs.append(json.loads("".join(buf)))
                    except Exception:
                        pass
                    buf = []
            continue
        if depth > 0:
            buf.append(ch)
    return recs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Preview only, no writes")
    ap.add_argument("--jobs", default=JOBS)
    ap.add_argument("--issues", default=ISSUES)
    args = ap.parse_args()

    d = json.load(open(args.jobs))
    jobs = {j["id"]: j for j in d.get("jobs", [])}
    NOW = datetime.now(timezone.utc).isoformat()

    # 1. Classify enabled+erroring jobs, pause user-gated, leave transient/unknown-running
    paused = {"nous": [], "openrouter": [], "google403": [], "owl": [], "unknown": []}
    left = []
    to_pause = []
    for jid, j in jobs.items():
        if not j.get("enabled"):
            continue
        if j.get("last_status") != "error" and not (j.get("last_error") or "").strip():
            continue
        fp = classify(j.get("last_error"), j.get("name"))
        if fp == "transient":
            left.append((jid, j.get("name"), "transient"))
            continue
        if fp == "unknown":
            left.append((jid, j.get("name"), "unknown: " + (j.get("last_error") or "")[:80]))
            continue
        to_pause.append((jid, fp))
        paused[fp].append(jid)

    if not args.dry_run:
        bk = args.jobs + ".bak-esc-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        json.dump(d, open(bk, "w"), indent=2)
        for jid, fp in to_pause:
            j = jobs[jid]
            j["enabled"] = False
            j["state"] = "paused"
            j["paused_at"] = NOW
            j["pause_reason"] = f"user_gated:{fp} - paused by escalation loop; re-enable on provider recovery"
        json.dump(d, open(args.jobs, "w"), indent=2)
        print(f"Backed up jobs.json -> {bk}")
    print(f"PAUSED (will pause): {len(to_pause)}  | LEFT RUNNING: {len(left)}")
    for k in ("nous", "openrouter", "google403", "owl"):
        print(f"  {k}: {len(paused[k])}")
    for jid, name, why in left:
        print(f"  LEFT {jid} {name}: {why[:90]}")

    # 2. Reconcile issues.jsonl jobs_paused to ACTUAL paused state
    recs = parse_issues(args.issues)
    actual_paused = {jid for jid, j in jobs.items()
                     if j.get("enabled") is False and j.get("state") == "paused"}
    for r in recs:
        bucket = FP_MAP.get(r.get("fingerprint"))
        if not bucket:
            continue
        if r.get("status") == "resolved" and not r.get("escalation_needed"):
            continue
        match = [jid for jid in actual_paused
                 if classify(jobs[jid].get("last_error"), jobs[jid].get("name")) == bucket]
        r["jobs_paused"] = sorted(match)
        r["status"] = "user_gated"
        r["escalation_needed"] = True
        r["pause_reason"] = f"{bucket} user-gated failure; paused by escalation loop {NOW[:10]}; re-enable on provider recovery"
        r["reconciled_at"] = NOW

    # 3. Duplicate detection: >1 open issue in the same bucket
    by_bucket = {}
    for r in recs:
        b = FP_MAP.get(r.get("fingerprint"))
        if b and r.get("escalation_needed"):
            by_bucket.setdefault(b, []).append(r.get("issue_id") or r.get("id"))
    print("\nDUPLICATE CANDIDATES (same bucket, overlapping):")
    for b, ids in by_bucket.items():
        if len(ids) > 1:
            print(f"  bucket={b}: {ids}  -> consider folding into one issue")

    if not args.dry_run:
        seen = set()
        out = []
        for r in recs:
            iid = r.get("issue_id") or r.get("id") or json.dumps(r)[:40]
            if iid not in seen:
                seen.add(iid)
                out.append(r)
        with open(args.issues, "w") as f:
            for r in out:
                f.write(json.dumps(r) + "\n")
        print(f"\nWrote {len(out)} issues to {args.issues}")
    else:
        print("\n[DRY RUN] no writes performed.")


if __name__ == "__main__":
    main()
