#!/usr/bin/env python3
import os
"""
scan_escalation_journal_gaps.py — escalation-loop journal-to-issues gap probe.

Walks ALL custodian journal dirs (profile + commons, including YYYY-MM-DD
subdirectories and loose files), parses each journal (handles JSON-list and
concatenated-JSONL via brace-depth), and for journals within --hours (default 24)
whose `escalation_needed` is truthy, cross-references the fingerprints /
`escalation_refs` they cite against the OPEN issues in the profile issues.jsonl.

Reports:
  * GAP  — a journal flagged escalation_needed:true but the cited fingerprint /
           issue_id is NOT present as an open issue in issues.jsonl (silent
           escalation drop, per custodian Step 8b / 8b-variant).
  * RECOVERY NOTE — journals that explicitly say an issue recovered / resolved
           (forward-stale reconciliation candidates).

Uses CONTENT timestamps (not file mtime) because journal file mtimes lag the
system clock by ~7h in this environment (see mentor cron-mtime-discovery-gotcha).

Read-only by default. With --write, it creates missing issues in issues.jsonl
(one per root-cause fingerprint). Use --write only after you have verified the
gap is real — auto-creating from a transient fingerprint pollutes issues.jsonl.

Usage:
  python3 scripts/scan_escalation_journal_gaps.py [--hours 24] [--write]
"""
import json, os, argparse, re
from datetime import datetime, timezone

JOURNAL_DIRS = [
    os.path.expanduser("~/.hermes/profiles/indigo/commons/journals/ocas-custodian"),
    os.path.expanduser("~/.hermes/commons/journals/ocas-custodian"),
]
PROFILE_ISSUES = os.path.expanduser("~/.hermes/profiles/indigo/commons/data/ocas-custodian/issues.jsonl")

TRANSIENT_MARKERS = ("transient", "noop", "shutdown", "rate_limit",
                     "resource_exhausted", "provider_error", "interpreter")

_ID_RE_BAD = re.compile(r"[^A-Za-z0-9_.:-]+")


def sanitize_id(raw):
    """Reduce a journal's raw fingerprint string to a clean, keyable token.

    Journal prose frequently embeds the fingerprint in a sentence, e.g.
    'oc_hello_operator_restart_drops_inflight (not present in prior light run's
    escalation set)'. Un-sanitized that produces an issue_id containing spaces
    and parentheses: it parses cleanly, raises no error, and is unreachable by
    every exact-key downstream lookup. Cut at the first whitespace and then
    allow-list the remaining characters.
    """
    s = str(raw).strip()
    s = s.split()[0] if s.split() else s      # drop any trailing parenthetical/prose
    s = _ID_RE_BAD.sub("_", s).strip("_")
    return s or "unnamed_fingerprint"


def brace_depth_parse(path):
    """Read a JSON/JSONL file and parse it via the shared custodian parser."""
    from custodian_common import parse_issues  # lazy: keeps --help working without deps
    try:
        with open(path) as f:
            return parse_issues(f.read())
    except Exception:
        return []


def get_ts(d):
    for k in ("timestamp", "run_ts", "date", "created_at", "time"):
        v = d.get(k) if isinstance(d, dict) else None
        if isinstance(v, str):
            # Fast path: datetime.fromisoformat parses ISO 8601 natively in C (~47x faster
            # than trial-and-error strptime exception loops across thousands of journals)
            try:
                return datetime.fromisoformat(v).timestamp()
            except Exception:
                for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ",
                            "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S.%fZ"):
                    try:
                        return datetime.strptime(v.replace("Z", "+0000"), fmt).timestamp()
                    except Exception:
                        pass
    return None


def collect_fps(obj, acc):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if 'fingerprint' in k.lower():
                if isinstance(v, str): acc.add(v)
                elif isinstance(v, list): acc.update(x for x in v if isinstance(x, str))
            if k in ('escalation_refs',) and isinstance(v, list):
                acc.update(x for x in v if isinstance(x, str))
            collect_fps(v, acc)
    elif isinstance(obj, list):
        for it in obj:
            collect_fps(it, acc)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=24.0)
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    cutoff = now.timestamp() - args.hours * 3600

    # open issues (exclude resolved) for DETECTION reporting...
    open_ids = set(); open_fps = set()
    # ...and an all-status presence set for WRITE suppression. A journal that cites a
    # fingerprint whose real issue was already resolved is still a genuine GAP to report
    # (detection stays correct), but --write must NOT re-create it. Conflating the two sets
    # is what manufactured phantom user_gated rows on every --write pass.
    seen_ids = set(); seen_fps = set()
    for r in brace_depth_parse(PROFILE_ISSUES):
        iid = r.get("issue_id") or r.get("id")
        fp = r.get("fingerprint") or r.get("error_fingerprint")
        if iid: seen_ids.add(iid)
        if fp: seen_fps.add(fp)
        if r.get("status") in ("resolved",):
            continue
        if iid: open_ids.add(iid)
        if fp: open_fps.add(fp)

    # scan journals
    files = []
    for d in JOURNAL_DIRS:
        for root, _, fs in os.walk(d):
            for fn in fs:
                if fn.endswith(".json"):
                    files.append(os.path.join(root, fn))

    gaps = []; recoveries = []
    for fp in files:
        for d in brace_depth_parse(fp):
            if not isinstance(d, dict):
                continue
            ts = get_ts(d)
            if ts is None or ts < cutoff:
                continue
            esc = d.get("escalation_needed")
            if isinstance(esc, str):
                esc = esc.lower() in ("true", "1", "yes")
            if not esc:
                continue
            fps = set(); collect_fps(d, fps)
            cited_ids = set(d.get("escalation_refs", []) or [])
            missing_ids = [i for i in cited_ids if i not in open_ids]
            missing_fps = [f for f in fps
                           if f not in open_fps
                           and not any(m in f.lower() for m in TRANSIENT_MARKERS)]
            if missing_ids or missing_fps:
                gaps.append((d.get("run_id") or fp.split("/")[-1], ts, missing_ids, missing_fps))
            blob = json.dumps(d).lower()
            if "recovered" in blob or "issue now resolved" in blob or "now resolved" in blob:
                recoveries.append((d.get("run_id") or fp.split("/")[-1], ts))

    print(f"Window: last {args.hours}h (since {datetime.fromtimestamp(cutoff, timezone.utc).isoformat()})")
    print(f"Open issues loaded: {len(open_ids)} (ids) / {len(open_fps)} (fingerprints)")
    print(f"Journals scanned: {len(files)}")

    print("\n=== GAP (escalation_needed but no matching open issue) ===")
    if not gaps:
        print("  none")
    for rid, ts, mids, mfs in gaps:
        print(f"  {datetime.fromtimestamp(ts, timezone.utc).isoformat()} {rid}")
        if mids: print(f"     missing issue_ids: {mids}")
        if mfs: print(f"     missing fingerprints: {mfs}")

    print("\n=== RECOVERY notes (forward-stale check candidates) ===")
    if not recoveries:
        print("  none")
    for rid, ts in recoveries:
        print(f"  {datetime.fromtimestamp(ts, timezone.utc).isoformat()} {rid}")

    if args.write and gaps:
        created = 0; skipped = 0
        new_recs = []
        for rid, ts, mids, mfs in gaps:
            for f in mfs:
                clean = sanitize_id(f)
                if clean in seen_fps or clean in seen_ids:
                    skipped += 1
                    continue
                rec = {
                    "issue_id": clean + "_" + now.strftime("%Y%m%d"),
                    "fingerprint": clean,
                    "status": "user_gated",
                    "escalation_needed": True,
                    "summary": (f"Auto-created from journal gap scan (run {rid}); fingerprint "
                                f"{clean} flagged escalation_needed but absent from issues.jsonl."),
                    "jobs_paused": [],
                    "created_at": now.isoformat(),
                }
                new_recs.append(rec)
                seen_fps.add(clean); seen_ids.add(rec["issue_id"])
                created += 1
        if new_recs:
            # Race-safe append: do NOT rewrite the whole file (a concurrent writer's rows
            # would be lost). Open in append mode so an interleaved write cannot clobber.
            with open(PROFILE_ISSUES, "a") as out:
                for r in new_recs:
                    out.write(json.dumps(r) + "\n")
        print(f"\nWROTE {created} missing-issue record(s) to {PROFILE_ISSUES}"
              f" (suppressed {skipped} already-present fingerprint(s))")


if __name__ == "__main__":
    main()