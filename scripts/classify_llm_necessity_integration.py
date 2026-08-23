#!/usr/bin/env python3
"""classify_llm_necessity_integration.py — Cron-health integration for LLM necessity.

Run as part of custodian:cron-health scan. This script:
1. Runs classify_llm_necessity.py --json
2. Loads the acknowledgment state file
3. Finds NEW unacknowledged candidates
4. Writes/updates a single oc_cron_llm_unnecessary issue in issues.jsonl
5. Updates the acknowledgment state

Run via terminal() from cron context:
    python3 ~/.hermes/skills/ocas-custodian/scripts/classify_llm_necessity_integration.py

REPORT-ONLY: NEVER auto-converts a job to no_agent.
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timezone

PROFILE = "indigo"
PROFILE_HOME = os.path.expanduser("~/.hermes/profiles/{PROFILE}")
CLASSIFY_SCRIPT = os.path.expanduser("~/.hermes/skills/ocas-custodian/scripts/classify_llm_necessity.py")
ISSUES_PATH = f"{PROFILE_HOME}/commons/data/ocas-custodian/issues.jsonl"
ACK_PATH = f"{PROFILE_HOME}/commons/data/ocas-custodian/llm_necessity_ack.json"

FINGERPRINT = "oc_cron_llm_unnecessary"
ISSUE_ID = "oc_cron_llm_unnecessary"


def run_classifier():
    """Run the classifier and return parsed JSON output."""
    result = subprocess.run(
        [sys.executable, CLASSIFY_SCRIPT, "--json", "--profile", PROFILE],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        raise SystemExit(f"Classifier failed: {result.stderr}")
    return json.loads(result.stdout)


def load_ack():
    """Load acknowledgment state."""
    if os.path.exists(ACK_PATH):
        with open(ACK_PATH) as f:
            return json.load(f)
    return {}


def save_ack(state):
    """Save acknowledgment state."""
    with open(ACK_PATH, "w") as f:
        json.dump(state, f, indent=2)


def load_issues():
    """Load existing issues from issues.jsonl (brace-depth parse)."""
    if not os.path.exists(ISSUES_PATH):
        return []
    with open(ISSUES_PATH) as f:
        raw = f.read()
    if not raw.strip():
        return []
    entries = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
            continue
        except json.JSONDecodeError:
            pass
        # Brace-depth parse for concatenated objects
        depth = 0
        cur = ""
        instr = False
        esc = False
        for ch in line:
            if ch == "\\" and not esc:
                esc = True
                cur += ch
                continue
            if ch == '"' and not esc:
                instr = not instr
            if not instr:
                if ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
            cur += ch
            if depth == 0 and cur.strip():
                try:
                    entries.append(json.loads(cur))
                except json.JSONDecodeError:
                    pass
                cur = ""
        if cur.strip():
            try:
                entries.append(json.loads(cur))
            except json.JSONDecodeError:
                pass
    return entries


def save_issues(entries):
    """Write all entries back to issues.jsonl, one JSON per line."""
    with open(ISSUES_PATH, "w") as f:
        for e in entries:
            f.write(json.dumps(e, default=str) + "\n")


def main():
    print(f"=== LLM Necessity Integration ({datetime.now(timezone.utc).isoformat()}) ===")

    # Step 1: Run classifier
    print("Running classifier...")
    results = run_classifier()
    summary = results["summary"]
    verdicts = results["verdicts"]

    unnecessary = [r for r in verdicts if r["verdict"] == "llm_unnecessary"]
    borderline = [r for r in verdicts if r["verdict"] == "llm_borderline"]

    print(f"  llm_unnecessary: {summary['llm_unnecessary']}")
    print(f"  llm_borderline:  {summary['llm_borderline']}")
    print(f"  llm_needed:      {summary['llm_needed']}")
    print(f"  already_no_agent:{summary['already_no_agent']}")

    all_candidates = unnecessary + borderline
    candidate_names = [r["name"] for r in all_candidates]

    if not candidate_names:
        print("No candidates found. Checking if existing issue needs resolution...")
        # Check if there's an existing open issue — if candidates are now 0, resolve it
        issues = load_issues()
        existing = [e for e in issues if e.get("fingerprint") == FINGERPRINT
                    and e.get("status") not in ("resolved", "closed")]
        if existing:
            print(f"  Resolving existing issue {existing[0].get('issue_id')} — no remaining candidates")
            for e in issues:
                if e.get("fingerprint") == FINGERPRINT and e.get("status") not in ("resolved", "closed"):
                    e["status"] = "resolved"
                    e["resolved_at"] = datetime.now(timezone.utc).isoformat()
                    e["escalation_needed"] = False
                    e["resolution_note"] = "All candidates resolved or converted"
            save_issues(issues)
            print("  Issue resolved.")
        else:
            print("  No existing issue — nothing to do.")
        print("Done.")
        return

    # Step 2: Load ack state
    ack = load_ack()
    print(f"  Acknowledged jobs: {len(ack)}")

    # Step 3: Find new candidates (not acknowledged)
    new_candidates = []
    for r in all_candidates:
        name = r["name"]
        if name not in ack:
            new_candidates.append(r)
        elif ack[name] == "converted":
            pass  # Already converted — don't re-report
        elif ack[name] == "intentional_llm":
            pass  # Intentionally kept as LLM
        elif ack[name] == "acknowledged":
            pass  # Already acknowledged
        else:
            new_candidates.append(r)  # Unknown status → re-report

    print(f"  New unacknowledged candidates: {len(new_candidates)}")

    # Step 4: Update issues.jsonl
    issues = load_issues()
    existing_issue = None
    for e in issues:
        if e.get("fingerprint") == FINGERPRINT:
            existing_issue = e
            break

    now = datetime.now(timezone.utc).isoformat()

    if new_candidates:
        affected = [r["name"] for r in new_candidates]
        signals_summary = {}
        for r in new_candidates:
            name = r["name"]
            sigs = r.get("signals", {})
            signals_summary[name] = {
                "verdict": r["verdict"],
                "reason": r["reason"],
                "positive_signals": sigs.get("positive_signals", []),
                "has_script": sigs.get("has_script", False),
            }

        if existing_issue and existing_issue.get("status") not in ("resolved", "closed"):
            # Update existing issue
            existing_components = existing_issue.get("affected_components", [])
            # Merge new candidates without duplicates
            for n in affected:
                if n not in existing_components:
                    existing_components.append(n)
            existing_issue["affected_components"] = existing_components
            existing_issue["updated_at"] = now
            existing_issue["detail"] = json.dumps(signals_summary, indent=2)
            print(f"  Updated existing issue: {existing_issue.get('issue_id')}")
        else:
            # Create new issue
            new_issue = {
                "timestamp": now,
                "run_id": f"llm-necessity-{now[:10]}",
                "issue_id": ISSUE_ID,
                "fingerprint": FINGERPRINT,
                "status": "open",
                "type": "cron_llm_waste",
                "severity": "low",
                "description": f"LLM unnecessary cron jobs: {len(new_candidates)} candidates may not need LLM reasoning",
                "detail": json.dumps(signals_summary, indent=2),
                "tier": 2,
                "escalation_needed": True,
                "affected_components": affected,
                "recommendation": "Each candidate should be reviewed: if the job's prompt is just 'run a script and report output', "
                                   "convert to no_agent with a wrapper script. If prompt genuinely needs reasoning, "
                                   "acknowledge as intentional_llm to suppress re-reporting.",
                "created_at": now,
            }
            issues.append(new_issue)
            print(f"  Created new issue with {len(affected)} candidates")

        save_issues(issues)

        # Step 4a: For acknowledged candidates that are no longer in the list, note them
        for name, status in list(ack.items()):
            if status == "converted" and name not in candidate_names:
                print(f"  {name}: already converted, no longer in candidates")
            elif status == "intentional_llm" and name not in candidate_names:
                print(f"  {name}: intentional_llm, no longer in candidates")

    # Step 5: No new candidates, but existing issue may need updating
    elif not new_candidates and candidate_names:
        # All existing candidates are acknowledged
        print("  All candidates already acknowledged.")
        if existing_issue and existing_issue.get("status") not in ("resolved", "closed"):
            # Keep the issue but note it's acknowledged
            existing_issue["updated_at"] = now
            existing_issue["detail"] = "All candidates acknowledged — no new action needed"
            save_issues(issues)
            print("  Updated existing issue with acknowledgment note.")

    # Step 6: Count acknowledged
    converted_count = sum(1 for v in ack.values() if v == "converted")
    intentional_count = sum(1 for v in ack.values() if v == "intentional_llm")
    acknowledged_count = sum(1 for v in ack.values() if v == "acknowledged")
    print(f"  Acknowledged: {acknowledged_count} pending, {converted_count} converted, {intentional_count} intentional_llm")

    print("Done. [REPORT ONLY — no jobs were modified]")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Custodian cron-health integration for LLM necessity: runs the classifier, checks acknowledgment state, and writes/updates a single oc_cron_llm_unnecessary issue. REPORT-ONLY — never auto-converts jobs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n  python3 classify_llm_necessity_integration.py\n  python3 classify_llm_necessity_integration.py --profile koda",
    )
    ap.add_argument("--profile", default=PROFILE, help="Profile name")
    args = ap.parse_args()
    PROFILE = args.profile
    PROFILE_HOME = os.path.expanduser("~/.hermes/profiles/{PROFILE}")
    ISSUES_PATH = f"{PROFILE_HOME}/commons/data/ocas-custodian/issues.jsonl"
    ACK_PATH = f"{PROFILE_HOME}/commons/data/ocas-custodian/llm_necessity_ack.json"
    main()