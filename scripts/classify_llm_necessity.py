#!/usr/bin/env python3
"""classify_llm_necessity.py — Custodian LLM-necessity classifier.

Reads the profile-scoped jobs.json, evaluates every enabled non-paused job
with no_agent == false against a deterministic heuristic, and emits a verdict
for each: llm_unnecessary (candidate to convert to no_agent), llm_borderline
(mechanical but needs a purpose-built wrapper), or llm_needed (genuinely needs
reasoning).

Heuristic derived from the 2026-07-12 manual operator audit of all 144 jobs
(47 pure-overhead convertible → currently 75 already no_agent, 8 borderline,
50 genuine).

Run from cron context via terminal():
    python3 ~/.hermes/skills/ocas-custodian/scripts/classify_llm_necessity.py
    python3 ~/.hermes/skills/ocas-custodian/scripts/classify_llm_necessity.py --profile indigo
"""

import json
import os
import re
import sys

# the agent is the default profile — this owns the cron fleet
PROFILE = "indigo"
JOBS_PATH_CANDIDATES = [
    os.path.expanduser("~/.hermes/profiles/{PROFILE}/cron/jobs.json"),
    os.path.expanduser("~/.hermes/cron/jobs.json"),
]

# ── Strong positive signals (candidate llm_unnecessary) ─────────────────────

# Self-update cluster
_SELF_UPDATE_PATTERNS = [
    r"\bself-update\b",
    r"\bcheck git for new commits\b",
    r"\bpull latest .* from GitHub\b",
    r"\bupdate_skill\.sh\b",
    r"\bcheck git",
]

# Script-wrapper: prompt's primary action is executing a script
# Covers: "Run <script>.py" / "Execute: python3 <script>" / "run the <name> script"
# Also covers: "Execute via Python script — do NOT load the full SKILL.md"
_SCRIPT_WRAPPER_PATTERNS = [
    r"^(Run|Execute)[:\s]+(python3|bash|sh|\./)?\s*\S+\.(py|sh)\b",
    r"^(Run|Execute)[:\s]+\S+[:\s]+(python3|bash|sh|\./)\s*\S+\.(py|sh)\b",
    r"^Run (the|this) .*(script|check|scan)",
    r"Execute via Python script",
]

# Needless skill-load + script: "Load <skill> then run <script>.py"
_SKILL_LOAD_SCRIPT_PATTERNS = [
    r"load (the|this) \S+ skill.*then (run|execute)",
    r"using the \S+ skill.*run the",
]

# ── Strong negative signals (genuinely needs LLM, do NOT flag) ──────────────

# Generation/reasoning verbs that need LLM
_GENERATION_VERBS = [
    r"\bgenerate\b", r"\bcompose\b", r"\bdraft\b", r"\bjudge\b",
    r"\bassess\b", r"\bdecide\b", r"\bclassify\b", r"\bevaluate\b",
    r"\breason\b", r"\bpropose\b", r"\binterpret\b", r"\bresearch\b",
    r"\breview\b", r"\bcritique\b", r"\bscore\b", r"\bcurate\b",
    r"\bmine\b", r"\bhumaniz", r"\bsynthesiz", r"\banalyz",
    r"\bidentify\b", r"\brecommend\b",
]

# Creative writing by an LLM
_WRITE_CREATIVE = [
    r"\bwrite\s+(a|an|the)\s+(post|haiku|briefing|draft|dream|summary|essay|letter|note)",
    r"compos(e|ing)\s+(a|an|the)\s+(haiku|post|content|briefing|message)",
    r"\bdraft\s+(a|an|the|this)",
]

# Persona framing — agent is playing a specific role
_PERSONA_FRAMING = [
    r"You are the agent",
    r"You are (a|an|the) (Engineering Manager|Koda Dispatcher|KODA|finch)",
    r"Pure LLM — no",
]

# Multi-source synthesis — needs LLM to combine/interpret information
_MULTI_SOURCE = [
    r"do NOT just summarize",
    r"do not just summarize",
    r"synthesiz",
    r"cross-reference",
]

# Dispatch / merge-gate / orchestration patterns
_DISPATCH_PATTERNS = [
    r"\bdispatcher\b",
    r"\bfinch:work\b",
    r"\bfinch:scan\b",
    r"\bEngineering Manager",
    r"\bmergeable\b",
    r"\bmerge pass\b",
    r"\breview pass\b",
    r"\bPHASE 0\b",
    r"\bpending assessment\b",
    r"\bpaper trading assessment\b",
]

# ── Known genuine-LLM jobs by name (never flag) ─────────────────────────────
# These are jobs whose prompts are genuinely LLM but don't always have
# explicit negative signals in the first 150 chars.
_KNOWN_GENUINE_LLM = [
    "taste:scan", "finch:scan", "finch:work",
    "daily-user-context", "bones:paper-trade",
    "daily-false-trigger-fix",
    "haiku:content-post", "haiku:content-review",
    "haiku:engage", "haiku:follow-maintenance",
    "haiku:haiku-post", "haiku:morning-scan",
    "vesper:morning", "vesper:evening",
    "vesper:deliver-morning", "vesper:deliver-evening",
    "Koda Dispatcher", "koda dispatcher",
    "art:studio", "art:engagement",
    "lucid:dream",
]


def load_jobs():
    for p in JOBS_PATH_CANDIDATES:
        if os.path.exists(p):
            with open(p) as f:
                d = json.load(f)
            return d.get("jobs", []) if isinstance(d, dict) else d
    raise SystemExit("jobs.json not found in candidates")


def has_positive_signal(prompt, script):
    """True if any STRONG positive signal (wasted LLM) matches."""
    pl = prompt.lower()

    # Signal 1: Script-field set but LLM — strong standalone.
    if script and isinstance(script, str) and script.strip():
        return True

    # Signal 2: Self-update cluster
    for pat in _SELF_UPDATE_PATTERNS:
        if re.search(pat, pl):
            return True

    # Signal 3: Script-wrapper pattern
    for pat in _SCRIPT_WRAPPER_PATTERNS:
        if re.search(pat, prompt):
            return True

    # Signal 4: Needless skill-load + script
    for pat in _SKILL_LOAD_SCRIPT_PATTERNS:
        if re.search(pat, pl):
            return True

    return False


def has_negative_signal(prompt, name):
    """True if the job genuinely needs LLM reasoning."""
    pl = prompt.lower()
    name_lower = name.lower() if name else ""

    # By-name exclusion list
    for known in _KNOWN_GENUINE_LLM:
        if known.lower() in name_lower:
            return True

    # Generation/reasoning verbs
    for pat in _GENERATION_VERBS:
        if re.search(pat, pl):
            return True

    # Creative write
    for pat in _WRITE_CREATIVE:
        if re.search(pat, pl):
            return True

    # Persona framing — don't need lower() here, persona has caps
    for pat in _PERSONA_FRAMING:
        if re.search(pat, prompt):
            return True

    # Multi-source synthesis
    for pat in _MULTI_SOURCE:
        if re.search(pat, pl):
            return True

    # Dispatch / code-review
    for pat in _DISPATCH_PATTERNS:
        if re.search(pat, pl):
            return True

    return False


def is_borderline(prompt, name):
    """Jobs that are mechanical but need a purpose-built wrapper."""
    pl = prompt.lower()
    # Check for exit-code interpretation needs
    if re.search(r"exit code", pl):
        return True
    # Check for skill-load patterns that aren't full synthesis
    if re.search(r"(run|check|scan|monitor)\s+(daily|weekly|regular)\s+\S+\s+(using|with)\s+the?\s+\S+\s+skill", pl):
        return True
    return False


def classify(job):
    """Return (verdict, reason, signals) for a single job."""
    name = job.get("name", "unknown") or ""
    prompt = job.get("prompt", "") or ""
    script = job.get("script")
    no_agent = job.get("no_agent", False)

    if no_agent or no_agent is True:
        return ("already_no_agent", "no_agent=true already", {})

    script_signal = bool(script and isinstance(script, str) and script.strip())
    positive_signals = []
    negative_signals = []

    pos = has_positive_signal(prompt, script)
    if pos:
        if script_signal:
            positive_signals.append("script_field_set")
        if any(re.search(p, prompt.lower()) for p in _SELF_UPDATE_PATTERNS):
            positive_signals.append("self_update")
        if any(re.search(p, prompt) for p in _SCRIPT_WRAPPER_PATTERNS):
            positive_signals.append("script_wrapper")
        if any(re.search(p, prompt.lower()) for p in _SKILL_LOAD_SCRIPT_PATTERNS):
            positive_signals.append("skill_load_script")

    neg = has_negative_signal(prompt, name)
    if neg:
        name_lower = name.lower()
        for known in _KNOWN_GENUINE_LLM:
            if known.lower() in name_lower:
                negative_signals.append("known_genuine_llm")
                break
        if not negative_signals:
            negative_signals.append("needs_llm_reasoning")

    signals_info = {
        "positive_signals": positive_signals,
        "negative_signals": negative_signals,
        "has_script": script_signal,
        "script_value": script if script_signal else None,
    }

    if neg:
        return ("llm_needed",
                f"Genuine LLM needed ({', '.join(negative_signals)})",
                signals_info)

    if not pos:
        return ("llm_needed", "Default — no positive signal found", signals_info)

    if is_borderline(prompt, name):
        return ("llm_borderline",
                "Mechanical but needs wrapper script; verify before converting",
                signals_info)

    return ("llm_unnecessary",
            f"Convert candidate ({', '.join(positive_signals)})",
            signals_info)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Classify LLM necessity for cron jobs")
    parser.add_argument("--profile", default=PROFILE, help="Profile name")
    parser.add_argument("--json", action="store_true",
                        help="Output machine-readable JSON instead of text")
    parser.add_argument("--unit-test", action="store_true",
                        help="Run unit tests against known prompts")
    args = parser.parse_args()

    if args.unit_test:
        return run_unit_tests()

    # Handle profile override
    profile = args.profile
    if profile != "indigo":
        JOBS_PATH_CANDIDATES[0] = os.path.expanduser("~/.hermes/profiles/{profile}/cron/jobs.json")

    jobs = load_jobs()

    llm_jobs = [
        j for j in jobs
        if j.get("enabled", True) is not False
        and j.get("state") != "paused"
        and not j.get("no_agent", False)
        and j.get("no_agent") is not True
    ]

    results = []
    counts = {
        "llm_unnecessary": 0, "llm_borderline": 0,
        "llm_needed": 0, "already_no_agent": 0,
        "total_jobs": len(jobs),
    }

    for j in jobs:
        if j.get("no_agent") is True or j.get("no_agent") == "true":
            counts["already_no_agent"] += 1

    for j in sorted(llm_jobs, key=lambda x: x.get("name", "")):
        verdict, reason, signals = classify(j)
        results.append({
            "name": j.get("name", "unknown"),
            "verdict": verdict,
            "reason": reason,
            "signals": signals,
        })
        counts[verdict] = counts.get(verdict, 0) + 1

    if args.json:
        output = {"summary": counts, "verdicts": results}
        print(json.dumps(output, indent=2))
    else:
        print(f"INDIGO CRON LLM-NECESSITY CLASSIFICATION  ({counts['total_jobs']} total)")
        print(f"{'='*60}")
        print(f"  Already no_agent:   {counts['already_no_agent']:3d}")
        print(f"  llm_unnecessary:    {counts['llm_unnecessary']:3d}  (convert candidate)")
        print(f"  llm_borderline:     {counts['llm_borderline']:3d}  (needs wrapper)")
        print(f"  llm_needed:         {counts['llm_needed']:3d}  (genuine LLM)")
        print()

        for bucket in ("llm_unnecessary", "llm_borderline", "llm_needed"):
            bucket_jobs = [r for r in results if r["verdict"] == bucket]
            if bucket_jobs:
                print(f"--- {bucket} ({len(bucket_jobs)}) ---")
                for r in bucket_jobs:
                    print(f"  {r['name']:45s} {r['reason']}")
                print()


def run_unit_tests():
    """Unit tests against representative prompts from each bucket."""
    test_cases = [
        # (name, prompt, script, no_agent, expected_verdict)
        # ── llm_unnecessary: self-update cluster ──
        ("bones:update",
         "Self-update: pull latest ocas-bones from GitHub source repository",
         None, False, "llm_unnecessary"),
        ("fellow:update",
         "Run ocas-fellow update: check git for new commits and apply them",
         None, False, "llm_unnecessary"),
        ("forge:update",
         "Run ocas-forge update: check git for new commits and apply them",
         None, False, "llm_unnecessary"),

        # ── llm_unnecessary: script_field_set ──
        ("Backup Hermes Sessions to GitHub",
         "Run the Hermes session backup script to GitHub LFS.",
         os.path.expanduser("~/.hermes/profiles/indigo/scripts/backup_system.sh"),
         False, "llm_unnecessary"),
        ("rally:update",
         "Run rally.update self-update from GitHub source",
         "update_rally.sh", False, "llm_unnecessary"),

        # ── llm_unnecessary: script_wrapper ──
        ("genie:disk-cleanup",
         "Run Genie: python3 ~/.hermes/profiles/indigo/scripts/genie.py. Report results.",
         None, False, "llm_unnecessary"),
        ("soul:sync",
         "Run the soul sync script and report results.",
         "soul_sync.py", False, "llm_unnecessary"),

        # ── llm_needed: generation verbs ──
        ("haiku:content-post",
         "You are composing a Bluesky post for the agent. YOU write it, fresh and in the loop",
         None, False, "llm_needed"),
        ("haiku:haiku-post",
         "You are composing an original haiku for the agent's Bluesky practice.",
         None, False, "llm_needed"),
        ("sands:evening-brief",
         "Generate evening schedule briefing using ocas-sands skill",
         None, False, "llm_needed"),

        # ── llm_needed: persona framing ──
        ("Koda Dispatcher — BOOK",
         "You are the Koda Dispatcher for BOOK (<external-repo>/BOOK). Repo at <projects-root>/github-staging/BOOK.",
         None, False, "llm_needed"),
        ("Engineering Manager — BOOK Escalation Handler",
         "You are the Engineering Manager. Load the engineering-manager skill.",
         None, False, "llm_needed"),

        # ── llm_needed: research/review ──
        ("taste:scan",
         "Daily email/calendar scan plus Spotify sync, PLUS Styx→Taste delta ingestion.",
         None, False, "llm_needed"),
        ("daily-user-context",
         "Generate the owner daily context block for USER.md. You implement the ocas-usercontext skill",
         None, False, "llm_needed"),
        ("scout:research",
         "Using the ocas-scout skill, run the weekly structured research cycle.",
         None, False, "llm_needed"),
        ("sands:morning-brief",
         "Generate morning schedule briefing using ocas-sands skill",
         None, False, "llm_needed"),

        # ── llm_needed: dispatch patterns ──
        ("dispatcher",
         "Read the monitor queue and dispatch any pending work.",
         None, False, "llm_needed"),
        ("dispatch:summary",
         "Run dispatch.status to generate dispatch summary showing inbox status",
         None, False, "llm_needed"),

        # ── llm_needed: multi-source synthesis ──
        ("rally:research",
         "Run the Rally daily research cycle. Execute the Python scripts directly — do NOT just summarize.",
         None, False, "llm_needed"),

        # ── already_no_agent ──
        ("Gateway Memory Watchdog",
         "Run: python3 ~/.hermes/scripts/gateway_memory_watchdog.py",
         "gateway_memory_watchdog.py", True, "already_no_agent"),
        ("dispatch:triage-morning",
         "Run dispatch morning triage. Execute the triage script and journal script.",
         "triage_morning.sh", True, "already_no_agent"),
    ]

    passed = 0
    failed = 0
    for name, prompt, script, no_agent, expected in test_cases:
        job = {"name": name, "prompt": prompt, "script": script, "no_agent": no_agent}
        verdict, reason, _ = classify(job)
        status = "PASS" if verdict == expected else "FAIL"
        if status == "PASS":
            passed += 1
        else:
            failed += 1
        print(f"  {status:4s} | {verdict:20s} (expected {expected:20s}) | {reason[:50]:50s} | {name}")

    print(f"\n{'='*60}")
    print(f"  {passed} passed, {failed} failed out of {len(test_cases)} tests")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    main()