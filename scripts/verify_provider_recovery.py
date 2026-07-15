#!/usr/bin/env python3
"""
verify_provider_recovery.py — escalation-loop provider-recovery verifier.

Reads the profile jobs.json, separates enabled+erroring jobs into STALE vs RECENT
by last_run_at age, derives the default provider/model from config.yaml, probes it
live via `hermes chat -q`, and prints a recommendation.

Usage:
  python3 verify_provider_recovery.py [--profile indigo] [--stale-hours 24] [--no-probe]

This is the concrete cross-check to run BEFORE re-pausing any job a probe
(find_missed_user_gated_jobs.py) flagged as MISSED, so you don't re-escalate
jobs whose errors are forward-stale (predate a verified provider recovery).
"""
import argparse
import json
import subprocess
from datetime import datetime, timezone


def load_jobs(profile):
    path = f"<hermes-root>/profiles/{profile}/cron/jobs.json"
    with open(path) as f:
        data = json.load(f)
    return data.get("jobs", []) if isinstance(data, dict) else data


def utc(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None


def default_provider(profile):
    cfg = f"<hermes-root>/profiles/{profile}/config.yaml"
    prov = model = None
    try:
        with open(cfg) as f:
            for line in f:
                if line.strip().startswith("provider:") and prov is None:
                    prov = line.split(":", 1)[1].strip()
                if line.strip().startswith("model:") and model is None:
                    model = line.split(":", 1)[1].strip()
    except Exception:
        pass
    return prov, model


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="indigo")
    ap.add_argument("--stale-hours", type=float, default=24.0)
    ap.add_argument("--no-probe", action="store_true")
    args = ap.parse_args()

    jobs = load_jobs(args.profile)
    now = datetime.now(timezone.utc)
    enabled_erroring = [
        j for j in jobs
        if j.get("enabled") and j.get("last_status") == "error" and j.get("last_error")
    ]
    recent, stale = [], []
    for j in enabled_erroring:
        lru = utc(j.get("last_run_at"))
        age = (now - lru).total_seconds() / 3600 if lru else None
        (recent if (age is None or age <= args.stale_hours) else stale).append((j, age))

    prov, model = default_provider(args.profile)
    print(f"Default provider={prov} model={model}")
    print(f"Enabled+erroring jobs: {len(enabled_erroring)} "
          f"(recent<={args.stale_hours}h: {len(recent)}, stale: {len(stale)})")

    for label, bucket in (("RECENT", recent), ("STALE", stale)):
        for j, age in bucket:
            age_s = f"{age:.1f}" if age is not None else "?"
            err = (j.get("last_error") or "")[:70].replace("\n", " ")
            print(f"  [{label}] {j.get('id')} {j.get('name')} age_h={age_s} err={err}")

    live = None
    if not args.no_probe and prov:
        try:
            cmd = ["hermes", "chat", "-q", "reply with only the word PONG"]
            if model:
                cmd += ["--provider", prov, "--model", model]
            else:
                cmd += ["--provider", prov]
            out = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            live = out.returncode == 0
            print(f"\nDefault provider probe ({prov}/{model}): "
                  f"{'LIVE' if live else 'DOWN'} (rc={out.returncode})")
        except Exception as e:
            print(f"\nDefault provider probe failed: {e}")
            live = None

    print("\nRECOMMENDATION:")
    if recent:
        print(f"  - {len(recent)} RECENT erroring job(s): verify fingerprint; if "
              f"user-gated and not already enrolled, re-pause/enroll per narrow criteria.")
    else:
        print("  - No RECENT erroring jobs: nothing to re-escalate.")
    if stale:
        print(f"  - {len(stale)} STALE erroring job(s): forward-stale unless provider "
              f"probe is DOWN. If provider LIVE, do NOT re-pause (errors predate recovery).")
    if live:
        print("  - Provider is LIVE: treat stale errors as self-clearing; do not re-enroll.")
    elif live is False:
        print("  - Provider is DOWN: genuine outage; re-verify per provider fingerprint "
              "and re-enroll if warranted.")


if __name__ == "__main__":
    main()
