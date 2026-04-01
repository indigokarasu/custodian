#!/usr/bin/env python3
"""
Custodian - OpenClaw system health monitor and auto-repair.

Usage: python3 custodian.py <command> [args]

Commands:
  init                  Create storage, register cron jobs
  scan.light            Fast health check (heartbeat)
  scan.deep             Full system scan (cron)
  verify <fix_id>       Verify fix outcome
  repair.auto           Apply all pending Tier 1 fixes
  repair.plan           Show Tier 2/3 repair plan
  issues.list           List open issues (JSON)
  issues.resolve <id>   Mark issue resolved
  status                Emit SkillStatus JSON
  schedule.show         Show scan schedule with activity heatmap
  update                Self-update from GitHub
"""

import sys
import os
import json
import re
import uuid
import shutil
import subprocess
import datetime
import tempfile
from pathlib import Path
from collections import defaultdict

# ── Paths ──────────────────────────────────────────────────────────────────────

HOME = Path.home()
SKILL_ROOT = Path(__file__).resolve().parent.parent   # scripts/../ = skill root
OPENCLAW_DOT = HOME / ".openclaw"
OPENCLAW_BASE = HOME / "openclaw"
DATA_DIR = OPENCLAW_BASE / "data" / "ocas-custodian"
JOURNALS_DIR = OPENCLAW_BASE / "journals" / "ocas-custodian"
CRON_FILE = OPENCLAW_DOT / "cron" / "jobs.json"
KNOWN_ISSUES_FILE = SKILL_ROOT / "references" / "known_issues.json"
REPAIR_PLAN_FILE = SKILL_ROOT / "references" / "plans" / "custodian-repair.plan.md"

# Skill discovery locations (highest precedence first)
SKILL_SEARCH_DIRS = [
    OPENCLAW_DOT / "workspace" / "skills",
    OPENCLAW_DOT / "skills",
]

JSONL_FILES = [
    "issues.jsonl",
    "fixes.jsonl",
    "cleanup_events.jsonl",
    "fix_effectiveness.jsonl",
    "learned_issues.jsonl",
    "skill_conformance.jsonl",
    "deferred_fixes.jsonl",
    "decisions.jsonl",
]


def _gateway_log_today():
    return Path("/tmp/openclaw") / f"openclaw-{datetime.date.today().isoformat()}.log"


def _gateway_log_persistent():
    return OPENCLAW_DOT / "logs" / "gateway.log"


# ── DataStore ─────────────────────────────────────────────────────────────────

class DataStore:
    data_dir = DATA_DIR
    journals_dir = JOURNALS_DIR

    def read_json(self, path):
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            return {}

    def write_json(self, path, data):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2))

    def read_jsonl(self, path):
        if not path.exists():
            return []
        records = []
        for line in path.read_text(errors="replace").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        return records

    def append_jsonl(self, path, record):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a") as f:
            f.write(json.dumps(record) + "\n")

    def rewrite_jsonl(self, path, records):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")

    def issues(self):
        return self.read_jsonl(self.data_dir / "issues.jsonl")

    def open_issues(self):
        return [i for i in self.issues()
                if i.get("status") not in ("resolved", "suppressed")]

    def fixes(self):
        return self.read_jsonl(self.data_dir / "fixes.jsonl")

    def fix_effectiveness(self):
        return self.read_jsonl(self.data_dir / "fix_effectiveness.jsonl")

    def learned_issues(self):
        return self.read_jsonl(self.data_dir / "learned_issues.jsonl")

    def config(self):
        return self.read_json(self.data_dir / "config.json")

    def activity_model(self):
        return self.read_json(self.data_dir / "activity_model.json")

    def schedule_state(self):
        return self.read_json(self.data_dir / "schedule_state.json")

    def append_issue(self, record):
        self.append_jsonl(self.data_dir / "issues.jsonl", record)

    def append_fix(self, record):
        self.append_jsonl(self.data_dir / "fixes.jsonl", record)

    def append_decision(self, record):
        self.append_jsonl(self.data_dir / "decisions.jsonl", record)


# ── Fingerprinter ─────────────────────────────────────────────────────────────

class Fingerprinter:
    def __init__(self, ds):
        self.known = {}
        self.learned = []
        if KNOWN_ISSUES_FILE.exists():
            try:
                self.known = json.loads(KNOWN_ISSUES_FILE.read_text())
            except json.JSONDecodeError:
                pass
        self.learned = ds.learned_issues()

    def match(self, line):
        """Return (fingerprint_id, spec) or (None, None)."""
        for fid, spec in self.known.items():
            for pattern in spec.get("match_patterns", []):
                if re.search(pattern, line, re.IGNORECASE):
                    return fid, spec
        for entry in self.learned:
            for pattern in entry.get("match_patterns", []):
                if re.search(pattern, line, re.IGNORECASE):
                    return entry.get("fingerprint_id", "learned"), entry
        return None, None


# ── CronManager ───────────────────────────────────────────────────────────────

class CronManager:
    def list_jobs(self):
        if not CRON_FILE.exists():
            return []
        try:
            data = json.loads(CRON_FILE.read_text())
            return data if isinstance(data, list) else data.get("jobs", [])
        except (json.JSONDecodeError, OSError):
            return []

    def find_job(self, name):
        for job in self.list_jobs():
            if job.get("name") == name:
                return job
        return None

    def job_exists(self, name):
        return self.find_job(name) is not None

    def disabled_jobs(self):
        return [j for j in self.list_jobs() if not j.get("enabled", True)]

    def add_cron_job(self, name, cron, message, tz="America/Los_Angeles"):
        r = subprocess.run(
            ["openclaw", "cron", "add",
             "--name", name,
             "--cron", cron,
             "--tz", tz,
             "--session", "isolated",
             "--message", message],
            capture_output=True, text=True
        )
        return r.returncode == 0

    def enable_job(self, job_id):
        r = subprocess.run(
            ["openclaw", "cron", "edit", job_id, "--enabled", "true"],
            capture_output=True, text=True
        )
        return r.returncode == 0

    def run_job(self, job_id):
        r = subprocess.run(
            ["openclaw", "cron", "run", job_id],
            capture_output=True, text=True
        )
        return r.returncode == 0


# ── ActivityModel ─────────────────────────────────────────────────────────────

class ActivityModel:
    def rebuild(self, ds):
        """Parse 14 days of gateway logs, compute hourly confidence, write model."""
        cutoff = datetime.datetime.now() - datetime.timedelta(days=14)
        hour_active_days = defaultdict(set)
        total_days = set()

        for line in self._collect_log_lines(14):
            ts = self._parse_timestamp(line)
            if not ts or ts < cutoff:
                continue
            # Count user-initiated events only
            if "message.processed" in line and re.search(r'"source"\s*:\s*"user"', line):
                day_str = ts.strftime("%Y-%m-%d")
                hour_active_days[ts.hour].add(day_str)
                total_days.add(day_str)

        total = max(len(total_days), 1)
        hourly = {}
        for h in range(24):
            conf = len(hour_active_days[h]) / total
            level = "high" if conf >= 0.75 else ("med" if conf >= 0.40 else "low")
            hourly[str(h)] = {"confidence": round(conf, 3), "level": level}

        now_h = datetime.datetime.now().hour
        now_level = hourly.get(str(now_h), {}).get("level", "low")
        current_state = "active" if now_level in ("high", "med") else "quiet"

        model = {
            "built_at": datetime.datetime.now().isoformat(),
            "window_days": 14,
            "total_active_days": len(total_days),
            "hourly": hourly,
            "current_state": current_state,
            "current_hour_confidence": now_level,
        }
        ds.write_json(ds.data_dir / "activity_model.json", model)
        return model

    def _collect_log_lines(self, days):
        lines = []
        for d in range(days):
            date = (datetime.date.today() - datetime.timedelta(days=d)).isoformat()
            p = Path("/tmp/openclaw") / f"openclaw-{date}.log"
            if p.exists():
                try:
                    lines.extend(p.read_text(errors="replace").splitlines())
                except OSError:
                    pass
        # Persistent log as supplement
        gw = _gateway_log_persistent()
        if gw.exists():
            try:
                lines.extend(gw.read_text(errors="replace").splitlines())
            except OSError:
                pass
        return lines

    def _parse_timestamp(self, line):
        for pat in (r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})",
                    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"):
            m = re.search(pat, line)
            if m:
                try:
                    return datetime.datetime.fromisoformat(m.group(1))
                except ValueError:
                    pass
        return None


# ── JournalWriter ─────────────────────────────────────────────────────────────

class JournalWriter:
    def write(self, ds, kind, run_id, scan_results):
        today = datetime.date.today().isoformat()
        day_dir = ds.journals_dir / today
        day_dir.mkdir(parents=True, exist_ok=True)

        eff = ds.fix_effectiveness()
        attempts = sum(e.get("attempts", 0) for e in eff)
        successes = sum(e.get("successes", 0) for e in eff)
        open_issues = ds.open_issues()

        entry = {
            "run_id": run_id,
            "kind": kind,
            "timestamp": datetime.datetime.now().isoformat(),
            "scan_type": scan_results.get("scan_type", "unknown"),
            "escalation_needed": scan_results.get("escalation_needed", False),
            "okr": {
                "success_rate": round(successes / max(attempts, 1), 3),
                "issues_detected": scan_results.get("issues_detected", 0),
                "issues_auto_fixed": scan_results.get("issues_auto_fixed", 0),
                "fix_success_rate": round(successes / max(attempts, 1), 3),
                "mean_time_to_fix_ms": scan_results.get("mean_time_to_fix_ms", 0),
                "open_residuals": len([i for i in open_issues if i.get("status") == "open"]),
                "escalations": scan_results.get("escalations", 0),
                "high_recurrence_fingerprints": scan_results.get("high_recurrence_fingerprints", 0),
                "skills_initialized": scan_results.get("skills_initialized", 0),
                "background_tasks_registered": scan_results.get("background_tasks_registered", 0),
                "schedule_score": scan_results.get("schedule_score", 0),
                "journal_completeness": scan_results.get("journal_completeness", 1.0),
            },
            "summary": scan_results.get("summary", ""),
            "findings": scan_results.get("findings", []),
        }

        path = day_dir / f"{run_id}.json"
        path.write_text(json.dumps(entry, indent=2))
        print(f"  Journal written: {path}")
        return entry


# ── Tier1Fixer ────────────────────────────────────────────────────────────────

class Tier1Fixer:
    """Execute safe, non-destructive, reversible Tier 1 auto-fixes."""

    def __init__(self, ds, cron):
        self.ds = ds
        self.cron = cron

    def apply(self, fingerprint_id, spec, context=None):
        ctx = context or {}
        fix_id = str(uuid.uuid4())[:8]
        result = {
            "fix_id": fix_id,
            "fingerprint_id": fingerprint_id,
            "applied_at": datetime.datetime.now().isoformat(),
            "outcome": "unknown",
            "command": spec.get("auto_fix", ""),
            "reversibility": spec.get("reversibility", ""),
            "pre_fix_state": None,
            "post_fix_state": None,
        }

        try:
            if fingerprint_id == "oc_cron_disabled_transient":
                job_id = ctx.get("job_id", "")
                if job_id:
                    ok = self.cron.enable_job(job_id)
                    result["outcome"] = "fix_applied" if ok else "fix_failed"
                else:
                    result["outcome"] = "skipped_no_job_id"

            elif fingerprint_id == "oc_cron_stuck_missed":
                job_id = ctx.get("job_id", "")
                if job_id:
                    ok = self.cron.run_job(job_id)
                    result["outcome"] = "fix_applied" if ok else "fix_failed"
                else:
                    result["outcome"] = "skipped_no_job_id"

            elif fingerprint_id in (
                "oc_intake_dir_missing",
                "oc_journal_dir_missing",
                "oc_skill_data_dir_missing",
            ):
                path_str = ctx.get("path", "")
                if path_str:
                    path = Path(path_str).expanduser()
                    # Safety: only create dirs under ~/openclaw/
                    if str(path).startswith(str(HOME / "openclaw")):
                        existed = path.exists()
                        path.mkdir(parents=True, exist_ok=True)
                        result["outcome"] = "fix_applied"
                        result["pre_fix_state"] = "exists" if existed else "missing"
                        result["post_fix_state"] = "exists"
                        # Write default config.json for data dir if not present
                        if fingerprint_id == "oc_skill_data_dir_missing":
                            cfg = path / "config.json"
                            if not cfg.exists():
                                skill_name = path.name
                                cfg.write_text(json.dumps({
                                    "skill": skill_name,
                                    "initialized_at": datetime.datetime.now().isoformat(),
                                }, indent=2))
                    else:
                        result["outcome"] = "skipped_unsafe_path"
                else:
                    result["outcome"] = "skipped_no_path"

            elif fingerprint_id == "oc_jsonl_oversized":
                file_path_str = ctx.get("path", "")
                if file_path_str:
                    file_path = Path(file_path_str).expanduser()
                    if file_path.exists() and str(file_path).startswith(str(HOME / "openclaw")):
                        dated = file_path.with_suffix(
                            f".{datetime.date.today().isoformat()}.jsonl"
                        )
                        file_path.rename(dated)
                        file_path.touch()
                        result["outcome"] = "fix_applied"
                        result["pre_fix_state"] = f"oversized: {file_path}"
                        result["post_fix_state"] = f"rotated to {dated.name}"
                    else:
                        result["outcome"] = "skipped"
                else:
                    result["outcome"] = "skipped_no_path"

            elif fingerprint_id == "oc_jsonl_malformed_lines":
                file_path_str = ctx.get("path", "")
                if file_path_str:
                    file_path = Path(file_path_str).expanduser()
                    if file_path.exists() and str(file_path).startswith(str(HOME / "openclaw")):
                        good, bad = [], []
                        for line in file_path.read_text(errors="replace").splitlines():
                            try:
                                json.loads(line)
                                good.append(line)
                            except json.JSONDecodeError:
                                if line.strip():
                                    bad.append(line)
                        if bad:
                            error_path = file_path.with_suffix(".error")
                            with open(error_path, "a") as f:
                                f.write("\n".join(bad) + "\n")
                            file_path.write_text("\n".join(good) + "\n")
                            result["outcome"] = "fix_applied"
                            result["pre_fix_state"] = f"{len(bad)} malformed lines"
                            result["post_fix_state"] = f"quarantined to {error_path.name}"
                        else:
                            result["outcome"] = "no_action_needed"
                    else:
                        result["outcome"] = "skipped"
                else:
                    result["outcome"] = "skipped_no_path"

            elif fingerprint_id == "oc_gateway_token_missing":
                r = subprocess.run(
                    ["openclaw", "doctor", "--generate-gateway-token"],
                    capture_output=True, text=True
                )
                result["outcome"] = "fix_applied" if r.returncode == 0 else "fix_failed"

            elif fingerprint_id == "oc_background_task_missing":
                task_name = ctx.get("task_name", "")
                schedule = ctx.get("schedule", "")
                message = ctx.get("message", "")
                if task_name and schedule and message:
                    ok = self.cron.add_cron_job(task_name, schedule, message)
                    result["outcome"] = "fix_applied" if ok else "fix_failed"
                else:
                    result["outcome"] = "skipped_missing_context"

            elif fingerprint_id == "oc_skill_uninitialized":
                skill_name = ctx.get("skill_name", "")
                if skill_name:
                    data_d = HOME / "openclaw" / "data" / skill_name
                    journal_d = HOME / "openclaw" / "journals" / skill_name
                    data_d.mkdir(parents=True, exist_ok=True)
                    journal_d.mkdir(parents=True, exist_ok=True)
                    cfg = data_d / "config.json"
                    if not cfg.exists():
                        cfg.write_text(json.dumps({
                            "skill": skill_name,
                            "initialized_at": datetime.datetime.now().isoformat(),
                        }, indent=2))
                    result["outcome"] = "fix_applied"
                    result["post_fix_state"] = f"{data_d} and {journal_d} created"
                else:
                    result["outcome"] = "skipped_no_skill_name"

            else:
                result["outcome"] = "no_handler"

        except Exception as e:
            result["outcome"] = "fix_failed"
            result["error"] = str(e)

        self.ds.append_fix(result)
        return result


# ── Helpers ───────────────────────────────────────────────────────────────────

def find_gateway_log():
    p = _gateway_log_today()
    if p.exists():
        return p
    p = _gateway_log_persistent()
    if p.exists():
        return p
    return None


def tail_log(path, n=100):
    try:
        return path.read_text(errors="replace").splitlines()[-n:]
    except OSError:
        return []


def extract_log_errors(lines):
    """
    Parse log lines that may be JSON (openclaw structured log) or plain text.
    Returns list of (message_str, level_str) for ERROR-level entries.
    """
    errors = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        # Try structured JSON log format first
        if line.startswith("{"):
            try:
                entry = json.loads(line)
                level = entry.get("_meta", {}).get("logLevelName", "")
                if level == "ERROR":
                    msg = entry.get("0", "") or entry.get("msg", "") or entry.get("message", "")
                    if msg:
                        errors.append(str(msg))
                continue
            except json.JSONDecodeError:
                pass
        # Plain text: look for ERROR keyword
        if re.search(r"\bERROR\b", line):
            errors.append(line)
    return errors


def installed_skills():
    seen = set()
    skills = []
    for root in SKILL_SEARCH_DIRS:
        if not root.exists():
            continue
        for child in sorted(root.iterdir()):
            if child.is_dir() and child.name not in seen and (child / "skill.json").exists():
                seen.add(child.name)
                skills.append(child)
    return skills


def is_skill_initialized(skill_name):
    d = HOME / "openclaw" / "data" / skill_name
    j = HOME / "openclaw" / "journals" / skill_name
    return d.exists() and (d / "config.json").exists() and j.exists()

def check_skill_directories(skill_name, ds):
    """
    Proactively check for missing intake/journal directories for a skill.
    Returns list of issue dicts for missing directories.
    """
    issues = []
    base_paths = [
        HOME / "openclaw" / "data" / skill_name,
        HOME / "openclaw" / "journals" / skill_name,
    ]
    
    # Check base data dir exists
    data_dir = base_paths[0]
    if data_dir.exists():
        # Check for intake subdirectory
        intake_dir = data_dir / "intake"
        if not intake_dir.exists():
            issues.append({
                "issue_id": str(uuid.uuid4())[:8],
                "fingerprint_id": "oc_intake_dir_missing",
                "status": "open",
                "tier": 1,
                "first_seen": datetime.datetime.now().isoformat(),
                "context": {
                    "path": str(intake_dir),
                    "skill_name": skill_name,
                },
                "source": "proactive_scan",
            })
        
        # Check for processed subdirectory
        processed_dir = intake_dir / "processed"
        if intake_dir.exists() and not processed_dir.exists():
            issues.append({
                "issue_id": str(uuid.uuid4())[:8],
                "fingerprint_id": "oc_intake_dir_missing",
                "status": "open",
                "tier": 1,
                "first_seen": datetime.datetime.now().isoformat(),
                "context": {
                    "path": str(processed_dir),
                    "skill_name": skill_name,
                },
                "source": "proactive_scan",
            })
    
    # Check journal dir exists
    journal_dir = base_paths[1]
    if not journal_dir.exists():
        issues.append({
            "issue_id": str(uuid.uuid4())[:8],
            "fingerprint_id": "oc_journal_dir_missing",
            "status": "open",
            "tier": 1,
            "first_seen": datetime.datetime.now().isoformat(),
            "context": {
                "path": str(journal_dir),
                "skill_name": skill_name,
            },
            "source": "proactive_scan",
        })
    
    return issues


def skill_background_tasks(skill_dir):
    """
    Collect declared background tasks from a skill package using three sources:

    1. skill.json → scheduled_tasks[] (authoritative, machine-readable)
    2. SKILL.md → ## Background tasks table (OCAS convention)
    3. Heuristic scan of SKILL.md/README.md for cron/heartbeat mentions
       (returns these as source="heuristic", tier=2 — needs human review)

    Returns list of dicts with keys: name, mechanism, schedule, command, source.
    """
    tasks = []
    seen_names = set()

    # Source 1: skill.json scheduled_tasks
    skill_json = skill_dir / "skill.json"
    if skill_json.exists():
        try:
            spec = json.loads(skill_json.read_text())
            for task in spec.get("scheduled_tasks", []):
                name = task.get("name", "")
                if name and name not in seen_names:
                    seen_names.add(name)
                    schedule = task.get("schedule", "")
                    mechanism = "heartbeat" if schedule == "heartbeat" else "cron"
                    tasks.append({
                        "name": name,
                        "mechanism": mechanism,
                        "schedule": schedule,
                        "command": task.get("command", name),
                        "source": "skill.json",
                        "heuristic": False,
                    })
        except (json.JSONDecodeError, OSError):
            pass

    # Source 2: SKILL.md ## Background tasks table
    skill_md = skill_dir / "SKILL.md"
    if skill_md.exists():
        text = skill_md.read_text(errors="replace")
        m = re.search(r"## Background tasks\s*\n(.*?)(?=\n## |\Z)", text, re.DOTALL)
        if m:
            for line in m.group(1).splitlines():
                parts = [p.strip() for p in line.split("|") if p.strip()]
                if len(parts) >= 4 and parts[0] not in ("Job", "---", "|-"):
                    name = parts[0].strip("`")
                    if name and name not in seen_names:
                        seen_names.add(name)
                        # Clean schedule: remove backticks and anything in parentheses
                        schedule_raw = parts[2].strip().strip("`")
                        schedule = re.sub(r"\s*\([^)]*\)", "", schedule_raw).strip()
                        tasks.append({
                            "name": name,
                            "mechanism": parts[1].strip(),
                            "schedule": schedule,
                            "command": parts[3].strip().strip("`"),
                            "source": "SKILL.md",
                            "heuristic": False,
                        })

    # Source 3: Heuristic scan — only if no tasks found from sources 1+2
    if not tasks:
        for doc_file in [skill_md, skill_dir / "README.md"]:
            if not doc_file.exists():
                continue
            text = doc_file.read_text(errors="replace")
            cron_hints = re.findall(
                r"(?:cron|heartbeat|schedule)[^\n]*(?:\d+\s+\*|\bevery\s+\d+|\bheartbeat\b)[^\n]*",
                text, re.IGNORECASE
            )
            for hint in cron_hints:
                hint = hint.strip()[:120]
                hint_name = f"{skill_dir.name}:unknown-task"
                if hint_name not in seen_names:
                    seen_names.add(hint_name)
                    tasks.append({
                        "name": hint_name,
                        "mechanism": "unknown",
                        "schedule": "unknown",
                        "command": "unknown",
                        "source": "heuristic",
                        "heuristic": True,
                        "hint_text": hint,
                    })
            if tasks:
                break  # stop after first doc file yields results

    return tasks


def optimize_schedule(model, current_hours):
    """Score 4-slot schedule and optionally shift. Returns (hours, score)."""
    hourly = model.get("hourly", {})

    def score(slots):
        total = 0
        for h in slots:
            lv = hourly.get(str(h % 24), {}).get("level", "low")
            total += {"high": 2, "med": 1, "low": -2}.get(lv, -2)
        return total

    cur_score = score(current_hours)
    if cur_score >= 6:
        return current_hours, cur_score

    confidence = model.get("current_hour_confidence", "low")
    if confidence == "low":
        return current_hours, cur_score
    if confidence == "med" and cur_score > 2:
        return current_hours, cur_score

    best, best_score = list(current_hours), cur_score
    for i, h in enumerate(current_hours):
        for delta in (-1, 1):
            candidate = list(current_hours)
            candidate[i] = (h + delta) % 24
            candidate_sorted = sorted(candidate)
            gaps_ok = all(
                (candidate_sorted[(j + 1) % 4] - candidate_sorted[j]) % 24 >= 2
                for j in range(4)
            )
            if gaps_ok:
                s = score(candidate)
                if s > best_score:
                    best, best_score = candidate, s

    return best, best_score


def age_str(ts):
    if not ts:
        return "unknown"
    try:
        dt = datetime.datetime.fromisoformat(ts)
        delta = datetime.datetime.now() - dt
        if delta.days > 0:
            return f"{delta.days}d"
        h = delta.seconds // 3600
        if h > 0:
            return f"{h}h"
        return f"{delta.seconds // 60}m"
    except ValueError:
        return "unknown"


def extract_error_text(line):
    m = re.search(r"(?:ERROR|error)[:\s]+(.+)", line, re.IGNORECASE)
    return m.group(1).strip()[:200] if m else line.strip()[:200]


def build_report(results, run_id, elapsed):
    lines = [
        "# Custodian Deep Scan Report",
        "",
        f"Run: `{run_id}` | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')} | {elapsed:.1f}s",
        "",
        "## Summary",
        "",
        f"- Issues detected: {results['issues_detected']}",
        f"- Auto-fixed: {results['issues_auto_fixed']}",
        f"- Escalations: {results['escalations']}",
        f"- Skills initialized: {results['skills_initialized']}",
        f"- Background tasks registered: {results['background_tasks_registered']}",
        f"- Schedule score: {results['schedule_score']}",
        "",
    ]
    if results.get("findings"):
        lines += ["## Findings", ""]
        for f in results["findings"]:
            fp_id = f.get("fingerprint", f.get("fingerprint_id", ""))
            lines.append(f"- [{f.get('type', '?')}] {fp_id or f.get('line', '')[:100]}")
        lines.append("")
    if results.get("search_candidates"):
        lines += ["## Unresolved (agent web search needed)", ""]
        for c in results["search_candidates"]:
            lines.append(f"- {c.get('error_text', '')[:120]}")
        lines.append("")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# Commands
# ══════════════════════════════════════════════════════════════════════════════

def cmd_init(args):
    ds = DataStore()
    cron = CronManager()
    run_id = str(uuid.uuid4())[:8]
    registered = []

    print("Custodian init")
    print("=" * 40)

    # 1. Create data directory and all JSONL files
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "reports").mkdir(exist_ok=True)
    for f in JSONL_FILES:
        p = DATA_DIR / f
        if not p.exists():
            p.touch()
            print(f"  Created {f}")

    # 2. Write default config.json (only if absent)
    config_path = DATA_DIR / "config.json"
    if not config_path.exists():
        config_path.write_text(json.dumps({
            "skill": "ocas-custodian",
            "version": "1.0.1",
            "initialized_at": datetime.datetime.now().isoformat(),
            "scan_window_minutes": 60,
            "max_jsonl_records": 10000,
            "schedule": {
                "deep_scan_hours_pt": [1, 7, 13, 19],
                "optimization_enabled": True,
                "optimization_min_days": 7,
            },
        }, indent=2))
        print("  Created config.json")
    else:
        print("  config.json: already exists")

    # 3. Create journals directory
    JOURNALS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"  Journals dir: {JOURNALS_DIR}")

    # 4. Register cron jobs (idempotent)
    cron_tasks = [
        {
            "name": "custodian:deep",
            "cron": "0 1,7,13,19 * * *",
            "message": "custodian.scan.deep",
        },
        {
            "name": "custodian:update",
            "cron": "0 0 * * *",
            "message": "custodian.update",
        },
    ]
    for task in cron_tasks:
        if cron.job_exists(task["name"]):
            print(f"  Cron {task['name']}: already registered")
        else:
            ok = cron.add_cron_job(task["name"], task["cron"], task["message"])
            status = "registered" if ok else "FAILED"
            print(f"  Cron {task['name']}: {status}")
            if ok:
                registered.append(task["name"])
    print("  Cron custodian:light: uses heartbeat mechanism (configure via openclaw heartbeat)")

    # 5. Copy repair plan to Mentor if present
    mentor_plans = HOME / "openclaw" / "data" / "ocas-mentor" / "plans"
    if mentor_plans.exists() and REPAIR_PLAN_FILE.exists():
        dest = mentor_plans / "custodian-repair.plan.md"
        if not dest.exists():
            shutil.copy(REPAIR_PLAN_FILE, dest)
            print("  Copied repair plan to Mentor")

    # 6. Write DecisionRecord
    ds.append_decision({
        "event": "custodian.init",
        "run_id": run_id,
        "timestamp": datetime.datetime.now().isoformat(),
        "cron_registered": registered,
    })

    print("\nInit complete.")


def cmd_scan_light(args):
    ds = DataStore()
    cron = CronManager()
    fp = Fingerprinter(ds)
    fixer = Tier1Fixer(ds, cron)
    jw = JournalWriter()
    run_id = str(uuid.uuid4())[:8]

    results = {
        "scan_type": "light",
        "issues_detected": 0,
        "issues_auto_fixed": 0,
        "escalations": 0,
        "background_tasks_registered": 0,
        "skills_initialized": 0,
        "findings": [],
        "summary": "",
    }

    print(f"[{run_id}] Custodian light scan — {datetime.datetime.now().isoformat()}")

    # Step 1: Check for disabled cron jobs
    disabled = cron.disabled_jobs()
    if disabled:
        for job in disabled:
            jid = job.get("id") or job.get("name", "")
            name = job.get("name", jid)
            print(f"  Disabled cron: {name}")
            issue_id = str(uuid.uuid4())[:8]
            ds.append_issue({
                "issue_id": issue_id,
                "fingerprint_id": "oc_cron_disabled_transient",
                "status": "open",
                "tier": 1,
                "first_seen": datetime.datetime.now().isoformat(),
                "context": {"job_id": jid},
            })
            fix = fixer.apply(
                "oc_cron_disabled_transient",
                fp.known.get("oc_cron_disabled_transient", {}),
                {"job_id": jid},
            )
            results["issues_detected"] += 1
            if fix["outcome"] == "fix_applied":
                results["issues_auto_fixed"] += 1
                print(f"    → re-enabled")
    else:
        print("  Cron jobs: all enabled")

    # Step 2: Tail gateway log, fingerprint ERROR lines
    log_path = find_gateway_log()
    if log_path:
        recent = tail_log(log_path, 100)
        error_lines = extract_log_errors(recent)
        print(f"  Log: {len(recent)} lines, {len(error_lines)} errors")
        seen_fids = set()
        for line in error_lines:
            fid, spec = fp.match(line)
            if fid and fid not in seen_fids:
                seen_fids.add(fid)
                tier = spec.get("tier", 3)
                results["issues_detected"] += 1
                results["findings"].append({"type": "log_error", "fingerprint": fid, "tier": tier})
                ds.append_issue({
                    "issue_id": str(uuid.uuid4())[:8],
                    "fingerprint_id": fid,
                    "status": "open",
                    "tier": tier,
                    "first_seen": datetime.datetime.now().isoformat(),
                })
                if tier == 1:
                    fix = fixer.apply(fid, spec)
                    if fix["outcome"] == "fix_applied":
                        results["issues_auto_fixed"] += 1
                        print(f"  Auto-fixed: {fid}")
                    else:
                        print(f"  Tier 1 fix failed: {fid} ({fix['outcome']})")
                else:
                    print(f"  Tier {tier} issue logged: {fid}")
    else:
        print("  Log: not found (checked /tmp/openclaw/ and ~/.openclaw/logs/)")

    # Step 3: Retry fix_attempted_failed items (up to 3 attempts)
    all_issues = ds.issues()
    retried = 0
    modified = False
    for issue in all_issues:
        if issue.get("status") == "fix_attempted_failed":
            retries = issue.get("retry_count", 0)
            if retries < 3:
                fid = issue.get("fingerprint_id", "")
                spec = fp.known.get(fid) or next(
                    (e for e in fp.learned if e.get("fingerprint_id") == fid), {}
                )
                if spec and spec.get("tier", 4) == 1:
                    fix = fixer.apply(fid, spec, issue.get("context", {}))
                    issue["retry_count"] = retries + 1
                    modified = True
                    if fix["outcome"] == "fix_applied":
                        issue["status"] = "fix_applied"
                        results["issues_auto_fixed"] += 1
                        retried += 1
    if modified:
        ds.rewrite_jsonl(ds.data_dir / "issues.jsonl", all_issues)
    if retried:
        print(f"  Retried {retried} previously-failed fix(es)")

    # Step 4: Check for uninitialized skills
    for skill_dir in installed_skills():
        skill_name = skill_dir.name
        if not is_skill_initialized(skill_name):
            print(f"  Uninitialized: {skill_name} — initializing")
            fix = fixer.apply(
                "oc_skill_uninitialized",
                fp.known.get("oc_skill_uninitialized", {}),
                {"skill_name": skill_name},
            )
            if fix["outcome"] == "fix_applied":
                results["skills_initialized"] += 1
                results["issues_auto_fixed"] += 1

    results["summary"] = (
        f"Light scan complete. "
        f"{results['issues_detected']} detected, "
        f"{results['issues_auto_fixed']} fixed."
    )
    print(f"\n{results['summary']}")
    jw.write(ds, "observation", run_id, results)


def cmd_scan_deep(args):
    ds = DataStore()
    cron = CronManager()
    fp = Fingerprinter(ds)
    fixer = Tier1Fixer(ds, cron)
    am = ActivityModel()
    jw = JournalWriter()
    run_id = str(uuid.uuid4())[:8]
    start = datetime.datetime.now()

    results = {
        "scan_type": "deep",
        "issues_detected": 0,
        "issues_auto_fixed": 0,
        "escalations": 0,
        "high_recurrence_fingerprints": 0,
        "background_tasks_registered": 0,
        "skills_initialized": 0,
        "schedule_score": 0,
        "mean_time_to_fix_ms": 0,
        "journal_completeness": 1.0,
        "findings": [],
        "search_candidates": [],
        "escalation_needed": False,
        "summary": "",
    }

    print(f"[{run_id}] Custodian deep scan — {start.isoformat()}")
    print("=" * 50)

    # Step 1: Load context
    print("1. Loading context...")
    eff = ds.fix_effectiveness()
    high_rec = [
        e for e in eff
        if e.get("attempts", 0) > 0
        and e.get("recurrence_after_fix", 0) / max(e.get("successes", 1), 1) > 0.5
    ]
    results["high_recurrence_fingerprints"] = len(high_rec)

    # Step 2: Collect
    print("2. Collecting logs and running doctor...")
    log_path = find_gateway_log()
    all_log_lines = []
    if log_path:
        try:
            all_log_lines = log_path.read_text(errors="replace").splitlines()
        except OSError:
            pass

    doctor_output = ""
    r = subprocess.run(
        ["openclaw", "doctor", "--non-interactive"],
        capture_output=True, text=True, timeout=30
    )
    if r.returncode == 0:
        doctor_output = r.stdout
        print(f"   doctor: {len(doctor_output.splitlines())} lines")
    else:
        print(f"   doctor: not available (exit {r.returncode})")

    # Step 3: Fingerprint + classify
    print("3. Fingerprinting errors...")
    seen_fids = set()
    open_fids = {i.get("fingerprint_id") for i in ds.open_issues()}

    error_messages = extract_log_errors(all_log_lines)
    for line in error_messages:
        fid, spec = fp.match(line)
        if fid and fid not in seen_fids:
            seen_fids.add(fid)
            tier = spec.get("tier", 3)
            results["issues_detected"] += 1
            results["findings"].append({"type": "log_error", "fingerprint_id": fid, "tier": tier})
            if fid not in open_fids:
                ds.append_issue({
                    "issue_id": str(uuid.uuid4())[:8],
                    "fingerprint_id": fid,
                    "status": "open",
                    "tier": tier,
                    "first_seen": datetime.datetime.now().isoformat(),
                    "recurrence_count": 1,
                })
        elif not fid:
            results["search_candidates"].append({
                "line": line[:200],
                "error_text": extract_error_text(line),
            })

    for line in doctor_output.splitlines():
        fid, spec = fp.match(line)
        if fid and fid not in seen_fids:
            seen_fids.add(fid)
            tier = spec.get("tier", 3)
            results["issues_detected"] += 1
            if fid not in open_fids:
                ds.append_issue({
                    "issue_id": str(uuid.uuid4())[:8],
                    "fingerprint_id": fid,
                    "status": "open",
                    "tier": tier,
                    "first_seen": datetime.datetime.now().isoformat(),
                    "source": "doctor",
                })

    print(f"   {results['issues_detected']} issues identified, "
          f"{len(results['search_candidates'])} unknown fingerprints")

    # Step 4: Rebuild activity model
    print("4. Rebuilding activity model...")
    model = am.rebuild(ds)
    current_state = model.get("current_state", "quiet")
    print(f"   State: {current_state} | confidence: {model.get('current_hour_confidence', 'low')}")

    # Step 5: Optimize schedule
    print("5. Optimizing schedule...")
    config = ds.config()
    current_hours = config.get("schedule", {}).get("deep_scan_hours_pt", [1, 7, 13, 19])
    active_days = model.get("total_active_days", 0)
    min_days = config.get("schedule", {}).get("optimization_min_days", 7)

    if active_days >= min_days:
        new_hours, score = optimize_schedule(model, current_hours)
        results["schedule_score"] = score
        if new_hours != current_hours:
            print(f"   Shifting: {sorted(current_hours)} → {sorted(new_hours)} (score {score})")
            config.setdefault("schedule", {})["deep_scan_hours_pt"] = new_hours
            ds.write_json(ds.data_dir / "config.json", config)
            new_cron = f"0 {','.join(str(h) for h in sorted(new_hours))} * * *"
            ds.write_json(ds.data_dir / "schedule_state.json", {
                "current_hours": new_hours,
                "previous_hours": current_hours,
                "score": score,
                "updated_at": datetime.datetime.now().isoformat(),
            })
            # Attempt to update the registered cron job schedule
            job = cron.find_job("custodian:deep")
            if job:
                job_id = job.get("id") or job.get("name", "custodian:deep")
                subprocess.run(
                    ["openclaw", "cron", "edit", job_id, "--cron", new_cron],
                    capture_output=True
                )
        else:
            print(f"   Schedule optimal (score {score})")
    else:
        results["schedule_score"] = -1
        print(f"   Deferred ({active_days}/{min_days} days needed)")

    # Step 6: Skill conformance check
    print("6. Skill conformance check...")
    registered_names = {j.get("name", "") for j in cron.list_jobs()}
    for skill_dir in installed_skills():
        tasks = skill_background_tasks(skill_dir)
        if not tasks:
            ds.append_jsonl(ds.data_dir / "skill_conformance.jsonl", {
                "skill": skill_dir.name,
                "status": "no_tasks_declared",
                "checked_at": datetime.datetime.now().isoformat(),
            })
            continue
        for task in tasks:
            task_name = task.get("name", "")
            mechanism = task.get("mechanism", "").strip()
            is_heuristic = task.get("heuristic", False)
            conformance = {
                "skill": skill_dir.name,
                "task": task_name,
                "source": task.get("source", "unknown"),
                "checked_at": datetime.datetime.now().isoformat(),
            }
            if is_heuristic:
                # Heuristic only — surface as Tier 2 for human review, never auto-register
                conformance["status"] = "heuristic_unverified"
                conformance["hint"] = task.get("hint_text", "")
                open_fids = {i.get("fingerprint_id") for i in ds.open_issues()}
                issue_key = f"heuristic:{skill_dir.name}"
                if issue_key not in open_fids:
                    ds.append_issue({
                        "issue_id": str(uuid.uuid4())[:8],
                        "fingerprint_id": "oc_background_task_missing",
                        "status": "open",
                        "tier": 2,
                        "first_seen": datetime.datetime.now().isoformat(),
                        "note": (
                            f"{skill_dir.name} may have undeclared background tasks. "
                            f"Hint: {task.get('hint_text', '')[:80]}"
                        ),
                        "context": {"skill_name": skill_dir.name, "source": "heuristic"},
                    })
                print(f"   Heuristic task hint: {skill_dir.name} — Tier 2 (needs review)")
            elif mechanism == "cron" and task_name and task_name not in registered_names:
                # Machine-readable cron task missing — Tier 1 auto-register
                schedule = task.get("schedule", "0 0 * * *")
                if re.match(r"^[\d\*,/\- ]+$", schedule):
                    print(f"   Missing cron: {task_name} — registering")
                    ok = cron.add_cron_job(task_name, schedule, task.get("command", task_name))
                    conformance["status"] = "registered" if ok else "failed"
                    results["background_tasks_registered"] += int(ok)
                    results["issues_auto_fixed"] += int(ok)
                else:
                    conformance["status"] = "skipped_invalid_schedule"
                    print(f"   Unrecognized schedule '{schedule}' for {task_name} — skipped")
            else:
                conformance["status"] = "ok"
            ds.append_jsonl(ds.data_dir / "skill_conformance.jsonl", conformance)

    # Step 7: Skill init pass + proactive directory check
    print("7. Skill init pass...")
    for skill_dir in installed_skills():
        skill_name = skill_dir.name
        
        # Proactively check for missing intake/journal directories
        dir_issues = check_skill_directories(skill_name, ds)
        for issue in dir_issues:
            fid = issue["fingerprint_id"]
            # Avoid duplicates - check if already open
            open_fids = {i.get("fingerprint_id") for i in ds.open_issues()}
            if fid not in open_fids:
                ds.append_issue(issue)
                results["issues_detected"] += 1
                print(f"   Detected missing dir: {fid} for {skill_name}")
                # Auto-fix Tier 1 immediately
                spec = fp.known.get(fid, {})
                fix = fixer.apply(fid, spec, issue.get("context", {}))
                if fix["outcome"] == "fix_applied":
                    results["issues_auto_fixed"] += 1
                    print(f"     → Fixed: created {issue.get('context', {}).get('path', 'directory')}")
        
        if not is_skill_initialized(skill_name):
            print(f"   Initializing: {skill_name}")
            fix = fixer.apply(
                "oc_skill_uninitialized",
                fp.known.get("oc_skill_uninitialized", {}),
                {"skill_name": skill_name},
            )
            if fix["outcome"] == "fix_applied":
                results["skills_initialized"] += 1
                results["issues_auto_fixed"] += 1

    # Step 8: Repair pass (Tier 1, activity-aware)
    print("8. Repair pass...")
    now_ts = datetime.datetime.now()
    open_issues = ds.open_issues()
    tier1 = [
        i for i in open_issues
        if i.get("tier", 4) == 1 and i.get("status") in ("open", "fix_attempted_failed")
    ]
    fix_times = []
    for issue in tier1:
        fid = issue.get("fingerprint_id", "")
        spec = fp.known.get(fid) or next(
            (e for e in fp.learned if e.get("fingerprint_id") == fid), {}
        )
        if not spec:
            continue
        # Activity-aware: active hours → only urgent (< 5 min old)
        if current_state == "active":
            try:
                age_secs = (now_ts - datetime.datetime.fromisoformat(
                    issue.get("first_seen", "")
                )).total_seconds()
                if age_secs > 300:
                    ds.append_jsonl(ds.data_dir / "deferred_fixes.jsonl", {
                        "issue_id": issue.get("issue_id"),
                        "fingerprint_id": fid,
                        "deferred_at": now_ts.isoformat(),
                        "reason": "active_hours_non_urgent",
                    })
                    continue
            except (ValueError, TypeError):
                pass
        t0 = datetime.datetime.now()
        fix = fixer.apply(fid, spec, issue.get("context", {}))
        elapsed_ms = int((datetime.datetime.now() - t0).total_seconds() * 1000)
        if fix["outcome"] == "fix_applied":
            results["issues_auto_fixed"] += 1
            fix_times.append(elapsed_ms)
            print(f"   Fixed: {fid}")

    if fix_times:
        results["mean_time_to_fix_ms"] = sum(fix_times) // len(fix_times)

    # Execute any deferred fixes if now quiet
    if current_state == "quiet":
        deferred = ds.read_jsonl(ds.data_dir / "deferred_fixes.jsonl")
        applied_deferred = 0
        for d_fix in deferred:
            fid = d_fix.get("fingerprint_id", "")
            spec = fp.known.get(fid, {})
            if spec:
                fix = fixer.apply(fid, spec)
                if fix["outcome"] == "fix_applied":
                    applied_deferred += 1
        if applied_deferred:
            ds.rewrite_jsonl(ds.data_dir / "deferred_fixes.jsonl", [])
            print(f"   Applied {applied_deferred} deferred fix(es)")

    # Step 9: Emit search candidates
    if results["search_candidates"]:
        sc_path = ds.data_dir / "search_candidates.json"
        sc_path.write_text(json.dumps({
            "generated_at": now_ts.isoformat(),
            "run_id": run_id,
            "candidates": results["search_candidates"],
        }, indent=2))
        print(f"9. {len(results['search_candidates'])} unknown fingerprints → search_candidates.json")
        print("   Agent: run web search pass using search_candidates.json")
    else:
        print("9. No unknown fingerprints")

    # Step 10: Escalation pass
    print("10. Escalation pass...")
    tier3_issues = [i for i in ds.open_issues() if i.get("tier", 4) >= 3]
    vesper_intake = HOME / "openclaw" / "data" / "ocas-vesper" / "intake"
    for issue in tier3_issues:
        results["escalations"] += 1
        results["escalation_needed"] = True
        if vesper_intake.exists():
            proposal = {
                "type": "anomaly_alert",
                "source": "ocas-custodian",
                "issue_id": issue.get("issue_id"),
                "fingerprint_id": issue.get("fingerprint_id"),
                "tier": issue.get("tier"),
                "created_at": now_ts.isoformat(),
            }
            proposal_file = vesper_intake / f"custodian-{issue.get('issue_id', run_id)}.json"
            proposal_file.write_text(json.dumps(proposal, indent=2))
    if tier3_issues:
        print(f"   Escalated {len(tier3_issues)} issue(s)")
    else:
        print("   No escalations")

    # Step 11: Write report
    elapsed = (datetime.datetime.now() - start).total_seconds()
    report_dir = ds.data_dir / "reports"
    report_dir.mkdir(exist_ok=True)
    report_path = report_dir / f"{start.strftime('%Y-%m-%d-%H%M')}.md"
    report_path.write_text(build_report(results, run_id, elapsed))
    print(f"\nReport: {report_path}")

    # Prune reports older than 7 days
    cutoff_date = datetime.date.today() - datetime.timedelta(days=7)
    for old in report_dir.glob("*.md"):
        try:
            if datetime.date.fromisoformat(old.name[:10]) < cutoff_date:
                old.unlink()
        except (ValueError, OSError):
            pass

    results["summary"] = (
        f"Deep scan in {elapsed:.1f}s: "
        f"{results['issues_detected']} detected, "
        f"{results['issues_auto_fixed']} fixed, "
        f"{results['escalations']} escalated."
    )
    print(results["summary"])

    # Step 12: Write journal
    kind = "action" if (results["issues_auto_fixed"] > 0
                        or results["background_tasks_registered"] > 0) else "observation"
    jw.write(ds, kind, run_id, results)


def cmd_verify(args):
    if not args:
        print("Usage: verify <fix_id>")
        sys.exit(1)

    fix_id = args[0]
    ds = DataStore()
    fp = Fingerprinter(ds)
    fixes = ds.fixes()
    fix = next((f for f in fixes if f.get("fix_id") == fix_id), None)
    if not fix:
        print(f"Fix {fix_id} not found")
        sys.exit(1)

    fid = fix.get("fingerprint_id", "")
    log_path = find_gateway_log()
    still_present = False
    if log_path:
        for line in tail_log(log_path, 50):
            matched_id, _ = fp.match(line)
            if matched_id == fid:
                still_present = True
                break

    consec = fix.get("consecutive_failures", 0)
    if still_present:
        consec += 1
        fix["outcome"] = "fix_attempted_failed"
        fix["consecutive_failures"] = consec
        print(f"Fix {fix_id}: FAILED — issue still present ({consec} consecutive failures)")
        if consec >= 2:
            issues = ds.issues()
            for issue in issues:
                if issue.get("fingerprint_id") == fid and issue.get("status") != "resolved":
                    issue["tier"] = 3
                    issue["status"] = "escalated"
            ds.rewrite_jsonl(ds.data_dir / "issues.jsonl", issues)
            print(f"  Promoted {fid} to Tier 3")
    else:
        fix["outcome"] = "fix_verified"
        fix["verified_at"] = datetime.datetime.now().isoformat()
        print(f"Fix {fix_id}: VERIFIED — issue resolved")

    ds.rewrite_jsonl(ds.data_dir / "fixes.jsonl", fixes)


def cmd_repair_auto(args):
    ds = DataStore()
    cron = CronManager()
    fp = Fingerprinter(ds)
    fixer = Tier1Fixer(ds, cron)

    pending = [
        i for i in ds.open_issues()
        if i.get("tier", 4) == 1 and i.get("status") in ("open", "fix_attempted_failed")
    ]
    if not pending:
        print("No pending Tier 1 fixes.")
        return

    applied = 0
    for issue in pending:
        fid = issue.get("fingerprint_id", "")
        spec = fp.known.get(fid) or next(
            (e for e in fp.learned if e.get("fingerprint_id") == fid), {}
        )
        if not spec:
            print(f"  ? {fid}: no spec found — skipping")
            continue
        fix = fixer.apply(fid, spec, issue.get("context", {}))
        mark = "✓" if fix["outcome"] == "fix_applied" else "✗"
        print(f"  {mark} {fid}: {fix['outcome']}")
        if fix["outcome"] == "fix_applied":
            applied += 1

    print(f"\nApplied {applied}/{len(pending)} Tier 1 fixes.")


def cmd_repair_plan(args):
    ds = DataStore()
    issues = [i for i in ds.open_issues() if i.get("tier", 4) in (2, 3)]
    if not issues:
        print("No Tier 2/3 issues requiring a plan.")
        return

    print("Repair Plan — Tier 2/3 Issues")
    print("=" * 50)
    for issue in issues:
        tier = issue.get("tier", "?")
        fid = issue.get("fingerprint_id", "unknown")
        iid = issue.get("issue_id", "?")
        print(f"\n[Tier {tier}] {fid} (id: {iid})")
        print(f"  Age: {age_str(issue.get('first_seen', ''))}")
        print(f"  Status: {issue.get('status', 'open')}")
        if tier == 2:
            print("  Action: Surface to user with proposed change — do not auto-apply")
        elif tier == 3:
            print("  Action: Escalate → Vesper intake + tag journal escalation_needed: true")
            print(f"  Mentor: mentor.plan.run custodian-repair --arg issue_id={iid}")


def cmd_issues_list(args):
    ds = DataStore()
    issues = ds.open_issues()
    output = {
        "count": len(issues),
        "issues": [
            {
                "issue_id": i.get("issue_id", ""),
                "fingerprint_id": i.get("fingerprint_id", ""),
                "tier": i.get("tier", "?"),
                "status": i.get("status", ""),
                "age": age_str(i.get("first_seen", "")),
                "recurrence_count": i.get("recurrence_count", 1),
            }
            for i in issues
        ],
    }
    print(json.dumps(output, indent=2))


def cmd_issues_resolve(args):
    if not args:
        print("Usage: issues.resolve <issue_id>")
        sys.exit(1)

    issue_id = args[0]
    ds = DataStore()
    issues = ds.issues()
    found = False
    for issue in issues:
        if issue.get("issue_id") == issue_id:
            issue["status"] = "resolved"
            issue["resolved_at"] = datetime.datetime.now().isoformat()
            found = True
            break
    if not found:
        print(f"Issue {issue_id} not found")
        sys.exit(1)
    ds.rewrite_jsonl(ds.data_dir / "issues.jsonl", issues)
    print(f"Issue {issue_id} resolved.")


def cmd_status(args):
    ds = DataStore()
    config = ds.config()
    open_issues = ds.open_issues()
    model = ds.activity_model()
    schedule = ds.schedule_state()
    eff = ds.fix_effectiveness()
    attempts = sum(e.get("attempts", 0) for e in eff)
    successes = sum(e.get("successes", 0) for e in eff)

    current_hours = (
        schedule.get("current_hours")
        or config.get("schedule", {}).get("deep_scan_hours_pt", [1, 7, 13, 19])
    )

    print(json.dumps({
        "skill": "ocas-custodian",
        "version": config.get("version", "1.0.1"),
        "state": "ok" if not open_issues else "degraded",
        "current_state": model.get("current_state", "unknown"),
        "open_issues": len(open_issues),
        "tier1_open": len([i for i in open_issues if i.get("tier") == 1]),
        "tier3_open": len([i for i in open_issues if i.get("tier", 0) >= 3]),
        "fix_success_rate": round(successes / max(attempts, 1), 3),
        "schedule_hours_pt": sorted(current_hours),
        "schedule_score": schedule.get("score", -1),
        "checked_at": datetime.datetime.now().isoformat(),
    }, indent=2))


def cmd_schedule_show(args):
    ds = DataStore()
    config = ds.config()
    state = ds.schedule_state()
    model = ds.activity_model()

    current = (
        state.get("current_hours")
        or config.get("schedule", {}).get("deep_scan_hours_pt", [1, 7, 13, 19])
    )
    score = state.get("score", "?")
    confidence = model.get("current_hour_confidence", "unknown")
    updated = state.get("updated_at", "never")

    print("Custodian Scan Schedule")
    print("=" * 40)
    print(f"Deep scan hours (PT): {sorted(current)}")
    print(f"Schedule score:       {score}/8")
    print(f"Hour confidence:      {confidence}")
    print(f"Last optimized:       {updated}")
    print()
    print("Hourly activity confidence (24h):")
    hourly = model.get("hourly", {})
    for h in range(24):
        lv = hourly.get(str(h), {}).get("level", "low")
        bar = {"high": "██", "med": "▓░", "low": "░░"}.get(lv, "??")
        marker = " ◄ scheduled" if h in (current or []) else ""
        print(f"  {h:02d}:00  {bar}  {lv}{marker}")


def cmd_update(args):
    """Self-update via gh CLI from GitHub, preserving data and journals."""
    print("Custodian self-update")
    print("=" * 40)

    r = subprocess.run(["which", "gh"], capture_output=True)
    if r.returncode != 0:
        print("ERROR: gh CLI not found. Install GitHub CLI first.")
        sys.exit(1)

    r = subprocess.run(
        ["gh", "release", "view", "--repo", "indigokarasu/custodian", "--json", "tagName"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        print(f"Could not reach remote: {r.stderr.strip()}")
        sys.exit(1)

    try:
        remote_version = json.loads(r.stdout).get("tagName", "").lstrip("v")
    except json.JSONDecodeError:
        remote_version = "unknown"

    local_version = DataStore().config().get("version", "1.0.1")
    print(f"Local: v{local_version}  Remote: v{remote_version}")

    if local_version == remote_version:
        print("Already up to date.")
        return

    print(f"Downloading v{remote_version}...")

    with tempfile.TemporaryDirectory() as tmp_str:
        tmp = Path(tmp_str)
        r2 = subprocess.run(
            ["gh", "release", "download", "--repo", "indigokarasu/custodian",
             "--pattern", "*.tar.gz", "--dir", tmp_str],
            capture_output=True, text=True,
        )
        if r2.returncode != 0:
            print(f"Download failed: {r2.stderr.strip()}")
            sys.exit(1)

        archives = list(tmp.glob("*.tar.gz"))
        if not archives:
            print("No archive in release assets.")
            sys.exit(1)

        extract_dir = tmp / "extracted"
        extract_dir.mkdir()
        subprocess.run(["tar", "xzf", str(archives[0]), "-C", str(extract_dir)], check=True)

        new_roots = [d for d in extract_dir.iterdir() if d.is_dir()]
        if not new_roots:
            print("Archive appears empty.")
            sys.exit(1)

        new_root = new_roots[0]
        skill_dir = SKILL_ROOT

        for item in new_root.iterdir():
            if item.name in ("data", "journals"):
                continue
            dest = skill_dir / item.name
            if item.is_dir():
                shutil.copytree(str(item), str(dest), dirs_exist_ok=True)
            else:
                shutil.copy2(str(item), str(dest))

    print(f"Updated to v{remote_version}.")


# ══════════════════════════════════════════════════════════════════════════════
# Dispatch
# ══════════════════════════════════════════════════════════════════════════════

COMMANDS = {
    "init": cmd_init,
    "scan.light": cmd_scan_light,
    "scan.deep": cmd_scan_deep,
    "verify": cmd_verify,
    "repair.auto": cmd_repair_auto,
    "repair.plan": cmd_repair_plan,
    "issues.list": cmd_issues_list,
    "issues.resolve": cmd_issues_resolve,
    "status": cmd_status,
    "schedule.show": cmd_schedule_show,
    "update": cmd_update,
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)
    cmd = sys.argv[1]
    rest = sys.argv[2:]
    if cmd not in COMMANDS:
        print(f"Unknown command: {cmd}")
        print(f"Available: {', '.join(sorted(COMMANDS))}")
        sys.exit(1)
    COMMANDS[cmd](rest)


if __name__ == "__main__":
    main()
