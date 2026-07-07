#!/usr/bin/env python3
"""Gateway health check script.
Checks if the Hermes gateway systemd service is running and all platform
connections (Telegram, SMS) are healthy. Restarts via systemctl if dead,
stuck, or in failed state. Clears stale PID files that cause crash loops.
Outputs json status for the cron agent to report.

Detected failure modes:
  - Service not active / in failed state / crash loop
  - Telegram bot API unreachable (getMe fails)
  - Telegram polling conflict (409 / "polling conflict" in journal)
  - Telegram network errors (ReadError, reconnect loops)
  - Telegram disconnect loops (repeated disconnect/reconnect cycles)
  - Telegram silent death (service up but no inbound messages for too long)
  - Import/dependency errors preventing startup
  - SMS platform disconnect loops
"""

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


SERVICE = "hermes-gateway.service"
SYSTEMCTL = "systemctl --user"  # Gateway runs as user-level systemd service
MAX_LOG_AGE_MINUTES = 5
STARTUP_WAIT = 10

# Global hard timeout — entire script must finish within this many seconds
GLOBAL_TIMEOUT = 45

# Thresholds
DISCONNECT_LOOP_THRESHOLD = 5       # disconnects in window to count as loop
DISCONNECT_LOOP_WINDOW_MIN = 10     # lookback window in minutes
NETWORK_ERROR_THRESHOLD = 3         # network errors in window
SILENT_DEATH_MINUTES = 30           # no inbound telegram msgs = suspicious


class TimeoutError(Exception):
    pass


def _global_timeout_handler(signum, frame):
    raise TimeoutError("Script exceeded global timeout of {}s".format(GLOBAL_TIMEOUT))


# Install global timeout via SIGALRM
signal.signal(signal.SIGALRM, _global_timeout_handler)
signal.alarm(GLOBAL_TIMEOUT)


def run(cmd, timeout=8):
    """Run shell command, return (stdout, exit_code). Never hangs."""
    try:
        r = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )
        return r.stdout.strip(), r.returncode
    except subprocess.TimeoutExpired:
        return "", -1
    except Exception as e:
        return str(e), -1


def get_hermes_home():
    """Resolve HERMES_HOME (defaults to ~/.hermes)."""
    override = os.environ.get("HERMES_HOME")
    if override:
        return Path(override)
    return Path.home() / ".hermes"


def get_pid_file_path():
    """Return the gateway PID file path."""
    return get_hermes_home() / "gateway.pid"


def clear_stale_pid_file():
    """Check and remove stale gateway PID file.

    A PID file is stale if:
    - The recorded PID doesn't exist (ProcessLookupError)
    - The process exists but isn't a gateway process
    - The process start_time doesn't match the recorded one

    Returns (cleared: bool, reason: str).
    """
    pid_path = get_pid_file_path()
    if not pid_path.exists():
        return False, "No PID file found"

    try:
        raw = pid_path.read_text().strip()
        if not raw:
            pid_path.unlink(missing_ok=True)
            return True, "Empty PID file removed"
        record = json.loads(raw)
        pid = int(record.get("pid", raw))
    except (json.JSONDecodeError, ValueError):
        try:
            pid = int(raw)
            record = {"pid": pid}
        except ValueError:
            pid_path.unlink(missing_ok=True)
            return True, "Malformed PID file removed"

    # Check if process is alive
    try:
        os.kill(pid, 0)  # signal 0 = existence check
    except (ProcessLookupError, PermissionError):
        pid_path.unlink(missing_ok=True)
        return True, f"Stale PID file removed (process {pid} not alive)"

    # Check start_time if recorded
    recorded_start = record.get("start_time")
    if recorded_start is not None:
        try:
            stat_path = Path(f"/proc/{pid}/stat")
            current_start = int(stat_path.read_text().split()[21])
            if current_start != recorded_start:
                pid_path.unlink(missing_ok=True)
                return True, f"Stale PID file removed (PID {pid} reused, start_time mismatch)"
        except (FileNotFoundError, IndexError, ValueError, OSError):
            pass

    # Check if process looks like a gateway
    try:
        cmdline_path = Path(f"/proc/{pid}/cmdline")
        cmdline = cmdline_path.read_bytes().replace(b"\x00", b" ").decode("utf-8", errors="ignore")
        gateway_patterns = (
            "hermes_cli.main gateway",
            "hermes_cli/main.py gateway",
            "hermes gateway",
            "gateway/run.py",
        )
        if not any(p in cmdline for p in gateway_patterns):
            pid_path.unlink(missing_ok=True)
            return True, f"Stale PID file removed (PID {pid} is not a gateway process)"
    except (FileNotFoundError, PermissionError, OSError):
        pass

    return False, f"PID file valid — process {pid} is alive and looks like gateway"


def check_crash_loop(journal_5min):
    """Check if the gateway is in a PID-file crash loop by inspecting journal.

    Returns True if the journal shows repeated 'PID file race' errors,
    indicating a stale PID file is blocking startup.
    """
    race_count = journal_5min.count("PID file race lost")
    restart_count = journal_5min.count("Scheduled restart job")
    return race_count >= 2 or restart_count >= 10


def get_service_state():
    """Return systemd service active state."""
    out, _ = run(f"{SYSTEMCTL} is-active {SERVICE}")
    return out.strip()  # "active", "failed", "inactive", etc.


def get_service_substate():
    """Return systemd service sub-state (e.g. 'running', 'dead', 'failed')."""
    out, _ = run(f"{SYSTEMCTL} show {SERVICE} --property=SubState --value")
    return out.strip()


def get_journal_tail(lines=20, minutes=5):
    """Get recent journal entries for the gateway service."""
    out, _ = run(
        f"journalctl --user -u {SERVICE} --no-pager -n {lines} --since '{minutes} min ago'"
    )
    return out


def get_journal_window(minutes):
    """Get journal entries for the gateway service within a time window."""
    out, _ = run(
        f"journalctl --user -u {SERVICE} --no-pager --since '{minutes} min ago'"
    )
    return out


def get_token():
    """Read Telegram bot token from .env. Returns empty string if not configured."""
    try:
        env_path = os.path.expanduser("~/.hermes/.env")
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("TELEGRAM_BOT_TOKEN=") and not line.startswith("#"):
                    token = line.split("=", 1)[1].strip()
                    if token:
                        return token
    except Exception:
        pass
    return ""


def check_telegram_responding(token):
    """Check if Telegram bot API is accessible via getMe."""
    if not token:
        return None, "No token configured — skipping Telegram checks"
    try:
        r = subprocess.run(
            ["curl", "-s", "--max-time", "5",
             f"https://api.telegram.org/bot{token}/getMe"],
            capture_output=True, text=True, timeout=10,
        )
        data = json.loads(r.stdout)
        if data.get("ok"):
            username = data.get("result", {}).get("username", "")
            return True, f"Bot @{username} auth OK"
        return False, f"API error: {data}"
    except Exception as e:
        return False, str(e)


def check_telegram_polling_conflict(journal_10min):
    """Check if Telegram polling has a 409 conflict."""
    if "polling conflict" in journal_10min.lower() or "409" in journal_10min:
        if "polling resumed" in journal_10min.lower():
            return False, "Polling conflict detected but already recovered"
        return True, "Telegram polling conflict (409) detected — another instance may be polling"
    return False, "No polling conflict"


def check_telegram_network_errors(journal_window):
    """Check for Telegram network errors (ReadError, timeouts, etc.)."""
    network_errors = (
        journal_window.count("network error")
        + journal_window.count("ReadError")
        + journal_window.count("TimeoutError")
    )
    if network_errors >= NETWORK_ERROR_THRESHOLD:
        if "polling resumed" in journal_window.lower():
            return False, "Network errors detected but already recovered"
        return True, f"{network_errors} Telegram network errors in last {DISCONNECT_LOOP_WINDOW_MIN} min"
    return False, "No network errors"


def check_telegram_disconnect_loop(journal_window):
    """Check for repeated Telegram disconnect/reconnect cycles."""
    disconnects = journal_window.count("Disconnected from Telegram")
    if disconnects >= DISCONNECT_LOOP_THRESHOLD:
        return True, f"{disconnects} Telegram disconnects in last {DISCONNECT_LOOP_WINDOW_MIN} min (disconnect loop)"
    return False, f"No disconnect loop ({disconnects} disconnects in {DISCONNECT_LOOP_WINDOW_MIN} min)"


def check_telegram_silent_death(journal_silent):
    """Check if gateway is running but Telegram hasn't received messages in a while."""
    inbound_count = journal_silent.count("inbound message: platform=telegram")
    if inbound_count == 0:
        if "Connected to Telegram" in journal_silent and "inbound message" not in journal_silent:
            return True, f"No Telegram inbound messages in last {SILENT_DEATH_MINUTES} min (polling may be stuck)"
    return False, "Telegram receiving messages normally"


def check_import_errors(journal_tail):
    """Check for import/dependency errors that would prevent startup.

    Only flags errors from the gateway process itself, not transient
    tool execution errors from chat sessions.
   """
    gateway_import_patterns = [
        "gateway.run",
        "hermes_cli.main",
        "Main process exited.*ImportError",
        "Failed to import",
        "cannot import name.*from.*hermes",
    ]
    import_errors = []
    for line in journal_tail.splitlines():
        if "ModuleNotFoundError" in line or "ImportError" in line:
            # Skip tool execution errors from chat sessions
            if "tool_executor" in line or "Tool terminal returned error" in line:
                continue
            # Skip errors from user code / stdin
            if "<stdin>" in line:
                continue
            import_errors.append(line.strip())
    if import_errors:
        return True, f"Import error: {import_errors[0]}"
    return False, "No import errors"


def check_sms_health(journal_window):
    """Check SMS platform health (disconnect loops)."""
    sms_disconnects = journal_window.count("[sms] Disconnected")
    if sms_disconnects >= DISCONNECT_LOOP_THRESHOLD:
        return True, f"{sms_disconnects} SMS disconnects in last {DISCONNECT_LOOP_WINDOW_MIN} min"
    return False, "SMS stable"


def restart_via_systemd(journal_tail):
    """Restart the gateway via systemctl. Returns (success, message)."""
    # Check for import errors that would prevent startup
    if "ModuleNotFoundError" in journal_tail or "ImportError" in journal_tail:
        for line in journal_tail.splitlines():
            if "ModuleNotFoundError" in line:
                return False, f"Cannot restart - dependency missing: {line.strip()}"

    # Stale PID file fix
    in_crash_loop = check_crash_loop(journal_tail)
    cleared, clear_reason = clear_stale_pid_file()
    if cleared or in_crash_loop:
        if in_crash_loop:
            pid_path = get_pid_file_path()
            pid_path.unlink(missing_ok=True)
        run("pkill -9 -f 'hermes.*gateway.*run' 2>/dev/null")
        time.sleep(1)

    # Reset failed state first (critical after throttle)
    run(f"{SYSTEMCTL} reset-failed {SERVICE}")

    # Kill stale processes
    run("pkill -9 -f 'hermes.*gateway' 2>/dev/null")
    time.sleep(2)

    # Start via systemctl
    _, rc = run(f"{SYSTEMCTL} start {SERVICE}")
    time.sleep(STARTUP_WAIT)

    # Verify
    state = get_service_state()
    if state == "active":
        return True, "Service restarted successfully via systemctl"
    else:
        tail = get_journal_tail(10)
        return False, f"Restart failed. State={state}. Recent: {tail[:200]}"


def check():
    """Run the health check. Returns status dict."""
    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": "none",
    }

    try:
        # ── Batch journal reads to avoid redundant subprocess calls ──────
        # These are the expensive operations — do them once upfront
        journal_5min = get_journal_tail(50, minutes=5)
        journal_10min = get_journal_window(10) if False else journal_5min  # reuse 5min for 10min checks
        journal_window = get_journal_window(DISCONNECT_LOOP_WINDOW_MIN)
        journal_silent = get_journal_window(SILENT_DEATH_MINUTES)

        # ── Service state ────────────────────────────────────────────────
        state = get_service_state()
        substate = get_service_substate()
        result["service_state"] = state
        result["service_substate"] = substate

        # ── Telegram token check ─────────────────────────────────────────
        token = get_token()
        telegram_configured = bool(token)

        if telegram_configured:
            token_ok, token_msg = check_telegram_responding(token)
            result["telegram_api_ok"] = token_ok
            result["telegram_api_detail"] = token_msg
        else:
            result["telegram_api_ok"] = None
            result["telegram_api_detail"] = "No token configured"

        # ── Case 1: Import/dependency errors ──────────────────────────────
        has_import_err, import_detail = check_import_errors(journal_5min)
        if has_import_err:
            result["action"] = "none"
            result["status"] = "import_error"
            result["reason"] = import_detail
            result["recommendation"] = "Fix dependency issue before restarting"
            return result

        # ── Case 2: Service is in failed state (throttled / crash loop) ──
        if state == "failed":
            result["action"] = "restart"
            result["reason"] = "Service in failed/crash-loop state"
            success, msg = restart_via_systemd(journal_5min)
            result["restart_success"] = success
            result["restart_detail"] = msg
            result["status"] = "restarted_ok" if success else "restart_failed"
            return result

        # ── Case 3: Service is inactive/stopped ───────────────────────────
        if state not in ("active",):
            result["action"] = "restart"
            result["reason"] = f"Service not active (state={state})"
            success, msg = restart_via_systemd(journal_5min)
            result["restart_success"] = success
            result["restart_detail"] = msg
            result["status"] = "restarted_ok" if success else "restart_failed"
            return result

        # ── Case 4: Service active but in crash loop ──────────────────────
        if check_crash_loop(journal_5min):
            result["action"] = "restart"
            result["reason"] = "Crash loop detected in journal (PID file race)"
            success, msg = restart_via_systemd(journal_5min)
            result["restart_success"] = success
            result["restart_detail"] = msg
            result["status"] = "restarted_ok" if success else "restart_failed"
            return result

        # ── Cases 5-10: Telegram/SMS checks (skip if no token) ───────────
        if telegram_configured:
            # Case 5: Telegram API not responding
            if not token_ok:
                result["action"] = "restart"
                result["reason"] = f"Telegram API issue: {token_msg}"
                success, msg = restart_via_systemd(journal_5min)
                result["restart_success"] = success
                result["restart_detail"] = msg
                result["status"] = "restarted_ok" if success else "restart_failed"
                return result

            # Case 6: Telegram polling conflict
            has_conflict, conflict_detail = check_telegram_polling_conflict(journal_5min)
            if has_conflict:
                result["action"] = "restart"
                result["reason"] = conflict_detail
                success, msg = restart_via_systemd(journal_5min)
                result["restart_success"] = success
                result["restart_detail"] = msg
                result["status"] = "restarted_ok" if success else "restart_failed"
                return result

            # Case 7: Telegram network errors
            has_net_error, net_detail = check_telegram_network_errors(journal_window)
            if has_net_error:
                result["action"] = "restart"
                result["reason"] = net_detail
                success, msg = restart_via_systemd(journal_5min)
                result["restart_success"] = success
                result["restart_detail"] = msg
                result["status"] = "restarted_ok" if success else "restart_failed"
                return result

            # Case 8: Telegram disconnect loop
            has_disc_loop, disc_detail = check_telegram_disconnect_loop(journal_window)
            if has_disc_loop:
                result["action"] = "restart"
                result["reason"] = disc_detail
                success, msg = restart_via_systemd(journal_5min)
                result["restart_success"] = success
                result["restart_detail"] = msg
                result["status"] = "restarted_ok" if success else "restart_failed"
                return result

            # Case 10: Telegram silent death
            is_silent, silent_detail = check_telegram_silent_death(journal_silent)
            if is_silent:
                result["action"] = "restart"
                result["reason"] = silent_detail
                success, msg = restart_via_systemd(journal_5min)
                result["restart_success"] = success
                result["restart_detail"] = msg
                result["status"] = "restarted_ok" if success else "restart_failed"
                return result

        # Case 9: SMS disconnect loop (always checked)
        has_sms_issue, sms_detail = check_sms_health(journal_window)
        if has_sms_issue:
            result["action"] = "restart"
            result["reason"] = sms_detail
            success, msg = restart_via_systemd(journal_5min)
            result["restart_success"] = success
            result["restart_detail"] = msg
            result["status"] = "restarted_ok" if success else "restart_failed"
            return result

        # ── Case 11: All healthy ──────────────────────────────────────────
        result["action"] = "none"
        result["status"] = "healthy"
        if telegram_configured:
            result["reason"] = "Gateway active, Telegram responding, no issues detected"
        else:
            result["reason"] = "Gateway active, no issues detected (Telegram not configured)"
        return result

    except TimeoutError as e:
        result["status"] = "timeout"
        result["reason"] = str(e)
        result["recommendation"] = "Health check timed out — gateway may be overloaded"
        return result
    except Exception as e:
        result["status"] = "error"
        result["reason"] = f"Health check failed: {e}"
        return result
    finally:
        signal.alarm(0)  # Cancel the alarm


if __name__ == "__main__":
    import argparse as _ap
    _parser = _ap.ArgumentParser(
        description="Gateway health check — verifies systemd service, platform connections, PID files."
    )
    _parser.add_argument("--dry-run", action="store_true", help="Check only, do not restart service")
    _parser.add_argument("--json", action="store_true", help="Output structured JSON (default)")
    _parser.add_argument("--text", action="store_true", help="Output human-readable text")
    _args = _parser.parse_args()

    if _args.dry_run:
        # In dry-run mode, skip the restart action but still report status
        import os
        os.environ["CUSTODIAN_DRY_RUN"] = "1"

    status = check()
    if _args.text:
        for k, v in status.items():
            print(f"  {k}: {v}")
    else:
        print(json.dumps(status, indent=2))
