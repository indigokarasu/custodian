# Stale gateway-restart marker → false post-restart escalation

**Confirmed 2026-07-24.** A custodian light-scan journal escalated
`oc_telegram_message_thread_not_found_20260724T0702Z` as "LIVE post-restart" with
`post_restart_tracebacks.telegram_message_thread_not_found: 4` and
`last_restart: 2026-07-23T04:02:50Z`. The 4 errors were real but ALL occurred at
**2026-07-23 20:50–21:01Z** — after that stated restart but **BEFORE the actual most-recent
gateway restart at 2026-07-24T04:02:18Z**. Relative to the true last restart, the errors are
PRE-restart → the issue is **DORMANT**, not live. The journal's `last_restart` field lagged a
later restart that occurred after the journal was written.

## The pitfall
A custodian journal's `last_restart` field (and any `post_restart_tracebacks` count derived
from it) can be **STALE**: a later gateway restart between the journal's reference scan and your
escalation-loop run invalidates the "post-restart" claim. Trusting the journal's restart
timestamp produces a **false escalation of a dormant signature** — the inverse of the
forward-stale trap. `verify_plugin_defect_postrestart.py` itself re-derives the true last restart
from the live log (robust), but a journal-born escalation that merely *cites* a restart window
without re-deriving it is unsafe.

## Verification recipe (terminal, never pipe-to-python — tirith blocks it)
```bash
# 1. Derive TRUE last restart from the LIVE log (PROFILE path, not ~/.hermes/logs/gateway.log)
grep -nE "Connecting to telegram|telegram connected|Starting Hermes|Received SIGTERM" \
  ~/.hermes/profiles/indigo/logs/gateway.log | tail -3
# → most recent timestamp = true_last_restart
```
```python
# 2. Count the signature strictly AFTER true_last_restart
import re
f="~/.hermes/profiles/indigo/logs/gateway.log"
lines=open(f,encoding="utf-8",errors="replace").read().splitlines()
restart=None; hits=[]
for ln in lines:
    if "telegram connected" in ln or "Starting Hermes" in ln or "Received SIGTERM" in ln:
        m=re.search(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}",ln); restart=m.group(1) if m else restart
    if "Message thread not found" in ln: hits.append(ln)
post=[h for h in hits if restart and h > restart]
print("true_last_restart:",restart,"total:",len(hits),"post_restart:",len(post))
```
If `post_restart == 0` → **DORMANT**. Resolve as false escalation via
`scripts/race_safe_issue_patch.py` (`--set status=resolved --set user_gated=false
--set escalation_needed=false`), do NOT re-escalate.

## Gateway delivery-target stale thread (Telegram) — fix decision
When the dormant signature is a Telegram "Message thread not found" on a topic id
(e.g. `8666597030:78835`):
- The stale target lives in `channel_directory.json` (gateway delivery registry), **NOT** a cron
  job `deliver` field. Confirm with `python3` over `jobs.json` that no enabled job has a
  `deliver` referencing the thread id.
- `grep` the live log: if the session `agent:main:telegram:dm:<chat>:<thread>` was evicted at the
  last restart and has **0 post-restart hits**, the topic is a deleted/stale entry.
- **Do NOT clear or re-point the `channel_directory.json` entry** even though it's stale: no live
  job/run references it, and editing the registry risks breaking <operator>'s ACTIVE threads (other
  topic ids in the same chat, e.g. 79978/79943). The correct fix for a dormant delivery-target
  defect is **NO fix** — leave the stale entry; the gateway won't re-touch it until a real
  delivery targets it (which won't happen post-eviction).
- Distinct from plugin memory-engine defects (`actor_check` / `seq_unique` CHECK-constraints),
  which ARE live post-restart and are handled by
  `references/escalation-loop-tier4-code-defect-verify.md` (annotate `user_gated`, do NOT restart
  gateway). Verify those with `verify_plugin_defect_postrestart.py` (it re-derives the true
  restart itself).

## Escalation-loop rule added
Before acting on ANY journal-born "post-restart" escalation (Step 2 traceback-gap signature, or
an issue whose note says "LIVE post-restart"), **re-derive the true last restart from the live
gateway.log** and re-count. If the signature's last occurrence predates the true restart, it is
dormant — close as a false escalation, do not re-escalate, do not restart the gateway or edit
delivery registries.
