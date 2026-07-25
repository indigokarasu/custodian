# Escalation Loop — Tier-4 Code-Defect Verification

When the escalation loop finds an open `oc_chronicle_*` / plugin code-defect issue
flagged `escalation_needed: true`, **DO NOT act on the flag alone.** The issue
tracker's `escalation_needed`/`status` fields can be stale relative to the
live code and the live gateway. Three independent checks must pass before any
patch is even drafted.

## 1. Re-derive post-restart recurrence from the gateway log

A signature that fired 14–19× pre-restart but 0× post-restart is
**pre-restart-only noise** (Custodian's own rule drops it). An open issue can
still carry the pre-restart premise long after the fault stopped recurring.

Procedure:
- Find the restart boundary in the active gateway log:
  `grep -n "Starting Hermes Gateway" ~/.hermes/profiles/indigo/logs/gateway.log | tail -1`
  → take the line number `RESTART_LINE`.
- Count signature occurrences **after** that line:
  `awk -v rl=$RESTART_LINE 'NR>rl && /SIGNATURE_REGEX/ {c++} END{print c+0}' ~/.hermes/profiles/indigo/logs/gateway.log`
- Or across all logs (covers rotated files):
  `grep -rhE "SIGNATURE" ~/.hermes/profiles/indigo/logs/*.log ~/.hermes/logs/*.log 2>/dev/null | grep -cE "2026-07-23 0[4-9]|2026-07-23 1[0-6]"`
- **If 0 post-restart → reclassify to `latent_dormant`** (set `status=latent_dormant`, `escalation_needed=false`, add `post_restart_recurrence=0` + `dormant_evidence`). PRESERVE the issue — do NOT delete it. Only a future live recurrence should reactivate it.

## 2. Read the LIVE loaded module to confirm the defect still exists

The flagged defect may already be **mitigated in current code**. Reading the
issue text is not enough.

Concrete confirmed example (2026-07-23): the `oc_chronicle_contextengine_compress_force_kwarg`
issue was open, but the live `/usr/local/lib/hermes-agent/agent/conversation_compression.py`
(and the `.venv` copy) **already wraps** `compress(... force=force, focus_topic=...)`
in `try/except TypeError` that retries `compress(messages, current_tokens=...)`
without the extra kwargs. The signature is effectively resolved in code even
though the issue stayed open. Post-restart recurrence = 0 confirmed it.

How to find the live module (not a stale copy):
```
python3 -c "import agent, os; print(os.path.dirname(agent.__file__))"
```
Read THAT path. Do NOT trust `gentube-output/`, `pr-work/`, or `work/` copies
— they are checkouts, not what the gateway imports.

General rule: for any code-defect issue, grep the live module for the failing
call/site. If a fallback, guard, or fix already covers it, treat the issue as
resolved-in-code (0 post-restart recurrence corroborates) and reclassify
rather than patching.

## 3. Plugin memory-engine defects are OUTSIDE the autonomous Tier-4 envelope

The fix-safety Tier-4 exception is scoped to `gateway/*.py` **SOURCE files**
(see `references/fix-safety.md`). A defect in the live memory engine — e.g.
`plugins/chronicle/engine/store.py` (the append path behind `state.db`) — is
NOT a gateway-source file. Two hard constraints then apply:

- **Never modify plugin/package files** autonomously (hard constraint, `fix-safety.md` line 10).
- **Never restart the gateway** to load a patch (hard constraint; the Tier-4
  exception explicitly says *the user* must restart).

Therefore, for a live plugin memory-engine defect:
1. **DO NOT auto-patch** the plugin file.
2. **DO NOT auto-restart** the gateway.
3. **ANNOTATE** the issue: `user_gated=true`, `user_gated_reason="Tier4 fix
   patches <file> ; safety envelope forbids autonomous plugin edit + gateway
   restart"`, plus `resolution_blocked_by="Draft patch exists but requires user
   to restart gateway; no documented fix recipe in known-code-fixes"`.
4. **Embed the proposed minimal patch** in the issue/journal (e.g. actor
   sanitization at `append_event` boundary: coerce out-of-enum actor to
   `'agent'` before the INSERT) so the user can apply it and run
   `hermes gateway restart`.
5. Leave the issue open/escalated and **surface it in the report** — it is a
   genuine escalation requiring <operator>'s decision, not a `[SILENT]`.

## Reusable awk recipe (copy-paste)

```bash
LOG=~/.hermes/profiles/indigo/logs/gateway.log
RESTART_LINE=$(grep -n "Starting Hermes Gateway" "$LOG" | tail -1 | cut -d: -f1)
echo -n "post-restart occurrences: "
awk -v rl="$RESTART_LINE" 'NR>rl && /SIGNATURE_REGEX/ {c++} END{print c+0}' "$LOG"
```

## Why this matters

This is the inverse of Step 8e (verify a *resolved* code-fix still covers all
references). Here the risk is the opposite: an issue is marked *open/escalated*
but the defect is already gone (pre-restart noise, or a fallback added upstream)
— acting on it wastes a gateway-restart cycle and risks patching live memory
code that doesn't need it. Always re-derive live state before acting.
