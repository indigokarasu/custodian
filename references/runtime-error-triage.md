# RuntimeError: ERROR — Cron Job Triage Pattern

## Pattern

A cron job fails with `RuntimeError: ERROR` (generic, no detail). This is **not** a code bug — it is the generic error raised when the LLM provider (OpenRouter) returns a transient failure.

## How to confirm

1. Check today's `errors.log` for the job name — if absent, the error is stale (already past).
2. Check rotated logs (`errors.log.1`, `agent.log.2`, `agent.log.3`) for the original timestamp.
3. In `agent.log*`, search for `Provider returned error` around the same time window.
4. If multiple cron jobs failed within the same 1–2 hour window with `Provider returned error`, it's a **provider transient wave** — not a per-job bug.

## What to do

- **Do not** attempt to fix the skill code or cron job logic.
- Mark the task as resolved with note: "Transient provider error — no recurrence."
- The cron job will self-heal on its next scheduled run.
- Only investigate further if the error recurs on 2+ consecutive days.

## Real example (2026-05-19)

- `sands:conflict-scan` failed at 14:09 UTC with `RuntimeError: ERROR`
- Same window: `dispatch:draft`, `praxis:review`, `rally:update` all hit `Provider returned error`
- No recurrence on 2026-05-20 — confirmed transient
- Root cause: OpenRouter provider errors during a concurrent cron window