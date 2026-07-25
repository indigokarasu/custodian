# No-Agent Exit-1 Verification: Pipe-Mask Trap & Real-Degraded Discrimination

## The pipe-mask trap (confirmed 2026-07-22 light scan)
When you manually re-run a `no_agent` cron script to confirm whether its
`Script exited with code 1` is stale or live, do NOT pipe the script's output
through `head` / `tail` / `grep` and then read `$?`:

    bash /path/script.sh | tail -20; echo "EXIT=$?"   # WRONG: $? is tail's exit (0)

`$?` after a pipe reflects the LAST command in the pipeline (`tail`, which
exits 0), silently masking the script's real non-zero exit. This produced a
false "stale" classification during the 2026-07-22 scan: the manual re-run
printed `EXIT=0`, but `hermes cron run <id>` returned `Ran now: failed.`
because the script genuinely exits 1 on a violated invariant.

**Correct verification pattern:**
- Authoritative: `hermes cron run <id>` — the gateway runs the script exactly
  as scheduled and flips `last_status`. Trust its `succeeded.` / `failed.`
  line over any manual re-run. Serial loop over ~17 IDs fits the 180s terminal
  cap; no_agent jobs return in ~2s.
- If you must run the script directly: capture to a file and check separately,
  use `PIPESTATUS` (`echo ${PIPESTATUS[0]}`), or run WITHOUT a pipe and read
  `$?` on the immediately following line.

## Real-degraded vs no-op exit-1 (rally:pipeline-watchdog, 2026-07-22)
A `no_agent` watchdog that exits 1 with `DEGRADED ... <invariant>` output is a
**genuine failure when the invariant it checks is real**, even though it looks
like a no-op wrapper. Discriminate:

- **No-op-by-design (Tier 2, leave running):** `monitor_journals.py` exits 1
  only when no NEW journals exist since the last check — a quiet-state signal,
  not a fault. Output contains no violated invariant.
- **Real degraded (actionable / escalate):** `rally:pipeline-watchdog.sh`
  checks pipeline invariants (staged rebalances, fresh trade plan, dry-run
  NameErrors) and exits 1 on ANY violation. `DEGRADED ... no_staged_rebalance`
  means 0 staged rebalances in `pending_actions.jsonl` — a real pipeline state.
  Confirm by reading the live data the script inspects (e.g. count `staged` in
  `pending_actions.jsonl`); if the invariant is truly violated, persist the
  issue (closes a journal→issues gap), it is NOT a no-op.

**Rule:** never classify a no_agent exit-1 as `oc_cron_no_agent_exit_1_noop`
until you have read the script and confirmed its exit-1 path is a quiet-state
signal, not an invariant violation. When in doubt, `hermes cron run <id>` is
the ground-truth probe — a manual piped re-run that returns `$?=0` is
meaningless.
