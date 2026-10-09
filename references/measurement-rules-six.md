# Measurement Rules — Six That Cause the Most Findings

These rules apply to nearly every run. Full catalogue (40+ rules): `references/measurement-pitfalls.md`.

## The Six Rules

1. **A sweep that reads 0 files is indistinguishable from a clean sweep.** Print the
   per-set file count and a per-file error line; check the exit code AND the `!!` count.
   `gzip.open` takes no `errors=`, so one shared opener raises on every file and every
   fingerprint returns 0 at once. Branch on the extension inside the loop.
2. **The denominator must physically host the numerator.** Assert
   `distinct_firing_jobs <= enabled_jobs` and `unclaimed <= occurrences_due` PER HOUR. A
   failing capacity assertion means the denominator is wrong, not the numerator.
   Denominator = occurrences due (`3600/n` for `every Nm`, not 1), parsed from `*`, `a-b`,
   `*/s` and comma lists, all as **ints**.
3. **Key every hourly bucket by DAY *AND* HOUR, in LOCAL time.** Log stamps and cron
   selectors share one timezone domain. Hour-only keys sum two days; UTC keys against
   local stamps produce confident nonsense. Reset per-file `carry = None` inside the loop
   and guard the clock domain.
4. **Every appended `issues.jsonl` row MUST set `issue_id` AND `escalation_needed`,** and
   the open count must be compared before and after. Expected delta =
   `is_open(this_row) - is_open(previous_last_row_for_this_issue_id)` — the open filter
   applied twice, no kind table, no special cases. A reopen of a CLOSED issue is `+1`, not
   `0`, even though the row supersedes. Use `scripts/append_issue_row.py`. Full recipe:
   `references/issues-jsonl-row-integrity.md`.
5. **Cron is MINUTE HOUR DOM MONTH DOW** — `f[0]` is the minute. `every Nm` is a 2-field
   string, so any `len(fields) >= 5` guard silently drops the busiest watchdogs.
   `schedule.expr == ''` means `kind: "interval"`, not a parse failure.
6. **Before publishing any zero or rate: name the file the signature lives in, grep the
   literal string once and READ the line, then count.** A filter that skips the line
   carrying the signature (`continue` before the match test), a wrong token (`unclaimed` vs
   `never claimed`), or a `" WARNING "` filter with a non-existent leading space each
   return a clean zero forever. An exactly-uniform zero across unrelated fingerprints is a
   filter bug until proven otherwise.

## Three More

All with full entries in `references/measurement-pitfalls.md`:

- **`run_id` strings** — build once by explicit concatenation (`strftime('%Y%m%dT%H%M%SZ')`
  already emits the `Z`) and grep for `ZZ` before writing.
- **Blocked in cron** — `find -delete` and inline `python3 -c`, so express destructive reap
  as a script file.
- **A COUNT in a status bucket is not permanence** — snapshot row IDs, wait 60-90s, re-read,
  compare BOTH `pid` and `process_id`.

## Tool Quirks in Cron Context

`read_file` dedup, pipe-to-interpreter, `write_file` failures, `execute_code` denial,
heredoc `$(date)` expansion, the `hermes cron` CLI path mismatch, inline `python3 -c`
blocked, Python 3.14 `strptime` rejections:
`references/cron-tool-failure-handling-table.md`.
