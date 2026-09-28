# Journal Timestamp Integrity (the UTC-vs-local pitfall, JOURNAL side)

Companion to the log-side pitfall in `execution-loops.md` (log lines are LOCAL). This file covers the
**journal's own timestamps**, which are the baseline the next scan trusts. Confirmed 2026-09-27.

## Derive the baseline from file mtime — never from a self-reported field

A successor scan needs "when was the last scan". Two fields claim to answer that: `run_id` (in the
filename) and `finished_utc` / `timestamp` (in the body). **Both are unreliable.** Use the journal
file's `mtime` (`os.path.getmtime`), which is written by the filesystem and cannot drift from the
act of writing.

```python
latest = max(glob.glob(f"{JD}/*/*.json"), key=os.path.getmtime)   # NOT sorted by run_id
cutoff_local = datetime.fromtimestamp(os.path.getmtime(latest), PDT)
```

## Two measured defects (2026-09-27, 493 journals audited)

**1. `run_id` embeds LOCAL time under a `Z` suffix — 24/493.**
Discriminate empirically, never assume:

| reading of `run_id` | median offset vs mtime | within 1 min |
|---|---|---|
| as UTC | 0.0 min | 394/493 (80%) |
| as local | 420 min | 3/493 (1%) |

The UTC convention is correct; the 24 are exceptions. Most recent: `custodian_light_20260927T113500Z`
(written 18:11:46Z, run_id implies 18:35Z). 5 have occurred since 2026-09-26.

**2. `finished_utc` later than the file's own mtime — 14 written in place** (up to +23 min recently,
+1433 min historically).

## The backfill trap — do NOT report the raw count

A raw `finished_utc > mtime` count gives **72/488** and overstates the real defect **5x**. A journal
copied or backfilled into place has an mtime that is not the writer's clock. Exclude a skewed journal
when either holds:

- its `mtime` sits far below that day's median mtime (the day's write frontier), or
- its `run_id`'s date differs from its directory's date.

After filtering: **14 real, 9 backfill artifacts**. Always apply the filter before publishing a number.

## Direction of harm — check before escalating severity

- `run_id` read as UTC when it is local → cutoff **~7h too old** → over-scans. Harmless.
- `finished_utc` read as the baseline when it is post-mtime → skips the gap. Harmful.
- Ordering journals by `run_id` instead of `mtime` picked a journal **47 min stale** on 2026-09-27 —
  the over-inclusive direction, so benign, but the ordering is still unreliable.

The dangerous direction (cutoff too NEW, silently skipping a window) is **not** reachable from these
instances, because the mislabel shifts the embedded time *earlier* than the true write. State this
explicitly when escalating: the finding is real but the observed impact is a stale baseline, not a
missed incident. Do not let "found a bug" inflate to "caused an outage".

## Why no Tier 1 fix

The writer is the journal-producing agent prompt, not a script in the skill package, and the skill
contract forbids modifying files inside a skill package. Record + escalate; the durable fix is the
writer reading one clock value (`datetime.now(timezone.utc)`) for both `run_id` and `finished_utc`.

## Related

- `execution-loops.md` — the log-side UTC-vs-local pitfall (same class, different artifact)
- `jobs-json-timestamp-offset-misread-pitfall.md` — the registry-side instance
