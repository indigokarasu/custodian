# Disk "growth": a bounded sawtooth mistaken for a runaway slope (measured 2026-10-01)

**Fingerprint:** `oc_disk_growth_rate_rationale_falsified`

Companion to `references/disk-threshold-df-ceiling-false-positive.md`. That
document fixes how the *level* is measured (the `df` ceiling). This one is the
upstream error: you measure the level perfectly and still manufacture a runaway
trend out of a flat band, because a **short window on a cyclic signal looks like
a slope.**

## The measurement

Nine exact `statvfs` readings on 2026-10-01, each already pinned into a
custodian journal:

```
00:11 79.550   02:01 80.256   02:02 80.269   04:04 81.229   05:11 81.586
06:14 81.545   06:50 80.856   12:19 81.169   12:39 81.283
```

Spread 2.036 pp, **two sign reversals**. That is an oscillation held in a
79.55–81.59% band, not a climb.

## How the false escalation was built

The 12:19Z journal escalated `oc_disk_deficit_growth_accelerating_20261001T1215Z`
("deficit deepening at a rate no Tier-1 lever can match"). Its evidence was a
**3-point slice**: 02:02Z 80.269 -> 04:05Z 81.229 -> 08:15Z 80.494 -> 12:15Z
81.169. Two of those four points sit on the same sawtooth cycle, and 08:15Z was
itself a post-prune **trough** (the 08:15Z cache purge moved 81.267 -> 80.494).
The 08:15 -> 12:15 comparison therefore measured *reclaim backfill*, and read it
as slope.

The decisive control was arithmetic, not another reading: **04:04:16Z read
81.229%**; 8.6 hours later the ratio was **81.283%**, a **+0.054 pp** difference.
At 8–11 GiB/day on a 95.85 GiB filesystem, 8.6h would move `used` by roughly
2.8–4 GiB and the ratio by +3 to +5 pp. The asserted growth rate and the
filesystem's own record cannot both be true. **The growth figures were the
artifact; the disk was fine.**

This matters because four open issues cited those rates as their rationale:
`oc_disk_deficit_growth_not_fully_attributed_20261001T0405Z`,
`oc_disk_over_80pct_residual_after_tier1_prune_20261001T0202Z`,
`oc_disk_over_80pct_tier1_cache_reclaim_20261001T081521Z`, and
`oc_chrome_browsermetrics_pma_spool_unbounded_growth_20261001T0450Z`
("7.03/6.89/5.62 GiB/day"; "8.46 GiB/day total").

## Rule

**Build the series from every journal that recorded a reading, then test shape
before slope.** Never assert a trend from a 2–4 point slice, and never let a
Tier-1 reclaim inside the window act as a data point you then measure *against* —
a reclaim trough is the phase of the cycle, not its zero.

Checks, in order:

1. **Sign reversals.** Two or more down-moves in the series ⇒ oscillation. A
   monotone climb has none.
2. **Control pair far apart in time.** Take a reading ≥6h old and compare to now.
   If the delta is ~0 while a high rate is claimed, the rate is false — do not
   reconcile the two.
3. **Arithmetic feasibility.** Claimed GiB/day × elapsed_hours ÷ filesystem size
   → expected pp change. Compare to observed pp change. Incompatible ⇒ the claim
   is the artifact.
4. **Partial-hour discipline** applies to trend claims exactly as it does to the
   `unclaimed` rate series: an hour only enters a baseline when complete.

## When the level IS genuinely over threshold

Withdraw the *rationale*, not the *condition*. Here the exact ratio was above 80%
and headroom to the true line was −6.1 GiB, so every issue whose core premise is
"disk sits over 80%" stayed open. The correction was written as a single
class-level row (`oc_disk_growth_rate_rationale_falsified_20261001T1242Z`)
listing the four affected issues, rather than four separate resolutions —
closing an issue whose premise merely changed would be its own false move.

## Tier-1 yield is itself the finding

Tier 1 levers were exercised to test the claim: gzip on 28 logs older than 7 days
plus compression of 5,794 `cron/output` files older than 7 days. **Net yield:
0.5 MiB.** Log and output reclamation cannot move this band. The consequence is
that repeating them as a remedy is not "insufficient" — it is futile, and
repeating it manufactures the appearance of effort. Real consumers were
`/root/.hermes` 21G, `/root/backups` 5.3G (chronicle.db 3.2G + state.db 759M),
`/root/indigo-repo` 2.1G, `/root/projects` 2.0G, `/root/.cache` 2.0G.

`state.db` was 0.752 GiB — under 1 GiB, so `oc_state_db_oversized` does not fire
on *either* prong of its dual threshold, and a VACUUM for 2.2 MB of freelist is
Tier-2 work with no upside.

## Re-escalation rule

Treat disk as growing again only on **two consecutive readings that both rise
AND land above the observed peak** (81.6% on 2026-10-01). A single rising
reading inside the band is sawtooth phase. Without this rule, the next scan
compares trough-to-peak, reinflates a rate, and re-escalates.

## Symptoms that this is the cause

- an escalation whose evidence is a handful of readings from one day
- growth cited in GiB/day that no independent pair of readings confirms
- "accelerating" or "climbing" while a reclaim ran inside the comparison window
- a rate that survives no check against the filesystem's own history