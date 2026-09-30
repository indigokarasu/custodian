# Disk threshold: `df` Use% rounds UP (measured 2026-09-30)

**Fingerprint:** `oc_disk_threshold_df_ceiling_false_positive`

## The measurement

On this host at 2026-09-30 21:25–21:43Z:

```
statvfs("/")  -> total 95.848 GiB, used 75.839 GiB, avail 19.993 GiB
used/(used+avail) = 79.1377%
used/total       = 79.1248%
df -h / Use% column = "80%"
statvfs vs df -B1 (used, avail) delta = 0 bytes
```

`df` **ceils** the percentage so a filesystem never displays less than its true
occupancy. Byte accounting is exact; only the integer column is biased, by up to
1 point.

## Why it bites custodian specifically

Both disk triggers read that column:

- the 80% compaction trigger in `disk-compaction.md` ("`df -h /` shows Use% > 80%")
- the `oc_state_db_oversized` dual threshold (`db>1GB` AND `disk>80%`)

## The false escalation it nearly produced

`oc_disk_chronicle_db_unprunable_growth_20260930T090400Z__interim_20260930T202135Z`
recorded `disk_pct: "80% -> 79%"` after a state.db VACUUM reclaimed 67 MB.
66 minutes later the column read `80%` again. Read verbatim that is
"the reclaim failed and disk is climbing back" — but exact usage had **never**
crossed 80%, was 79.14%, and had **4.13 GiB** of headroom. Acting on it would
also have re-run a VACUUM for 576 freelist pages (2.2 MB, 3% of the original
yield) and re-taken the WAL lock, which the deep scan measured blocking live
cron turns.

## Rule

**Never threshold on the `df` Use% column.** Use an exact ratio:

```python
s = os.statvfs("/")
used = s.f_blocks * s.f_frsize - s.f_bfree * s.f_frsize
avail = s.f_bavail * s.f_frsize
pct = 100.0 * used / (used + avail)     # df's own denominator, without ceiling
```

Bytes to reach exactly 80% is `4 * avail - used` (solve `used/(used+avail)=0.8`),
which gives the headroom figure to report instead of a bare percentage.

## Symptoms that this is the cause

- a percentage "crossed" a threshold and then un-crossed after a reclaim that freed real bytes
- two sources disagree: `df -h` says 80%, hand-computed `used/total` says 79.x
- an escalation about disk "climbing back" with no corresponding growth in any named file

## Related

`oc_disk_chronicle_db_unprunable_growth_20260930T090400Z` (the actual disk root
cause, Tier 3 owner-gated: chronicle.db is all live rows, freelist ~270 pages).
This defect is about how that pressure is *measured*, and is independent of it.