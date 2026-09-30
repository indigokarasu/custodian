# Log-signature false positives from the millisecond field

**Class:** silent false escalation · **First isolated:** 2026-09-27 · **Rule landed:** 2026-09-29
**Issue:** `oc_log_signature_msfield_false_positive_20260928T0009Z` (tier 3, confidence 0.95)
**Proposal:** `insight-20260928-fingerprint-msfield-fix` (tier 3, `status: open` → resolved by this fix)

## The defect

Every Hermes log line is stamped:

```
2026-09-21 05:21:20,429 INFO gateway.run: Restart deferred: waiting on 11 active work unit(s) …
```

The **millisecond field is a bare, unmarked number.** A fingerprint matcher that greps the raw
line for a provider status code therefore cannot distinguish:

- `HTTP 503 from upstream` — a real outage
- `this INFO line happened at .503 seconds` — nothing

The three tokens that matter (`503`, `402`, `429`) are all legal millisecond values, and all
three are provider fingerprints. A `402` match in particular escalates as a **user-gated
credits exhaustion**, which burns operator attention on an outage that never happened.

## Measured on this host (2026-09-29)

| Measure | Value |
|---|---|
| Log files scanned | 147 (both `/root/.hermes/logs/` and `/root/.hermes/profiles/indigo/logs/`) |
| Total lines | ~1.71 M |
| Naive raw-line substring matches for 503/402/429 | 8,742 (503: 2,367 · 402: 3,074 · 429: 3,301) |
| Matches that exist **only** because of the ms field | **1,806** (503: 928 · 402: 446 · 429: 432) — **20.7% of all naive matches** |
| After stripping the prefix | 6,936 |
| Lines with an ms field of exactly 503/402/429 that carry a REAL status reference in the body | **1** (of 1,807 sampled) |
| Prior scan's raw-line count (2026-09-27, 47-min window) | 503: 10, 402: 2, 429: 5 |
| Same window after stripping the prefix | **503: 0, 402: 0, 429: 0** |

Logger prefixes on those lines — i.e. what was actually being matched:

```
656  INFO hermes_cli.web_server
220  INFO hermes_cli.plugins
180  INFO gateway.run
 94  INFO hermes_plugins.photon_platform.adapter
 62  WARNING cron.jobs
 51  INFO __main__
 45  INFO cron.scheduler
 43  INFO hermes_plugins.telegram_platform.adapter
```

Not one is a provider error. The single genuine hit was an `errors.log.2` line carrying a real
`InternalServerError` with `provider=custom`.

## The rule

Before **any numeric** status match, strip the leading prefix and match the remainder:

```python
import re
PREFIX = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}\s')
body = PREFIX.sub('', line)          # also strip bracketed '[,NNN]' ms fields some loggers emit
if re.search(r'\b503\b', body):      # now the match means something
    ...
```

Non-numeric patterns — `never claimed`, `database is locked`, `context window of 2048` — are
unaffected, so this is safe to apply unconditionally.

## Symptom check

A provider fingerprint appears on lines that are **all `INFO`/`WARNING` with no status word**
(`status`, `HTTP`, `upstream`, `provider`, `credits`, `rate limit`). Suspect this class before
investigating a real outage.

## Why this scales

The false-positive count is a direct function of log volume, so **the busiest hours are the ones
most likely to fabricate an outage escalation** — precisely when a scan is under the most
pressure to report something. Same structural class as the `errors.log`-mirrors-`agent.log` 2×
rule and the traceback-continuation collapse rule; all three are dedup rules that must be applied
before a count becomes a finding.

## Boundary note

Custodian's responsibility boundary forbids it from modifying files inside a skill package —
this fix belongs to Forge/Mentor, and was executed by the escalation execution loop (the
designated Forge/Mentor actor for code-level InsightProposals).
