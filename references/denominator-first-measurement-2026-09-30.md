# Denominator-first measurement discipline (2026-09-30)

Two measurement artifacts appeared in a single light scan, both capable of
manufacturing an escalation out of nothing. Both are recorded here because the
existing guards in `execution-loops.md` did not catch them.

## 1. Signature normalization silently converts a LOG-LINE count into an EVENT count

Normalizing a log line for grouping (digits -> `N`, ids -> `HEX`) also
normalizes the **timestamp**, so every event for one job collapses into a
single bucket. Printing that bucket's *line count* then reads as though one job
produced 38 lost occurrences.

Measured 2026-09-29 20:30 PDT: grouped output showed `finch:work` as the top
offender at 38. Direct extraction gave **9 distinct occurrence instants across
3 days, 1 in the window**. 38 was log lines across 3 files for 9 events.

**Rule:** never derive an event count from a normalized-signature bucket.
Count on `(job_id, occurrence_instant)` first, group for display second. The
tells: a repeat ratio that is not ~1.0x, or a top-job number that exceeds the
total number of distinct events in the window.

## 2. A rate comparison needs a denominator built from hours that EXIST

A per-job rate is only as good as the baseline. When the log-file set contains
only 5 populated hours, the mean over those 5 collapses and every later hour
reads as a multiple of it.

Measured 2026-09-29: pass 1 on a 3-file set produced 5 populated hours
(09,10,17,18,19), mean 0.450 evt/job, and reported the current hour as
"VARIABLE, 2.28x the mean". Pass 2 on the full pinned set with all 12 populated
full hours gave mean 1.085 and put the current hour at **0.97x, below mean**.
The first pass would have filed a phantom saturation escalation, and the deep
scan 7 minutes earlier had already concluded the class is flat.

**Rule (extends the `hours that actually exist` note in execution-loops
Step 7b):** before printing any mean, print the populated-hour list and the
per-hour series alongside it. A ratio above 1.0 against a mean built from fewer
than ~8 populated hours is not a finding. Re-measure before writing.

## 3. Ordering rule that actually held

Both errors were caught **before** anything was written to the journal. The
journal is what downstream runs treat as a premise, so a plausible wrong number
written early costs more than an hour of extra measurement. Order is:
measure -> print the denominator -> re-measure if the ratio is surprising ->
write.

Related: `references/log-signature-msfield-false-positive.md` (ms-field false
positives), `references/cron-tool-failure-handling-table.md` (log-set pinning).
All four are the same class: the artifact lives in the measurement, not the
system.
