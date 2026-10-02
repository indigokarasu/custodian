# A whole-file decode probe reports 0 non-UTF-8 files when the tree holds compressed ones

Measured 2026-10-01 during a light scan chasing
`UnicodeDecodeError: 'utf-8' codec can't decode byte 0x8b in position 1`.

## The wrong probe

```python
for fp in walk(root):
    try:
        open(fp, "rb").read()          # reads BYTES -> no decode ever happens
    except UnicodeDecodeError as e: ...
```

Reading in binary mode raises `UnicodeDecodeError` **never**. A variant using
`open(fp, encoding="utf-8").read()` over the same tree reported
`scanned 19562 files / non-utf8: 0` — a clean bill of health that directly
contradicted a `UnicodeDecodeError` in the log set minutes earlier.

## The tell

**A probe whose result contradicts the log is a probe bug until proven
otherwise.** Do not go looking for the system defect to make the probe
"correct". Cross-check the probe against an independent signature before
believing either side.

## The right probe — magic bytes

`0x8b` at position 1 is gzip: the magic is `1f 8b`.

```python
with open(fp, "rb") as fh:
    head = fh.read(2)
if head == b"\x1f\x8b": ...
```

Same pass over the same 19,562 files: **17,824 gzip files, 66.06 MiB**. One probe,
same denominator, opposite verdict. Add the other containers you expect
(`PK\x03\x04`, `\x1f\x8b`, `BZh`, `\xfd7zXZ`) — the general rule is *match the
container header*, not *try to decode and count failures*.

## Root cause that produced it

`plugins/chronicle/engine/curation.py::_task_journal_ingest` read every file
under `sources.ocas_journals.paths` with `encoding="utf-8"` and caught only
`OSError`. `UnicodeError` is a `ValueError`, not an `OSError`, so the guard
cannot see it; the handler raised, `run_once`'s `handler(payload)` loop aborted,
and **every remaining task in that curation run was lost** — not just ingest.

Minimal fix (Tier 4 / user-gated — plugin memory-engine files are outside the
autonomous envelope per `references/fix-safety.md`): widen the existing except
tuple to `except (OSError, UnicodeError):`. Decompressing the archives instead
was rejected: it turns a storage-compression decision into memory content and
makes 17,824 archives first-class inputs.

## Scan checklist addition

When a `UnicodeDecodeError` names a file you believe is plain text, run the
magic-byte probe over the source tree **before** classifying it as corruption.
Compressed archives in a text-scanned tree are a self-inflicted failure wearing
a corrupt-data costume.