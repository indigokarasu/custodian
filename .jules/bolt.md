## 2026-07-16 - Fast ISO 8601 Timestamp Parsing in Journal Probes
**Learning:** `datetime.fromisoformat` in Python 3.11+ parses ISO 8601 timestamps natively in C (~47x faster than trial-and-error `datetime.strptime` exception loops) across thousands of journal files.
**Action:** Use `datetime.fromisoformat(v)` as the primary timestamp parser in journal/log scanner probes before falling back to format-specific `strptime` loops.

## 2026-07-16 - Canonical JSON Stream Parsing & Regex Pre-compilation in Custodian
**Learning:** `custodian_common.parse_issues` uses C-optimized `json.JSONDecoder().raw_decode()` and is ~9x faster than custom character-by-character scanner loops while safely handling string fields containing braces. Additionally, pre-compiling regexes and extracting signals in a single pass avoids redundant evaluation cycles in classifier probes.
**Action:** Always import `parse_issues` from `custodian_common` when reading `issues.jsonl` files instead of rolling custom char-by-char JSON scanners, and pre-compile regex patterns at module load in probe scripts.

## 2026-09-27 - Single-Pass Log Scanning with Lazy Timestamp Extraction
**Learning:** In log scanning probes (`verify_plugin_defect_postrestart.py`), combining two file read passes into a single pass with buffered signature hits, pre-compiled regex patterns, and lazy regex timestamp evaluation yields a ~40% speedup on large gateway logs (~100k lines).
**Action:** Single-pass log scanners should buffer hit tuples `(key, timestamp)` during file read and defer timestamp regex matching (`TS_RE`) until a restart marker or pattern hit is encountered.

## 2026-10-15 - Single JSON Pre-loading in Iterative Reconciliation Scanners
**Learning:** In probe scripts like `reopen_false_resolutions.py`, re-parsing `jobs.json` inside per-issue evaluation loops creates O(N) redundant disk I/O and JSON parsing overhead while risking unclosed file descriptors.
**Action:** Pre-load reference JSON files once in `main()` before looping over issue entries and pass the loaded data structure to helper functions, while using context managers (`with open(...)`) for safe resource handling.
