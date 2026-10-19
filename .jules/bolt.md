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

## 2026-10-28 - Pre-Grouping Classification Lookups in Reconciliation Loops
**Learning:** In reconciliation loops (`escalation_exec_pause_reconcile.py`), classifying each paused job repeatedly inside per-issue evaluation loops creates O(recs * actual_paused) string matching cycles. Pre-classifying and grouping paused jobs by bucket before entering the issue loop reduces overhead to O(actual_paused + recs) (~51x speedup).
**Action:** Pre-classify items into dictionary buckets prior to iterating over container objects that query item classifications.

## 2026-11-12 - Unified Single-Pass Regexes for Heuristic Classifiers
**Learning:** In `classify_llm_necessity.py`, evaluating list-of-regex patterns (`any(p.search(text) for p in patterns)`) incurs Python loop overhead across every evaluated job prompt. Combining sub-pattern string lists into unified single-pass regexes (`re.compile("|".join(patterns))`) delegates alternation directly to the C regex engine and speeds up classification matching by ~4x (~21.5 μs vs ~82.6 μs per verb set check).
**Action:** Combine list-based regex patterns into single `|`-delimited compiled regexes when performing boolean hit checks in classifier loops.

## 2026-11-20 - Pre-Computing Fingerprints and Filter Sets in Status Probes
**Learning:** In status probe scripts (`confirm_provider_recovery.py`), evaluating `fp_of(j.get("last_error"))` and date checks inside nested per-issue `affected_job_ids` loops creates redundant string matching and property accesses. Pre-computing `enabled_err_fps` dict and `today_ok_ids` set once upfront reduces execution time by ~30% (1.42x speedup).
**Action:** Pre-compute fingerprint mappings and status filter ID sets upfront before iterating through container objects in probe scripts.

## 2026-12-04 - Single-Pass Unified Regexes for Secret Audit Scans
**Learning:** In `secret_audit.py`, iterating through 14 separate prefix regex patterns for every line of scanned files in `scan_file()` incurs Python loop overhead. Combining prefix sub-patterns into a single named-capture-group regex (`(?P<g_0>...)|(?P<g_1>...)`) delegates alternation directly to the C regex engine, speeding up prefix secret scanning by ~23% (3.57s -> 2.76s for 50,000 lines).
**Action:** Combine list-of-regex patterns into indexed named-group unified regexes (`(?P<g_i>pattern)`) when scanning lines for token classification in audit scripts.

## 2026-12-18 - Single-Pass Classification & Top-Level Helpers in Probe Loops
**Learning:** Re-evaluating classification functions across multiple filtering/reporting loops (e.g. in `bucket_error_jobs.py` and `classify_error_jobs.py`) creates redundant O(N) string matching passes. Pre-classifying items once into dictionary buckets or tuples `(item, classification)` eliminates redundant passes (~1.3x-1.7x speedup). Additionally, replacing inline lambda closures inside generator expressions (e.g. in `verify_escalation_state.py`) with top-level helper functions avoids repeatedly generating closure objects (~1.5x speedup).
**Action:** Pre-classify collections into tuples/buckets before downstream filtering loops, and extract repeated boolean predicate expressions into top-level helper functions instead of inline lambdas.

## 2027-01-08 - Avoiding JSON Serialization for Substring Phrase Matching in Probe Loops
**Learning:** In journal gap scanning (`scan_escalation_journal_gaps.py`), calling `json.dumps(d).lower()` to check for recovery phrases across thousands of journal records incurs C/Python JSON re-encoding overhead. Replacing `json.dumps(d).lower()` with `str(d).lower()` yields a ~2.5x speedup (from ~1.52s down to ~0.60s per 1000 records).
**Action:** Use `str(d).lower()` instead of `json.dumps(d).lower()` when performing boolean substring/phrase checks on parsed dictionary objects in high-throughput probe loops.

## 2027-01-22 - Lazy Journal Window Fetching in Gateway Health Check
**Learning:** In `gateway_health_check.py`, fetching 10-min (`DISCONNECT_LOOP_WINDOW_MIN`) and 30-min (`SILENT_DEATH_MINUTES`) journal window outputs upfront via `journalctl` subprocess calls creates redundant I/O and process creation overhead when early health checks (e.g. inactive or failed service state) trigger an early return. Deferring window fetches lazily until needed reduces subprocess calls by up to ~66% on early return paths.
**Action:** Fetch expensive journal window ranges lazily on-demand only when earlier status or token health checks pass and require those log windows.

## 2027-02-05 - Unified Pre-Compiled Regex Matching in Escalation Probes
**Learning:** In `find_missed_user_gated_jobs.py`, checking `sub in low` inside nested Python loops for each job's `last_error` incurred Python loop iteration overhead. Pre-compiling sub-patterns into unified `|`-delimited `re.compile(..., re.IGNORECASE)` objects delegates matching directly to the C regex engine, speeding up error classification matching.
**Action:** Pre-compile multi-keyword string pattern sets into unified `re.compile("|".join(...))` regexes at module load in classification and escalation probes.

## 2027-02-19 - Substring Pre-filtering for Line-Based JSON Scanners
**Learning:** In `race_safe_issue_patch.py`, calling `parse_line()` (`json.loads()`) for every line in `issues.jsonl` incurs heavy Python JSON decoding overhead across thousands of lines. Adding a fast C-level substring pre-check and pre-computing loop-invariant status flags outside the line loop yields a real speedup when patching target issues in large JSONL stores.
**Action:** In line-based JSON mutation/patching scripts, use a fast substring check before invoking `json.loads()`, and lift loop-invariant flag calculations outside per-line iteration loops. **The substring must be a SUPERSET of what the parse-based match accepts**: test both the literal value and its `json.dumps(v)[1:-1]` escaped form, because an identifier containing a quote, backslash, or non-ASCII character is written escaped on disk (`\u00e9`) and a literal `value in line` test silently skips the matching row.

## 2027-03-05 - Concurrent Network Probing in Status Verification Scripts
**Learning:** In network status probes (`verify_all_provider_credentials.py`), testing multiple distinct credentials or endpoints serially incurs high cumulative network latency $O(\sum t_i)$. Utilizing `concurrent.futures.ThreadPoolExecutor` to probe endpoints concurrently reduces execution latency to $O(\max t_i)$ (~3x-10x speedup across multiple provider endpoints) while preserving result ordering.
**Action:** Parallelize independent network status probes using `ThreadPoolExecutor` and lazily import `concurrent.futures` inside execution functions to avoid top-level import overhead during CLI evaluations like `--help`.

## 2027-03-19 - Pre-computing Lowercased Marker Tuples and Keyword Sets in Line Scanners
**Learning:** In `secret_audit.py`, calling `m.lower()` on tuple elements inside a generator expression for every scanned line incurred repeated string lowercasing overhead (`any(m.lower() in low for m in REDACTED_MARKERS)`). Pre-computing `_LOWER_REDACTED_MARKERS` and static keyword sets (`_EXCLUDED_ENTROPY_WORDS`) at module import yields a ~1.46x speedup on line-level redaction checks across large file scans.
**Action:** Pre-compute lowercased tuples (`tuple(m.lower() for m in CONSTANTS)`) and keyword sets at module load instead of evaluating `.lower()` on list/tuple elements inside per-line generator expressions.

## 2027-04-02 - Predicate Order Optimization in Journal Record Scanners
**Learning:** In `scan_escalation_journal_gaps.py`, evaluating `get_ts(d)` before checking `d.get("escalation_needed")` incurred ISO 8601 string parsing and dict key inspection across every record. Reordering evaluation to check cheap boolean/status predicates first short-circuits ~95% of non-escalation records before timestamp parsing, yielding a ~3.6x speedup (0.827s -> 0.226s per 50,000 records).
**Action:** Order filter conditions in record scanning loops so cheap boolean flags or type guards run before expensive property parsing or ISO 8601 timestamp conversion functions.

## 2027-04-16 - Set Lookups and Counter Aggregation in Integration Probes
**Learning:** In `classify_llm_necessity_integration.py`, checking `name not in candidate_names` (where `candidate_names` was a list) performed $O(A \times C)$ linear searches per acknowledged item, and evaluating `sum(1 for v in ack.values() if v == ...)` three times created redundant dictionary iterations. Converting candidate names to a static set (`candidate_names_set`) reduced lookup time by ~90x (~88.8s -> ~0.98s for 10,000 checks), and replacing multiple generator scans with `Counter(ack.values())` yielded a ~1.32x speedup.
**Action:** Convert candidate lists to `set` objects before performing element membership checks in loops, and use `collections.Counter` for single-pass state frequency counting.
