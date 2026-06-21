# Web Search Protocol

Fire when: fingerprint unknown, recurrence increased since last search, last search not actionable, not escalated/suppressed, < 5 attempts.

Query mutation sequence: (1) `{error} agent skill`, (2) `{error} {tech_context}`, (3) `{error_pattern} fix`, (4) `{component} {failure_mode}`, (5) `{failure_mode} root cause diagnosis`.

On actionable result: attempt fix, append to `learned_issues.jsonl` if successful. On no result: record and continue mutation on next recurrence.

**Confidence integration:** After a successful web search fix, create a new entry in `fix_effectiveness.jsonl` with `attempts: 1, successes: 1`. This bootstraps the confidence model for future occurrences.
