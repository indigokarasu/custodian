# issues.jsonl Row Integrity + Gap-Check False Positives

Two failure modes that are both **silent**: nothing errors, nothing is logged, and
a scan reports a clean result that is wrong in opposite directions.

## 1. A row without `issue_id` is unreachable by every sanctioned tool

**Confirmed 2026-09-28.** `deep-scan-20260928T090000Z` appended its
`oc_git_sync_unreachable_remote_404` row without an `issue_id` or `id` key.
The row parsed fine — so no error surfaced — but **every** tool that reads or
mutates issues keys on `(issue_id or id)`:

| Script | Key expression |
|---|---|
| `parse_issues_jsonl.py` | `key = e.get('issue_id') or e.get('id')` |
| `race_safe_issue_patch.py` | `(o.get("issue_id") or o.get("id")) == args.issue_id` |
| `verify_escalation_state.py` | `iid = e.get("issue_id") or e.get("id")` |
| `reopen_false_resolutions.py` | `e.get("issue_id") or e.get("id")` |
| `confirm_provider_recovery.py` | `i.get("issue_id") or i.get("id", "")` |

So a dedup, a status flip, or a false-resolution reopen that *should* have hit
this row silently misses it. `race_safe_issue_patch.py --issue-id <anything>`
reports `NOT FOUND` against it, permanently.

**Rule:** every append to `issues.jsonl` sets `issue_id`. Audit structurally —
count rows missing it — because nothing else detects it:

```python
bad = [x for x in rows if not (x.get("issue_id") or x.get("id"))]
```

**Fixing it is NOT a `race_safe_issue_patch.py` job** — that tool selects its
target *by* `issue_id`, so it can never find a row that has none. Mirror its
safety pattern but select by `fingerprint`:

1. `shutil.copy2` the file to a `.bak.light-<ts>` sibling.
2. Read lines, rewrite ONLY the matching line in a minimal window.
3. Never brace-parse the whole file and `os.replace` it — the top-of-hour
   `custodian:light` sibling writes concurrently and can clobber unrelated rows.
4. Immediate re-read verify; up to 5 attempts with sleep.
5. **Refuse to run unless the fingerprint matches exactly 1 row.**
6. Re-validate with the *sanctioned* tool afterwards
   (`race_safe_issue_patch.py --issue-id <new-id> --set ... --require-status <s>`),
   not with the script that made the change.

## 2. Gap checks must match issue ids by PREFIX

**Confirmed 2026-09-28.** Step 8b extracts fingerprints from a prior journal with
`re.findall(r'oc_[a-z0-9_]+', text)`. Issue ids end in a **timestamp suffix with
an uppercase `Z`**: `oc_host_memory_pressure_swap_io_wait_20260927T2305Z`. The
character class stops at `Z`, so the extracted token truncates and is not in the
id set — and the check reports **three missing issues that all exist**.

A gap check that manufactures gaps is worse than no gap check: it trains the
reader to ignore it.

**Rule:** compare by prefix, and keep case-folding separate from matching:

```python
ids = {str(x.get("issue_id") or x.get("fingerprint") or "").lower() for x in rows}
present = any(i.startswith(tok) for i in ids)     # NOT `tok in ids`
```

A `False` from an exact-match gap check is a **claim to re-derive, not a
finding**. Re-check by prefix before writing anything to `issues.jsonl`.

## 3. `fingerprint` != `issue_id` — a SECOND false-gap, same check

**Confirmed 2026-09-28** (light scan `20260928T1004Z`). Applying §2's prefix rule
still reported a gap: journal `0606Z` named `oc_skill_md_char_limit_overflow`, and
no id *starts with* that token — because the row is

```
issue_id    = 'oc_engineering_manager_skillmd_char_limit_20260928T0606Z'
fingerprint = 'oc_skill_md_char_limit_overflow'
```

The two fields name the same defect in different vocabularies (subject-first vs
class-first). A journal quotes whichever its author wrote, and §2's rule matches
only `issue_id`. So prefix-matching `issue_id` alone still manufactures gaps.

**Rule:** build the match set from **both** fields and match against the union —
this is the form that actually closes the check:

```python
labels = set()
for e in rows:
    for fld in ("issue_id", "id", "fingerprint"):
        v = e.get(fld)
        if v:
            labels.add(str(v).lower())
present = any(l.startswith(tok.lower()) for l in labels)
```

Three false-gap variants now, one root cause: **a gap check is a claim, never a
finding, until re-derived by prefix against both id fields.** Record the token
that failed and the row you re-checked in the journal, so the next scan can tell
a manufactured gap from a real one without re-reading the whole file.

## 4. An `id`-only row is reachable — do not "repair" it

**Confirmed 2026-09-28.** 5 of 130 rows carry `id` and no `issue_id`. Every
sanctioned tool keys on `(issue_id or id)`, so these rows are fully reachable:
a dedup or status flip hits them normally. Treating an id-only row as the §1
defect would mean rewriting 5 healthy rows to satisfy a uniformity preference.

**Rule:** §1's audit must be *truthiness* — `not (e.get("issue_id") or e.get("id"))`
— never a key-presence test. A row with `id` set is healthy. Report the
id/issue_id naming split as info-only, and note the same 5 rows each scan so the
count is not re-derived from scratch.
