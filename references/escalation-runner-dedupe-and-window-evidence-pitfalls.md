# Two escalation-runner pitfalls (confirmed 2026-09-28)

Both were live in the same run. Each produced a *wrong verdict that looked clean*,
which is the worst failure shape for a monitoring loop.

## 1. Dedupe an append-only `issues.jsonl` by LAST ROW, not by worst status

**The store is append-only.** A change to an issue is a NEW row with the same
`issue_id`, not a mutation of the old one. Dedupe must therefore keep the **last**
row per `issue_id`.

Worst-status-wins (`open` > `resolved`) is wrong for an append-only log: it treats a
historical `open` row as current state and reports an issue as open after it was
resolved. Measured on 2026-09-28: worst-status reported **41 open** and listed
`oc_auxiliary_paid_lane_free_only_disabled_20260927T021254Z` as open; last-row-wins
reported **40** with that issue `resolved @ 22:10:09Z`. The extra row was the *real*
defect in my probe, not in the store.

**Rule**
```python
seq = {}
for o in rows:                      # file order == append order
    seq[o.get("issue_id") or o.get("id") or ""] = o   # last wins
```
Worst-status dedupe is correct only for a store that mutates rows in place
(`custodian_issues` tool output, a query result). Pick the rule by the store's write
semantics, not by convenience.

**Tell:** the two rules disagree and the file is append-only. One of them is
manufacturing an open issue that does not exist.

## 2. A window with no *real* activity cannot close a behavioural question

The 22:06Z cycle left this question open: *"full closure needs a real auxiliary task
to run with zero new 'PAID lane engaged' warnings."* The 22:15Z cycle found **0**
`PAID lane engaged` lines in the window and could have closed it.

It did not, because the window also contained **zero real auxiliary model calls**.
The only `openrouter` lines present were plugin *provider registration*
(`Plugin 'openrouter' registered image_gen provider: nous`) — emitted at every
gateway start regardless of whether any model is ever routed.

Counting registration as "the subsystem ran" makes the clean **vacuous**: the test
passes because nothing happened, not because the fix works. That is how an issue gets
closed twice on evidence that never exercised the fix.

**Rule — separate the two questions before closing anything:**
- *Was the guard consulted?* Needs a **substantive** event: the subsystem under test
  actually attempting the guarded operation.
- *Did the bad outcome recur?* Absence of the warning.

Only the second can be answered by a quiet window. If the first is false, the honest
grade is `config_verified_behaviour_unexercised` — record the config-level proof, keep
the residual question, and say plainly that the gate was never observed firing.

**Tells that a window is vacuous, not clean:**
- the only matching lines are startup/registration chatter (`registered ... provider`)
- the subsystem's own log lines have **no success-path entries at all** — a subsystem
  that only ever logs registration has not been exercised
- the pattern also matches a shorter, unrelated string (`openrouter` ⊃
  `openrouter/free`) — match the *signature*, not the provider name

## 3. Bonus: classifying log lines by keyword produces a fake success rate

A first pass classified 157 `context_engine` lines as `INIT_OK` because they matched
`registered`. All 157 were `tried to register command '/chronicle' which is already
registered. Skipping.` — a *failure* to register, carrying the word "registered".

The authoritative signal for whether the engine works is its own mode field:

```
chronicle_context_status: mode=memory_aware      56
chronicle_context_status: mode=heuristic_fallback 49
```

**Rule:** when a component emits an explicit `mode=` / `status=` / `state=` field, use
it as ground truth and never infer health from a keyword. A keyword classifier will
happily report a 61.6% success rate for a subsystem that is failing every time.
