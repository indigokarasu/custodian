# Custodian Interactive Menu

When invoked interactively (via `/` command), present a two-level menu using the `clarify` tool so the user can pick which function to run.

## Level 1 — Category selection (max 4 choices)

```python
result = clarify(
    question="What would you like to do?",
    choices=[
        "Scan — light or deep system health scan",
        "Issues — list or auto-repair known issues",
        "Repair & Verify — generate repair plan, verify a fix",
        "Status — show system status",
    ]
)
```

## Level 2 — Action selection based on Level 1 choice

- **Scan** → clarify with choices: "scan.light — Light system health scan", "scan.deep — Deep system health verification"
- **Issues** → clarify with choices: "issues.list — List known issues", "repair.auto — Auto-repair known issues"
- **Repair & Verify** → clarify with choices: "repair.plan — Generate repair plan", "verify — Verify a specific fix"
- **Status** → run "status — Show system status" directly (single action — no sub-menu needed)

After the user selects an action, execute it following the relevant procedure in this skill. Loop back to the menu after each action completes, until the user chooses to exit or sends `/stop`.

## Response parsing

Match the user's response against the full choice string. Extract the action key by splitting on `" — "` and taking the first segment. If the response doesn't match any known choice (user typed free-form via "Other"), match key prefixes case-insensitively. Re-present the current menu level on no match.

## Platform adaptation

On CLI, choices are navigable with arrow keys. On messaging platforms, choices render as a numbered list. The two-level hierarchy ensures no more than 4 options appear at any level on any platform.