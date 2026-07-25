# Provider 401 Diagnosis — Distinguishing Transient vs. Auth Failure

## Overview

When cron jobs fail with HTTP 401, the root cause depends on **which provider** is failing. This reference provides a systematic approach to diagnosing 401 errors.

## Step 1: Identify the failing provider

```bash
journalctl --user -u hermes-gateway --since "YYYY-MM-DD" --no-pager 2>/dev/null | grep "401" | grep -oP 'provider=\K[^ ]+' | sort | uniq -c
```

This shows which providers are generating 401s and how many times.

## Step 2: Extract the base_url for each 401

```bash
journalctl --user -u hermes-gateway --since "YYYY-MM-DD" --no-pager 2>/dev/null | grep "401" | grep -oP 'base_url=\K[^ ]+' | sort | uniq -c
```

Common `base_url` values:
- `https://openrouter.ai/api/v1` — OpenRouter (usually transient)
- `https://inference-api.nousresearch.com/v1` — Nous Portal (check `hermes auth`)
- `https://app.manifest.build/v1/` — Manifest.build custom provider (check API key)
- `https://api.anthropic.com/v1` — Anthropic (check ANTHROPIC_API_KEY)

## Step 3: Check if the failure is provider-wide or job-specific

If only some jobs using a provider are failing → job-specific issue.
If ALL jobs using a provider are failing → provider-wide auth issue.

## Step 4: Cross-reference with task list

Before creating a new issue, check `<hermes-home>/commons/data/ocas-finch/task-list.json` for existing open tasks with the same root cause. Consolidate rather than duplicate.

## Key Insight (2026-06-04)

Multiple providers can coexist in the same Hermes instance. The primary provider (OpenRouter) can be healthy while a secondary custom provider (manifest.build) has auth failures. Always check `base_url` before concluding "it's transient."
