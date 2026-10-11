# custodian_scan Tool — Verified Broken (2026-10-04)

Read this **before trusting any `custodian_scan` output as a census.**

Issue: `oc_custodian_scan_tool_returns_light_and_misattributes_identical_evidence_20261004T0301Z`.

## What it did wrong (measured 2026-10-04)

- Called with `mode: deep`, it returned `"mode": "light"`.
- Charged ONE traceback to FOUR distinct `fingerprint_id`s with byte-identical evidence.
- Included entries dated two days before the run.
- Wrote its journal to `/root/.hermes/commons/...` instead of the profile commons root.

## Rule

Treat `custodian_scan` output as **unverified** until BOTH hold:

1. `returned_mode == requested_mode`.
2. No two entries share identical evidence across distinct fingerprint ids.

Until then, do **not** use its counts as a census. Instead run
`scripts/custodian_sweep.py` (self-test first) plus direct registry probes.

Related: `references/scan-surface-is-a-plugin-tool.md` (a scan reporting
`custodian.scan.light` is "not a hermes command"), and the sweep false-clean class in
`references/measurement-pitfalls.md`.
