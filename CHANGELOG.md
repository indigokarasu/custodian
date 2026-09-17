# Changelog

All notable changes to ocas-custodian are recorded in this file.

## [3.1.0] - 2026-09-16

### Changed
- **Two-stage self-healing contract** — enforced decision-log → repair → re-validation gate before any fix is marked resolved (`spec-ocas-recovery.md`).
- **Recovery log compaction** — recovery/evidence logs compacted when exceeding 1,000 entries (retain per-day rollups, drop stale per-run entries).