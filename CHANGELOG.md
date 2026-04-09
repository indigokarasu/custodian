## [1.2.2] - 2026-04-06

### Added
- OKRs section in SKILL.md with formal OKR definitions for skill evaluation
- YAML-formatted skill_okrs with metrics (fix_success_rate, skill_init_coverage, scan_detection_accuracy) and evaluation windows

## [2026-04-04] Spec Compliance Update

### Changes
- Added missing SKILL.md sections per ocas-skill-authoring-rules.md
- Updated skill.json with required metadata fields
- Ensured all storage layouts and journal paths are properly declared
- Aligned ontology and background task declarations with spec-ocas-ontology.md

### Validation
- ✓ All required SKILL.md sections present
- ✓ All skill.json fields complete
- ✓ Storage layout properly declared
- ✓ Journal output paths configured
- ✓ Version: 1.2.0 → 1.2.1

# CHANGELOG

## [1.3.1] - 2026-04-08

### Storage Architecture Update

- Replaced $OCAS_DATA_ROOT variable with platform-native {agent_root}/commons/ convention
- Replaced intake directory pattern with journal payload convention
- Added errors/ as universal storage root alongside journals/
- Inter-skill communication now flows through typed journal payload fields
- No invented environment variables — skills ask the agent for its root directory


## [1.3.0] - 2026-04-08

### Multi-Platform Compatibility Migration

- Adopted agentskills.io open standard for skill packaging
- Replaced skill.json with YAML frontmatter in SKILL.md
- Replaced hardcoded ~/openclaw/ paths with {agent_root}/commons/ for platform portability
- Abstracted cron/heartbeat registration to declarative metadata pattern
- Added metadata.hermes and metadata.openclaw extension points
- Compatible with both OpenClaw and Hermes Agent


## [1.2.0] - 2026-04-02

### Added
- Structured entity observations in journal payloads (`entities_observed`, `relationships_observed`, `preferences_observed`)
- `user_relevance` tagging on journal observations (default `agent_only` for infrastructure entities)
- Elephas journal cooperation in skill cooperation section

### Changed
- Removed "does not emit Signals to Elephas" — Custodian now records entity observations in journals

## [1.5.1] - 2026-03-31

### Added
- Required SKILL.md sections for OCAS specification compliance
- Filesystem field in skill.json

### Changed
- Documentation improvements for better maintainability

## 1.0.2 — 2026-03-30

### Added
- Ontology mapping: Custodian explicitly documented as system-health-only skill with no entity extraction

## Prior

See git log for earlier history.
