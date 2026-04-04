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
