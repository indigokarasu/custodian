# Fixing Missing Skill Journals Directories

During a light scan on 2026-06-30, the ocas-custodian skill identified three skills with missing journals directories:
- ocas-critique
- ocas-genie
- skilllab

<<<<<<< Updated upstream
All three skills had their data directories and config.json files present, but were missing their `<hermes-home>/commons/journals/<skill_name>/` directories.
=======
All three skills had their data directories and config.json files present, but were missing their `~/.hermes/commons/journals/<skill_name>/` directories.
>>>>>>> Stashed changes

## Fix Applied

For each affected skill:
```bash
<<<<<<< Updated upstream
mkdir -p <hermes-home>/profiles/indigo/commons/journals/ocas-critique/
mkdir -p <hermes-home>/profiles/indigo/commons/journals/ocas-genie/
mkdir -p <hermes-home>/profiles/indigo/commons/journals/skilllab/
=======
mkdir -p ~/.hermes/profiles/indigo/commons/journals/ocas-critique/
mkdir -p ~/.hermes/profiles/indigo/commons/journals/ocas-genie/
mkdir -p ~/.hermes/profiles/indigo/commons/journals/skilllab/
>>>>>>> Stashed changes
```

## Verification

After applying the fix, the following directories existed:
<<<<<<< Updated upstream
- `<hermes-home>/profiles/indigo/commons/journals/ocas-critique/` (directory)
- `<hermes-home>/profiles/indigo/commons/journals/ocas-genie/` (directory)
- `<hermes-home>/profiles/indigo/commons/journals/skilllab/` (directory)
=======
- `~/.hermes/profiles/indigo/commons/journals/ocas-critique/` (directory)
- `~/.hermes/profiles/indigo/commons/journals/ocas-genie/` (directory)
- `~/.hermes/profiles/indigo/commons/journals/skilllab/` (directory)
>>>>>>> Stashed changes

This corresponds to the `oc_skill_uninitialized` pattern documented in the skill's known issues, specifically addressing missing journals directories when data directories and config files are already present.

## Prevention

To prevent this issue in the future:
1. Ensure skill initialization includes creating journals directories (step 3 in the standard initialization sequence)
2. When creating new skills, include journals directory setup in the initialization process
3. Regular custodian scans will detect and flag missing skill journals directories