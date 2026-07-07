# Fixing Missing Skill Data Directories

During a light scan on 2026-06-30, the ocas-custodian skill identified two skills with missing data directories:
- ocas-haiku
- ocas-scout

Both skills were missing their `<hermes-root>/commons/data/<skill_name>/` directories and default config.json files.

## Fix Applied

For each affected skill:
```bash
mkdir -p <hermes-root>/commons/data/ocas-haiku/
echo '{}' > <hermes-root>/commons/data/ocas-haiku/config.json

mkdir -p <hermes-root>/commons/data/ocas-scout/
echo '{}' > <hermes-root>/commons/data/ocas-scout/config.json
```

## Verification

After applying the fix, the following directories and files existed:
- `<hermes-root>/commons/data/ocas-haiku/` (directory)
- `<hermes-root>/commons/data/ocas-haiku/config.json` (file with `{}`)
- `<hermes-root>/commons/data/ocas-scout/` (directory)
- `<hermes-root>/commons/data/ocas-scout/config.json` (file with `{}`)

This corresponds to the `oc_skill_data_dir_missing` and `oc_skill_uninitialized` patterns documented in the skill's known issues.

## Prevention

To prevent this issue in the future:
1. Ensure skill initialization includes creating data directories and default config files
2. When creating new skills, include data directory setup in the initialization process
3. Regular custodian scans will detect and flag missing skill data directories