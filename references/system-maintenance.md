# System Maintenance — Storage Monitoring & Cleanup

Custodian monitors disk usage and cleans caches as part of its system health role.

## Common Storage Hogs

| Source | Location | Typical Size | Safe to Clear |
|--------|----------|-------------|---------------|
| Docker | images, containers, volumes | 10s of GB | `docker system prune -a --volumes -f` |
| Python pip cache | `~/.cache/pip` | 1-5 GB | `pip cache purge` |
| Node.js cache | `~/.npm`, `~/.cache/npm` | 1-3 GB | `npm cache clean --force` |
| HuggingFace models | `~/.cache/huggingface` | 1-20 GB | Safe if models can be re-downloaded |
| Playwright browsers | `~/.cache/ms-playwright` | 1-3 GB | `rm -rf ~/.cache/ms-playwright` |
| Hermes state/sessions | `~/.hermes/state.db`, `state-snapshots`, `sessions` | 10s of GB | **Do NOT clear** without explicit instruction |
| Hermes agent packages | `/usr/local/lib/hermes-agent` | Varies | **Do NOT clear** |

## Safe Cleanup Commands

```bash
# Docker
docker system prune -a --volumes -f

# Python
pip cache purge

# Node.js
npm cache clean --force

# HuggingFace
rm -rf ~/.cache/huggingface

# Browsers
rm -rf ~/.cache/ms-playwright <fs-root>/.cache/camoufox

# Other caches (safe)
rm -rf <fs-root>/.cache/{uv,pnpm,chroma,typescript,mesa_shader_cache,pipx,mozilla,starship}
```

## Rules

1. **Never clear `<hermes-home>`** without explicit user instruction.
2. **Never clear Docker images** if running containers are critical.
3. **Never clear system binaries** (Chrome, CUDA libraries).
4. **Always verify** with `df -h /` after cleanup.
5. **Report** space reclaimed and new free space.

## Disk Pressure Thresholds

Custodian checks disk during deep scans:
- **>85% full**: Alert + recommend cleanup
- **>90% full**: Urgent — run safe cleanups automatically, escalate if insufficient
- **>95% full**: Critical — escalate immediately

## State Snapshot Cleanup

Custodian's own `custodian:update` job can generate 14GB+ in state snapshots.
Check and prune old snapshots when disk >80%:
```bash
# Remove snapshots older than 7 days
find <hermes-home>/state-snapshots -name "*.json.gz" -mtime +7 -delete
```