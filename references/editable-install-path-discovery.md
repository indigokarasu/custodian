# Editable Install Path Discovery

## When to Use

When a plugin's source code doesn't seem to match the running behavior — e.g., you edited the file at `~/.hermes/plugins/<name>/` but the changes aren't taking effect.

## Problem

Hermes may use `pip install -e` (editable/vendored installs) which creates a finder module that maps import names to paths **different** from the plugin directory. Editing the plugin directory file has no effect.

## Discovery Method

```python
import importlib
spec = importlib.util.find_spec('hermes_<plugin_name>_plugin')
if spec:
    print(spec.origin)  # The ACTIVE file
```

Or terminal:
```bash
python3 -c "
import importlib
spec = importlib.util.find_spec('hermes_custodian_plugin')
print(spec.origin)
"
```

The finder module (if present) is at:
```
/usr/local/lib/hermes-agent/venv/lib/python3.11/site-packages/__editable___hermes_<plugin_name>_finder.py
```

Inside, the `MAPPING` dict maps package names to actual filesystem paths.

## Real Example (2026-06-09)

**Custodian plugin — three copies existed simultaneously:**

| Location | Path | Active? | Notes |
|----------|------|---------|-------|
<<<<<<< Updated upstream
| Editable (pip -e) | `<hermes-home>/profiles/indigo/home/.hermes/plugins/custodian/hermes_custodian_plugin/__init__.py` | **YES** | This is what Python actually loads |
| Plugin directory | `<hermes-home>/plugins/custodian/hermes_custodian_plugin/__init__.py` | No | Belongs to default profile |
| Profile plugin dir | `<hermes-home>/profiles/indigo/plugins/custodian/` | No | Old version |
=======
| Editable (pip -e) | `~/.hermes/profiles/indigo/home/.hermes/plugins/custodian/hermes_custodian_plugin/__init__.py` | **YES** | This is what Python actually loads |
| Plugin directory | `~/.hermes/plugins/custodian/hermes_custodian_plugin/__init__.py` | No | Belongs to default profile |
| Profile plugin dir | `~/.hermes/profiles/indigo/plugins/custodian/` | No | Old version |
>>>>>>> Stashed changes

The editable finder's MAPPING had:
```python
MAPPING = {
<<<<<<< Updated upstream
    'hermes_custodian_plugin': '<hermes-home>/profiles/indigo/home/.hermes/plugins/custodian/hermes_custodian_plugin'
=======
    'hermes_custodian_plugin': '~/.hermes/profiles/indigo/home/.hermes/plugins/custodian/hermes_custodian_plugin'
>>>>>>> Stashed changes
}
```

## Cross-Profile Write Guard

<<<<<<< Updated upstream
The file at `<hermes-home>/plugins/<plugin>/` belongs to the **default** profile. If running under profile `indigo`, `patch()` will refuse with:
=======
The file at `~/.hermes/plugins/<plugin>/` belongs to the **default** profile. If running under profile `indigo`, `patch()` will refuse with:
>>>>>>> Stashed changes

```
Cross-profile write blocked by soft guard
```

Use `terminal()` to edit it, or fix the editable path instead.

## Lesson

**Never assume the plugin directory is the active code path.** Always verify via `importlib.util.find_spec()` before editing.