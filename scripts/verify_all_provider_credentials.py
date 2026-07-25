#!/usr/bin/env python3
"""
verify_all_provider_credentials.py — Custodian probe: enumerate EVERY credential
in the profile config and test each one's liveness directly.

WHY: A "provider recovered" verdict is only valid if the SAME credential the
failing job uses was probed. A config carries multiple distinct providers
(top-level `model:` block, `providers:`, `auxiliary.*`, `fallback_model`).
Probing only the gateway default (tencent/hy3:free) gives a false green.

Run:
  python3 verify_all_provider_credentials.py [profile_dir]
profile_dir defaults to ~/.hermes/profiles/indigo
"""
import json
import os
import sys
import socket
import urllib.request
import urllib.error

try:
    import yaml
except ImportError:
    yaml = None

DEFAULT_PROFILE = os.path.expanduser("~/.hermes/profiles/indigo")


def load_config(profile):
    p = os.path.join(profile, "config.yaml")
    if not os.path.isfile(p):
        p = os.path.expanduser("~/.hermes/config.yaml")
    if yaml is None:
        raise SystemExit("PyYAML required: pip install pyyaml")
    with open(p) as f:
        return yaml.safe_load(f), p


def collect(cfg):
    creds = []  # (label, base_url, api_key)
    m = cfg.get("model") or {}
    if isinstance(m, dict) and m.get("base_url"):
        creds.append(("model: block", m.get("base_url"), m.get("api_key")))
    for name, p in (cfg.get("providers") or {}).items():
        if isinstance(p, dict) and p.get("base_url"):
            creds.append((f"providers.{name}", p.get("base_url"), p.get("api_key")))
    aux = cfg.get("auxiliary") or {}
    if isinstance(aux, dict):
        for sub, v in aux.items():
            if isinstance(v, dict) and v.get("base_url"):
                creds.append((f"auxiliary.{sub}", v.get("base_url"), v.get("api_key")))
    fm = cfg.get("fallback_model")
    if isinstance(fm, dict) and fm.get("base_url"):
        creds.append(("fallback_model", fm.get("base_url"), fm.get("api_key")))
    return creds


def probe(label, base_url, api_key):
    if not base_url:
        return "NO_BASE_URL"
    if not api_key:
        return "NO_KEY (untestable)"
    url = base_url.rstrip("/") + "/models"
    try:
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {api_key}"})
        r = urllib.request.urlopen(req, timeout=20)
        return f"LIVE ({r.status})"
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode()[:120]
        except Exception:
            pass
        return f"DEAD HTTP {e.code}: {body}"
    except socket.timeout:
        return "TIMEOUT"
    except Exception as e:
        return f"ERR {type(e).__name__}: {str(e)[:80]}"


def main(profile=None):
    profile = profile or DEFAULT_PROFILE
    cfg, path = load_config(profile)
    print(f"Config: {path}")
    creds = collect(cfg)
    seen = set()
    uniq = []
    for label, bu, key in creds:
        kp = (key or "")[:12]
        sig = (bu, kp)
        if sig in seen:
            continue
        seen.add(sig)
        uniq.append((label, bu, key))
    print(f"Distinct credentials found: {len(uniq)}\n")
    any_dead = False
    for label, bu, key in uniq:
        res = probe(label, bu, key)
        flag = ""
        if res.startswith("DEAD") or res.startswith("ERR"):
            any_dead = True
            flag = "  <-- CHECK"
        print(f"  [{label}]")
        print(f"    base_url: {bu}")
        print(f"    key: {(key or '')[:12]}...")
        print(f"    result: {res}{flag}")
    print()
    print("VERDICT:", "ONE OR MORE CREDENTIALS DEAD" if any_dead else "ALL PROBED CREDENTIALS LIVE")
    if any_dead:
        print("Before marking any provider/auth issue 'resolved' or 'forward-stale', confirm the "
              "DEAD credential is not the one the failing job uses.")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Custodian probe: enumerate every credential in the profile config and test each one's liveness directly. A 'provider recovered' verdict is only valid if the SAME credential the failing job uses was probed.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n  python3 verify_all_provider_credentials.py\n  python3 verify_all_provider_credentials.py --profile koda",
    )
    ap.add_argument("--profile", default=DEFAULT_PROFILE, help="Profile name or HOME dir")
    args = ap.parse_args()
    main(args.profile)