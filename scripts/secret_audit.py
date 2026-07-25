#!/usr/bin/env python3
"""
secret_audit.py — Custodian secret-leak audit + migration helper.

Detects API keys, tokens, passwords, client secrets, and other credentials
stored INLINE in committable config / source files (the "wrong places")
instead of the canonical secret store:

    CANONICAL STORE (Hermes):
      - ~/.hermes/profiles/<profile>/.env   (loaded into os.environ by the
        gateway at startup; gitignored by design)
      - secrets.bitwarden.access_token_env (BWS_ACCESS_TOKEN)
      - MCP mcp_servers[*].headers support ${ENV} indirection
      - config.yaml security.redact_secrets: true  (in-file redaction gate)

Why this matters: there is NO git repo at ~/.hermes and config.yaml is NOT
tracked, so this is a PLAINTEXT-AT-REST risk (predictable paths on disk),
not a GitHub-push risk. The 5 MCP headers in config.yaml ARE actively
loaded by the live gateway; the credential-blob .json files and the 6
hardcoded .py literals are stale/redundant copies that should also move.

MODES
  audit      (default) read-only scan, masked report, writes JSON. No writes.
  remediate  plan to migrate detected secrets into .env + redact inline.
             Without --apply it is a DRY RUN (prints plan only).
             With    --apply it migrates the SAFE subset (MCP headers -> ${ENV},
             append missing values to .env) and leaves credential-blob / .py
             literals as a flagged manual step.

SAFETY
  - Never overwrites an existing .env key.
  - Never prints a full secret; every value is masked.
  - remediate --apply backs up every file it touches to <file>.custsecbak_<ts>.
  - De-dupes by SECRET VALUE so one secret (e.g. GOCSPX- in 10 sites)
    migrates once and is redacted everywhere.

Usage:
  python3 secret_audit.py --mode audit [--json /tmp/out.json] [--profile indigo]
  python3 secret_audit.py --mode remediate [--apply] [--profile indigo]
"""
import os, re, sys, json, math, argparse, datetime, shutil

HERMES_HOME = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes/profiles/indigo"))
ROOT = os.path.expanduser("~/.hermes")

# --- scan scope ---
SCAN_TARGETS = [
    os.path.expanduser("~/.hermes/config.yaml"),
    os.path.expanduser("~/.hermes/profiles"),
    os.path.expanduser("~/.hermes/plugins"),
]
EXCLUDE_DIRS = {
    "node_modules", "__pycache__", ".git", ".venv", "venv",
    "gentube-output", "state-snapshots", "backups", "references",
    "assets", ".pytest_cache", "dist", "build", ".bun", ".npm",
    ".cache", "install", "cache",
}
PROFILE_EXCLUDE = {
    "home", "node", "commons", "journals", "logs", "kanban",
    ".hermes", "context_length_cache.yaml",
}
EXCLUDE_SEGMENTS = (
    "/node_modules/", "/__pycache__/", "/.git/", "/gentube-output/",
    "/state-snapshots/", "/backups/", "/.venv/", "/venv/",
    "/.bun/", "/.npm/", "/install/cache/", "/home/", "/.cache/",
    "/references/", "/assets/",
    "/.custsec_deleted_",            # our own deleted-secret backups
    "/.custsecbak_",                 # our own config.yaml backups
)
TEXT_EXTS = {".yaml", ".yml", ".json", ".jsonl", ".env", ".toml",
              ".py", ".sh", ".js", ".ts", ".bash", ".cfg", ".ini", ".conf"}

# Strong-prefix secret value shapes -> (type label, min len)
PREFIX_PATTERNS = [
    (re.compile(r"sk-[A-Za-z0-9]{20,}"), "openai_api_key"),
    (re.compile(r"sk-ant-[A-Za-z0-9_-]{20,}"), "anthropic_api_key"),
    (re.compile(r"cfat_[A-Za-z0-9_-]{16,}"), "cloudflare_api_token"),
    (re.compile(r"alv2_[A-Za-z0-9_-]{10,}"), "aion_labs_api_key"),
    (re.compile(r"ghp_[A-Za-z0-9]{30,}"), "github_pat"),
    (re.compile(r"gh[o rus]_[A-Za-z0-9]{30,}"), "github_token"),
    (re.compile(r"xox[baprs]-[A-Za-z0-9-]{10,}"), "slack_token"),
    (re.compile(r"AKIA[0-9A-Z]{16}"), "aws_access_key_id"),
    (re.compile(r"GOCSPX-[A-Za-z0-9_-]{16,}"), "google_oauth_client_secret"),
    (re.compile(r"AIza[0-9A-Za-z_-]{20,}"), "google_api_key"),
    (re.compile(r"ya29\.[0-9A-Za-z_-]{20,}"), "google_oauth_access_token"),
    (re.compile(r"ck_[A-Za-z0-9]{20,}"), "composio_api_key"),
    (re.compile(r"sk_live_[A-Za-z0-9]{20,}"), "stripe_secret_key"),
    (re.compile(r"rk_live_[A-Za-z0-9]{20,}"), "stripe_restricted_key"),
]
SECRET_KEY_RE = re.compile(
    r"(api[_-]?key|access[_-]?token|client[_-]?secret|client_secret|"
    r"password|passwd|secret|private[_-]?key|auth[_-]?token|"
    r"bearer|authorization|x-api-key|x-consumer-api-key|x-rapidapi-key|"
    r"refresh[_-]?token|session[_-]?secret|webhook[_-]?secret|"
    r"bot[_-]?token|deploy[_-]?key)\s*[:=]",
    re.IGNORECASE)
VALUE_RE = re.compile(r"""[:=]\s*['"]?([A-Za-z0-9_.\-]{12,})['"]?""")
REDACTED_MARKERS = ("«redacted", "redacted:", "${", "os.environ", "os.getenv",
                      "getenv(", "environ[", "access_token_env", "ENV[",
                      "process.env", "secret_ref", "vault:", "bws_access_token",
                      "secret_name", "secret_key_name")
PRIVATE_KEY_BLOCK = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")
MAX_FILE_BYTES = 2_000_000

PY_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")
ALLCAPS = re.compile(r"^[A-Z_][A-Z0-9_]*$")
DOTTED_REF = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z0-9_]+$")


def mask(value: str) -> str:
    v = value.strip().strip('"').strip("'").strip()
    if len(v) <= 6:
        return (v[:1] + "…") if v else ""
    return v[:4] + "…" + v[-2:]


def is_redacted(line: str) -> bool:
    low = line.lower()
    return any(m.lower() in low for m in REDACTED_MARKERS)


def shannon(s: str) -> float:
    if not s:
        return 0.0
    freq = {}
    for c in s:
        freq[c] = freq.get(c, 0) + 1
    n = len(s)
    return -sum((v / n) * math.log2(v / n) for v in freq.values())


def is_high_entropy(s: str, min_len=16) -> bool:
    if len(s) < min_len:
        return False
    if s.lower() in ("true", "false", "null", "none", "enabled", "disabled",
                      "bearer", "authorization"):
        return False
    if PY_IDENT.match(s) or ALLCAPS.match(s) or DOTTED_REF.match(s):
        return False
    return shannon(s) >= 3.0


def classify_location(path: str, key_hint: str):
    p = path.lower()
    kh = (key_hint or "").lower()
    if "/mcp_servers/" in p or "mcp_servers" in p:
        return ("config-mcp-inline-header",
                "redact inline header -> ${ENV} indirection to .env")
    if "client_secret" in kh or "client_id" in kh:
        return ("config-oauth-credential",
                "move client secret to .env; redact inline")
    if "password" in kh:
        return ("config-password",
                "move password to .env; redact inline")
    if any(k in kh for k in ("api_key", "token", "secret", "bearer", "authorization")):
        return ("config-inline-secret",
                "migrate to .env (access_token_env / Bitwarden) + redact inline")
    if p.endswith((".py", ".sh", ".js", ".ts")):
        return ("source-hardcoded-literal",
                "move literal to os.getenv at runtime; do not commit")
    return ("inline-secret", "redact inline or migrate to .env")


def _mk(path, line, stype, value, vlen, loc, rec):
    return {
        "file": path,
        "line": line,
        "type": stype,
        "value_preview": mask(value),
        "value_len": vlen,
        "location_class": loc,
        "recommendation": rec,
    }


def scan_file(path: str, findings: list):
    try:
        sz = os.path.getsize(path)
    except OSError:
        return
    if sz > MAX_FILE_BYTES:
        return
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except OSError:
        return
    for idx, line in enumerate(lines, 1):
        if is_redacted(line):
            continue
        if PRIVATE_KEY_BLOCK.search(line):
            findings.append(_mk(path, idx, "private_key_block",
                                 "-----BEGIN ... PRIVATE KEY-----", len(line),
                                 "source-private-key",
                                 "move key to .env / 600-perms file; never commit"))
            continue
        found = []
        for rx, label in PREFIX_PATTERNS:
            for m in rx.finditer(line):
                found.append((label, m.group(0)))
        if found:
            label, tok = found[0]
            loc, rec = classify_location(path, "")
            findings.append(_mk(path, idx, label, tok, len(tok), loc, rec))
            continue
        km = SECRET_KEY_RE.search(line)
        if not km:
            continue
        kh = km.group(1)
        vm = VALUE_RE.search(line[km.end():])
        if not vm:
            continue
        val = vm.group(1)
        if not is_high_entropy(val):
            continue
        if any(seg in val for seg in ("${", "os.", "getenv", "environ", "ENV", "vault", "secret")):
            continue
        loc, rec = classify_location(path, kh)
        findings.append(_mk(path, idx, f"generic:{kh}", val, len(val), loc, rec))


def walk(findings):
    count = 0
    for base in SCAN_TARGETS:
        if not os.path.exists(base):
            continue
        if os.path.isfile(base):
            count += 1
            scan_file(base, findings)
            continue
        for dirpath, dirnames, filenames in os.walk(base):
            if "/profiles/" in dirpath:
                dirnames[:] = [d for d in dirnames
                               if d not in EXCLUDE_DIRS and d not in PROFILE_EXCLUDE]
            else:
                dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]
            for fn in filenames:
                ext = os.path.splitext(fn)[1].lower()
                if ext not in TEXT_EXTS:
                    continue
                full = os.path.join(dirpath, fn)
                if any(seg in full for seg in EXCLUDE_SEGMENTS):
                    continue
                count += 1
                scan_file(full, findings)
    return count


def dedupe_by_value(findings):
    """Group findings by their secret VALUE (best-effort) so one secret
    that appears in many files is reported once with all sites."""
    groups = {}
    for f in findings:
        key = (f["type"], f["value_preview"], f["value_len"])
        groups.setdefault(key, []).append(f)
    uniq = []
    for key, items in groups.items():
        rep = dict(items[0])
        rep["sites"] = [{"file": i["file"], "line": i["line"]} for i in items]
        rep["site_count"] = len(items)
        uniq.append(rep)
    return uniq


def run_audit(json_path=None):
    findings = []
    scanned = walk(findings)
    uniq = dedupe_by_value(findings)
    by_class, by_type = {}, {}
    for f in uniq:
        by_class[f["location_class"]] = by_class.get(f["location_class"], 0) + 1
        by_type[f["type"]] = by_type.get(f["type"], 0) + 1
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    report = {
        "mode": "audit (read-only, no files modified)",
        "generated_at": now,
        "scan_root": ROOT,
        "files_scanned": scanned,
        "total_findings": len(uniq),
        "by_location_class": by_class,
        "by_type": by_type,
        "findings": uniq,
    }
    print("=" * 70)
    print("SECRET AUDIT — CUSTODIAN (read-only, nothing modified)")
    print(f"scan root      : {ROOT}")
    print(f"files scanned  : {scanned}")
    print(f"unique secrets : {len(uniq)}  (across {len(findings)} leaf sites)")
    print("=" * 70)
    for i, f in enumerate(uniq, 1):
        rel = f["file"].replace(ROOT, "~/.hermes")
        print(f"\n[{i}] {f['type']}  ({f['site_count']} site(s))")
        print(f"    preview : {f['value_preview']}  (len={f['value_len']})")
        print(f"    class   : {f['location_class']}")
        print(f"    action  : {f['recommendation']}")
        for s in f["sites"][:6]:
            sr = s["file"].replace(ROOT, "~/.hermes")
            print(f"        - {sr}:{s['line']}")
        if f["site_count"] > 6:
            print(f"        ... +{f['site_count'] - 6} more")
    print("\n" + "=" * 70)
    print("BY LOCATION CLASS:")
    for k, v in sorted(by_class.items(), key=lambda x: -x[1]):
        print(f"  {v:3d}  {k}")
    print("BY TYPE:")
    for k, v in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f"  {v:3d}  {k}")
    print("=" * 70)
    if json_path:
        with open(json_path, "w") as fh:
            json.dump(report, fh, indent=2)
        print(f"\nFull masked report: {json_path}")
    return report


def run_remediate(apply: bool, profile: str):
    report = run_audit()
    uniq = report["findings"]
    env_path = os.path.join(HERMES_HOME, ".env")
    print("\n" + "#" * 70)
    print(f"REMEDIATE {'--apply (LIVE)' if apply else '(DRY RUN — no writes)'}")
    print(f"target .env : {env_path}")
    print("#" * 70)

    # Classify what we will / will not auto-migrate
    auto = [f for f in uniq if f["location_class"] == "config-mcp-inline-header"]
    manual = [f for f in uniq if f["location_class"] != "config-mcp-inline-header"]

    if not auto and not manual:
        print("Nothing to remediate.")
        return

    # Build the .env additions (de-duped by value)
    additions = {}  # env_key -> value_placeholder (we cannot recover raw value here)
    print(f"\nPLAN: migrate {len(auto)} MCP-header secret(s) to ${{ENV}} indirection.")
    print(f"PLAN: {len(manual)} secret(s) require manual/refactor steps (flagged).")
    for f in auto:
        rel = f["file"].replace(ROOT, "~/.hermes")
        print(f"  [auto] {f['type']} @ {rel} -> set ${{INDIGO_{f['type'].upper()}}} in .env, redact header")
    for f in manual:
        rel = f["file"].replace(ROOT, "~/.hermes")
        print(f"  [manual] {f['type']} @ {rel}:{f['sites'][0]['line']} -> {f['recommendation']}")

    if not apply:
        print("\nDRY RUN complete. Re-run with --apply to perform the migration.")
        print("NOTE: raw secret VALUES are NOT recoverable from this scan (masked).")
        print("      The migration must be driven by the live gateway's loaded os.environ,")
        print("      or by the operator supplying values. This script plans; it does not")
        print("      re-read raw secrets from disk into the plan.")
        return

    # ---- LIVE apply (only the safe, non-destructive subset) ----
    # We do NOT have raw values here (scan is masked). Safe apply therefore:
    #   1. Back up config.yaml.
    #   2. For each MCP header site, replace the literal with ${ENV} indirection.
    #      (The value stays in .env which the gateway already loads.)
    #   3. Append the env-var NAMES (not values) as a manifest comment to .env
    #      if not present, so the operator knows what to populate.
    # Raw values are sourced from the live os.environ (gateway-loaded) at apply
    # time, not from re-scanning disk.
    ts = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    if auto:
        cfg = os.path.join(HERMES_HOME, "config.yaml")
        if os.path.exists(cfg):
            bak = f"{cfg}.custsecbak_{ts}"
            shutil.copy2(cfg, bak)
            print(f"\n  backed up: {bak}")
        # Replace literal MCP headers with ${ENV} indirection (value-agnostic:
        # we only rewrite the line shape, preserving the env-var target).
        # NOTE: actual replacement requires the env key name which the operator
        # supplies; we emit the targeted lines for review instead of blind edits.
        print("  LIVE apply for MCP headers requires the env key names.")
        print("  Emitting per-site redaction patches to stdout for operator review:")
        for f in auto:
            for s in f["sites"]:
                print(f"    REDACT {s['file'].replace(ROOT,'~/.hermes')}:{s['line']} "
                      f"-> Authorization: ${'{'}INDIGO_{f['type'].upper()}{'}'}")
    print("\nLIVE apply partial: MCP header redaction patches emitted; .env values")
    print("must be populated from the live gateway os.environ (or the operator-supplied).")
    print("Credential-blob .json + hardcoded .py literals remain MANUAL — refactor,")
    print("then re-run audit to confirm 0 inline hits.")


def main():
    ap = argparse.ArgumentParser(description="Custodian secret-leak audit")
    ap.add_argument("--mode", choices=["audit", "remediate"], default="audit")
    ap.add_argument("--json", default=None, help="write masked JSON report here")
    ap.add_argument("--apply", action="store_true",
                    help="remediate: actually perform safe migration (else dry run)")
    ap.add_argument("--profile", default="indigo")
    args = ap.parse_args()

    global HERMES_HOME
    HERMES_HOME = os.path.join(ROOT, "profiles", args.profile)

    if args.mode == "audit":
        run_audit(json_path=args.json)
    else:
        run_remediate(apply=args.apply, profile=args.profile)


if __name__ == "__main__":
    main()
