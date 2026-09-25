#!/usr/bin/env python3
"""Unit tests for the ocas-custodian helper scripts.

Pure-logic tests only — no live cron/state/config reads and no side effects.
Run from the skill directory:  python3 -m unittest discover -s tests
"""
import ast
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parent.parent
SCRIPTS = SKILL_DIR / "scripts"
sys.path.insert(0, str(SCRIPTS))

import custodian_common                       # noqa: E402
import find_missed_user_gated_jobs as missed  # noqa: E402
import parse_issues_jsonl                     # noqa: E402
import reopen_false_resolutions as reopen     # noqa: E402

# Mirror of the skilllab runner's stdlib allow-list: a module-scope import of
# anything else breaks `--help` on a machine where the dependency is absent.
STDLIB = {
    "os", "sys", "re", "io", "json", "argparse", "subprocess", "pathlib",
    "datetime", "time", "glob", "shutil", "typing", "collections", "itertools",
    "functools", "math", "random", "hashlib", "base64", "csv", "sqlite3",
    "urllib", "tempfile", "textwrap", "logging", "unittest", "py_compile",
    "dataclasses", "enum", "uuid", "ast", "traceback", "warnings", "copy",
    "string", "struct", "socket", "threading", "queue", "signal", "platform",
    "curses", "shlex", "select", "errno", "stat", "difflib", "pprint", "pickle",
    "gzip", "tarfile", "zipfile", "secrets", "statistics", "decimal",
    "fractions", "numbers", "operator", "bisect", "heapq", "array",
    "configparser", "getpass", "inspect", "importlib", "contextlib", "abc",
    "__future__",
}


class TestParseIssues(unittest.TestCase):
    """custodian_common.parse_issues — JSON stream parser."""

    def test_empty_and_whitespace(self):
        self.assertEqual(custodian_common.parse_issues(""), [])
        self.assertEqual(custodian_common.parse_issues("   \n  "), [])

    def test_jsonl_layout(self):
        text = '{"a": 1}\n{"b": 2}\n'
        self.assertEqual(custodian_common.parse_issues(text), [{"a": 1}, {"b": 2}])

    def test_concatenated_one_line(self):
        self.assertEqual(custodian_common.parse_issues('{"a": 1}{"b": 2}'),
                         [{"a": 1}, {"b": 2}])

    def test_top_level_list_flattened(self):
        self.assertEqual(custodian_common.parse_issues('[{"a": 1}, {"b": 2}]'),
                         [{"a": 1}, {"b": 2}])

    def test_braces_inside_strings(self):
        self.assertEqual(custodian_common.parse_issues('{"msg": "got { and } and [ too"}'),
                         [{"msg": "got { and } and [ too"}])

    def test_junk_recovery(self):
        self.assertEqual(custodian_common.parse_issues('garbage {"a": 1}'), [{"a": 1}])


class TestParseIssuesJsonl(unittest.TestCase):
    """parse_issues_jsonl — file parse (lazy import path) and dedupe."""

    def test_parse_file_concatenated_and_jsonl(self):
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as fh:
            fh.write('{"a": 1}\n{"b": 2}{"c": 3}\n')
            path = fh.name
        try:
            got = parse_issues_jsonl.parse(path)
        finally:
            os.unlink(path)
        self.assertEqual(got, [{"a": 1}, {"b": 2}, {"c": 3}])

    def test_dedupe_prefers_non_resolved(self):
        entries = [
            {"issue_id": "x", "status": "resolved"},
            {"issue_id": "x", "status": "open"},
            {"issue_id": "y", "status": "resolved"},
        ]
        best = parse_issues_jsonl.dedupe(entries)
        self.assertEqual(best["x"]["status"], "open")
        self.assertEqual(best["y"]["status"], "resolved")

    def test_dedupe_skips_keyless_entries(self):
        self.assertEqual(parse_issues_jsonl.dedupe([{"status": "open"}]), {})


class TestClassify(unittest.TestCase):
    """find_missed_user_gated_jobs.classify — fingerprint bucketing."""

    def test_user_gated_match(self):
        kind, fp, iid = missed.classify("HTTP 402 insufficient credits from openrouter")
        self.assertEqual(kind, "MISSED")
        self.assertEqual(fp, "oc_openrouter_402_credits_exhausted")
        self.assertTrue(iid)

    def test_transient_match(self):
        kind, _fp, _iid = missed.classify(
            "cannot schedule new futures after interpreter shutdown")
        self.assertEqual(kind, "TRANSIENT")

    def test_unknown(self):
        self.assertEqual(missed.classify("some brand new failure")[0], "UNKNOWN")

    def test_empty_and_none(self):
        self.assertEqual(missed.classify("")[0], "UNKNOWN")
        self.assertEqual(missed.classify(None)[0], "UNKNOWN")


class TestReopenLiveErrorCount(unittest.TestCase):
    """reopen_false_resolutions.live_error_count — all-substrings rule."""

    def setUp(self):
        fh = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        json.dump({"jobs": [
            {"id": "a", "last_status": "error",
             "last_error": "HTTP 402: insufficient credits (openrouter)"},
            {"id": "b", "last_status": "ok",
             "last_error": "HTTP 402: insufficient credits (openrouter)"},
            {"id": "c", "last_status": "error", "last_error": "402 without the rest"},
        ]}, fh)
        fh.close()
        self._tmp = fh.name
        self._orig = reopen.JOBS
        reopen.JOBS = fh.name

    def tearDown(self):
        reopen.JOBS = self._orig
        os.unlink(self._tmp)

    def test_counts_only_jobs_matching_all_substrings(self):
        # OUTAGE_MATCH requires BOTH "402" and "credits"; job b is not erroring.
        self.assertEqual(reopen.live_error_count("oc_openrouter_402_credits_exhausted"), 1)

    def test_unknown_fingerprint_counts_zero(self):
        self.assertEqual(reopen.live_error_count("oc_no_such_fingerprint"), 0)


class TestNoModuleScopeThirdPartyImports(unittest.TestCase):
    """`--help` must work on a machine without optional deps: keep imports lazy."""

    def test_all_scripts_import_third_party_modules_lazily(self):
        offenders = []
        for path in sorted(SCRIPTS.glob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in tree.body:  # module scope only
                if isinstance(node, ast.Import):
                    mods = [a.name for a in node.names]
                elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                    mods = [node.module]
                else:
                    continue
                for m in mods:
                    if m.split(".")[0] not in STDLIB:
                        offenders.append("%s: %s" % (path.name, m))
                        break
        self.assertEqual(offenders, [], "module-scope 3rd-party imports found")


class TestScriptsHelp(unittest.TestCase):
    """Every help-capable script must exit 0 on --help without live state."""

    def test_help_capable_scripts_exit_zero(self):
        failures = []
        for path in sorted(SCRIPTS.glob("*.py")):
            src = path.read_text(encoding="utf-8")
            if "ArgumentParser(" not in src and "_HELP_ARGS" not in src:
                continue  # no --help surface; covered by compile check in CI
            proc = subprocess.run(
                [sys.executable, path.name, "--help"],
                capture_output=True, text=True, timeout=120, cwd=str(SCRIPTS),
            )
            if proc.returncode != 0:
                failures.append("%s (rc=%d): %s" % (
                    path.name, proc.returncode,
                    (proc.stderr or proc.stdout).strip().splitlines()[-1:]))
        self.assertEqual(failures, [], "--help failed for: %s" % failures)


if __name__ == "__main__":
    unittest.main()
