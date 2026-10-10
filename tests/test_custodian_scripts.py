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

import confirm_provider_recovery as cpr       # noqa: E402
import custodian_common                       # noqa: E402
import escalation_exec_pause_reconcile as esc # noqa: E402
import find_missed_user_gated_jobs as missed  # noqa: E402
import parse_issues_jsonl                     # noqa: E402
import race_safe_issue_patch as patch         # noqa: E402
import reopen_false_resolutions as reopen     # noqa: E402
import scan_escalation_journal_gaps as gaps   # noqa: E402
import verify_plugin_defect_postrestart as vp # noqa: E402
import verify_provider_recovery as vpr        # noqa: E402

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


class TestScanEscalationJournalGaps(unittest.TestCase):
    """scan_escalation_journal_gaps.get_ts — ISO 8601 & fallback parsing."""

    def test_get_ts_iso_formats(self):
        # Standard ISO 8601 with Z
        d1 = {"timestamp": "2026-07-16T12:34:56Z"}
        ts1 = gaps.get_ts(d1)
        self.assertIsNotNone(ts1)

        # ISO 8601 with subsecond resolution and Z
        d2 = {"created_at": "2026-07-16T12:34:56.789123Z"}
        ts2 = gaps.get_ts(d2)
        self.assertIsNotNone(ts2)
        self.assertAlmostEqual(ts2 - ts1, 0.789123, places=3)

        # ISO 8601 with explicit timezone offset
        d3 = {"run_ts": "2026-07-16T12:34:56+00:00"}
        ts3 = gaps.get_ts(d3)
        self.assertEqual(ts1, ts3)

    def test_get_ts_missing_or_invalid(self):
        self.assertIsNone(gaps.get_ts({}))
        self.assertIsNone(gaps.get_ts({"timestamp": "invalid date string"}))
        self.assertIsNone(gaps.get_ts({"timestamp": 12345678}))

    def test_recovery_phrase_detection(self):
        d1 = {"summary": "Issue now resolved after API key rotation."}
        d2 = {"notes": "Service recovered."}
        d3 = {"status": "still failing"}
        for d in (d1, d2):
            d_str = str(d).lower()
            self.assertTrue(
                "recovered" in d_str or "issue now resolved" in d_str or "now resolved" in d_str
            )
        d3_str = str(d3).lower()
        self.assertFalse(
            "recovered" in d3_str or "issue now resolved" in d3_str or "now resolved" in d3_str
        )


class TestVerifyPluginDefectPostrestart(unittest.TestCase):
    """verify_plugin_defect_postrestart.scan_log — pre vs post restart bucketing."""

    def test_scan_log_bucketing(self):
        log_content = (
            "2026-07-22 10:00:00 CHECK constraint failed: actor\n"
            "2026-07-22 10:05:00 Received SIGTERM - restart\n"
            "2026-07-22 10:10:00 CHECK constraint failed: actor\n"
            "2026-07-22 10:15:00 UNIQUE constraint failed\n"
        )
        with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as fh:
            fh.write(log_content)
            path = fh.name

        try:
            last_restart, counts, last_ts = vp.scan_log(path, vp.DEFAULT_PATTERNS)
            self.assertEqual(last_restart, "2026-07-22T10:05:00")
            self.assertEqual(counts["actor_check"], {"pre": 1, "post": 1})
            self.assertEqual(counts["seq_unique"], {"pre": 0, "post": 1})
            self.assertEqual(counts["compress_force"], {"pre": 0, "post": 0})
            self.assertEqual(last_ts["actor_check"], "2026-07-22T10:10:00")
            self.assertEqual(last_ts["seq_unique"], "2026-07-22T10:15:00")
        finally:
            os.unlink(path)

    def test_scan_log_nonexistent_file(self):
        self.assertIsNone(vp.scan_log("/no/such/log/file.log", vp.DEFAULT_PATTERNS))


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


class TestVerifyProviderRecovery(unittest.TestCase):
    """verify_provider_recovery — load_jobs, utc, and default_provider profile loading."""

    def test_utc_iso_parsing(self):
        ts = vpr.utc("2026-07-16T12:34:56Z")
        self.assertIsNotNone(ts)
        self.assertIsNone(vpr.utc(None))
        self.assertIsNone(vpr.utc("invalid timestamp"))

    def test_load_jobs_and_default_provider_profile_paths(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            profile_dir = Path(tmpdir) / ".hermes" / "profiles" / "testprof"
            cron_dir = profile_dir / "cron"
            cron_dir.mkdir(parents=True)
            jobs_file = cron_dir / "jobs.json"
            jobs_file.write_text(json.dumps({"jobs": [{"id": "j1", "enabled": True}]}))

            cfg_file = profile_dir / "config.yaml"
            cfg_file.write_text("provider: openrouter\nmodel: anthropic/claude-3-5-sonnet\n")

            orig_expanduser = os.path.expanduser
            def mock_expanduser(path):
                if path.startswith("~"):
                    return str(Path(tmpdir) / path[2:])
                return path

            try:
                os.path.expanduser = mock_expanduser
                jobs = vpr.load_jobs("testprof")
                self.assertEqual(len(jobs), 1)
                self.assertEqual(jobs[0]["id"], "j1")

                prov, model = vpr.default_provider("testprof")
                self.assertEqual(prov, "openrouter")
                self.assertEqual(model, "anthropic/claude-3-5-sonnet")
            finally:
                os.path.expanduser = orig_expanduser


class TestRaceSafeIssuePatch(unittest.TestCase):
    """race_safe_issue_patch — line patching and fast-path verification."""

    def test_parse_line_and_coerce(self):
        self.assertEqual(patch.parse_line('{"issue_id": "oc_1", "status": "open"}'),
                         {"issue_id": "oc_1", "status": "open"})
        self.assertIsNone(patch.parse_line("   \n"))
        self.assertIsNone(patch.parse_line("invalid json"))

        self.assertEqual(patch.coerce("true"), True)
        self.assertEqual(patch.coerce("false"), False)
        self.assertEqual(patch.coerce("null"), None)
        self.assertEqual(patch.coerce("123"), 123)
        self.assertEqual(patch.coerce("open"), "open")

    def test_main_patch_and_verify(self):
        lines = [
            json.dumps({"issue_id": "oc_1", "status": "user_gated", "escalation_needed": True}),
            json.dumps({"issue_id": "oc_2", "status": "user_gated", "escalation_needed": True}),
        ]
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as fh:
            fh.write("\n".join(lines) + "\n")
            path = fh.name

        try:
            old_argv = sys.argv
            sys.argv = [
                "race_safe_issue_patch.py",
                "--issue-id", "oc_1",
                "--set", "status=resolved",
                "--set", "escalation_needed=false",
                "--path", path,
            ]
            with self.assertRaises(SystemExit) as cm:
                patch.main()
            self.assertEqual(cm.exception.code, 0)

            with open(path) as f:
                updated_lines = [json.loads(ln) for ln in f if ln.strip()]

            # oc_1 should be updated and have resolved_at set
            self.assertEqual(updated_lines[0]["status"], "resolved")
            self.assertFalse(updated_lines[0]["escalation_needed"])
            self.assertIn("resolved_at", updated_lines[0])

            # oc_2 should be unchanged
            self.assertEqual(updated_lines[1]["status"], "user_gated")
            self.assertTrue(updated_lines[1]["escalation_needed"])
        finally:
            sys.argv = old_argv
            if os.path.exists(path):
                os.unlink(path)

    def test_escaped_issue_id_is_not_skipped_by_prefilter(self):
        """The fast-path substring test must stay a SUPERSET of the parse match.

        An id containing a character JSON escapes (non-ASCII, quote, backslash)
        is written escaped on disk, so a literal `issue_id in line` pre-filter is
        a false negative: the row is silently skipped and the script reports
        NOT FOUND on an object that matches. Regression for the Bolt ⚡
        pre-filter added in #25.
        """
        for issue_id in ["oc_état_1", 'oc_"odd"_id', "oc_back\\slash_id"]:
            with self.subTest(issue_id=issue_id):
                lines = [json.dumps({"issue_id": issue_id, "status": "user_gated"})]
                with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as fh:
                    fh.write("\n".join(lines) + "\n")
                    path = fh.name

                try:
                    old_argv = sys.argv
                    sys.argv = [
                        "race_safe_issue_patch.py",
                        "--issue-id", issue_id,
                        "--set", "status=resolved",
                        "--path", path,
                    ]
                    with self.assertRaises(SystemExit) as cm:
                        patch.main()
                    self.assertEqual(cm.exception.code, 0)

                    with open(path) as f:
                        after = [json.loads(ln) for ln in f if ln.strip()]
                    self.assertEqual(after[0]["status"], "resolved")
                finally:
                    sys.argv = old_argv
                    if os.path.exists(path):
                        os.unlink(path)


class TestConfirmProviderRecovery(unittest.TestCase):
    """confirm_provider_recovery — fp_of classification and main recovery confirmation logic."""

    def test_fp_of(self):
        self.assertEqual(cpr.fp_of("Provided authentication token is expired"), "token_expired")
        self.assertEqual(cpr.fp_of("HTTP 402: insufficient credits"), "openrouter_402")
        self.assertEqual(cpr.fp_of("portal.nousresearch.com 401 error"), "nous_401")
        self.assertEqual(cpr.fp_of("owl-alpha 404 No endpoints found"), "owl_404")
        self.assertEqual(cpr.fp_of("random error"), "other")
        self.assertEqual(cpr.fp_of(None), "other")


class TestEscalationExecPauseReconcile(unittest.TestCase):
    """escalation_exec_pause_reconcile — classify and reconciliation pre-grouping."""

    def test_classify(self):
        self.assertEqual(esc.classify("portal.nousresearch.com invalid key", "job1"), "nous")
        self.assertEqual(esc.classify("HTTP 402 insufficient credits", "job2"), "openrouter")
        self.assertEqual(esc.classify("403 Forbidden", "monitor:list"), "google403")
        self.assertEqual(esc.classify("ResourceExhausted limit", "job3"), "transient")
        self.assertEqual(esc.classify("random failure", "job4"), "unknown")

    def test_reconciliation_pre_grouping(self):
        jobs = {
            "j1": {"enabled": False, "state": "paused", "last_error": "portal.nousresearch.com 401", "name": "j1"},
            "j2": {"enabled": False, "state": "paused", "last_error": "402 credits openrouter", "name": "j2"},
            "j3": {"enabled": False, "state": "paused", "last_error": "403 Forbidden", "name": "j3"},
        }
        actual_paused = {jid for jid, j in jobs.items()
                         if j.get("enabled") is False and j.get("state") == "paused"}
        paused_by_bucket = {}
        for jid in actual_paused:
            b = esc.classify(jobs[jid].get("last_error"), jobs[jid].get("name"))
            paused_by_bucket.setdefault(b, []).append(jid)

        self.assertEqual(sorted(paused_by_bucket.get("nous", [])), ["j1"])
        self.assertEqual(sorted(paused_by_bucket.get("openrouter", [])), ["j2"])
        self.assertEqual(sorted(paused_by_bucket.get("google403", [])), ["j3"])
        self.assertEqual(paused_by_bucket.get("owl", []), [])


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
