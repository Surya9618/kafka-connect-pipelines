#!/usr/bin/env python3
"""
Stdlib-only tests for apply_config.py. No live Kafka Connect needed:
the HTTP layer (dr_common.get/put_connector_config) is mocked.

Run from the repo root:
  python3 -B -m unittest discover -s tests -v
"""

import io
import os
import sys
import unittest
from contextlib import redirect_stdout, redirect_stderr
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
import apply_config  # noqa: E402

PRIMARY = "jdbc:mysql://primary:3306/db"
DR = "jdbc:mysql://dr:3306/db"
CONN_CFG = {"user": None, "password": None, "headers": {}, "timeout": 5}


def entry(name, primary=PRIMARY, dr=DR, key="connection.url", groups=None):
    e = {"name": name, "config_key": key, "primary_url": primary, "dr_url": dr, "type": "jdbc"}
    if groups is not None:
        e["groups"] = groups
    return e


def run_quiet(fn, *args):
    out, err = io.StringIO(), io.StringIO()
    code = 0
    with redirect_stdout(out), redirect_stderr(err):
        try:
            fn(*args)
        except SystemExit as ex:
            code = ex.code
    return code, out.getvalue() + err.getvalue()


class MappingGuardTests(unittest.TestCase):
    def test_valid_entry(self):
        self.assertIsNone(apply_config.mapping_problem(entry("a"), "primary-to-dr"))
        self.assertIsNone(apply_config.mapping_problem(entry("a"), "dr-to-primary"))

    def test_blank_dr_url_rejected_both_directions(self):
        e = entry("a", dr="")
        self.assertIn("target", apply_config.mapping_problem(e, "primary-to-dr"))
        self.assertIn("source", apply_config.mapping_problem(e, "dr-to-primary"))

    def test_whitespace_only_rejected(self):
        self.assertIsNotNone(apply_config.mapping_problem(entry("a", dr="   "), "primary-to-dr"))

    def test_missing_dr_url_key_rejected(self):
        e = entry("a")
        del e["dr_url"]
        self.assertIsNotNone(apply_config.mapping_problem(e, "primary-to-dr"))

    def test_identical_rejected(self):
        self.assertIn("identical", apply_config.mapping_problem(entry("a", dr=PRIMARY), "primary-to-dr"))

    def test_blank_config_key_rejected(self):
        self.assertIsNotNone(apply_config.mapping_problem(entry("a", key=""), "primary-to-dr"))


class ApplyTests(unittest.TestCase):
    def setUp(self):
        self.live = {
            "good": {"connector.class": "X", "connection.url": PRIMARY, "tasks.max": "1"},
            "bad": {"connector.class": "X", "connection.url": PRIMARY, "tasks.max": "1"},
        }
        self.get = mock.patch.object(apply_config.common, "get_connector_config",
                                     side_effect=lambda o, u, name, *a: dict(self.live[name])).start()
        self.put = mock.patch.object(apply_config.common, "put_connector_config").start()
        self.addCleanup(mock.patch.stopall)

    def test_apply_refuses_whole_run_if_any_entry_invalid(self):
        entries = [entry("good"), entry("bad", dr="")]
        code, out = run_quiet(apply_config.cmd_apply, entries, "primary-to-dr", None, CONN_CFG, "http://x")
        self.assertEqual(code, 1)
        self.assertIn("No connectors were changed", out)
        self.put.assert_not_called()
        self.get.assert_not_called()

    def test_apply_puts_full_config_with_only_key_changed(self):
        code, _ = run_quiet(apply_config.cmd_apply, [entry("good")], "primary-to-dr", None, CONN_CFG, "http://x")
        self.assertIn(code, (0, None))
        self.put.assert_called_once()
        sent = self.put.call_args[0][-1]
        self.assertEqual(sent, {"connector.class": "X", "connection.url": DR, "tasks.max": "1"})

    def test_apply_skips_when_already_at_target(self):
        self.live["good"]["connection.url"] = DR
        code, out = run_quiet(apply_config.cmd_apply, [entry("good")], "primary-to-dr", None, CONN_CFG, "http://x")
        self.assertIn(code, (0, None))
        self.put.assert_not_called()
        self.assertIn("[SKIP]", out)

    def test_validate_flags_invalid_entry(self):
        entries = [entry("good"), entry("bad", dr="")]
        code, out = run_quiet(apply_config.cmd_validate, entries, "primary-to-dr", None, CONN_CFG, "http://x")
        self.assertEqual(code, 1)
        self.assertIn("[INVALID] bad", out)
        self.assertIn("[OK]    good", out)


class FilterTests(unittest.TestCase):
    ENTRIES = [entry("a", groups=["retail"]), entry("b", groups=["tier1"]), entry("c")]

    def names(self, conns, groups):
        with redirect_stderr(io.StringIO()):
            return [e["name"] for e in apply_config.filter_connectors(self.ENTRIES, conns, groups)]

    def test_all(self):
        self.assertEqual(self.names("ALL", ""), ["a", "b", "c"])

    def test_all_plus_group_means_group_only(self):
        self.assertEqual(self.names("ALL", "retail"), ["a"])

    def test_union(self):
        self.assertEqual(self.names("c", "tier1"), ["b", "c"])

    def test_no_match(self):
        self.assertEqual(self.names("zzz", ""), [])


if __name__ == "__main__":
    unittest.main()
