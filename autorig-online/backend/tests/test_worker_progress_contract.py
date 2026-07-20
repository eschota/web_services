"""Regression tests for worker terminal-line parsing."""

import unittest

from worker_progress_contract import (
    latest_terminal_failure_reason,
    terminal_failure_reason_from_line,
)


class WorkerProgressContractTests(unittest.TestCase):
    def test_nested_failure_in_warning_is_not_terminal(self):
        line = (
            "WARNING: Pose preparation failed; switched to SOSISKA fallback. "
            "Reason: FAILURE: After 6 attempts, rig creation failed"
        )
        self.assertIsNone(terminal_failure_reason_from_line(line))
        self.assertIsNone(latest_terminal_failure_reason(line))

    def test_dedicated_failure_line_is_terminal(self):
        text = "working\n  FAILURE: invalid model buffer\n"
        self.assertEqual("invalid model buffer", latest_terminal_failure_reason(text))

    def test_worker_timestamp_prefix_preserves_marker_anchoring(self):
        self.assertEqual(
            "invalid buffer",
            terminal_failure_reason_from_line(
                "2026-07-10 15:32:58.141 FAILURE: invalid buffer"
            ),
        )
        self.assertIsNone(
            terminal_failure_reason_from_line(
                "2026-07-10 15:32:58.141 WARNING: fallback: FAILURE: nested"
            )
        )

    def test_bom_and_legacy_terminal_prefixes_are_supported(self):
        self.assertEqual(
            "fatal import",
            terminal_failure_reason_from_line("\ufeffFATAL: fatal import"),
        )
        self.assertEqual("Unknown failure", terminal_failure_reason_from_line("ERROR:"))

    def test_latest_terminal_line_wins(self):
        text = "FAILURE: first\r\nWARNING: recovered\r\nFAILURE: final"
        self.assertEqual("final", latest_terminal_failure_reason(text))


if __name__ == "__main__":
    unittest.main()
