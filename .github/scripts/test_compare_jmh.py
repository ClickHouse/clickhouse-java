#!/usr/bin/env python3
"""End-to-end tests for `compare-jmh.py`.

Runs `compare-jmh.py` against every scenario in `test_data/` via
subprocess and asserts on both the rendered markdown and the
`--summary-output` counters. Designed to be run locally
(`python3 .github/scripts/test_compare_jmh.py`) and from CI without
extra dependencies — only the standard library is used.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Dict, List

HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "compare-jmh.py"
DATA = HERE / "test_data"


def _parse_summary(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def _run_compare(
    case: str,
    *,
    threshold: float = 10.0,
    extra_args: List[str] | None = None,
) -> Dict[str, object]:
    """Invoke compare-jmh.py against a fixture; return its outputs.

    Returns a dict with: returncode, stdout, stderr, markdown, summary.
    """
    case_dir = DATA / case
    assert case_dir.is_dir(), f"missing test fixture: {case_dir}"

    tmp = Path(tempfile.mkdtemp(prefix=f"cmp-jmh-{case}-"))
    try:
        out_md = tmp / "out.md"
        summary = tmp / "summary.env"
        cmd = [
            sys.executable,
            str(SCRIPT),
            "--baseline",
            str(case_dir / "baseline"),
            "--current",
            str(case_dir / "current"),
            "--threshold-pct",
            str(threshold),
            "--output",
            str(out_md),
            "--summary-output",
            str(summary),
        ]
        if extra_args:
            cmd.extend(extra_args)
        proc = subprocess.run(cmd, capture_output=True, text=True)
        result: Dict[str, object] = {
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "markdown": out_md.read_text(encoding="utf-8") if out_md.exists() else "",
            "summary": _parse_summary(summary) if summary.exists() else {},
        }
        return result
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# Pre-computed marker strings — keep in sync with compare-jmh.py.
ARROW_REGRESS = "\u2b07\ufe0f"
ARROW_IMPROVE = "\u2b06\ufe0f"
ARROW_NOISE = "\u2796"
DOT_REGRESS = "\U0001f534"
DOT_IMPROVE = "\U0001f7e2"


class CompareJmhTest(unittest.TestCase):
    """Each case in test_data/ has a dedicated test asserting on the
    headline shape, the bullet markers, and the summary counters."""

    def test_all_improvements(self) -> None:
        r = _run_compare("all_improvements")
        self.assertEqual(r["returncode"], 0)
        md = r["markdown"]
        self.assertIn("no regressions", md)
        self.assertIn(ARROW_IMPROVE, md)
        self.assertIn(DOT_IMPROVE, md)
        self.assertNotIn(ARROW_REGRESS, md)
        self.assertNotIn(DOT_REGRESS, md)
        s = r["summary"]
        self.assertEqual(s.get("regressions"), "0")
        self.assertEqual(s.get("improvements"), "2")
        self.assertEqual(s.get("matched"), "2")

    def test_all_regressions(self) -> None:
        r = _run_compare("all_regressions")
        self.assertEqual(r["returncode"], 0)
        md = r["markdown"]
        self.assertIn("regression(s) over", md)
        self.assertIn(ARROW_REGRESS, md)
        self.assertIn(DOT_REGRESS, md)
        # Bold markdown around the metric label.
        self.assertIn("**Time", md)
        s = r["summary"]
        self.assertEqual(s.get("regressions"), "2")
        # No row "purely improved" → improvements should be 0.
        self.assertEqual(s.get("improvements"), "0")

    def test_mixed(self) -> None:
        r = _run_compare("mixed")
        self.assertEqual(r["returncode"], 0)
        md = r["markdown"]
        # Both directions present.
        self.assertIn(ARROW_REGRESS, md)
        self.assertIn(ARROW_IMPROVE, md)
        self.assertIn(DOT_REGRESS, md)
        self.assertIn(DOT_IMPROVE, md)
        # Discriminator suffix `[limit=…]` appears for the two
        # `queryV2` variants of the same benchmark.
        self.assertIn("[limit=10000]", md)
        self.assertIn("[limit=100000]", md)
        # The unique-named benchmark must NOT get a discriminator.
        self.assertNotIn("`JDBCQuery.selectJDBCV2` `[", md)
        s = r["summary"]
        self.assertEqual(s.get("matched"), "4")
        self.assertGreater(int(s.get("regressions", "0")), 0)

    def test_no_alloc(self) -> None:
        # No `gc.alloc.rate.norm` present anywhere → script must
        # still compare Time and emit a diagnostic on stderr.
        r = _run_compare("no_alloc")
        self.assertEqual(r["returncode"], 0)
        self.assertIn("no `gc.alloc.rate.norm`", r["stderr"])
        md = r["markdown"]
        # Time regression should still be detected and 🔴-tagged…
        self.assertIn(DOT_REGRESS, md)
        # …but no Alloc/op metric is ever 🟢/🔴 because we have no
        # baseline/current data for it.
        self.assertIn("Alloc/op", md)
        # The detail-table cell should fall back to "(—)" for alloc.
        self.assertIn("Alloc/op — → — (—)", md)
        s = r["summary"]
        self.assertEqual(s.get("matched"), "2")

    def test_noise_only(self) -> None:
        r = _run_compare("noise_only")
        self.assertEqual(r["returncode"], 0)
        md = r["markdown"]
        self.assertIn("no changes over 10%", md)
        # Every row should be on the noise arrow…
        self.assertIn(ARROW_NOISE, md)
        # …and there should be no red/green dots anywhere.
        self.assertNotIn(DOT_REGRESS, md)
        # 🟢 *is* in the header for the OK case, so don't assert on
        # DOT_IMPROVE alone.
        s = r["summary"]
        self.assertEqual(s.get("regressions"), "0")
        self.assertEqual(s.get("improvements"), "0")

    def test_only_in_pr(self) -> None:
        r = _run_compare("only_in_pr")
        self.assertEqual(r["returncode"], 0)
        md = r["markdown"]
        self.assertIn("Benchmarks only in PR run", md)
        self.assertIn("QueryClient.queryV3New", md)
        s = r["summary"]
        # one shared row matched.
        self.assertEqual(s.get("matched"), "1")

    def test_only_in_baseline(self) -> None:
        r = _run_compare("only_in_baseline")
        self.assertEqual(r["returncode"], 0)
        md = r["markdown"]
        self.assertIn("Benchmarks only in baseline run", md)
        self.assertIn("QueryClient.queryV0Removed", md)
        s = r["summary"]
        self.assertEqual(s.get("matched"), "1")

    def test_empty_intersection(self) -> None:
        r = _run_compare("empty_intersection")
        self.assertEqual(r["returncode"], 0)
        md = r["markdown"]
        self.assertIn("_No benchmarks matched between baseline and PR._", md)
        # Both unique-side sections still appear as <details>.
        self.assertIn("Benchmarks only in PR run", md)
        self.assertIn("Benchmarks only in baseline run", md)
        s = r["summary"]
        self.assertEqual(s.get("matched"), "0")
        self.assertEqual(s.get("regressions"), "0")
        self.assertEqual(s.get("improvements"), "0")

    def test_threshold_knob(self) -> None:
        # The same fixture flips from "regression" to "ok" when the
        # threshold is widened past the largest delta.
        strict = _run_compare("all_regressions", threshold=10.0)
        lenient = _run_compare("all_regressions", threshold=200.0)
        self.assertGreater(int(strict["summary"]["regressions"]), 0)
        self.assertEqual(lenient["summary"]["regressions"], "0")
        self.assertIn("no changes", lenient["markdown"])


if __name__ == "__main__":
    # `-v` prints each scenario name so failures are obvious in CI logs.
    unittest.main(verbosity=2)
