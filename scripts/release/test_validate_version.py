#!/usr/bin/env python3

import tempfile
import unittest
from pathlib import Path

from validate_version import validate


POM = """\
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <properties><revision>{}</revision></properties>
</project>
"""


class ValidateVersionTest(unittest.TestCase):
    def validate_fixture(self, version, revision, release=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            version_file = root / "VERSION"
            pom_file = root / "pom.xml"
            version_file.write_text(version + "\n", encoding="utf-8")
            pom_file.write_text(POM.format(revision), encoding="utf-8")
            return validate(version_file, pom_file, release)

    def test_development_revision_matches_release_version(self):
        self.assertEqual(
            [], self.validate_fixture("1.2.3-rc1", "1.2.3-rc1-SNAPSHOT")
        )

    def test_release_revision_must_not_be_snapshot(self):
        self.assertEqual([], self.validate_fixture("1.2.3", "1.2.3", release=True))
        self.assertTrue(
            self.validate_fixture("1.2.3", "1.2.3-SNAPSHOT", release=True)
        )

    def test_rejects_snapshot_in_version_file(self):
        self.assertTrue(
            self.validate_fixture("1.2.3-SNAPSHOT", "1.2.3-SNAPSHOT")
        )

    def test_rejects_revision_drift(self):
        self.assertTrue(self.validate_fixture("1.2.3", "1.2.4-SNAPSHOT"))


if __name__ == "__main__":
    unittest.main()
