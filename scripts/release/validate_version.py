#!/usr/bin/env python3
"""Validate VERSION against the Maven CI-friendly root revision."""

import argparse
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


VERSION_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z][0-9A-Za-z.-]*)?$")
MAVEN_NAMESPACE = {"m": "http://maven.apache.org/POM/4.0.0"}


def validate(version_file: Path, pom_file: Path, release: bool):
    errors = []
    version = version_file.read_text(encoding="utf-8").strip()
    if not VERSION_RE.fullmatch(version):
        errors.append(
            "VERSION must be a release version such as 1.2.3 or 1.2.3-rc1 "
            "(SNAPSHOT is not allowed)"
        )

    try:
        root = ET.parse(str(pom_file)).getroot()
    except ET.ParseError as exc:
        return errors + ["cannot parse {}: {}".format(pom_file, exc)]

    revision_node = root.find("m:properties/m:revision", MAVEN_NAMESPACE)
    if revision_node is None or revision_node.text is None:
        errors.append("root pom.xml must define properties/revision")
        return errors

    revision = revision_node.text.strip()
    expected = version if release else version + "-SNAPSHOT"
    if revision != expected:
        errors.append(
            "root revision is {!r}; expected {!r} from VERSION".format(
                revision, expected
            )
        )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version-file", type=Path, default=Path("VERSION"))
    parser.add_argument("--pom", type=Path, default=Path("pom.xml"))
    parser.add_argument(
        "--release",
        action="store_true",
        help="require an exact release revision instead of the development SNAPSHOT",
    )
    args = parser.parse_args()

    errors = validate(args.version_file, args.pom, args.release)
    if errors:
        for error in errors:
            print("version validation failed: " + error, file=sys.stderr)
        return 1

    mode = "release" if args.release else "development"
    print("VERSION and root revision are consistent ({})".format(mode))
    return 0


if __name__ == "__main__":
    sys.exit(main())
