#!/usr/bin/env python3
"""Check relative Markdown links in maintained repository documentation."""

from __future__ import print_function

import argparse
import re
import sys
from pathlib import Path
from urllib.parse import unquote


LINK_PATTERN = re.compile(r"!?\[[^\]]*\]\(([^)\s]+)(?:\s+['\"][^)]*['\"])?\)")
FENCE_PATTERN = re.compile(r"^\s*(```|~~~)")
IGNORED_PREFIXES = ("#", "/", "http://", "https://", "mailto:", "tel:", "data:")


def maintained_files(root):
    candidates = [
        root / "README.md",
        root / "CONTRIBUTING.md",
        root / ".github" / "pull_request_template.md",
    ]
    for docs_root in (root / "docs",):
        if docs_root.exists():
            candidates.extend(docs_root.rglob("*.md"))
            candidates.extend(docs_root.rglob("*.mdx"))
    return sorted(path for path in candidates if path.is_file())


def links_in(path):
    in_fence = False
    with path.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if FENCE_PATTERN.match(line):
                in_fence = not in_fence
                continue
            if in_fence:
                continue
            for match in LINK_PATTERN.finditer(line):
                yield line_number, match.group(1).strip("<>")


def check_file(root, path):
    failures = []
    for line_number, destination in links_in(path):
        if destination.startswith(IGNORED_PREFIXES):
            continue
        destination = unquote(destination.split("#", 1)[0].split("?", 1)[0])
        if not destination:
            continue
        target = (path.parent / destination).resolve()
        if not target.exists():
            failures.append(
                "{}:{}: {} -> {}".format(
                    path.relative_to(root), line_number, destination, target
                )
            )
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="*", help="Markdown files to check")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    files = [Path(name).resolve() for name in args] if args.files else maintained_files(root)
    failures = []
    for path in files:
        failures.extend(check_file(root, path))

    if failures:
        print("Broken relative documentation links:", file=sys.stderr)
        for failure in failures:
            print("  " + failure, file=sys.stderr)
        return 1

    print("Checked {} documentation files; relative links are valid.".format(len(files)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
