#!/usr/bin/env python3
"""Verify and stage the GitHub release artifact set."""

import argparse
import hashlib
import shutil
import sys
from pathlib import Path


MODULES = (
    "clickhouse-client",
    "clickhouse-data",
    "clickhouse-http-client",
    "clickhouse-jdbc",
    "clickhouse-r2dbc",
    "client-v2",
    "jdbc-v2",
    "packages/clickhouse-jdbc-all",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", type=Path, default=Path("release-assets"))
    args = parser.parse_args()

    candidates = []
    missing = []
    for module in MODULES:
        target = Path(module) / "target"
        main_jar = target / "{}-{}.jar".format(Path(module).name, args.version)
        if not main_jar.is_file():
            missing.append(str(main_jar))
        artifact_prefix = "{}-{}".format(Path(module).name, args.version)
        candidates.extend(
            path
            for path in sorted(target.glob("*.jar"))
            if path.name.startswith(artifact_prefix)
            and not path.name.startswith(artifact_prefix + "-SNAPSHOT")
        )

    jdbc_bundle = Path("clickhouse-jdbc/target/bundle")
    candidates.extend(
        path
        for path in sorted(
            jdbc_bundle.glob("clickhouse-jdbc-{}*.jar".format(args.version))
        )
        if not path.name.startswith(
            "clickhouse-jdbc-{}-SNAPSHOT".format(args.version)
        )
    )

    sboms = [Path("target/bom.json"), Path("target/bom.xml")]
    for sbom in sboms:
        if not sbom.is_file():
            missing.append(str(sbom))
        else:
            candidates.append(sbom)

    if missing:
        for path in missing:
            print("missing required release artifact: " + path, file=sys.stderr)
        return 1

    if args.output.exists() and any(args.output.iterdir()):
        print("release asset output is not empty: " + str(args.output), file=sys.stderr)
        return 1
    args.output.mkdir(parents=True, exist_ok=True)
    copied = {}
    for source in candidates:
        destination = args.output / source.name
        if destination.name in copied and copied[destination.name] != source:
            print(
                "duplicate release asset name: {} and {}".format(
                    copied[destination.name], source
                ),
                file=sys.stderr,
            )
            return 1
        shutil.copy2(str(source), str(destination))
        copied[destination.name] = source

    manifest = args.output / "SHA256SUMS"
    lines = [
        "{}  {}".format(sha256(path), path.name)
        for path in sorted(args.output.iterdir())
        if path.name != manifest.name
    ]
    manifest.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("staged {} release assets in {}".format(len(lines), args.output))
    return 0


if __name__ == "__main__":
    sys.exit(main())
