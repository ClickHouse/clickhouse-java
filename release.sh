#!/bin/sh
set -eu

RELEASE_VERSION=${1:-}
if [ -z "$RELEASE_VERSION" ]; then
  echo "Usage: $0 <release-version>" >&2
  exit 1
fi

case "$RELEASE_VERSION" in
  *-SNAPSHOT)
    echo "Release version must not contain -SNAPSHOT" >&2
    exit 1
    ;;
esac

echo "Preparing development revision for release: $RELEASE_VERSION"
printf '%s\n' "$RELEASE_VERSION" > VERSION

sed -i "s|<revision>.*</revision>|<revision>${RELEASE_VERSION}-SNAPSHOT</revision>|" pom.xml
find ./examples -type f -name pom.xml -exec \
  sed -i "s|<clickhouse-java.version>.*</clickhouse-java.version>|<clickhouse-java.version>${RELEASE_VERSION}-SNAPSHOT</clickhouse-java.version>|" '{}' \;
find ./performance -type f -name pom.xml -exec \
  sed -i "s|<ch.jdbc.revision>.*</ch.jdbc.revision>|<ch.jdbc.revision>${RELEASE_VERSION}-SNAPSHOT</ch.jdbc.revision>|" '{}' \;

python3 scripts/release/validate_version.py

