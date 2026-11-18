#!/bin/sh
# Updates the project version across the repository.
#
# Run from the repository root:
#   ./scripts/set_version.sh <release-version>
#
# Arguments:
#   <release-version>  Version without "-SNAPSHOT" suffix (e.g. 0.9.5 or 0.12.0-rc1).
#
# Updates:
#   - VERSION: sets to <release-version>
#   - pom.xml: sets <clickhouse-java.version> to <release-version>
#   - examples/**/pom.xml: sets <clickhouse-java.version> to <release-version>-SNAPSHOT
#   - performance/**/pom.xml: sets <ch.jdbc.revision> to <release-version>-SNAPSHOT

echo $1
RELEASE_VERSION=$1
if [ -z "$RELEASE_VERSION" ]; then
  echo "Usage: $0 <release-version>"
  exit 1
fi
echo "Release version: $RELEASE_VERSION"

# write version to VERSION file (consumed by the release workflow)
printf '%s\n' "$RELEASE_VERSION" > VERSION

# update version in main pom.xml 
sed -i "s|<clickhouse-java.version>.*<\/clickhouse-java.version>|<clickhouse-java.version>${RELEASE_VERSION}<\/clickhouse-java.version>|g" pom.xml

# udpate examples with new version 
find ./examples/ -type f -name "pom.xml" -exec sed -i "s|<clickhouse-java.version>.*<\/clickhouse-java.version>|<clickhouse-java.version>${RELEASE_VERSION}-SNAPSHOT<\/clickhouse-java.version>|g" '{}' \;
find ./performance/ -type f -name "pom.xml" -exec sed -i "s|<ch.jdbc.revision>.*<\/ch.jdbc.revision>|<ch.jdbc.revision>${RELEASE_VERSION}-SNAPSHOT<\/ch.jdbc.revision>|g" '{}' \;

