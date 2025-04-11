#!/bin/sh

echo $1
RELEASE_VERSION=$1
if [ -z "$RELEASE_VERSION" ]; then
  echo "Usage: $0 <release-version>"
  exit 1
fi
echo "Release version: $RELEASE_VERSION"

# update version in main pom.xml 
sed -i "s|<clickhouse-java.version>.*<\/clickhouse-java.version>|<clickhouse-java.version>${RELEASE_VERSION}<\/clickhouse-java.version>|g" pom.xml

# udpate examples with new version 
find ./examples/ -type f -name "pom.xml" -exec sed -i "s|<clickhouse-java.version>.*<\/clickhouse-java.version>|<clickhouse-java.version>${RELEASE_VERSION}-SNAPSHOT<\/clickhouse-java.version>|g" '{}' \;
find ./examples/ -type f -name "gradle.properties" -exec sed -i "s|^ch_java_client_version=.*$|ch_java_client_version=${RELEASE_VERSION}|g" '{}' \;

