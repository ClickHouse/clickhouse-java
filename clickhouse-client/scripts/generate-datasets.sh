#!/usr/bin/env bash
#
# Generates a test data set for every type supported by GenerateTestDataSet into
# clickhouse-client/src/test/resources/datasets, one file per type:
#   <type>.sql  - SET statements for required settings, CREATE TABLE and INSERT (storable types)
#   <type>.txt  - rows as SQL tuples only (Interval and Nothing types, which cannot be stored in a table)
#   <type>.tsv  - the same rows in the TabSeparated format, exactly as ClickHouse writes them, for example
#                 the output of `SELECT * FROM test_data ORDER BY id FORMAT TSV`
#
# Usage: clickhouse-client/scripts/generate-datasets.sh [--skip-build] [--seed <long>]
#   --skip-build  do not run `mvn test-compile`, use already compiled test classes
#   --seed        random seed passed to the generator, its default seed otherwise

set -euo pipefail

MODULE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DIR="$(cd "${MODULE_DIR}/.." && pwd)"
OUTPUT_DIR="${MODULE_DIR}/src/test/resources/datasets"
CLASSES_DIR="${MODULE_DIR}/target/test-classes"
MAIN_CLASS="com.clickhouse.client.testdata.GenerateTestDataSet"

SKIP_BUILD=false
SEED_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-build) SKIP_BUILD=true; shift ;;
        --seed) SEED_ARGS=(--seed "$2"); shift 2 ;;
        -h|--help) sed -n '2,12p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

# Array(Nullable(String)) -> array_nullable_string, DateTime64(3, 'UTC') -> datetime64_3_utc
file_name() {
    echo "$1" | tr '[:upper:]' '[:lower:]' | sed -e 's/[^a-z0-9]\{1,\}/_/g' -e 's/^_//' -e 's/_$//'
}

generate() {
    local type="$1" format="$2" extension="$3"
    local file
    file="${OUTPUT_DIR}/$(file_name "${type}").${extension}"
    java -cp "${CLASSES_DIR}" "${MAIN_CLASS}" "${SEED_ARGS[@]+"${SEED_ARGS[@]}"}" \
        --format "${format}" --output "${file}" "value ${type}"
}

if [[ "${SKIP_BUILD}" == false ]]; then
    (cd "${ROOT_DIR}" && mvn -q -pl clickhouse-client test-compile)
fi

# Type lists are defined in GenerateTestDataSet, so tests can use the same types
mapfile -t STORABLE_TYPES < <(java -cp "${CLASSES_DIR}" "${MAIN_CLASS}" --list-types storable)
mapfile -t VALUES_ONLY_TYPES < <(java -cp "${CLASSES_DIR}" "${MAIN_CLASS}" --list-types values-only)

mkdir -p "${OUTPUT_DIR}"
rm -f "${OUTPUT_DIR}"/*.sql "${OUTPUT_DIR}"/*.txt "${OUTPUT_DIR}"/*.tsv

for type in "${STORABLE_TYPES[@]}"; do
    generate "${type}" sql sql
    generate "${type}" tsv tsv
done
for type in "${VALUES_ONLY_TYPES[@]}"; do
    generate "${type}" values txt
    generate "${type}" tsv tsv
done

echo "Generated $(ls "${OUTPUT_DIR}"/*.tsv | wc -l) data sets in ${OUTPUT_DIR}"
