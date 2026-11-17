#!/usr/bin/env bash
#
# Generates a test data set for every type supported by GenerateTestDataSet into
# clickhouse-client/src/test/resources/datasets, one file per type:
#   <type>.sql  - SET statements for required settings, CREATE TABLE and INSERT (storable types)
#   <type>.txt  - rows as SQL tuples only (Interval and Nothing types, which cannot be stored in a table)
#   <type>.tsv  - the same rows in the TabSeparated format, exactly as ClickHouse writes them, for example
#                 the output of `SELECT * FROM test_data ORDER BY id FORMAT TSV`
#   index.tsv   - tab-separated index (name, type, clickhouse_type, kind) of the generated data sets
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

generate() {
    local name="$1" type="$2" format="$3" extension="$4"
    local file="${OUTPUT_DIR}/${name}.${extension}"
    java -cp "${CLASSES_DIR}" "${MAIN_CLASS}" "${SEED_ARGS[@]+"${SEED_ARGS[@]}"}" \
        --format "${format}" --output "${file}" "value ${type}"
}

if [[ "${SKIP_BUILD}" == false ]]; then
    (cd "${ROOT_DIR}" && mvn -q -pl clickhouse-client test-compile)
fi

mkdir -p "${OUTPUT_DIR}"
rm -f "${OUTPUT_DIR}"/*.sql "${OUTPUT_DIR}"/*.txt "${OUTPUT_DIR}"/*.tsv

INDEX_FILE="${OUTPUT_DIR}/index.tsv"
printf 'name\ttype\tclickhouse_type\tkind\n' > "${INDEX_FILE}"

count=0
while IFS=$'\t' read -r name type clickhouse_type kind; do
    if [[ "${kind}" == "storable" ]]; then
        generate "${name}" "${type}" sql sql
    else
        generate "${name}" "${type}" values txt
    fi
    generate "${name}" "${type}" tsv tsv
    printf '%s\t%s\t%s\t%s\n' "${name}" "${type}" "${clickhouse_type}" "${kind}" >> "${INDEX_FILE}"
    count=$((count + 1))
done < <(java -cp "${CLASSES_DIR}" "${MAIN_CLASS}" --list-index)

echo "Generated ${count} data sets in ${OUTPUT_DIR}"
