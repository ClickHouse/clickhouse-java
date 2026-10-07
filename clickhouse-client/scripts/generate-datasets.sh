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

STORABLE_TYPES=(
    # Integers and floating point
    "Int8" "UInt8" "Int16" "UInt16" "Int32" "UInt32" "Int64" "UInt64"
    "Int128" "UInt128" "Int256" "UInt256"
    "Float32" "Float64"
    # Decimals
    "Decimal(9, 2)" "Decimal(18, 6)" "Decimal(38, 0)" "Decimal(76, 38)"
    "Decimal32(4)" "Decimal64(8)" "Decimal128(18)" "Decimal256(38)"
    # Bool and strings
    "Bool" "String" "BinaryString" "FixedString(1)" "FixedString(16)"
    # Dates and time
    "Date" "Date32" "Time" "Time64(3)" "Time64(9)"
    "DateTime('UTC')" "DateTime('America/New_York')"
    "DateTime64(0, 'UTC')" "DateTime64(3, 'UTC')" "DateTime64(6, 'Europe/Berlin')" "DateTime64(9, 'Asia/Kolkata')"
    # Network, identifiers and enums
    "IPv4" "IPv6" "UUID" "Enum8" "Enum16"
    # Geo
    "Point" "Ring" "LineString" "Polygon" "MultiLineString" "MultiPolygon"
    # Composite
    "Nullable(Int32)" "Nullable(String)" "Nullable(Decimal(18, 6))" "Nullable(DateTime64(3, 'UTC'))" "Nullable(UUID)"
    "LowCardinality(String)" "LowCardinality(Nullable(String))" "LowCardinality(FixedString(16))"
    "Array(Int32)" "Array(String)" "Array(Nullable(String))" "Array(Array(Int8))" "Array(LowCardinality(String))"
    "Tuple(Int32, String)" "Tuple(a Int64, b Nullable(String), c Array(UInt8))"
    "Map(String, Int32)" "Map(Int8, String)" "Map(String, Array(Nullable(Float64)))" "Map(UUID, Tuple(Int32, String))"
    "Nested(id UInt32, name String)"
)

VALUES_ONLY_TYPES=(
    "IntervalNanosecond" "IntervalMicrosecond" "IntervalMillisecond" "IntervalSecond" "IntervalMinute"
    "IntervalHour" "IntervalDay" "IntervalWeek" "IntervalMonth" "IntervalQuarter" "IntervalYear"
    "Nothing" "Array(Nullable(Nothing))"
)

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
