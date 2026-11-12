package com.clickhouse.client.datatypes.readers;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.clickhouse.client.datatypes.readers.KnownIssue.Deviation.MISSING_EXCEPTION;
import static com.clickhouse.client.datatypes.readers.KnownIssue.Deviation.READ_FAILURE;
import static com.clickhouse.client.datatypes.readers.KnownIssue.Deviation.UNEXPECTED_EXCEPTION;
import static com.clickhouse.client.datatypes.readers.KnownIssue.Deviation.WRONG_EXCEPTION;
import static com.clickhouse.client.datatypes.readers.KnownIssue.Deviation.WRONG_VALUE;
import static com.clickhouse.client.datatypes.readers.ReaderAccessor.*;
import static com.clickhouse.data.ClickHouseFormat.Native;

/**
 * Known deviations of the binary readers from {@link ReaderExpectations}. Remove an entry once the reader is fixed;
 * {@link AbstractBinaryReaderDataSetTest#testKnownIssuesAreCurrent()} fails for entries that do not happen anymore.
 */
final class ReaderKnownIssues {

    private static final String INTEGERS = "U?Int(8|16|32|64|128|256)|Nullable\\(Int32\\)";
    private static final String FLOATS = "Float(32|64)";
    private static final String DECIMALS = "(Nullable\\()?Decimal.*";
    private static final String NUMBERS = INTEGERS + "|" + FLOATS + "|" + DECIMALS;
    private static final String ENUMS = "Enum(8|16)";
    private static final String STRINGS = "(Nullable\\(|LowCardinality\\()*(Binary)?String\\)*";
    private static final String NULLABLE = "(LowCardinality\\()?Nullable\\(.*";
    private static final String TIME = "Time(64\\(\\d\\))?";
    private static final String GEO = "Point|Ring|LineString|Polygon|MultiLineString|MultiPolygon";
    private static final String NATIVE_COMPOSITES = "LowCardinality\\(.*|Array\\((Nullable|Array|LowCardinality).*"
            + "|Tuple\\(.*|Map\\(.*|Nested\\(.*";

    static final List<KnownIssue> ALL = Collections.unmodifiableList(Arrays.asList(
            // invalid conversions throw other exceptions than ClientException
            KnownIssue.of(WRONG_EXCEPTION, "accessor casts the value without checking the column type")
                    .throwing(ClassCastException.class)
                    .accessors(UUID, TUPLE, INET4_ADDRESS, INET6_ADDRESS, GEO_POINT, GEO_RING, GEO_POLYGON,
                            GEO_MULTI_POLYGON, ENUM8, ENUM16, CLICKHOUSE_BITMAP),
            KnownIssue.of(WRONG_EXCEPTION, "integer overflow is not wrapped into ClientException")
                    .throwing(ArithmeticException.class).accessors(BYTE, SHORT, INTEGER, LONG)
                    .types(NUMBERS + "|" + ENUMS),
            KnownIssue.of(WRONG_EXCEPTION, "a string that is not a number is not wrapped into ClientException")
                    .throwing(NumberFormatException.class)
                    .accessors(BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BIG_INTEGER, BIG_DECIMAL).types(STRINGS),
            KnownIssue.of(WRONG_EXCEPTION, "NaN and infinity are not wrapped into ClientException")
                    .throwing(NumberFormatException.class).accessors(BIG_DECIMAL).types(FLOATS),
            KnownIssue.of(WRONG_EXCEPTION, "IPv6 address that is not IPv4-mapped is not wrapped into ClientException")
                    .throwing(IllegalArgumentException.class).accessors(INET4_ADDRESS).types("IPv6"),
            KnownIssue.of(MISSING_EXCEPTION, "invalid accessors return null for NULL instead of throwing")
                    .types(NULLABLE).onNullOnly(),

            // lossy conversions return a value instead of throwing ClientException
            KnownIssue.of(MISSING_EXCEPTION, "integer accessors truncate fractions and wrap overflowing values, "
                    + "the overflow check compares already truncated values")
                    .accessors(BYTE, SHORT, INTEGER, LONG, BIG_INTEGER).types(NUMBERS),
            KnownIssue.of(MISSING_EXCEPTION, "getBoolean returns true for any number other than 0 and false for "
                    + "any string other than 'true'").accessors(BOOLEAN),
            KnownIssue.of(MISSING_EXCEPTION, "getFloat returns infinity for values out of the float range")
                    .accessors(FLOAT).types("Float64|Int256|UInt128|UInt256"),
            KnownIssue.of(MISSING_EXCEPTION, "getEnum8 truncates Enum16 values").accessors(ENUM8).types("Enum16"),
            KnownIssue.of(MISSING_EXCEPTION, "getTuple returns geo values as arrays").accessors(TUPLE).types(GEO),
            KnownIssue.of(MISSING_EXCEPTION, "getLocalTime wraps Time values that are negative or 24 hours and more")
                    .accessors(LOCAL_TIME).types(TIME),

            // valid conversions return wrong values
            KnownIssue.of(WRONG_VALUE, "getBigInteger converts large values through long")
                    .accessors(BIG_INTEGER).types(FLOATS + "|Decimal\\(38, 0\\)"),
            KnownIssue.of(WRONG_VALUE, "negative Time64 values with a fraction are one second off")
                    .accessors(DURATION, TEMPORAL_AMOUNT, INSTANT, LOCAL_DATE_TIME).types("Time64\\(\\d\\)"),
            KnownIssue.of(WRONG_VALUE, "IPv4-mapped IPv6 values are read as 4 byte IPv4 addresses")
                    .accessors(BYTE_ARRAY).types("IPv6"),
            KnownIssue.of(UNEXPECTED_EXCEPTION, "getBigDecimal parses the enum name as a number")
                    .throwing(NumberFormatException.class).accessors(BIG_DECIMAL).types(ENUMS),
            KnownIssue.of(UNEXPECTED_EXCEPTION, "getInstant throws NullPointerException for NULL")
                    .throwing(NullPointerException.class).accessors(INSTANT).types(NULLABLE).onNullOnly(),

            // getString differs from the text ClickHouse writes
            KnownIssue.of(WRONG_VALUE, "getString formats floats with Double.toString, for example 0.0 and 1.0E-7")
                    .accessors(STRING).types(FLOATS + "|" + GEO),
            KnownIssue.of(WRONG_VALUE, "getString keeps trailing zeros of decimals, for example 0.000000 and 0E-38")
                    .accessors(STRING).types(DECIMALS),
            KnownIssue.of(WRONG_VALUE, "getString wraps Time values to a day and drops trailing zeros of Time64")
                    .accessors(STRING).types(TIME),
            KnownIssue.of(WRONG_VALUE, "getString drops trailing zeros of the DateTime64 fraction")
                    .accessors(STRING).types("(Nullable\\()?DateTime64.*"),
            KnownIssue.of(WRONG_VALUE, "getString does not compress IPv6 addresses, for example 0:0:0:0:0:0:0:0")
                    .accessors(STRING).types("IPv6"),
            KnownIssue.of(WRONG_VALUE, "getString separates array elements with ', '")
                    .accessors(STRING).types("Array\\(.*"),
            KnownIssue.of(WRONG_VALUE, "getString of Tuple, Map and Nested values is Java toString()")
                    .accessors(STRING).types("Tuple\\(.*|Map\\(.*|Nested\\(.*"),

            // Native format
            KnownIssue.of(READ_FAILURE, "Native reader decodes values row by row, so it cannot read types with "
                    + "column-oriented serialization").types(NATIVE_COMPOSITES).formats(Native),
            KnownIssue.of(WRONG_VALUE, "Native reader returns wrong values for types with column-oriented "
                    + "serialization before reading fails").types(NATIVE_COMPOSITES).formats(Native),
            KnownIssue.of(WRONG_VALUE, "Native reader ignores the DateTime column timezone and uses the JVM default")
                    .accessors(STRING, ZONED_DATE_TIME, OFFSET_DATE_TIME, LOCAL_DATE, LOCAL_DATE_TIME, LOCAL_TIME)
                    .types("DateTime\\('.*'\\)").formats(Native)
    ));

    private ReaderKnownIssues() {
    }
}
