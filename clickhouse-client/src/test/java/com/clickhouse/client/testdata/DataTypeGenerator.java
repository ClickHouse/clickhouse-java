package com.clickhouse.client.testdata;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Describes a ClickHouse data type for test data generation: the type expression, the values
 * of the type as Java objects and how each value is written as a SQL literal.
 *
 * <p>Generated values are client independent and use the following Java representation:
 * <ul>
 * <li>Int8 - {@code Byte}, UInt8 and Int16 - {@code Short}, UInt16 and Int32 - {@code Integer},
 * UInt32 and Int64 - {@code Long}, UInt64 and wider integers - {@code BigInteger}</li>
 * <li>Float32 - {@code Float}, Float64 - {@code Double}, Decimal - {@code BigDecimal} with the type scale</li>
 * <li>Bool - {@code Boolean}, String and FixedString - {@code String} (binary strings - {@code byte[]}),
 * FixedString values are always padded with {@code '\0'} to the full length</li>
 * <li>Date and Date32 - {@code LocalDate}, Time and Time64 - signed {@code Duration},
 * DateTime and DateTime64 - {@code ZonedDateTime} in the column timezone</li>
 * <li>IPv4 - {@code Inet4Address}, IPv6 - {@code Inet6Address} (IPv4-mapped addresses stay {@code Inet6Address}),
 * UUID - {@code UUID}, Enum8 and Enum16 - {@code String} name of the constant</li>
 * <li>Interval - {@code Period} (day and longer units) or {@code Duration}</li>
 * <li>Point - {@code double[]}, Ring and LineString - {@code double[][]}, Polygon and MultiLineString -
 * {@code double[][][]}, MultiPolygon - {@code double[][][][]}</li>
 * <li>Array, Tuple and Nested - {@code List}, Map - {@code LinkedHashMap}, NULL - {@code null}</li>
 * </ul>
 */
public interface DataTypeGenerator {

    /**
     * @return ClickHouse type expression, for example {@code Nullable(Decimal(18, 4))}
     */
    String getType();

    /**
     * Generates values of the type. Implementations must only use the given random source so that
     * the same seed produces the same values.
     *
     * @param random source of randomness
     * @return non-empty list of values, may contain {@code null} for nullable types
     */
    List<Object> generate(Random random);

    /**
     * @param value one of the generated values
     * @return SQL literal (or constant expression) usable in a {@code VALUES} clause
     */
    String toSqlLiteral(Object value);

    /**
     * Text values are byte strings: each char is one byte (ISO-8859-1), so strings are UTF-8 encoded and
     * binary strings are written as is.
     *
     * @param value one of the generated values
     * @return value as ClickHouse writes it in a {@code TabSeparated} column: escaped, {@code \N} for {@code null}
     */
    String toTsv(Object value);

    /**
     * @param value one of the generated values
     * @return value as ClickHouse writes it inside composite types in text formats, for example as an array
     *         element: strings are quoted, {@code NULL} for {@code null}; a byte string like {@link #toTsv}
     */
    String toQuotedText(Object value);

    /**
     * @return server settings required to create or insert into a column of this type
     */
    default Map<String, String> getRequiredSettings() {
        return Collections.emptyMap();
    }

    /**
     * @return {@code false} when the type cannot be used as a table column (Interval, Nothing)
     */
    default boolean isStorable() {
        return true;
    }
}
