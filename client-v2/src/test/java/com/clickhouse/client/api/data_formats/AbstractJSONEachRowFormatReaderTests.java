package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

public abstract class AbstractJSONEachRowFormatReaderTests extends BaseIntegrationTest {

    /** Number of rows generated per primitive column. */
    private static final int ROW_COUNT = 5;

    /** Fixed seed so generated random values are stable across runs and parser implementations. */
    private static final long RANDOM_SEED = 0xC0FFEEL;

    /** Shared list of primitive cases, populated once per JVM and reused by every test class. */
    private static final List<PrimitiveTypeCase> PRIMITIVE_CASES =
            buildPrimitiveCases(new Random(RANDOM_SEED));

    protected Client client;

    private String primitivesTable;

    @BeforeClass(groups = {"integration"})
    public void setUpPrimitivesTable() throws Exception {
        primitivesTable = "test_json_each_row_primitives_" +
                getClass().getSimpleName().toLowerCase(Locale.ROOT);

        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        try (Client setupClient = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .build()) {

            setupClient.execute("DROP TABLE IF EXISTS " + primitivesTable).get().close();

            StringBuilder create = new StringBuilder("CREATE TABLE ")
                    .append(primitivesTable)
                    .append(" (id UInt32");
            for (PrimitiveTypeCase c : PRIMITIVE_CASES) {
                create.append(", ").append(c.columnName).append(' ').append(c.chType);
            }
            create.append(") ENGINE = MergeTree ORDER BY id");
            setupClient.execute(create.toString()).get().close();

            StringBuilder insert = new StringBuilder("INSERT INTO ")
                    .append(primitivesTable).append(" VALUES ");
            for (int row = 0; row < ROW_COUNT; row++) {
                if (row > 0) {
                    insert.append(", ");
                }
                insert.append('(').append(row);
                for (PrimitiveTypeCase c : PRIMITIVE_CASES) {
                    insert.append(", ").append(c.sqlLiterals.get(row));
                }
                insert.append(')');
            }
            setupClient.execute(insert.toString()).get().close();
        }
    }

    @AfterClass(groups = {"integration"})
    public void tearDownPrimitivesTable() throws Exception {
        if (primitivesTable == null) {
            return;
        }
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        try (Client teardownClient = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .build()) {
            teardownClient.execute("DROP TABLE IF EXISTS " + primitivesTable).get().close();
        }
    }

    @BeforeMethod(groups = {"integration"})
    public void setUp() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .build();
    }

    @AfterMethod(groups = {"integration"})
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    private QuerySettings newJsonEachRowSettings() {
        return new QuerySettings()
                .setFormat(ClickHouseFormat.JSONEachRow);
    }

    /**
     * Settings used by the primitive accessor tests. Integer and floating-point values
     * are returned unquoted so they materialise as {@code Number} instances (whose
     * {@code longValue}/{@code doubleValue} calls match the typed accessors).
     * Decimal values are kept quoted (the ClickHouse default) so that JSON parsers
     * that materialise unquoted JSON numbers as {@code Double} (e.g. Jackson with
     * default settings) do not lose precision on large {@code Decimal} values.
     */
    private QuerySettings newJsonEachRowSettingsForPrimitives() {
        return newJsonEachRowSettings()
                .serverSetting("output_format_json_quote_64bit_integers", "0")
                .serverSetting("output_format_json_quote_64bit_floats", "0")
                .serverSetting("output_format_json_quote_decimals", "1");
    }

    protected abstract ClickHouseTextFormatReader createReader(QueryResponse response) throws IOException;

    // ------------------------------------------------------------------
    // Parameterized primitive value tests
    // ------------------------------------------------------------------

    @DataProvider(name = "primitiveTypeCases")
    public Object[][] primitiveTypeCases() {
        Object[][] rows = new Object[PRIMITIVE_CASES.size()][1];
        for (int i = 0; i < PRIMITIVE_CASES.size(); i++) {
            rows[i][0] = PRIMITIVE_CASES.get(i);
        }
        return rows;
    }

    /**
     * Runs the same assertions for every primitive type by reading the column populated
     * in {@link #setUpPrimitivesTable()}. For each row it asserts that the value can be
     * read via the type-appropriate accessor (both by name and by index) and that a set
     * of accessors that cannot convert the value throw an exception.
     */
    @Test(groups = {"integration"}, dataProvider = "primitiveTypeCases")
    public void testPrimitiveTypeAccessors(PrimitiveTypeCase tc) throws Exception {
        String sql = "SELECT " + tc.columnName + " FROM " + primitivesTable + " ORDER BY id";

        try (QueryResponse response =
                     client.query(sql, newJsonEachRowSettingsForPrimitives()).get();
             ClickHouseTextFormatReader reader = createReader(response)) {

            for (int row = 0; row < ROW_COUNT; row++) {
                Assert.assertTrue(reader.hasNext(),
                        "missing row " + row + " for " + tc);
                Assert.assertNotNull(reader.next(),
                        "null row " + row + " for " + tc);

                Object expected = tc.expectedValues.get(row);

                Object actualByName = tc.readByName.apply(reader, tc.columnName);
                Assert.assertTrue(tc.equality.test(actualByName, expected),
                        "row " + row + " by name for " + tc +
                                ": expected=" + expected + ", actual=" + actualByName);

                Object actualByIndex = tc.readByIndex.apply(reader, 1);
                Assert.assertTrue(tc.equality.test(actualByIndex, expected),
                        "row " + row + " by index for " + tc +
                                ": expected=" + expected + ", actual=" + actualByIndex);
            }

            Assert.assertFalse(reader.hasNext(), "extra rows for " + tc);
            Assert.assertNull(reader.next(), "extra row payload for " + tc);
        }

        try (QueryResponse response =
                     client.query(sql, newJsonEachRowSettingsForPrimitives()).get();
             ClickHouseTextFormatReader reader = createReader(response)) {

            Assert.assertNotNull(reader.next(), "row needed for incompatibility checks: " + tc);

            for (IncompatibleAccessor accessor : tc.incompatibleAccessors) {
                assertAccessorThrows(reader, accessor.byName,
                        accessor.name + " by name on " + tc);
                assertAccessorThrows(reader, accessor.byIndex,
                        accessor.name + " by index on " + tc);
            }
        }
    }

    private static void assertAccessorThrows(ClickHouseTextFormatReader reader,
                                             Consumer<ClickHouseTextFormatReader> call,
                                             String context) {
        try {
            call.accept(reader);
            Assert.fail("Expected exception when invoking " + context);
        } catch (RuntimeException expected) {
            // Any RuntimeException is acceptable - DateTimeParseException,
            // NumberFormatException, ClassCastException, IllegalArgumentException
            // and UnsupportedOperationException are all valid signals that the
            // accessor cannot convert the stored value.
        }
    }

    // ------------------------------------------------------------------
    // Remaining non-value-focused tests
    // ------------------------------------------------------------------

    @Test(groups = {"integration"})
    public void testSchemaInference() throws Exception {
        // Numeric inference depends on parser materialization, so this test checks
        // that numerics do not collapse to String and stable scalar types still map.
        String sql = "SELECT toInt64(42) as col_int, toFloat64(3.14) as col_float, " +
                     "true as col_bool, 'val' as col_str";

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get();
             ClickHouseTextFormatReader reader = createReader(response)) {

            Assert.assertNotNull(reader.getSchema());
            Assert.assertEquals(reader.getSchema().getColumns().size(), 4);

            Assert.assertNotEquals(reader.getSchema().getColumnByIndex(1).getDataType(), ClickHouseDataType.String);
            Assert.assertNotEquals(reader.getSchema().getColumnByIndex(2).getDataType(), ClickHouseDataType.String);
            Assert.assertEquals(reader.getSchema().getColumnByIndex(3).getDataType(), ClickHouseDataType.Bool);
            Assert.assertEquals(reader.getSchema().getColumnByIndex(4).getDataType(), ClickHouseDataType.String);
        }
    }

    @Test(groups = {"integration"})
    public void testEmptyData() throws Exception {
        String sql = "SELECT * FROM remote('127.0.0.1', system.one) WHERE dummy > 1";

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get();
             ClickHouseTextFormatReader reader = createReader(response)) {

            Assert.assertFalse(reader.hasNext());
            Assert.assertNull(reader.next());
            Assert.assertEquals(reader.getSchema().getColumns().size(), 0);
        }
    }

    @Test(groups = {"integration"})
    public void testReadValueAndHasValue() throws Exception {
        String sql = "SELECT 7 as id, 'abc' as name, CAST(NULL AS Nullable(String)) as missing";

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get();
             ClickHouseTextFormatReader reader = createReader(response)) {

            reader.next();

            Number id = reader.readValue("id");
            Assert.assertNotNull(id);
            Assert.assertEquals(id.intValue(), 7);
            Assert.assertEquals((String) reader.readValue(2), "abc");

            Assert.assertTrue(reader.hasValue("id"));
            Assert.assertTrue(reader.hasValue(2));
            Assert.assertFalse(reader.hasValue("missing"));
            Assert.assertFalse(reader.hasValue(3));
            Assert.assertFalse(reader.hasValue("not_a_column"));
        }
    }

    @Test(groups = {"integration"})
    public void testListAccessor() throws Exception {
        String sql = "SELECT [1, 2, 3] as arr";

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get();
             ClickHouseTextFormatReader reader = createReader(response)) {

            reader.next();

            List<Number> values = reader.getList("arr");
            Assert.assertNotNull(values);
            Assert.assertEquals(values.size(), 3);
            Assert.assertEquals(values.get(0).intValue(), 1);
            Assert.assertEquals(values.get(1).intValue(), 2);
            Assert.assertEquals(values.get(2).intValue(), 3);

            List<Number> byIndex = reader.getList(1);
            Assert.assertNotNull(byIndex);
            Assert.assertEquals(byIndex.size(), 3);
        }
    }

    @Test(groups = {"integration"}, expectedExceptions = IllegalArgumentException.class)
    public void testNewBinaryFormatReaderRejectsJsonEachRow() throws Exception {
        String sql = "SELECT 1 as id";

        try (QueryResponse response = client.query(sql, newJsonEachRowSettings()).get()) {
            client.newBinaryFormatReader(response);
        }
    }

    // ------------------------------------------------------------------
    // Test case definitions for primitive types
    // ------------------------------------------------------------------

    /**
     * Describes one primitive ClickHouse column under test. Each case carries the SQL
     * literals used to populate the row, the expected Java values, the accessor
     * functions that are expected to succeed, and a list of accessors that must throw
     * for values of this type.
     */
    public static final class PrimitiveTypeCase {
        final String columnName;
        final String chType;
        final List<String> sqlLiterals;
        final List<Object> expectedValues;
        final BiFunction<ClickHouseTextFormatReader, String, Object> readByName;
        final BiFunction<ClickHouseTextFormatReader, Integer, Object> readByIndex;
        final BiPredicate<Object, Object> equality;
        final List<IncompatibleAccessor> incompatibleAccessors;

        PrimitiveTypeCase(String columnName, String chType,
                          List<String> sqlLiterals,
                          List<Object> expectedValues,
                          BiFunction<ClickHouseTextFormatReader, String, Object> readByName,
                          BiFunction<ClickHouseTextFormatReader, Integer, Object> readByIndex,
                          BiPredicate<Object, Object> equality,
                          List<IncompatibleAccessor> incompatibleAccessors) {
            this.columnName = columnName;
            this.chType = chType;
            this.sqlLiterals = sqlLiterals;
            this.expectedValues = expectedValues;
            this.readByName = readByName;
            this.readByIndex = readByIndex;
            this.equality = equality;
            this.incompatibleAccessors = incompatibleAccessors;
        }

        @Override
        public String toString() {
            return columnName + " (" + chType + ")";
        }
    }

    /** Pair of accessor invocations (by name and by index) that must throw for the case's column. */
    public static final class IncompatibleAccessor {
        final String name;
        final Consumer<ClickHouseTextFormatReader> byName;
        final Consumer<ClickHouseTextFormatReader> byIndex;

        IncompatibleAccessor(String name,
                             Consumer<ClickHouseTextFormatReader> byName,
                             Consumer<ClickHouseTextFormatReader> byIndex) {
            this.name = name;
            this.byName = byName;
            this.byIndex = byIndex;
        }
    }

    private static List<PrimitiveTypeCase> buildPrimitiveCases(Random rnd) {
        List<PrimitiveTypeCase> cases = new ArrayList<>();

        // ---- Signed integers ---------------------------------------------------
        cases.add(intCase("col_int8", "Int8",
                Arrays.asList(
                        Byte.MIN_VALUE, Byte.MAX_VALUE, (byte) 0,
                        (byte) (rnd.nextInt(256) - 128),
                        (byte) (rnd.nextInt(256) - 128)),
                Number::byteValue,
                (r, n) -> r.getByte(n),
                (r, i) -> r.getByte(i)));

        cases.add(intCase("col_int16", "Int16",
                Arrays.asList(
                        Short.MIN_VALUE, Short.MAX_VALUE, (short) 0,
                        (short) (rnd.nextInt(65536) - 32768),
                        (short) (rnd.nextInt(65536) - 32768)),
                Number::shortValue,
                (r, n) -> r.getShort(n),
                (r, i) -> r.getShort(i)));

        cases.add(intCase("col_int32", "Int32",
                Arrays.asList(
                        Integer.MIN_VALUE, Integer.MAX_VALUE, 0,
                        rnd.nextInt(), rnd.nextInt()),
                Number::intValue,
                (r, n) -> r.getInteger(n),
                (r, i) -> r.getInteger(i)));

        cases.add(intCase("col_int64", "Int64",
                Arrays.asList(
                        Long.MIN_VALUE, Long.MAX_VALUE, 0L,
                        rnd.nextLong(), rnd.nextLong()),
                Number::longValue,
                (r, n) -> r.getLong(n),
                (r, i) -> r.getLong(i)));

        // ---- Unsigned integers -------------------------------------------------
        cases.add(intCase("col_uint8", "UInt8",
                Arrays.asList(
                        (short) 0, (short) 255, (short) 128,
                        (short) rnd.nextInt(256),
                        (short) rnd.nextInt(256)),
                Number::shortValue,
                (r, n) -> r.getShort(n),
                (r, i) -> r.getShort(i)));

        cases.add(intCase("col_uint16", "UInt16",
                Arrays.asList(
                        0, 65535, 32768,
                        rnd.nextInt(65536),
                        rnd.nextInt(65536)),
                Number::intValue,
                (r, n) -> r.getInteger(n),
                (r, i) -> r.getInteger(i)));

        cases.add(intCase("col_uint32", "UInt32",
                Arrays.asList(
                        0L, 4294967295L, 1L,
                        (long) rnd.nextInt() & 0xFFFFFFFFL,
                        (long) rnd.nextInt() & 0xFFFFFFFFL),
                Number::longValue,
                (r, n) -> r.getLong(n),
                (r, i) -> r.getLong(i)));

        // UInt64 max (2^64 - 1) does not fit into a signed long, so we read it as
        // BigInteger and use BigInteger equality everywhere.
        List<Object> uint64Values = Arrays.asList(
                BigInteger.ZERO,
                new BigInteger("18446744073709551615"),
                BigInteger.ONE,
                new BigInteger(63, rnd),
                new BigInteger(64, rnd));
        cases.add(new PrimitiveTypeCase(
                "col_uint64", "UInt64",
                sqlLiteralsFromValues(uint64Values, v -> "toUInt64('" + v + "')"),
                uint64Values,
                (r, n) -> r.getBigInteger(n),
                (r, i) -> r.getBigInteger(i),
                AbstractJSONEachRowFormatReaderTests::equalsByEquals,
                incompatibleForNumericValue("col_uint64", 1)));

        // ---- Floating-point ----------------------------------------------------
        List<Float> floatValues = Arrays.asList(
                0.0f, Float.MAX_VALUE, -Float.MAX_VALUE,
                rnd.nextFloat() * 1_000f - 500f,
                rnd.nextFloat() * 1_000f - 500f);
        cases.add(new PrimitiveTypeCase(
                "col_float32", "Float32",
                sqlLiteralsFromValues(floatValues,
                        v -> "toFloat32(" + Float.toString(v) + ")"),
                toObjectList(floatValues),
                (r, n) -> r.getFloat(n),
                (r, i) -> r.getFloat(i),
                AbstractJSONEachRowFormatReaderTests::approximatelyEqualsFloat,
                incompatibleForNumericValue("col_float32", 1)));

        List<Double> doubleValues = Arrays.asList(
                0.0d, Double.MAX_VALUE, -Double.MAX_VALUE,
                rnd.nextDouble() * 1_000d - 500d,
                rnd.nextDouble() * 1_000d - 500d);
        cases.add(new PrimitiveTypeCase(
                "col_float64", "Float64",
                sqlLiteralsFromValues(doubleValues,
                        v -> "toFloat64(" + Double.toString(v) + ")"),
                toObjectList(doubleValues),
                (r, n) -> r.getDouble(n),
                (r, i) -> r.getDouble(i),
                AbstractJSONEachRowFormatReaderTests::approximatelyEqualsDouble,
                incompatibleForNumericValue("col_float64", 1)));

        // ---- Decimal -----------------------------------------------------------
        List<BigDecimal> decimalValues = Arrays.asList(
                new BigDecimal("0.0000"),
                new BigDecimal("99999999999999.9999"),
                new BigDecimal("-99999999999999.9999"),
                new BigDecimal(rnd.nextLong() % 1_000_000_000L)
                        .movePointLeft(4),
                new BigDecimal(rnd.nextLong() % 1_000_000_000L)
                        .movePointLeft(4));
        cases.add(new PrimitiveTypeCase(
                "col_decimal", "Decimal(18, 4)",
                sqlLiteralsFromValues(decimalValues,
                        v -> "toDecimal64('" + v.toPlainString() + "', 4)"),
                toObjectList(decimalValues),
                (r, n) -> r.getBigDecimal(n),
                (r, i) -> r.getBigDecimal(i),
                AbstractJSONEachRowFormatReaderTests::equalsBigDecimal,
                incompatibleForNumericValue("col_decimal", 1)));

        // ---- Bool --------------------------------------------------------------
        List<Boolean> boolValues = Arrays.asList(false, true, false, rnd.nextBoolean(), rnd.nextBoolean());
        cases.add(new PrimitiveTypeCase(
                "col_bool", "Bool",
                sqlLiteralsFromValues(boolValues, Object::toString),
                toObjectList(boolValues),
                (r, n) -> r.getBoolean(n),
                (r, i) -> r.getBoolean(i),
                AbstractJSONEachRowFormatReaderTests::equalsByEquals,
                incompatibleForBoolValue("col_bool", 1)));

        // ---- String ------------------------------------------------------------
        List<String> stringValues = Arrays.asList(
                "",
                "hello world",
                randomAsciiString(rnd, 32),
                randomAsciiString(rnd, 16),
                "line1\nline2\twith special chars: 'quoted'");
        cases.add(new PrimitiveTypeCase(
                "col_string", "String",
                sqlLiteralsFromValues(stringValues,
                        AbstractJSONEachRowFormatReaderTests::toClickHouseStringLiteral),
                toObjectList(stringValues),
                (r, n) -> r.getString(n),
                (r, i) -> r.getString(i),
                AbstractJSONEachRowFormatReaderTests::equalsByEquals,
                incompatibleForStringValue("col_string", 1)));

        // ---- Date --------------------------------------------------------------
        List<LocalDate> dateValues = Arrays.asList(
                LocalDate.of(1970, 1, 1),       // Date min
                LocalDate.of(2149, 6, 6),       // Date max
                LocalDate.of(2000, 1, 1),
                randomDate(rnd),
                randomDate(rnd));
        cases.add(new PrimitiveTypeCase(
                "col_date", "Date",
                sqlLiteralsFromValues(dateValues, v -> "toDate('" + v + "')"),
                toObjectList(dateValues),
                (r, n) -> r.getLocalDate(n),
                (r, i) -> r.getLocalDate(i),
                AbstractJSONEachRowFormatReaderTests::equalsByEquals,
                incompatibleForDateValue("col_date", 1)));

        // ---- UUID --------------------------------------------------------------
        List<UUID> uuidValues = Arrays.asList(
                new UUID(0L, 0L),
                new UUID(-1L, -1L),
                UUID.fromString("11111111-2222-3333-4444-555555555555"),
                new UUID(rnd.nextLong(), rnd.nextLong()),
                new UUID(rnd.nextLong(), rnd.nextLong()));
        cases.add(new PrimitiveTypeCase(
                "col_uuid", "UUID",
                sqlLiteralsFromValues(uuidValues, v -> "toUUID('" + v + "')"),
                toObjectList(uuidValues),
                (r, n) -> r.getUUID(n),
                (r, i) -> r.getUUID(i),
                AbstractJSONEachRowFormatReaderTests::equalsByEquals,
                incompatibleForUuidValue("col_uuid", 1)));

        return cases;
    }

    // ------------------------------------------------------------------
    // Case factories and helpers
    // ------------------------------------------------------------------

    /**
     * Builds a case for a signed/unsigned integer column whose expected Java type is a
     * boxed integer. Values are passed via {@code toXxx('literal')} casts so the SQL
     * parser does not need to evaluate large unary expressions.
     */
    private static PrimitiveTypeCase intCase(
            String columnName, String chType,
            List<? extends Number> values,
            java.util.function.Function<Number, Object> normalize,
            BiFunction<ClickHouseTextFormatReader, String, Object> readByName,
            BiFunction<ClickHouseTextFormatReader, Integer, Object> readByIndex) {

        List<Object> expected = new ArrayList<>(values.size());
        for (Number v : values) {
            expected.add(normalize.apply(v));
        }
        List<String> literals = sqlLiteralsFromValues(values,
                v -> "to" + chType + "('" + v + "')");
        return new PrimitiveTypeCase(columnName, chType, literals, expected,
                readByName, readByIndex,
                AbstractJSONEachRowFormatReaderTests::equalsByEquals,
                incompatibleForNumericValue(columnName, 1));
    }

    private static <T> List<String> sqlLiteralsFromValues(List<T> values,
                                                          java.util.function.Function<T, String> toLiteral) {
        List<String> literals = new ArrayList<>(values.size());
        for (T v : values) {
            literals.add(toLiteral.apply(v));
        }
        return literals;
    }

    private static <T> List<Object> toObjectList(List<T> values) {
        return new ArrayList<>(values);
    }

    // ---- Equality helpers ------------------------------------------------------

    private static boolean equalsByEquals(Object actual, Object expected) {
        if (expected == null) {
            return actual == null;
        }
        return expected.equals(actual);
    }

    private static boolean equalsBigDecimal(Object actual, Object expected) {
        if (expected == null) {
            return actual == null;
        }
        if (!(actual instanceof BigDecimal)) {
            return false;
        }
        return ((BigDecimal) expected).compareTo((BigDecimal) actual) == 0;
    }

    private static boolean approximatelyEqualsFloat(Object actual, Object expected) {
        if (actual == null || expected == null) {
            return actual == expected;
        }
        float a = ((Number) actual).floatValue();
        float e = ((Number) expected).floatValue();
        if (Float.compare(a, e) == 0) {
            return true;
        }
        if (Float.isInfinite(a) || Float.isInfinite(e) || Float.isNaN(a) || Float.isNaN(e)) {
            return Float.compare(a, e) == 0;
        }
        float tolerance = Math.max(Math.ulp(e) * 4f, Math.abs(e) * 1e-6f);
        return Math.abs(a - e) <= tolerance;
    }

    private static boolean approximatelyEqualsDouble(Object actual, Object expected) {
        if (actual == null || expected == null) {
            return actual == expected;
        }
        double a = ((Number) actual).doubleValue();
        double e = ((Number) expected).doubleValue();
        if (Double.compare(a, e) == 0) {
            return true;
        }
        if (Double.isInfinite(a) || Double.isInfinite(e) || Double.isNaN(a) || Double.isNaN(e)) {
            return Double.compare(a, e) == 0;
        }
        double tolerance = Math.max(Math.ulp(e) * 4d, Math.abs(e) * 1e-12d);
        return Math.abs(a - e) <= tolerance;
    }

    // ---- Incompatible accessor sets -------------------------------------------

    /**
     * Accessors that always fail (or fail for non-numeric content). For numeric columns
     * we rely on date/UUID/temporal accessors to throw because the value cannot be
     * parsed as a date or UUID. For unsupported accessors we expect
     * {@link UnsupportedOperationException} from the reader implementation.
     */
    private static List<IncompatibleAccessor> incompatibleForNumericValue(String name, int index) {
        return Arrays.asList(
                new IncompatibleAccessor("getLocalDate",
                        r -> r.getLocalDate(name),
                        r -> r.getLocalDate(index)),
                new IncompatibleAccessor("getLocalTime",
                        r -> r.getLocalTime(name),
                        r -> r.getLocalTime(index)),
                new IncompatibleAccessor("getLocalDateTime",
                        r -> r.getLocalDateTime(name),
                        r -> r.getLocalDateTime(index)),
                new IncompatibleAccessor("getUUID",
                        r -> r.getUUID(name),
                        r -> r.getUUID(index)),
                new IncompatibleAccessor("getZonedDateTime",
                        r -> r.getZonedDateTime(name),
                        r -> r.getZonedDateTime(index)),
                new IncompatibleAccessor("getInstant",
                        r -> r.getInstant(name),
                        r -> r.getInstant(index)),
                new IncompatibleAccessor("getInet4Address",
                        r -> r.getInet4Address(name),
                        r -> r.getInet4Address(index)));
    }

    private static List<IncompatibleAccessor> incompatibleForBoolValue(String name, int index) {
        // Numeric accessors fail because Boolean is not a Number, and string parsers
        // fail because "true" / "false" cannot be parsed as a number, date, or UUID.
        return Arrays.asList(
                new IncompatibleAccessor("getByte",
                        r -> r.getByte(name),
                        r -> r.getByte(index)),
                new IncompatibleAccessor("getInteger",
                        r -> r.getInteger(name),
                        r -> r.getInteger(index)),
                new IncompatibleAccessor("getLong",
                        r -> r.getLong(name),
                        r -> r.getLong(index)),
                new IncompatibleAccessor("getBigInteger",
                        r -> r.getBigInteger(name),
                        r -> r.getBigInteger(index)),
                new IncompatibleAccessor("getBigDecimal",
                        r -> r.getBigDecimal(name),
                        r -> r.getBigDecimal(index)),
                new IncompatibleAccessor("getLocalDate",
                        r -> r.getLocalDate(name),
                        r -> r.getLocalDate(index)),
                new IncompatibleAccessor("getUUID",
                        r -> r.getUUID(name),
                        r -> r.getUUID(index)),
                new IncompatibleAccessor("getZonedDateTime",
                        r -> r.getZonedDateTime(name),
                        r -> r.getZonedDateTime(index)));
    }

    private static List<IncompatibleAccessor> incompatibleForStringValue(String name, int index) {
        // String content here is not numeric, not a date, and not a UUID, so numeric
        // and temporal accessors must throw.
        return Arrays.asList(
                new IncompatibleAccessor("getByte",
                        r -> r.getByte(name),
                        r -> r.getByte(index)),
                new IncompatibleAccessor("getInteger",
                        r -> r.getInteger(name),
                        r -> r.getInteger(index)),
                new IncompatibleAccessor("getLong",
                        r -> r.getLong(name),
                        r -> r.getLong(index)),
                new IncompatibleAccessor("getDouble",
                        r -> r.getDouble(name),
                        r -> r.getDouble(index)),
                new IncompatibleAccessor("getBigInteger",
                        r -> r.getBigInteger(name),
                        r -> r.getBigInteger(index)),
                new IncompatibleAccessor("getBigDecimal",
                        r -> r.getBigDecimal(name),
                        r -> r.getBigDecimal(index)),
                new IncompatibleAccessor("getLocalDate",
                        r -> r.getLocalDate(name),
                        r -> r.getLocalDate(index)),
                new IncompatibleAccessor("getUUID",
                        r -> r.getUUID(name),
                        r -> r.getUUID(index)),
                new IncompatibleAccessor("getZonedDateTime",
                        r -> r.getZonedDateTime(name),
                        r -> r.getZonedDateTime(index)));
    }

    private static List<IncompatibleAccessor> incompatibleForDateValue(String name, int index) {
        // Date columns arrive as strings, so numeric accessors throw ClassCastException
        // and time-only / date-time accessors fail to parse the YYYY-MM-DD shape.
        return Arrays.asList(
                new IncompatibleAccessor("getInteger",
                        r -> r.getInteger(name),
                        r -> r.getInteger(index)),
                new IncompatibleAccessor("getLong",
                        r -> r.getLong(name),
                        r -> r.getLong(index)),
                new IncompatibleAccessor("getDouble",
                        r -> r.getDouble(name),
                        r -> r.getDouble(index)),
                new IncompatibleAccessor("getBigDecimal",
                        r -> r.getBigDecimal(name),
                        r -> r.getBigDecimal(index)),
                new IncompatibleAccessor("getLocalTime",
                        r -> r.getLocalTime(name),
                        r -> r.getLocalTime(index)),
                new IncompatibleAccessor("getLocalDateTime",
                        r -> r.getLocalDateTime(name),
                        r -> r.getLocalDateTime(index)),
                new IncompatibleAccessor("getUUID",
                        r -> r.getUUID(name),
                        r -> r.getUUID(index)),
                new IncompatibleAccessor("getZonedDateTime",
                        r -> r.getZonedDateTime(name),
                        r -> r.getZonedDateTime(index)));
    }

    private static List<IncompatibleAccessor> incompatibleForUuidValue(String name, int index) {
        return Arrays.asList(
                new IncompatibleAccessor("getInteger",
                        r -> r.getInteger(name),
                        r -> r.getInteger(index)),
                new IncompatibleAccessor("getLong",
                        r -> r.getLong(name),
                        r -> r.getLong(index)),
                new IncompatibleAccessor("getDouble",
                        r -> r.getDouble(name),
                        r -> r.getDouble(index)),
                new IncompatibleAccessor("getBigDecimal",
                        r -> r.getBigDecimal(name),
                        r -> r.getBigDecimal(index)),
                new IncompatibleAccessor("getLocalDate",
                        r -> r.getLocalDate(name),
                        r -> r.getLocalDate(index)),
                new IncompatibleAccessor("getLocalDateTime",
                        r -> r.getLocalDateTime(name),
                        r -> r.getLocalDateTime(index)),
                new IncompatibleAccessor("getZonedDateTime",
                        r -> r.getZonedDateTime(name),
                        r -> r.getZonedDateTime(index)));
    }

    // ---- Random value helpers -------------------------------------------------

    private static String randomAsciiString(Random rnd, int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            // Printable ASCII range except quote (32-126 except 39 and 92).
            char c;
            do {
                c = (char) (32 + rnd.nextInt(95));
            } while (c == '\'' || c == '\\');
            sb.append(c);
        }
        return sb.toString();
    }

    private static LocalDate randomDate(Random rnd) {
        // Stay well inside the Date range (1970-01-01..2149-06-06) to avoid time-zone
        // edge effects when the server re-serialises the value into JSON.
        int year = 1971 + rnd.nextInt(170);
        int month = 1 + rnd.nextInt(12);
        int day = 1 + rnd.nextInt(LocalDate.of(year, month, 1).lengthOfMonth());
        return LocalDate.of(year, month, day);
    }

    private static String toClickHouseStringLiteral(String value) {
        StringBuilder sb = new StringBuilder(value.length() + 2);
        sb.append('\'');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\'':
                    sb.append("\\'");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                default:
                    sb.append(c);
                    break;
            }
        }
        sb.append('\'');
        return sb.toString();
    }
}
