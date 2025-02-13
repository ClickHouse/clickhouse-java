package com.clickhouse.client;

import com.clickhouse.client.ClickHouseClientBuilder.Agent;
import com.clickhouse.client.ClickHouseTransaction.XID;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.config.ClickHouseBufferingMode;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.config.ClickHouseRenameMethod;
import com.clickhouse.data.ClickHouseArraySequence;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseExternalTable;
import com.clickhouse.data.ClickHouseFile;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;
import com.clickhouse.data.ClickHouseVersion;
import com.clickhouse.data.ClickHouseWriter;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.clickhouse.data.value.ClickHouseBigDecimalValue;
import com.clickhouse.data.value.ClickHouseBigIntegerValue;
import com.clickhouse.data.value.ClickHouseByteValue;
import com.clickhouse.data.value.ClickHouseDateTimeValue;
import com.clickhouse.data.value.ClickHouseEnumValue;
import com.clickhouse.data.value.ClickHouseIntegerValue;
import com.clickhouse.data.value.ClickHouseIpv4Value;
import com.clickhouse.data.value.ClickHouseIpv6Value;
import com.clickhouse.data.value.ClickHouseLongValue;
import com.clickhouse.data.value.ClickHouseOffsetDateTimeValue;
import com.clickhouse.data.value.ClickHouseStringValue;
import com.clickhouse.data.value.UnsignedByte;
import com.clickhouse.data.value.UnsignedInteger;
import com.clickhouse.data.value.UnsignedLong;
import com.clickhouse.data.value.UnsignedShort;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public abstract class ClientIntegrationTest extends BaseIntegrationTest {
    protected void checkRowCount(String queryOrTableName, int expectedRowCount) throws ClickHouseException {
        try (ClickHouseClient client = getClient()) {
            checkRowCount(newRequest(client, getServer()).format(ClickHouseFormat.RowBinaryWithNamesAndTypes),
                    queryOrTableName, expectedRowCount);
        }
    }

    protected void checkRowCount(ClickHouseRequest<?> request, String queryOrTableName, int expectedRowCount)
            throws ClickHouseException {
        String sql = queryOrTableName.indexOf(' ') > 0 ? queryOrTableName
                : "select count(1) from ".concat(queryOrTableName);
        try (ClickHouseResponse response = request.query(sql).executeAndWait()) {
            int count = 0;
            for (ClickHouseRecord r : response.records()) {
                if (count == 0) {
                    Assert.assertEquals(r.getValue(0).asInteger(), expectedRowCount);
                }
                count++;
            }
            Assert.assertEquals(count, 1);
        }
    }

    protected boolean checkServerVersion(ClickHouseClient client, ClickHouseNode server, String range)
            throws ClickHouseException {
        try (ClickHouseResponse response = newRequest(client, server)
                .format(ClickHouseFormat.RowBinaryWithNamesAndTypes).query("select version()").executeAndWait()) {
            return ClickHouseVersion.of(response.firstRecord().getValue(0).asString()).check(range);
        }
    }

    protected List<ClickHouseResponseSummary> sendAndWait(ClickHouseNode server, String sql, String... more)
            throws ClickHouseException {
        try {
            return ClickHouseClient.send(server, sql, more).get();
        } catch (InterruptedException | ExecutionException e) {
            throw ClickHouseException.of(e, server);
        }
    }

    protected List<ClickHouseResponseSummary> sendAndWait(ClickHouseNode server, String sql,
            ClickHouseValue[] templates, Object[]... params) throws ClickHouseException {
        try {
            return ClickHouseClient.send(server, sql, templates, params).get();
        } catch (InterruptedException | ExecutionException e) {
            throw ClickHouseException.of(e, server);
        }
    }

    protected ClickHouseResponseSummary execute(ClickHouseRequest<?> request, String sql) throws ClickHouseException {
        try (ClickHouseResponse response = request.query(sql).executeAndWait()) {
            for (ClickHouseRecord record : response.records()) {
                for (ClickHouseValue value : record) {
                    Assert.assertNotNull(value, "Value should never be null");
                }
            }

            return response.getSummary();
        }
    }

    protected ClickHouseRequest<?> newRequest(ClickHouseClient client, ClickHouseNode server) {
        ClickHouseRequest<?> request = client.read(server);
        setClientOptions(request);
        return request;
    }

    protected ClickHouseRequest<?> newRequest(ClickHouseClient client, ClickHouseNodes servers) {
        ClickHouseRequest<?> request = client.read(servers);
        setClientOptions(request);
        return request;
    }

    private void setClientOptions(ClickHouseRequest<?> request) {
        Map<ClickHouseOption, Serializable> options = getClientOptions();
        if (options != null) {
            for (Entry<ClickHouseOption, Serializable> e : options.entrySet()) {
                request.option(e.getKey(), e.getValue());
            }
        }
    }

    protected abstract ClickHouseProtocol getProtocol();

    protected abstract Class<? extends ClickHouseClient> getClientClass();

    protected Map<ClickHouseOption, Serializable> getClientOptions() {
        return Collections.emptyMap();
    }

    protected ClickHouseClientBuilder initClient(ClickHouseClientBuilder builder) {
        return builder;
    }

    protected ClickHouseClient getClient(ClickHouseConfig... configs) {
        return initClient(ClickHouseClient.builder()).config(new ClickHouseConfig(configs))
                .nodeSelector(ClickHouseNodeSelector.of(getProtocol())).build();
    }

    protected ClickHouseClient getSecureClient(ClickHouseConfig... configs) {
        return initClient(ClickHouseClient.builder())
                .config(new ClickHouseConfig(configs))
                .nodeSelector(ClickHouseNodeSelector.of(getProtocol()))
                .build();
    }

    protected ClickHouseNode getSecureServer(ClickHouseNode base) {
        return getSecureServer(getProtocol(), base);
    }

    protected ClickHouseNode getSecureServer() {
        return getSecureServer(getProtocol());
    }

    protected ClickHouseNode getServer(ClickHouseNode base) {
        return getServer(getProtocol(), base);
    }

    protected ClickHouseNode getServer() {
        if (isCloud()) return getSecureServer(getProtocol());
        return getServer(getProtocol());
    }

    @DataProvider(name = "compressionMatrix")
    protected Object[][] getCompressionMatrix() {
        ClickHouseFormat[] formats = new ClickHouseFormat[] {
                ClickHouseFormat.RowBinaryWithNamesAndTypes,
                ClickHouseFormat.TabSeparatedWithNamesAndTypes
        };
        ClickHouseBufferingMode[] modes = new ClickHouseBufferingMode[] {
                ClickHouseBufferingMode.RESOURCE_EFFICIENT,
                ClickHouseBufferingMode.PERFORMANCE
        };
        boolean[] bools = new boolean[] { true, false };
        Object[][] array = new Object[formats.length * modes.length * 2 * 2][4];
        int i = 0;
        for (ClickHouseFormat format : formats) {
            for (ClickHouseBufferingMode mode : modes) {
                for (boolean compress : bools) {
                    for (boolean decompress : bools) {
                        array[i++] = new Object[] { format, mode, compress, decompress };
                    }
                }
            }
        }
        return array;
    }

    @DataProvider(name = "requestCompressionMatrix")
    protected Object[][] getRequestCompressionMatrix() {
        return new Object[][] {
                { ClickHouseCompression.NONE, -2, 2, 1 },
                { ClickHouseCompression.BROTLI, -2, 12, 1 }, // [-1, 11]
                { ClickHouseCompression.BZ2, -2, 2, 1 },
                { ClickHouseCompression.DEFLATE, -2, 10, 1 }, // [0, 9]
                { ClickHouseCompression.GZIP, -2, 10, 1 }, // [-1, 9]
                { ClickHouseCompression.LZ4, -2, 19, 1 }, // [0, 18]
                { ClickHouseCompression.SNAPPY, -2, 513, 1024 }, // [1 * 1024, 32 * 1024]
                { ClickHouseCompression.XZ, -2, 10, 1 }, // [0, 9]
                { ClickHouseCompression.ZSTD, -2, 23, 1 }, // [0, 22]
        };
    }

    @DataProvider(name = "mixedCompressionMatrix")
    protected Object[][] getMixedCompressionMatrix() {
        ClickHouseCompression[] supportedRequestCompression = { ClickHouseCompression.NONE, ClickHouseCompression.LZ4,
                ClickHouseCompression.ZSTD };
        ClickHouseCompression[] supportedResponseCompression = { ClickHouseCompression.NONE,
                ClickHouseCompression.BROTLI, ClickHouseCompression.BZ2, ClickHouseCompression.DEFLATE,
                ClickHouseCompression.GZIP, ClickHouseCompression.LZ4, ClickHouseCompression.XZ,
                ClickHouseCompression.SNAPPY, ClickHouseCompression.ZSTD };
        Object[][] matrix = new Object[supportedRequestCompression.length * supportedResponseCompression.length][];
        int i = 0;
        for (ClickHouseCompression reqComp : supportedRequestCompression) {
            for (ClickHouseCompression respComp : supportedResponseCompression) {
                matrix[i++] = new Object[] { reqComp, respComp };
            }
        }
        return matrix;
    }

    @DataProvider(name = "primitiveArrayMatrix")
    protected Object[][] getPrimitiveArrayMatrix() {
        return new Object[][] {
                { "Int8", new int[] { -1, 2, -3, 4, -5 } },
                { "UInt8", new int[] { 1, 2, 3, 4, 5 } },
                { "Int16", new int[] { -1, 2, -3, 4, -5 } },
                { "UInt16", new int[] { 1, 2, 3, 4, 5 } },
                { "Int32", new int[] { -1, 2, -3, 4, -5 } },
                { "UInt32", new int[] { 1, 2, 3, 4, 5 } },
                { "Int64", new int[] { -1, 2, -3, 4, -5 } },
                { "UInt64", new int[] { 1, 2, 3, 4, 5 } },
                { "Float32", new int[] { 1, -2, 3, -4, 5 } },
                { "Float64", new int[] { 1, -2, 3, -4, 5 } },

                { "Nullable(Int8)", new Byte[] { null, 2, -3, 4, -5 } },
                // { "Nullable(UInt8)", new Short[] { 1, null, 3, 4, 5 } },
                { "Nullable(UInt8)",
                        new UnsignedByte[] { UnsignedByte.ONE, null, UnsignedByte.valueOf((byte) 3),
                                UnsignedByte.valueOf((byte) 4), UnsignedByte.valueOf((byte) 5) } },
                { "Nullable(Int16)", new Short[] { -1, 2, null, 4, -5 } },
                // { "Nullable(UInt16)", new Integer[] { 1, 2, 3, null, 5 } },
                { "Nullable(UInt16)",
                        new UnsignedShort[] { UnsignedShort.ONE, UnsignedShort.valueOf((short) 2),
                                UnsignedShort.valueOf((short) 3), null, UnsignedShort.valueOf((short) 5) } },
                { "Nullable(Int32)", new Integer[] { -1, 2, -3, 4, null } },
                // { "Nullable(UInt32)", new Long[] { 1L, 2L, 3L, null, 5L } },
                { "Nullable(UInt32)",
                        new UnsignedInteger[] { UnsignedInteger.ONE, UnsignedInteger.TWO, UnsignedInteger.valueOf(3),
                                null, UnsignedInteger.valueOf(5) } },
                { "Nullable(Int64)", new Long[] { -1L, 2L, null, 4L, -5L } },
                // { "Nullable(UInt64)", new Long[] { 1L, null, 3L, 4L, 5L } },
                { "Nullable(UInt64)",
                        new UnsignedLong[] { UnsignedLong.ONE, null, UnsignedLong.valueOf(3L), UnsignedLong.valueOf(4L),
                                UnsignedLong.valueOf(5L) } },
                { "Nullable(Float32)", new Float[] { null, -2F, 3F, -4F, 5F } },
                { "Nullable(Float64)", new Double[] { 1D, null, 3D, -4D, 5D } },
        };
    }

    @DataProvider(name = "primitiveArrayLowCardinalityMatrix")
    protected Object[][] getPrimitiveArrayLowCardinalityMatrix() {
        return new Object[][]{
                { "LowCardinality(Int8)", new int[] { -1, 2, -3, 4, -5 } },
                { "LowCardinality(UInt8)", new int[] { 1, 2, 3, 4, 5 } },
                { "LowCardinality(Int16)", new int[] { -1, 2, -3, 4, -5 } },
                { "LowCardinality(UInt16)", new int[] { 1, 2, 3, 4, 5 } },
                { "LowCardinality(Int32)", new int[] { -1, 2, -3, 4, -5 } },
                { "LowCardinality(UInt32)", new int[] { 1, 2, 3, 4, 5 } },
                { "LowCardinality(Int64)", new int[] { -1, 2, -3, 4, -5 } },
                { "LowCardinality(UInt64)", new int[] { 1, 2, 3, 4, 5 } },
                { "LowCardinality(Float32)", new int[] { 1, -2, 3, -4, 5 } },
                { "LowCardinality(Float64)", new int[] { 1, -2, 3, -4, 5 } },

                { "LowCardinality(Nullable(Int8))", new Byte[] { -1, 2, -3, 4, -5 } },
                // { "LowCardinality(Nullable(UInt8))", new Short[] { 1, 2, 3, 4, 5 } },
                { "LowCardinality(Nullable(UInt8))",
                        new UnsignedByte[] { UnsignedByte.ONE, UnsignedByte.valueOf((byte) 2),
                                UnsignedByte.valueOf((byte) 3), UnsignedByte.valueOf((byte) 4),
                                UnsignedByte.valueOf((byte) 5) } },
                { "LowCardinality(Nullable(Int16))", new Short[] { -1, 2, -3, 4, -5 } },
                // { "LowCardinality(Nullable(UInt16))", new Integer[] { 1, 2, 3, 4, 5 } },
                { "LowCardinality(Nullable(UInt16))",
                        new UnsignedShort[] { UnsignedShort.ONE, UnsignedShort.valueOf((short) 2),
                                UnsignedShort.valueOf((short) 3), UnsignedShort.valueOf((short) 4),
                                UnsignedShort.valueOf((short) 5) } },
                { "LowCardinality(Nullable(Int32))", new Integer[] { -1, 2, -3, 4, -5 } },
                // { "LowCardinality(Nullable(UInt32))", new Long[] { 1L, 2L, 3L, 4L, 5L } },
                { "LowCardinality(Nullable(UInt32))",
                        new UnsignedInteger[] { UnsignedInteger.ONE, UnsignedInteger.TWO, UnsignedInteger.valueOf(3),
                                UnsignedInteger.valueOf(4), UnsignedInteger.valueOf(5) } },
                { "LowCardinality(Nullable(Int64))", new Long[] { -1L, 2L, -3L, 4L, -5L } },
                // { "LowCardinality(Nullable(UInt64))", new Long[] { 1L, 2L, 3L, 4L, 5L } },
                { "LowCardinality(Nullable(UInt64))",
                        new UnsignedLong[] { UnsignedLong.ONE, UnsignedLong.TWO, UnsignedLong.valueOf(3L),
                                UnsignedLong.valueOf(4L), UnsignedLong.valueOf(5L) } },
                { "LowCardinality(Nullable(Float32))", new Float[] { null, -2F, 3F, -4F, 5F } },
                { "LowCardinality(Nullable(Float64))", new Double[] { 1D, null, 3D, -4D, 5D } },
        };
    }

    @DataProvider(name = "fileProcessMatrix")
    protected Object[][] getFileProcessMatrix() {
        return new Object[][] {
                { true, true },
                { true, false },
                { false, true },
                { false, false },
        };
    }

    @DataProvider(name = "renameMethods")
    protected Object[][] getRenameMethods() {
        return new Object[][] {
                new Object[] { null, "a b c", " ", "d.E_f" },
                new Object[] { ClickHouseRenameMethod.NONE, "a b c", " ", "d.E_f" },
                new Object[] { ClickHouseRenameMethod.REMOVE_PREFIX, "a b c", " ", "E_f" },
                new Object[] { ClickHouseRenameMethod.TO_CAMELCASE, "aBC", "", "d.EF" },
                new Object[] { ClickHouseRenameMethod.TO_CAMELCASE_WITHOUT_PREFIX, "aBC", "", "EF" },
                new Object[] { ClickHouseRenameMethod.TO_UNDERSCORE, "a_b_c", "", "d._e_f" },
                new Object[] { ClickHouseRenameMethod.TO_UNDERSCORE_WITHOUT_PREFIX, "a_b_c", "", "E_f" }, };
    }

    @DataProvider(name = "simpleTypeProvider")
    protected Object[][] getSimpleTypes() {
        return new Object[][] {
                { ClickHouseDataType.Enum8.name() + "('v-1' = -1, 'v0' = 0, 'v+1' = 1)", "v0", "v-1", "v+1" },
                { ClickHouseDataType.Enum16.name() + "('v-1' = -1, 'v0' = 0, 'v+1' = 1)", "v0", "v-1", "v+1" },
                { ClickHouseDataType.Int8.name(), "0", "-1", "1" },
                { ClickHouseDataType.UInt8.name(), "0", "255", "1" },
                { ClickHouseDataType.Int16.name(), "0", "-1", "1" },
                { ClickHouseDataType.UInt16.name(), "0", "65535", "1" },
                { ClickHouseDataType.Int32.name(), "0", "-1", "1" },
                { ClickHouseDataType.UInt32.name(), "0", "4294967295", "1" },
                { ClickHouseDataType.Int64.name(), "0", "-1", "1" },
                { ClickHouseDataType.UInt64.name(), "0", "18446744073709551615", "1" },
                { ClickHouseDataType.Int128.name(), "0", "-1", "1" },
                { ClickHouseDataType.UInt128.name(), "0", "340282366920938463463374607431768211455", "1" },
                { ClickHouseDataType.Int256.name(), "0", "-1", "1" },
                { ClickHouseDataType.UInt256.name(), "0",
                        "115792089237316195423570985008687907853269984665640564039457584007913129639935", "1" },
                { ClickHouseDataType.Float32.name(), "0.0", "-1.0", "1.0" },
                { ClickHouseDataType.Float64.name(), "0.0", "-1.0", "1.0" },
                { ClickHouseDataType.Date.name(), "1970-01-01", "1970-01-01", "1970-01-02" },
                { ClickHouseDataType.Date32.name(), "1970-01-01", "1969-12-31", "1970-01-02" },
                { ClickHouseDataType.DateTime.name(), "1970-01-01 00:00:00", "1970-01-01 00:00:00",
                        "1970-01-01 00:00:01" },
                { ClickHouseDataType.DateTime32.name(), "1970-01-01 00:00:00", "1970-01-01 00:00:00",
                        "1970-01-01 00:00:01" },
                { ClickHouseDataType.DateTime64.name() + "(3)", "1970-01-01 00:00:00", "1969-12-31 23:59:59.999",
                        "1970-01-01 00:00:00.001" },
                { ClickHouseDataType.Decimal.name() + "(10,9)", "0E-9", "-1.000000000", "1.000000000" },
                { ClickHouseDataType.Decimal32.name() + "(1)", "0.0", "-1.0", "1.0" },
                { ClickHouseDataType.Decimal64.name() + "(3)", "0.000", "-1.000", "1.000" },
                { ClickHouseDataType.Decimal128.name() + "(5)", "0.00000", "-1.00000", "1.00000" },
                { ClickHouseDataType.Decimal256.name() + "(7)", "0E-7", "-1.0000000", "1.0000000" },
                { ClickHouseDataType.FixedString.name() + "(3)", "0\0\0", "-1\0", "1\0\0" },
                { ClickHouseDataType.String.name(), "0", "-1", "1" },
                { ClickHouseDataType.UUID.name(), "00000000-0000-0000-0000-000000000000",
                        "00000000-0000-0000-ffff-ffffffffffff", "00000000-0000-0000-0000-000000000001" } };
    }

    @DataProvider(name = "loadBalancingPolicies")
    protected Object[][] getLoadBalancingPolicies() {
        return new Object[][]{
                {ClickHouseLoadBalancingPolicy.of(null)},
                {ClickHouseLoadBalancingPolicy.of(ClickHouseLoadBalancingPolicy.FIRST_ALIVE)},
                {ClickHouseLoadBalancingPolicy.of(ClickHouseLoadBalancingPolicy.ROUND_ROBIN)},
                {ClickHouseLoadBalancingPolicy.of(ClickHouseLoadBalancingPolicy.RANDOM)},
        };
    }

    @Test(groups = { "unit" })
    public void testInitialization() {
        Assert.assertNotNull(getProtocol(), "The client should support non-null protocol");
        Assert.assertNotEquals(getProtocol(), ClickHouseProtocol.ANY,
                "The client should support a specific protocol instead of ANY");

        try (ClickHouseClient client1 = ClickHouseClient.builder()
                .nodeSelector(ClickHouseNodeSelector.of(getProtocol())).build();
                ClickHouseClient client2 = ClickHouseClient.builder().options(getClientOptions())
                        .option(ClickHouseClientOption.ASYNC, false)
                        .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.ANY)).build();
                ClickHouseClient client3 = ClickHouseClient.newInstance();
                ClickHouseClient client4 = ClickHouseClient.newInstance(getProtocol());
                ClickHouseClient client5 = getClient()) {
            ClickHouseClient[] clients = new ClickHouseClient[] { client1, client2, client3, client4, client5 };
            for (int i = 0; i < clients.length; i++) {
                ClickHouseClient client = clients[i];
                Assert.assertEquals(client.getClass(), Agent.class,
                        "Client #" + (i + 1) + " should be an agent, but it's " + client.getClass());
                Assert.assertEquals(((Agent) client).getClient().getClass(), getClientClass(),
                        "Client #" + (i + 1) + " is not " + getClientClass() + " but " + client.getClass());
                Assert.assertTrue(client.accept(getProtocol()),
                        "Client #" + (i + 1) + " should support protocol: " + getProtocol());
            }
        }
    }

    @Test(groups = { "integration" })
    public void testOpenCloseClient() throws ClickHouseException {
        int count = 10;
        int timeout = 3000;
        ClickHouseNode server = getServer();
        for (int i = 0; i < count; i++) {
            try (ClickHouseClient client = getClient();
                    ClickHouseResponse response = newRequest(client, server).query("select 1")
                            .executeAndWait()) {
                Assert.assertEquals(response.firstRecord().getValue(0).asInteger(), 1);
            }
            Assert.assertTrue(getClient().ping(server, timeout));
        }
    }

    @Test(dataProvider = "compressionMatrix", groups = { "integration" })
    public void testCompression(ClickHouseFormat format, ClickHouseBufferingMode bufferingMode, boolean compressRequest,
            boolean compressResponse) throws ClickHouseException {
        if (isCloud()) return; //TODO: testCompression - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        ClickHouseNode server = getServer();
        String uuid = UUID.randomUUID().toString();
        sendAndWait(server, "create table if not exists test_compress_decompress(id UUID)engine=Memory");
        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest<?> request = newRequest(client, server).format(format)
                    .option(ClickHouseClientOption.RESPONSE_BUFFERING, bufferingMode)
                    .compressServerResponse(compressResponse)
                    .decompressClientRequest(compressRequest);
            // start with insert
            try (ClickHouseResponse resp = request
                    .query("insert into test_compress_decompress values(:uuid)").params(ClickHouseStringValue.of(uuid))
                    .executeAndWait()) {
                Assert.assertNotNull(resp);
            }
            int expectedRows = 1;
            try (ClickHouseResponse resp = request.write().table("test_compress_decompress")
                    .format(ClickHouseFormat.CSV).data(ClickHouseInputStream.of("'" + uuid + "'\n'" + uuid + "'"))
                    .executeAndWait()) {
                Assert.assertNotNull(resp);
            }
            expectedRows += 2;

            boolean hasResult = false;
            try (ClickHouseResponse resp = request
                    .query("select id, count(1) n from test_compress_decompress where id = :uuid group by id")
                    .params(ClickHouseStringValue.of(uuid)).executeAndWait()) {
                ClickHouseRecord r = resp.firstRecord();
                Assert.assertEquals(r.getValue(0).asString(), uuid);
                Assert.assertEquals(r.getValue(1).asInteger(), expectedRows);
                hasResult = true;
            }
            Assert.assertTrue(hasResult, "Should have at least one result");

            // empty results
            try (ClickHouseResponse resp = request.query("create database if not exists system").executeAndWait()) {
                ClickHouseResponseSummary summary = resp.getSummary();
                Assert.assertEquals(summary.getReadRows(), 0L);
                Assert.assertEquals(summary.getWrittenRows(), 0L);
            }

            // let's also check if failures can be captured successfully as well
            ClickHouseException exp = null;
            try (ClickHouseResponse resp = request.use(uuid)
                    .query("select currentUser(), timezone(), version(), getSetting('readonly') readonly FORMAT RowBinaryWithNamesAndTypes")
                    .executeAndWait()) {
                Assert.fail("Query should fail");
            } catch (ClickHouseException e) {
                exp = e;
            }
            Assert.assertEquals(exp.getErrorCode(), 81, "Expected error code 81 but we got: " + exp.getMessage());
        }
    }

    @Test(dataProvider = "requestCompressionMatrix", groups = "integration")
    public void testCompressedRequest(ClickHouseCompression compression, int startLevel, int endLevel, int step)
            throws Exception {
        final ClickHouseNode server = getServer();
        final int readBufferSize = ClickHouseDataConfig.getDefaultReadBufferSize();

        if (compression == ClickHouseCompression.SNAPPY) {
            if (!checkServerVersion(getClient(), server, "[22.3,)")) {
                throw new SkipException("Snappy decompression was supported since 22.3");
            }
        }

        final int chunkSize = new ClickHouseConfig().getRequestChunkSize();
        final int randomLength = chunkSize * 5 + (int) (System.currentTimeMillis() % chunkSize);
        for (int i = startLevel; i <= endLevel; i += step) {
            final String tableName = ClickHouseUtils.format("test_%s_request_compress_%s_level%s",
                    server.getProtocol().name().toLowerCase(), compression.encoding(),
                    i < 0 ? "_" + Math.abs(i) : Integer.toString(i));
            final ByteArrayOutputStream o = new ByteArrayOutputStream();
            try (ClickHouseOutputStream out = ClickHouseOutputStream.of(o, readBufferSize, compression, i, null)) {
                out.write("1,23\n4,56".getBytes());
                out.flush();
            } catch (IOException e) {
                throw ClickHouseException.of(e, server);
            }
            try (ClickHouseClient client = getClient()) {
                final ClickHouseRequest<?> request = newRequest(client, server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .compressServerResponse(false)
                        .decompressClientRequest(true, compression, i);
                ClickHouseClient.send(server, "drop table if exists " + tableName,
                        "create table " + tableName + "(i Int32, s String)engine=MergeTree() order by i").get();
                try (ClickHouseResponse response = request.write().table(tableName).data(w -> {
                    for (int k = 0; k < randomLength; k++) {
                        BinaryStreamUtils.writeInt32(w, k);
                        w.writeUnicodeString(Integer.toString(k));
                    }
                }).executeAndWait()) {
                    // ignore
                }

                try (ClickHouseResponse response = request
                        .external(
                                // external table with compressed data
                                ClickHouseExternalTable.builder().name("x").columns("i Int32, s String")
                                        .compression(compression)
                                        .format(ClickHouseFormat.CSV)
                                        .content(new ByteArrayInputStream(o.toByteArray())).build(),
                                // external table without compression
                                ClickHouseExternalTable.builder().name("y").columns("s String, i Int32")
                                        .format(ClickHouseFormat.TSV)
                                        .content(new ByteArrayInputStream("32\t1\n43\t2\n54\t3\n65\t4".getBytes()))
                                        .build())
                        .query("select x.* from x inner join y on x.i = y.i where i in (select i from " + tableName
                                + ") ORDER BY 1")
                        .set("select_sequential_consistency", isCloud() ? 1 : null)
                        .executeAndWait()) {
                    int j = 0;

                    for (ClickHouseRecord r : response.records()) {
                        Assert.assertEquals(r.getValue(0).asInteger(), j == 0 ? 1 : 4);
                        Assert.assertEquals(r.getValue(1).asInteger(), j == 0 ? 23 : 56);
                        j++;
                    }
                    Assert.assertEquals(j, 2);
                }
            }
        }
    }

    @Test(dataProvider = "mixedCompressionMatrix", groups = "integration")
    public void testDecompressResponse(ClickHouseCompression reqComp, ClickHouseCompression respComp) throws Exception {
        if (reqComp == ClickHouseCompression.SNAPPY || respComp == ClickHouseCompression.BZ2) {
            if (!checkServerVersion(getClient(), getServer(), "[22.10,)")) {
                throw new SkipException("Snappy and bz2 were all supported since 22.10");
            }
        }

        final ClickHouseNode server = getServer();
        final int rows = 50_000;
        final String sql = ClickHouseUtils.format("select number n, toString(number+1) s from numbers(%d)", rows);
        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest<?> request = newRequest(client, server)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .decompressClientRequest(true, reqComp)
                    .compressServerResponse(true, respComp)
                    .query(sql);
            try (ClickHouseResponse response = request.executeAndWait()) {
                int i = 0;
                for (ClickHouseRecord r : response.records()) {
                    Assert.assertEquals(r.getValue(0).asInteger(), i++);
                    Assert.assertEquals(r.getValue(1).asInteger(), i);
                }
                Assert.assertEquals(i, rows);
            }

            File tmp = ClickHouseUtils.createTempFile(respComp.encoding(), respComp.fileExtension(), true);
            try (ClickHouseResponse response = request.output(tmp.toString()).format(ClickHouseFormat.CSV)
                    .executeAndWait()) {
                response.close();

                Assert.assertTrue(tmp.exists());
                Assert.assertNotEquals(Files.size(tmp.toPath()), 0L);

                int i = 0;
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                        ClickHouseProtocol.GRPC == server.getProtocol() && respComp == ClickHouseCompression.LZ4
                                ? new FramedLZ4CompressorInputStream(new FileInputStream(tmp))
                                : ClickHouseInputStream.of(new FileInputStream(tmp),
                                        request.getConfig().getReadBufferSize(), respComp,
                                        request.getConfig().getResponseCompressLevel(), null)))) {
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        Assert.assertEquals(line, ClickHouseUtils.format("%d,\"%d\"", i++, i));
                    }
                }
                Assert.assertEquals(i, rows);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testFormat() throws ClickHouseException {
        String sql = "select 1, 2";
        ClickHouseNode node = getServer();

        try (ClickHouseClient client = getClient()) {
            try (ClickHouseResponse response = newRequest(client, node)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes).query(sql).executeAndWait()) {
                Assert.assertEquals(response.getColumns().size(), 2);
                int counter = 0;
                for (ClickHouseRecord record : response.records()) {
                    Assert.assertEquals(record.getValue(0).asShort(), 1);
                    Assert.assertEquals(record.getValue(1).asShort(), 2);
                    counter++;
                }
                Assert.assertEquals(counter, 1);
            }

            // now let's try again using unsupported formats
            try (ClickHouseResponse response = newRequest(client, node).query(sql)
                    .format(ClickHouseFormat.CSV).executeAndWait()) {
                String results = new BufferedReader(
                        new InputStreamReader(response.getInputStream(), StandardCharsets.UTF_8)).lines()
                        .collect(Collectors.joining("\n"));
                Assert.assertEquals(results, "1,2");
            }

            try (ClickHouseResponse response = newRequest(client, node).query(sql)
                    .format(ClickHouseFormat.JSONEachRow).executeAndWait()) {
                String results = new BufferedReader(
                        new InputStreamReader(response.getInputStream(), StandardCharsets.UTF_8)).lines()
                        .collect(Collectors.joining("\n"));
                Assert.assertEquals(results, "{\"1\":1,\"2\":2}");
            }
        }
    }

    @Test(groups = "integration")
    public void testNonExistDb() throws ClickHouseException {
        ClickHouseNode server = getServer();

        try {
            ClickHouseClient.send(server, "drop database non_exist_db").get();
            Assert.fail("Exception is excepted");
        } catch (ExecutionException e) {
            ClickHouseException ce = ClickHouseException.of(e.getCause(), server);
            Assert.assertEquals(ce.getErrorCode(), 81, "Expected error code 81 but we got: " + ce.getMessage());
        } catch (InterruptedException e) {
            Assert.fail("Failed execute due to interruption", e);
        }

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = newRequest(client, server).use("non_exist_db")
                        .query("select 1").execute().get()) {
            Assert.fail("Exception is excepted");
        } catch (ExecutionException e) {
            ClickHouseException ce = ClickHouseException.of(e.getCause(), server);
            Assert.assertEquals(ce.getErrorCode(), 81, "Expected error code 81 but we got: " + ce.getMessage());
        } catch (InterruptedException e) {
            Assert.fail("Failed execute due to interruption", e);
        }

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = newRequest(client, server).use("").query("select 1")
                        .execute().get()) {
            Assert.assertEquals(resp.firstRecord().getValue(0).asInteger(), 1);
        } catch (Exception e) {
            Assert.fail("Should not have exception");
        }

        String db = new StringBuilder().append('`').append(UUID.randomUUID().toString()).append('`').toString();
        try (ClickHouseClient client = getClient()) {
            try (ClickHouseResponse resp = newRequest(client, server).use("")
                    .query("create database " + db).execute().get()) {
            }
            try (ClickHouseResponse resp = newRequest(client, server).use("")
                    .query("drop database " + db).execute().get()) {
            }
        } catch (Exception e) {
            Assert.fail("Should not have exception");
        } finally {
            dropDatabase(db);
        }
    }

    @Test(dataProvider = "primitiveArrayMatrix", groups = "integration")
    public void testPrimitiveArray(String baseType, Object expectedValues) throws ClickHouseException {
        ClickHouseNode server = getServer();

        String tableName = "test_primitive_array_"
                + baseType.replace('(', '_').replace(')', ' ').trim().toLowerCase();
        String tableColumns = String.format("a1 Array(%1$s), a2 Array(Array(%1$s)), a3 Array(Array(Array(%1$s)))",
                baseType);
        sendAndWait(server, "drop table if exists " + tableName,
                "create table " + tableName + " (" + tableColumns + ")engine=Memory",
                "insert into " + tableName + String.format(
                        " values(%2$s, [[123],[],[4], %2$s], [[[12],[3],[],[4,5]],[[123],[],[4], %2$s]])", baseType,
                        ClickHouseColumn.of("", ClickHouseDataType.Array, false,
                                ClickHouseColumn.of("", baseType)).newArrayValue(server.config).update(expectedValues)
                                .toSqlExpression()));

        checkPrimitiveArrayValues(server, tableName, tableColumns, baseType, expectedValues);
    }

    @Test(dataProvider = "primitiveArrayLowCardinalityMatrix", groups = "integration")
    public void testPrimitiveArrayWithLowCardinality(String baseType, Object expectedValues) throws ClickHouseException {
        ClickHouseNode server = getServer();

        String tableName = "test_primitive_array_"
                + baseType.replace('(', '_').replace(')', ' ').trim().toLowerCase();
        String tableColumns = String.format("a1 Array(%1$s), a2 Array(Array(%1$s)), a3 Array(Array(Array(%1$s)))",
                baseType);
        try {
            sendAndWait(server, "drop table if exists " + tableName,
                    "create table " + tableName + " (" + tableColumns + ")engine=Memory",
                    "insert into " + tableName + String.format(
                            " values(%2$s, [[123],[],[4], %2$s], [[[12],[3],[],[4,5]],[[123],[],[4], %2$s]])", baseType,
                            ClickHouseColumn.of("", ClickHouseDataType.Array, false,
                                            ClickHouseColumn.of("", baseType)).newArrayValue(server.config).update(expectedValues)
                                    .toSqlExpression()));
        } catch (ClickHouseException e) {
            try (ClickHouseClient client = getClient()) {
                if (e.getErrorCode() == ClickHouseException.ERROR_SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY &&
                        checkServerVersion(client, server, "[24.2,)")) {
                    return;
                }
            } catch ( Exception e1) {
                Assert.fail("Failed to check server version", e1);
            }

            Assert.fail("Exception code is " + e.getErrorCode(), e);
        }
        checkPrimitiveArrayValues(server, tableName, tableColumns, baseType, expectedValues);
    }

    private void checkPrimitiveArrayValues(ClickHouseNode server, String tableName, String tableColumns, String baseType, Object expectedValues) throws ClickHouseException {
        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest<?> request = newRequest(client, server)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes);
            try (ClickHouseResponse response = request.write().table(tableName).data(o -> {
                ClickHouseConfig config = request.getConfig();
                List<ClickHouseColumn> columns = ClickHouseColumn.parse(tableColumns);
                ClickHouseDataProcessor processor = ClickHouseDataStreamFactory.getInstance().getProcessor(config, null,
                        o, null, columns);
                ClickHouseColumn baseColumn = ClickHouseColumn.of("", baseType);
                Class<?> javaClass = expectedValues.getClass() == int[].class ? baseColumn.getPrimitiveClass(config)
                        : baseColumn.getObjectClass(config);
                ClickHouseColumn currentColumn = columns.get(0);

                ClickHouseArraySequence arr = currentColumn.newArrayValue(config);
                arr.update(expectedValues);
                processor.getSerializer(config, currentColumn).serialize(arr, o);

                currentColumn = columns.get(1);
                ClickHouseArraySequence val = currentColumn.newArrayValue(config);
                val.allocate(1, javaClass, 2).setValue(0, arr);
                processor.getSerializer(config, currentColumn).serialize(val, o);

                currentColumn = columns.get(2);
                arr = currentColumn.newArrayValue(config);
                arr.allocate(1, javaClass, 3).setValue(0, val);
                processor.getSerializer(config, currentColumn).serialize(arr, o);
            }).executeAndWait()) {
                // ignore
            }

            try (ClickHouseResponse response = request.query("select * from " + tableName).executeAndWait()) {
                for (ClickHouseRecord r : response.records()) {
                    ClickHouseArraySequence val = (ClickHouseArraySequence) r.getValue(0);
                    Assert.assertEquals(val.asObject(), val.copy().update(expectedValues).asObject());

                    ClickHouseArraySequence arr = (ClickHouseArraySequence) r.getValue(1);
                    val = arr.getValue(arr.length() - 1, ClickHouseColumn.of("c", String.format("Array(%s)", baseType))
                            .newArrayValue(request.getConfig()));
                    Assert.assertEquals(val.asObject(), val.copy().update(expectedValues).asObject());

                    arr = (ClickHouseArraySequence) r.getValue(2);
                    val = arr.getValue(arr.length() - 1,
                            ClickHouseColumn.of("c", String.format("Array(Array(%s))", baseType))
                                    .newArrayValue(request.getConfig()));
                    val = val.getValue(val.length() - 1, ClickHouseColumn.of("c", String.format("Array(%s)", baseType))
                            .newArrayValue(request.getConfig()));
                    Assert.assertEquals(val.asObject(), val.copy().update(expectedValues).asObject());
                }
            }
        }
    }


    @Test(groups = { "integration" })
    public void testQueryWithNoResult() throws ExecutionException, InterruptedException {
        String sql = "select * from system.numbers limit 0";

        try (ClickHouseClient client = getClient()) {
            // header without row
            try (ClickHouseResponse response = newRequest(client, getServer())
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes).query(sql).execute().get()) {
                Assert.assertFalse(response.getInputStream().isClosed(), "Input stream should NOT be closed");
                Assert.assertEquals(response.getColumns().size(), 1);
                Assert.assertNotEquals(response.getColumns(), ClickHouseDataProcessor.DEFAULT_COLUMNS);
                Assert.assertFalse(response.getInputStream().isClosed(), "Input stream should NOT be closed");
                for (ClickHouseRecord record : response.records()) {
                    Assert.fail(ClickHouseUtils.format("Should have no record, but we got: %s", record));
                }
                Assert.assertTrue(response.getInputStream().isClosed(),
                        "Input stream should have been closed since there's no data");
            }

            // no header and row
            try (ClickHouseResponse response = newRequest(client, getServer())
                    .format(ClickHouseFormat.RowBinary).query(sql).execute().get()) {
                Assert.assertFalse(response.getInputStream().isClosed(), "Input stream should NOT be closed");
                Assert.assertEquals(response.getColumns(), Collections.emptyList());
                Assert.assertTrue(response.getInputStream().isClosed(),
                        "Input stream should have been closed since there's no data");
                for (ClickHouseRecord record : response.records()) {
                    Assert.fail(ClickHouseUtils.format("Should have no record, but we got: %s", record));
                }
            }

            // custom header and row
            try (ClickHouseResponse response = newRequest(client, getServer())
                    .format(ClickHouseFormat.RowBinary).query(sql).execute().get()) {
                Assert.assertFalse(response.getInputStream().isClosed(), "Input stream should NOT be closed");
                Assert.assertEquals(response.getColumns(), Collections.emptyList());
                Assert.assertTrue(response.getInputStream().isClosed(),
                        "Input stream should have been closed since there's no data");
                for (ClickHouseRecord record : response.records()) {
                    Assert.fail(ClickHouseUtils.format("Should have no record, but we got: %s", record));
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testQuery() {
        testQuery(10000);
    }
    public void testQuery(int totalRecords) {
        ClickHouseNode server = getServer();

        try (ClickHouseClient client = getClient()) {
            // "select * from system.data_type_families"
            String sql = "select number, toString(number) from system.numbers limit " + totalRecords;

            try (ClickHouseResponse response = newRequest(client, server)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .set("send_logs_level", "trace")
                    .set("enable_optimize_predicate_expression", 1)
                    //.set("log_queries_min_type", "EXCEPTION_WHILE_PROCESSING")
                    .set("async_insert", isCloud() ? 0 : null)
                    .query(sql).execute().get()) {
                Assert.assertFalse(response.getInputStream().isClosed(), "Input stream should NOT be closed");
                List<ClickHouseColumn> columns = response.getColumns();
                Assert.assertFalse(response.getInputStream().isClosed(), "Input stream should NOT be closed");
                int index = 0;
                for (ClickHouseRecord record : response.records()) {
                    String col1 = String.valueOf(record.getValue(0).asBigInteger());
                    String col2 = record.getValue(1).asString();
                    Assert.assertEquals(record.size(), columns.size());
                    Assert.assertEquals(col1, col2);
                    Assert.assertEquals(col1, String.valueOf(index++));
                }
                Assert.assertTrue(response.getInputStream().isClosed(),
                        "Input stream should have been closed since there's no data");

                Assert.assertEquals(index, totalRecords);
            } catch (Exception e) {
                Assert.fail("Query failed", e);
            }
        }
    }

    @Test(groups = "integration")
    public void testQueryInSameThread() throws ExecutionException, InterruptedException {
        ClickHouseNode server = getServer();

        try (ClickHouseClient client = ClickHouseClient.builder().nodeSelector(ClickHouseNodeSelector.EMPTY)
                .option(ClickHouseClientOption.ASYNC, false).build()) {
            CompletableFuture<ClickHouseResponse> future = newRequest(client, server)
                    .format(ClickHouseFormat.TabSeparatedWithNamesAndTypes).query("select 1,2").execute();
            // Assert.assertTrue(future instanceof ClickHouseImmediateFuture);
            Assert.assertTrue(future.isDone());
            try (ClickHouseResponse resp = future.get()) {
                Assert.assertFalse(resp.getInputStream().isClosed(), "Input stream should NOT be closed");
                Assert.assertEquals(resp.getColumns().size(), 2);
                Assert.assertFalse(resp.getInputStream().isClosed(), "Input stream should NOT be closed");
                for (ClickHouseRecord record : resp.records()) {
                    Assert.assertEquals(record.size(), 2);
                    Assert.assertEquals(record.getValue(0).asInteger(), 1);
                    Assert.assertEquals(record.getValue(1).asInteger(), 2);
                }
                Assert.assertTrue(resp.getInputStream().isClosed(),
                        "Input stream should have been closed since there's no data");
                // ClickHouseResponseSummary summary = resp.getSummary();
                // Assert.assertEquals(summary.getStatistics().getRows(), 1);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testMutation() throws ClickHouseException {
        ClickHouseNode node = getServer();

        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest<?> request = newRequest(client, node)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .set("send_logs_level", "trace")
                    .set("enable_optimize_predicate_expression", 1)
                    .set("log_queries_min_type", "EXCEPTION_WHILE_PROCESSING");
            execute(request, "drop table if exists test_mutation;");
            execute(request, "create table if not exists test_mutation(a String, b UInt32) engine = Memory;");
            execute(request, "insert into test_mutation values('a', 1)('b', 2)");
        }
    }

    @Test(groups = "integration")
    public void testQueryIntervalTypes() throws ExecutionException, InterruptedException {
        ClickHouseNode server = getServer();

        try (ClickHouseClient client = getClient()) {
            for (ClickHouseDataType type : new ClickHouseDataType[] { ClickHouseDataType.IntervalYear,
                    ClickHouseDataType.IntervalQuarter, ClickHouseDataType.IntervalMonth,
                    ClickHouseDataType.IntervalWeek, ClickHouseDataType.IntervalDay, ClickHouseDataType.IntervalHour,
                    ClickHouseDataType.IntervalMinute, ClickHouseDataType.IntervalSecond }) {
                try (ClickHouseResponse resp = newRequest(client, server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query(ClickHouseUtils.format(
                                "select to%1$s(0), to%1$s(-1), to%1$s(1), to%1$s(%2$d), to%1$s(%3$d)", type.name(),
                                Long.MIN_VALUE, Long.MAX_VALUE))
                        .execute().get()) {
                    List<ClickHouseRecord> records = new ArrayList<>();
                    for (ClickHouseRecord record : resp.records()) {
                        records.add(record);
                    }

                    Assert.assertEquals(records.size(), 1);
                    ClickHouseRecord r = records.get(0);
                    Assert.assertEquals(r.getValue(0).asString(), "0");
                    Assert.assertEquals(r.getValue(1).asString(), "-1");
                    Assert.assertEquals(r.getValue(2).asString(), "1");
                    Assert.assertEquals(r.getValue(3).asString(), String.valueOf(Long.MIN_VALUE));
                    Assert.assertEquals(r.getValue(4).asString(), String.valueOf(Long.MAX_VALUE));
                }
            }
        }
    }

    @Test(groups = "integration")
    public void testReadWriteDateTimeTypes() throws ClickHouseException {
        ClickHouseNode server = getServer();

        sendAndWait(server, "drop table if exists test_datetime_types",
                "create table test_datetime_types(no UInt8, d0 DateTime32, d1 DateTime64(5), d2 DateTime(3), d3 DateTime64(3, 'Asia/Chongqing')) engine=MergeTree ORDER BY no");
        sendAndWait(server, "insert into test_datetime_types values(:no, :d0, :d1, :d2, :d3)",
                new ClickHouseValue[] { ClickHouseIntegerValue.ofNull(),
                        ClickHouseDateTimeValue.ofNull(0, ClickHouseValues.UTC_TIMEZONE),
                        ClickHouseDateTimeValue.ofNull(3, ClickHouseValues.UTC_TIMEZONE),
                        ClickHouseDateTimeValue.ofNull(9, ClickHouseValues.UTC_TIMEZONE),
                        ClickHouseOffsetDateTimeValue.ofNull(3, TimeZone.getTimeZone("Asia/Chongqing")) },
                new Object[] { 0, "1970-01-01 00:00:00", "1970-01-01 00:00:00.123456",
                        "1970-01-01 00:00:00.123456789", "1970-02-01 12:34:56.789" },
                new Object[] { 1, -1, -1, -1, -1 }, new Object[] { 2, 1, 1, 1, 1 },
                new Object[] { 3, 2.1, 2.1, 2.1, 2.1 });

        String selectString = "SELECT * EXCEPT(no) FROM test_datetime_types ORDER BY no";

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = newRequest(client, server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .set("select_sequential_consistency", isCloud() ? 1 : null)
                        .query(selectString).executeAndWait()) {
            List<ClickHouseRecord> list = new ArrayList<>();
            for (ClickHouseRecord record : resp.records()) {
                list.add(record);
            }

            Assert.assertEquals(list.size(), 4);
        }
    }

    @Test(groups = "integration")
    public void testReadWriteDomains() throws ClickHouseException, UnknownHostException {
        ClickHouseNode server = getServer();

        sendAndWait(server, "drop table if exists test_domain_types",
                "create table test_domain_types(no UInt8, ipv4 IPv4, nipv4 Nullable(IPv4), ipv6 IPv6, nipv6 Nullable(IPv6)) engine=MergeTree ORDER BY no");

        sendAndWait(server, "insert into test_domain_types values(:no, :i0, :i1, :i2, :i3)",
                new ClickHouseValue[] { ClickHouseIntegerValue.ofNull(), ClickHouseIpv4Value.ofNull(),
                        ClickHouseIpv4Value.ofNull(), ClickHouseIpv6Value.ofNull(), ClickHouseIpv6Value.ofNull() },
                new Object[] { 0,
                        (Inet4Address) InetAddress
                                .getByAddress(new byte[] { (byte) 0, (byte) 0, (byte) 0, (byte) 0 }),
                        null,
                        Inet6Address.getByAddress(null,
                                new byte[] { (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                                        (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                                        (byte) 0,
                                        (byte) 0 },
                                null),
                        null },
                new Object[] { 1,
                        (Inet4Address) InetAddress
                                .getByAddress(new byte[] { (byte) 0, (byte) 0, (byte) 0, (byte) 1 }),
                        (Inet4Address) InetAddress
                                .getByAddress(new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF }),
                        Inet6Address.getByAddress(null,
                                new byte[] { (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                                        (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                                        (byte) 0,
                                        (byte) 1 },
                                null),
                        Inet6Address.getByAddress(null,
                                new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                                        (byte) 0xFF,
                                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF },
                                null) });

        String selectString = "SELECT * EXCEPT(no) FROM test_domain_types ORDER BY no";

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = newRequest(client, server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .set("select_sequential_consistency", isCloud() ? 1 : null)
                        .query(selectString).executeAndWait()) {
            List<ClickHouseRecord> list = new ArrayList<>();
            for (ClickHouseRecord record : resp.records()) {
                list.add(record);
            }

            Assert.assertEquals(list.size(), 2);
        }
    }

    @Test(groups = "integration")
    public void testReadWriteEnumTypes() throws ClickHouseException {
        ClickHouseNode server = getServer();

        sendAndWait(server, "drop table if exists test_enum_types",
                "create table test_enum_types(no UInt8, e01 Nullable(Enum8('a'=-1,'b'=2,'c'=0)), e1 Enum8('a'=-1,'b'=2,'c'=0), "
                        + "e02 Nullable(Enum16('a'=-1,'b'=2,'c'=0)), e2 Enum16('a'=-1,'b'=2,'c'=0)) engine=MergeTree ORDER BY no");
        sendAndWait(server, "insert into test_enum_types values(:no, :e01, :e1, :e02, :e2)",
                new ClickHouseValue[] { ClickHouseByteValue.ofNull(),
                        ClickHouseEnumValue
                                .ofNull(ClickHouseColumn.of("column", "Enum8('dunno'=-1)").getEnumConstants()),
                        ClickHouseEnumValue
                                .ofNull(ClickHouseColumn.of("column", "Enum8('a'=-1,'b'=2,'c'=0)").getEnumConstants()),
                        ClickHouseEnumValue
                                .ofNull(ClickHouseColumn.of("column", "Enum16('a'=-1,'b'=2,'c'=0)").getEnumConstants()),
                        ClickHouseEnumValue
                                .ofNull(ClickHouseColumn.of("column", "Enum16('dunno'=2)").getEnumConstants()), },
                new Object[] { 0, null, "b", null, "dunno" },
                new Object[] { 1, "dunno", 2, "a", 2 });

        String selectQuery = "SELECT * EXCEPT(no) FROM test_enum_types ORDER BY no";

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = newRequest(client, server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .set("select_sequential_consistency", isCloud() ? 1 : null)
                        .query(selectQuery).executeAndWait()) {
            int count = 0;
            for (ClickHouseRecord r : resp.records()) {
                if (count++ == 0) {
                    Assert.assertEquals(r.getValue(0).asShort(), (short) 0);
                    Assert.assertEquals(r.getValue(0).asString(), null);
                    Assert.assertEquals(r.getValue(0).asObject(), null);
                    Assert.assertEquals(r.getValue(1).asInteger(), 2);
                    Assert.assertEquals(r.getValue(1).asString(), "b");
                    Assert.assertEquals(r.getValue(1).asObject(), "b");
                    Assert.assertEquals(r.getValue(2).asLong(), 0L);
                    Assert.assertEquals(r.getValue(2).asString(), null);
                    Assert.assertEquals(r.getValue(2).asObject(), null);
                    Assert.assertEquals(r.getValue(3).asByte(), (byte) 2);
                    Assert.assertEquals(r.getValue(3).asString(), "b");
                    Assert.assertEquals(r.getValue(3).asObject(), "b");
                } else {
                    Assert.assertEquals(r.getValue(0).asByte(), (byte) -1);
                    Assert.assertEquals(r.getValue(0).asString(), "a");
                    Assert.assertEquals(r.getValue(0).asObject(), "a");
                    Assert.assertEquals(r.getValue(1).asShort(), (short) 2);
                    Assert.assertEquals(r.getValue(1).asString(), "b");
                    Assert.assertEquals(r.getValue(1).asObject(), "b");
                    Assert.assertEquals(r.getValue(2).asInteger(), -1);
                    Assert.assertEquals(r.getValue(2).asString(), "a");
                    Assert.assertEquals(r.getValue(2).asObject(), "a");
                    Assert.assertEquals(r.getValue(3).asLong(), 2L);
                    Assert.assertEquals(r.getValue(3).asString(), "b");
                    Assert.assertEquals(r.getValue(3).asObject(), "b");
                }
            }

            Assert.assertEquals(count, 2);
        }
    }

    @Test(groups = "integration")
    public void testReadWriteGeoTypes() throws ClickHouseException {
        ClickHouseNode server = getServer();

        sendAndWait(server, "set allow_experimental_geo_types=1", "drop table if exists test_geo_types",
                "create table test_geo_types(no UInt8, p Point, r Ring, pg Polygon, mp MultiPolygon) engine=MergeTree order by no");

        // write
        sendAndWait(server,
                "insert into test_geo_types values(0, (0,0), " + "[(0,0),(0,0)], [[(0,0),(0,0)],[(0,0),(0,0)]], "
                        + "[[[(0,0),(0,0)],[(0,0),(0,0)]],[[(0,0),(0,0)],[(0,0),(0,0)]]])",
                "insert into test_geo_types values(1, (-1,-1), "
                        + "[(-1,-1),(-1,-1)], [[(-1,-1),(-1,-1)],[(-1,-1),(-1,-1)]], "
                        + "[[[(-1,-1),(-1,-1)],[(-1,-1),(-1,-1)]],[[(-1,-1),(-1,-1)],[(-1,-1),(-1,-1)]]])",
                "insert into test_geo_types values(2, (1,1), " + "[(1,1),(1,1)], [[(1,1),(1,1)],[(1,1),(1,1)]], "
                        + "[[[(1,1),(1,1)],[(1,1),(1,1)]],[[(1,1),(1,1)],[(1,1),(1,1)]]])");

        // read
        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = newRequest(client, server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .set("select_sequential_consistency", isCloud() ? 1 : null)
                        .query("select * from test_geo_types order by no")
                        .executeAndWait()) {
            List<String[]> records = new ArrayList<>();
            for (ClickHouseRecord record : resp.records()) {
                String[] values = new String[record.size()];
                int index = 0;
                for (ClickHouseValue v : record) {
                    values[index++] = v.asString();
                }
                records.add(values);
            }

            System.out.println(records);
            System.out.println(records.size());
            System.out.println(records.get(0)[1]);

            Assert.assertEquals(records.size(), 3);
            Assert.assertEquals(records.get(0)[1], "(0.0,0.0)");
            Assert.assertEquals(records.get(0)[2], "[(0.0,0.0),(0.0,0.0)]");
            Assert.assertEquals(records.get(0)[3], "[[(0.0,0.0),(0.0,0.0)],[(0.0,0.0),(0.0,0.0)]]");
            Assert.assertEquals(records.get(0)[4],
                    "[[[(0.0,0.0),(0.0,0.0)],[(0.0,0.0),(0.0,0.0)]],[[(0.0,0.0),(0.0,0.0)],[(0.0,0.0),(0.0,0.0)]]]");
            Assert.assertEquals(records.get(1)[1], "(-1.0,-1.0)");
            Assert.assertEquals(records.get(1)[2], "[(-1.0,-1.0),(-1.0,-1.0)]");
            Assert.assertEquals(records.get(1)[3], "[[(-1.0,-1.0),(-1.0,-1.0)],[(-1.0,-1.0),(-1.0,-1.0)]]");
            Assert.assertEquals(records.get(1)[4],
                    "[[[(-1.0,-1.0),(-1.0,-1.0)],[(-1.0,-1.0),(-1.0,-1.0)]],[[(-1.0,-1.0),(-1.0,-1.0)],[(-1.0,-1.0),(-1.0,-1.0)]]]");
            Assert.assertEquals(records.get(2)[1], "(1.0,1.0)");
            Assert.assertEquals(records.get(2)[2], "[(1.0,1.0),(1.0,1.0)]");
            Assert.assertEquals(records.get(2)[3], "[[(1.0,1.0),(1.0,1.0)],[(1.0,1.0),(1.0,1.0)]]");
            Assert.assertEquals(records.get(2)[4],
                    "[[[(1.0,1.0),(1.0,1.0)],[(1.0,1.0),(1.0,1.0)]],[[(1.0,1.0),(1.0,1.0)],[(1.0,1.0),(1.0,1.0)]]]");
        }
    }

    @Test(dataProvider = "simpleTypeProvider", groups = "integration")
    public void testReadWriteSimpleTypes(String dataType, String zero, String negativeOne, String positiveOne)
            throws ClickHouseException {
        ClickHouseNode server = getServer();

        String typeName = dataType;
        String columnName = typeName.toLowerCase();
        int currIdx = columnName.indexOf('(');
        if (currIdx > 0) {
            columnName = columnName.substring(0, currIdx);
        }
        String dropTemplate = "drop table if exists test_%s";
        String createTemplate = "create table test_%1$s(no UInt8, %1$s %2$s, n%1$s Nullable(%2$s)) engine=MergeTree ORDER BY no";
        String insertTemplate = "insert into table test_%s values(%s, %s, %s)";

        String negativeOneValue = "-1";
        String zeroValue = "0";
        String positiveOneValue = "1";
        if (dataType.startsWith(ClickHouseDataType.FixedString.name())) {
            negativeOneValue = "'-1'";
            zeroValue = "'0'";
            positiveOneValue = "'1'";
        } else if (dataType.startsWith(ClickHouseDataType.UUID.name())) {
            negativeOneValue = ClickHouseUtils.format("'%s'", ClickHouseIntegerValue.of(-1).asUuid());
            zeroValue = ClickHouseUtils.format("'%s'", ClickHouseIntegerValue.of(0).asUuid());
            positiveOneValue = ClickHouseUtils.format("'%s'", ClickHouseIntegerValue.of(1).asUuid());
        }

        try {
            ClickHouseClient
                    .send(server, ClickHouseUtils.format(dropTemplate, columnName),
                            ClickHouseUtils.format(createTemplate, columnName, typeName),
                            ClickHouseUtils.format(insertTemplate, columnName, 0, zeroValue, null),
                            ClickHouseUtils.format(insertTemplate, columnName, 1, zeroValue, zeroValue),
                            ClickHouseUtils.format(insertTemplate, columnName, 2, negativeOneValue, negativeOneValue),
                            ClickHouseUtils.format(insertTemplate, columnName, 3, positiveOneValue, positiveOneValue))
                    .get();
        } catch (ExecutionException e) {
            // maybe the type is just not supported, for example: Date32
            Throwable cause = e.getCause();
            Assert.assertTrue(cause instanceof ClickHouseException);
            return;
        } catch (InterruptedException e) {
            Assert.fail("Test was interrupted", e);
        }

        String selectQuery = "SELECT * EXCEPT(no), version() FROM test_%s ORDER BY no";

        ClickHouseVersion version = null;
        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = newRequest(client, server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query(ClickHouseUtils.format(selectQuery, columnName))
                        .set("select_sequential_consistency", isCloud() ? 1 : null)
                        .executeAndWait()) {
            List<String[]> records = new ArrayList<>();
            for (ClickHouseRecord record : resp.records()) {
                String[] values = new String[record.size()];
                int index = 0;
                for (ClickHouseValue v : record) {
                    values[index++] = v.asString();
                }
                records.add(values);
            }

            Assert.assertEquals(records.size(), 4);
            Assert.assertEquals(records.get(0)[0], zero);
            Assert.assertEquals(records.get(0)[1], null);
            if (version == null) {
                version = ClickHouseVersion.of(records.get(0)[2]);
            }

            Assert.assertEquals(records.get(1)[0], zero);
            Assert.assertEquals(records.get(1)[1], zero);
            Assert.assertEquals(records.get(3)[0], positiveOne);
            Assert.assertEquals(records.get(3)[1], positiveOne);

            if ((ClickHouseDataType.DateTime.name().equals(dataType)
                    || ClickHouseDataType.DateTime32.name().equals(dataType)) && version.getMajorVersion() == 21
                    && version.getMinorVersion() == 3) {
                // skip DateTime and DateTime32 negative test on 21.3 since it's not doing well
                // see https://github.com/ClickHouse/ClickHouse/issues/29835 for more
            } else {
                Assert.assertEquals(records.get(2)[0], negativeOne);
                Assert.assertEquals(records.get(2)[1], negativeOne);
            }
        }
    }

    @Test(groups = "integration")
    public void testReadWriteMap() throws ClickHouseException {
        ClickHouseNode server = getServer();

        try {
            ClickHouseClient
                    .send(server, "drop table if exists test_map_types",
                            "create table test_map_types(no UInt32, m Map(LowCardinality(String), Int32), n Map(String, Array(Nullable(DateTime64(3, 'Asia/Chongqing'))))) engine=MergeTree ORDER BY no")
                    .get();
        } catch (ExecutionException e) {
            // looks like LowCardinality(String) as key is not supported even in 21.8
            Throwable cause = e.getCause();
            Assert.assertTrue(cause instanceof ClickHouseException);
            return;
        } catch (InterruptedException e) {
            Assert.fail("Test was interrupted", e);
        }

        // write
        sendAndWait(server, "insert into test_map_types values (1, {'key1' : 1}, {'a' : [], 'b' : [null]})");
        try {
            ClickHouseClient.send(server, "insert into test_map_types values (:n,:m,:x)",
                    new String[][] {
                            new String[] { "-1", "{'key-1' : -1}",
                                    "{'a' : [], 'b' : [ '2022-03-30 00:00:00.123', null ]}" },
                            new String[] { "-2", "{'key-2' : -2}", "{'key-2' : [null]}" } })
                    .get();
            ClickHouseClient.send(server, "insert into test_map_types values (3, :m, {})",
                    Collections.singletonMap("m", "{'key3' : 3}")).get();
        } catch (Exception e) {
            Assert.fail("Insertion failed", e);
        }

        String selectQuery = "SELECT * EXCEPT(no) FROM test_map_types ORDER BY no";

        // read
        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = newRequest(client, server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .set("select_sequential_consistency", isCloud() ? 1 : null)
                        .query(selectQuery).executeAndWait()) {
            List<Object[]> records = new ArrayList<>();
            for (ClickHouseRecord r : resp.records()) {
                Object[] values = new Object[r.size()];
                int index = 0;
                for (ClickHouseValue v : r) {
                    values[index++] = v.asObject();
                }
                records.add(values);
            }

            Assert.assertEquals(records.size(), 4);
        }
    }

    @Test(groups = "integration")
    public void testReadWriteUInt64() throws ClickHouseException {
        ClickHouseNode server = getServer();

        // INSERT INTO test_table VALUES (10223372036854775100)
        sendAndWait(server, "drop table if exists test_uint64_values",
                "create table test_uint64_values(no UInt8, v0 UInt64, v1 UInt64, v2 UInt64, v3 UInt64) engine=MergeTree ORDER BY no");
        sendAndWait(server, "insert into test_uint64_values values(:no, :v0, :v1, :v2, :v3)",
                new ClickHouseValue[] { ClickHouseIntegerValue.ofNull(),
                        ClickHouseLongValue.ofNull(true), ClickHouseStringValue.ofNull(),
                        ClickHouseBigIntegerValue.ofNull(), ClickHouseBigDecimalValue.ofNull() },
                new Object[] { 0, 0L, "0", BigInteger.ZERO, BigDecimal.ZERO },
                new Object[] { 1, 1L, "1", BigInteger.ONE, BigDecimal.ONE },
                new Object[] { 2, Long.MAX_VALUE, Long.toString(Long.MAX_VALUE), BigInteger.valueOf(Long.MAX_VALUE),
                        BigDecimal.valueOf(Long.MAX_VALUE) },
                new Object[] { 3, -8223372036854776516L, "10223372036854775100", new BigInteger("10223372036854775100"),
                        new BigDecimal("10223372036854775100") });

        String selectQuery = "SELECT * EXCEPT(no) FROM test_uint64_values ORDER BY no";

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = newRequest(client, server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .set("select_sequential_consistency", isCloud() ? 1 : null)
                        .query(selectQuery).executeAndWait()) {
            int count = 0;
            for (ClickHouseRecord r : resp.records()) {
                if (count == 0) {
                    Assert.assertEquals(r.getValue(0).asLong(), 0L);
                    Assert.assertEquals(r.getValue(1).asLong(), 0L);
                    Assert.assertEquals(r.getValue(2).asLong(), 0L);
                    Assert.assertEquals(r.getValue(3).asLong(), 0L);
                } else if (count == 1) {
                    Assert.assertEquals(r.getValue(0).asLong(), 1L);
                    Assert.assertEquals(r.getValue(1).asLong(), 1L);
                    Assert.assertEquals(r.getValue(2).asLong(), 1L);
                    Assert.assertEquals(r.getValue(3).asLong(), 1L);
                } else if (count == 2) {
                    Assert.assertEquals(r.getValue(0).asLong(), Long.MAX_VALUE);
                    Assert.assertEquals(r.getValue(1).asLong(), Long.MAX_VALUE);
                    Assert.assertEquals(r.getValue(2).asLong(), Long.MAX_VALUE);
                    Assert.assertEquals(r.getValue(3).asLong(), Long.MAX_VALUE);
                } else if (count == 3) {
                    Assert.assertEquals(r.getValue(0).asString(), "10223372036854775100");
                    Assert.assertEquals(r.getValue(1).asBigInteger(), new BigInteger("10223372036854775100"));
                    Assert.assertEquals(r.getValue(2).asBigDecimal(), new BigDecimal("10223372036854775100"));
                    Assert.assertEquals(r.getValue(3).asLong(), -8223372036854776516L);
                }
                count++;
            }

            Assert.assertEquals(count, 4);
        }
    }

    @Test(groups = "integration")
    public void testWriteFixedString() throws ClickHouseException {
        ClickHouseNode server = getServer();
        sendAndWait(server, "drop table if exists test_write_fixed_string",
                "create table test_write_fixed_string(a Int8, b FixedString(3)) engine=MergeTree ORDER BY a");
        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest<?> req = newRequest(client, server)
                    .set("async_insert", isCloud() ? 0 : null)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes);
            try (ClickHouseResponse resp = req.write().table("test_write_fixed_string").data(o -> {
                o.writeByte((byte) 1);
                o.writeBytes(ClickHouseStringValue.of("a").asBinary(3));
            }).executeAndWait()) {
                // ignore
            }
            try (ClickHouseResponse resp = req.write().table("test_write_fixed_string").data(o -> {
                o.writeByte((byte) 2);
                o.writeBytes(ClickHouseStringValue.of("abc").asBinary(3));
            }).executeAndWait()) {
                // ignore
            }
            try (ClickHouseResponse resp = req.write().table("test_write_fixed_string").data(o -> {
                o.writeByte((byte) 3);
                o.writeBytes(ClickHouseStringValue.of("abcd").asBinary(3));
            }).executeAndWait()) {
                Assert.fail("Should fail to insert because the string was too long");
            } catch (ClickHouseException e) {
                Assert.assertTrue(e.getErrorCode() >= 33);
            }

            try (ClickHouseResponse resp = req
                    .copy()
                    .set("select_sequential_consistency", isCloud() ? 1 : null)
                    .query("select b from test_write_fixed_string order by a")
                    .executeAndWait()) {
                int i = 0;
                for (ClickHouseRecord r : resp.records()) {
                    Assert.assertEquals(r.getValue(0).asString(), i == 0 ? "a\0\0" : "abc");
                    i++;
                }
                Assert.assertEquals(i, 2);
            }
        }
    }

    @Test(groups = "integration")
    public void testQueryWithMultipleExternalTables() throws ExecutionException, InterruptedException {
        ClickHouseNode server = getServer();

        int tables = 30;
        int rows = 10;
        try (ClickHouseClient client = getClient()) {
            try (ClickHouseResponse resp = newRequest(client, server)
                    .query("drop table if exists test_ext_data_query")
                    .execute().get()) {
            }

            String ddl = "create table test_ext_data_query (\n" + "   Cb String,\n" + "   CREATETIME DateTime64(3),\n"
                    + "   TIMESTAMP UInt64,\n" + "   Cc String,\n" + "   Ca1 UInt64,\n" + "   Ca2 UInt64,\n"
                    + "   Ca3 UInt64\n" + ") engine = MergeTree()\n" + "PARTITION BY toYYYYMMDD(CREATETIME)\n"
                    + "ORDER BY (Cb, CREATETIME, Cc);";
            try (ClickHouseResponse resp = newRequest(client, server).query(ddl).execute().get()) {
            }
        }

        String template = "avgIf(Ca1, Cb in L%1$d) as avgCa1%2$d, sumIf(Ca1, Cb in L%1$d) as sumCa1%2$d, minIf(Ca1, Cb in L%1$d) as minCa1%2$d, maxIf(Ca1, Cb in L%1$d) as maxCa1%2$d, anyIf(Ca1, Cb in L%1$d) as anyCa1%2$d, avgIf(Ca2, Cb in L%1$d) as avgCa2%2$d, sumIf(Ca2, Cb in L%1$d) as sumCa2%2$d, minIf(Ca2, Cb in L%1$d) as minCa2%2$d, maxIf(Ca2, Cb in L%1$d) as maxCa2%2$d, anyIf(Ca2, Cb in L%1$d) as anyCa2%2$d, avgIf(Ca3, Cb in L%1$d) as avgCa3%2$d, sumIf(Ca3, Cb in L%1$d) as sumCa3%2$d, minIf(Ca3, Cb in L%1$d) as minCa3%2$d, maxIf(Ca3, Cb in L%1$d) as maxCa3%2$d, anyIf(Ca3, Cb in L%1$d) as anyCa3%2$d";
        StringBuilder sql = new StringBuilder().append("select ");
        List<ClickHouseExternalTable> extTableList = new ArrayList<>(tables);
        for (int i = 0; i < tables; i++) {
            sql.append(ClickHouseUtils.format(template, i, i + 1)).append(',');
            List<String> valueList = new ArrayList<>(rows);
            for (int j = i, size = i + rows; j < size; j++) {
                valueList.add(String.valueOf(j));
            }
            String dnExtString = String.join("\n", valueList);
            InputStream inputStream = new ByteArrayInputStream(dnExtString.getBytes(StandardCharsets.UTF_8));
            ClickHouseExternalTable extTable = ClickHouseExternalTable.builder().name("L" + i).content(inputStream)
                    .addColumn("Cb", "String").build();
            extTableList.add(extTable);
        }

        if (tables > 0) {
            sql.deleteCharAt(sql.length() - 1);
        } else {
            sql.append('*');
        }
        sql.append(
                " from test_ext_data_query where TIMESTAMP >= 1625796480 and TIMESTAMP < 1625796540 and Cc = 'eth0'");

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = newRequest(client, server).query(sql.toString())
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes).external(extTableList).execute().get()) {
            Assert.assertNotNull(resp.getColumns());
            Assert.assertTrue(tables <= 0 || resp.records().iterator().hasNext());
        }
    }

    @Test(groups = { "integration" })
    public void testCustomRead() throws ClickHouseException, IOException {
        long limit = 1000L;
        long count = 0L;
        ClickHouseNode server = getServer();
        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest<?> request = newRequest(client, server)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .query("select * from numbers(:limit)").params(String.valueOf(limit));
            ClickHouseConfig config = request.getConfig();
            try (ClickHouseResponse response = request.executeAndWait()) {
                ClickHouseInputStream input = response.getInputStream();
                List<ClickHouseColumn> list = response.getColumns();
                ClickHouseColumn[] columns = list.toArray(new ClickHouseColumn[0]);
                ClickHouseValue[] values = ClickHouseValues.newValues(config, columns);
                ClickHouseDataProcessor processor = ClickHouseDataStreamFactory.getInstance()
                        .getProcessor(config, input, null, null, list);
                int len = columns.length;
                while (input.available() > 0) {
                    for (int i = 0; i < len; i++) {
                        Assert.assertEquals(processor.read(values[i]).asLong(), count++);
                    }
                }
            }
        }

        Assert.assertEquals(count, 1000L);
    }

    @Test(groups = { "integration" })
    public void testCustomWriter() throws ClickHouseException {
        ClickHouseNode server = getServer();
        sendAndWait(server, "drop table if exists test_custom_writer",
                "create table test_custom_writer(a Int8) engine=MergeTree ORDER BY a");

        try (ClickHouseClient client = getClient()) {
            AtomicInteger i = new AtomicInteger(1);
            ClickHouseWriter w = o -> {
                o.write(i.getAndIncrement());
            };
            ClickHouseRequest.Mutation req = newRequest(client, server).write()
                    .format(ClickHouseFormat.RowBinary)
                    .table("test_custom_writer");
            for (boolean b : new boolean[] { true, false }) {
                req.option(ClickHouseClientOption.ASYNC, b);

                try (ClickHouseResponse resp = req.data(w).execute().get()) {
                    Assert.assertNotNull(resp);
                } catch (Exception e) {
                    Assert.fail("Failed to call execute() followed by get(): async=" + b, e);
                }
                Assert.assertTrue(req.getInputStream().get().isClosed(), "Input stream should have been closed");

                try (ClickHouseResponse resp = req.data(w).executeAndWait()) {
                    Assert.assertNotNull(resp);
                } catch (Exception e) {
                    Assert.fail("Failed to call executeAndWait(): async=" + b, e);
                }
                Assert.assertTrue(req.getInputStream().get().isClosed(), "Input stream should have been closed");
            }

            String selectQuery = "select count(1) from test_custom_writer";

            try (ClickHouseResponse resp = newRequest(client, server)
                    .query(selectQuery)
                    .set("select_sequential_consistency", isCloud() ? 1 : null)
                    .executeAndWait()) {
                Assert.assertEquals(resp.firstRecord().getValue(0).asInteger(), i.get() - 1);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testDumpAndLoadFile() throws ClickHouseException, IOException {
        ClickHouseNode server = getServer();
        sendAndWait(server, "drop table if exists test_dump_load_file",
                "create table test_dump_load_file(a UInt64, b Nullable(String)) engine=MergeTree() order by a");

        final int rows = 10000;
        final Path tmp = Paths.get(System.getProperty("java.io.tmpdir"), "file.csv");
        ClickHouseFile file = ClickHouseFile.of(tmp);
        try {
            ClickHouseClient.dump(server,
                    ClickHouseUtils.format(
                            "select number a, if(modulo(number, 2) = 0, null, toString(number)) b from numbers(%d)",
                            rows),
                    file).get();
        } catch (Exception e) {
            Assert.fail("Failed to dump data", e);
        }
        Assert.assertTrue(Files.exists(tmp), ClickHouseUtils.format("File [%s] should exist", tmp));
        Assert.assertTrue(Files.size(tmp) > 0, ClickHouseUtils.format("File [%s] should have content", tmp));

        try {
            ClickHouseClient.load(server, "test_dump_load_file", file).get();
        } catch (Exception e) {
            Assert.fail("Failed to load file", e);
        }

        try (ClickHouseClient client = getClient();
                ClickHouseResponse response = newRequest(client, server)
                        .query("select count(1) from test_dump_load_file")
                        .set("select_sequential_consistency", isCloud() ? 1 : null)
                        .executeAndWait()) {
            Assert.assertEquals(response.firstRecord().getValue(0).asInteger(), rows);
        }

        try (ClickHouseClient client = getClient();
                ClickHouseResponse response = newRequest(client, server)
                        .query("select count(1) from test_dump_load_file where b is null")
                        .set("select_sequential_consistency", isCloud() ? 1 : null)
                        .executeAndWait()) {
            Assert.assertEquals(response.firstRecord().getValue(0).asInteger(), rows / 2);
        }
    }

    @Test(groups = { "integration" })
    public void testDump() throws ExecutionException, InterruptedException, IOException {
        ClickHouseNode server = getServer();

        Path temp = Files.createTempFile("dump", ".tsv");
        Assert.assertEquals(Files.size(temp), 0L);

        int lines = 10000;
        ClickHouseResponseSummary summary = ClickHouseClient
                .dump(server, ClickHouseUtils.format("select * from numbers(%d)", lines), temp.toString(),
                        ClickHouseCompression.NONE, ClickHouseFormat.TabSeparated)
                .get();
        Assert.assertNotNull(summary);
        // Assert.assertEquals(summary.getReadRows(), lines);

        int counter = 0;
        for (String line : Files.readAllLines(temp)) {
            Assert.assertEquals(String.valueOf(counter++), line);
        }
        Assert.assertEquals(counter, lines);

        Files.delete(temp);
    }

    @Test(dataProvider = "fileProcessMatrix", groups = "integration")
    public void testDumpFile(boolean gzipCompressed, boolean useOneLiner)
            throws ExecutionException, InterruptedException, IOException {
        ClickHouseNode server = getServer();
        if (server.getProtocol() != ClickHouseProtocol.GRPC && server.getProtocol() != ClickHouseProtocol.HTTP) {
            throw new SkipException("Skip as only http and grpc implementation work well");
        }

        final File file = ClickHouseUtils.createTempFile();
        final ClickHouseFile wrappedFile = ClickHouseFile.of(file,
                gzipCompressed ? ClickHouseCompression.GZIP : ClickHouseCompression.NONE, ClickHouseFormat.CSV);
        String query = "select number, if(number % 2 = 0, null, toString(number)) str from numbers(10)";
        final ClickHouseResponseSummary summary;
        if (useOneLiner) {
            summary = ClickHouseClient.dump(server, query, wrappedFile).get();
        } else {
            try (ClickHouseClient client = getClient();
                    ClickHouseResponse response = newRequest(client, server).output(wrappedFile).query(query).execute()
                            .get()) {
                summary = response.getSummary();
            }
        }
        Assert.assertNotNull(summary);
        long fileSize = Files.size(file.toPath());
        Assert.assertTrue(fileSize > 0L, "Expects an non-empty file being created");
        try (InputStream in = gzipCompressed ? new GZIPInputStream(new FileInputStream(file))
                : new FileInputStream(file); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            ClickHouseInputStream.pipe(in, out, 512);
            String content = new String(out.toByteArray(), StandardCharsets.US_ASCII);
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < 10; i++) {
                builder.append(i).append(',');
                if (i % 2 == 0) {
                    builder.append("\\N");
                } else {
                    builder.append('"').append(i).append('"');
                }
                builder.append('\n');
            }
            Assert.assertEquals(content, builder.toString());
        } finally {
            file.delete();
        }
    }

    @Test(groups = { "integration" })
    public void testCustomLoad() throws ClickHouseException {
        ClickHouseNode server = getServer();

        sendAndWait(server, "drop table if exists test_custom_load",
                "CREATE table test_custom_load(n UInt32, s Nullable(String)) engine = MergeTree() order by n");

        try {
            ClickHouseClient.load(server, "test_custom_load",
                    new ClickHouseWriter() {
                        @Override
                        public void write(ClickHouseOutputStream output) throws IOException {
                            output.write("1\t\\N\n".getBytes(StandardCharsets.US_ASCII));
                            output.write("2\t123".getBytes(StandardCharsets.US_ASCII));
                        }
                    }, ClickHouseCompression.NONE, ClickHouseFormat.TabSeparated).get();
        } catch (Exception e) {
            Assert.fail("Failed to load data", e);
        }

        String selectQuery = "SELECT * FROM test_custom_load ORDER BY n";

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = newRequest(client, server)
                        .query(selectQuery)
                        .set("select_sequential_consistency", isCloud() ? 1 : null)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes).executeAndWait()) {
            Assert.assertNotNull(resp.getColumns());
            List<String[]> values = new ArrayList<>();
            for (ClickHouseRecord record : resp.records()) {
                String[] arr = new String[2];
                arr[0] = record.getValue(0).asString();
                arr[1] = record.getValue(1).asString();
                values.add(arr);
            }

            Assert.assertEquals(values.size(), 2);
            Assert.assertEquals(values.get(0), new String[] { "1", null });
            Assert.assertEquals(values.get(1), new String[] { "2", "123" });
        }
    }

    @Test(groups = { "integration" })
    public void testLoadCsv() throws ExecutionException, InterruptedException, IOException {
        ClickHouseNode server = getServer();

        List<ClickHouseResponseSummary> summaries = ClickHouseClient
                .send(server, "DROP TABLE IF EXISTS test_load_csv",
                        "CREATE TABLE test_load_csv(n UInt32) ENGINE = MergeTree ORDER BY n")
                .get();
        Assert.assertNotNull(summaries);
        Assert.assertEquals(summaries.size(), 2);

        Path temp = Files.createTempFile("data", ".tsv");
        Assert.assertEquals(Files.size(temp), 0L);

        int lines = 10000;
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < lines; i++) {
            builder.append(i).append('\n');
        }
        Files.write(temp, builder.toString().getBytes(StandardCharsets.US_ASCII));
        Assert.assertTrue(Files.size(temp) > 0L);

        ClickHouseResponseSummary summary = ClickHouseClient.load(server, "test_load_csv", temp.toString(),
                ClickHouseCompression.NONE, ClickHouseFormat.TabSeparated).get();
        Assert.assertNotNull(summary);

        String selectQuery = "SELECT count(1) FROM test_load_csv";

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = newRequest(client, server)
                        .query(selectQuery)
                        .set("select_sequential_consistency", isCloud() ? 1 : null)
                        .executeAndWait()) {
            Assert.assertEquals(resp.firstRecord().getValue(0).asInteger(), lines);
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        }

        selectQuery = "SELECT min(n), max(n), count(1), uniqExact(n) FROM test_load_csv";

        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = newRequest(client, server)
                        .query(selectQuery)
                        .set("select_sequential_consistency", isCloud() ? 1 : null)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes).executeAndWait()) {
            Assert.assertNotNull(resp.getColumns());
            for (ClickHouseRecord record : resp.records()) {
                Assert.assertNotNull(record);
                Assert.assertEquals(record.getValue(0).asLong(), 0L);
                Assert.assertEquals(record.getValue(1).asLong(), lines - 1);
                Assert.assertEquals(record.getValue(2).asLong(), lines);
                Assert.assertEquals(record.getValue(3).asLong(), lines);
            }
        } catch (ClickHouseException e) {
            throw new RuntimeException(e);
        } finally {
            Files.delete(temp);
        }
    }

    @Test(dataProvider = "fileProcessMatrix", groups = "integration")
    public void testLoadFile(boolean gzipCompressed, boolean useOneLiner) throws ClickHouseException, IOException {
        ClickHouseNode server = getServer();
        if (server.getProtocol() != ClickHouseProtocol.GRPC && server.getProtocol() != ClickHouseProtocol.HTTP) {
            throw new SkipException("Skip as only http and grpc implementation work well");
        }

        File file = File.createTempFile("chc", ".data");
        Object[][] data = new Object[][] {
                { 1, "12345" },
                { 2, "23456" },
                { 3, "\\N" },
                { 4, "x" },
                { 5, "y" },
        };
        try (OutputStream out = gzipCompressed ? new GZIPOutputStream(new FileOutputStream(file))
                : new FileOutputStream(file)) {
            for (Object[] row : data) {
                out.write((row[0] + "," + row[1]).getBytes(StandardCharsets.US_ASCII));
                if ((int) row[0] != 5) {
                    out.write(10);
                }
            }
            out.flush();
        }

        sendAndWait(server, "drop table if exists test_load_file",
                "create table test_load_file(a Int32, b Nullable(String)) engine=MergeTree() order by a");
        ClickHouseFile wrappedFile = ClickHouseFile.of(file,
                gzipCompressed ? ClickHouseCompression.GZIP : ClickHouseCompression.NONE, ClickHouseFormat.CSV);
        if (useOneLiner) {
            try {
                ClickHouseClient.load(server, "test_load_file", wrappedFile).get();
            } catch (Exception e) {
                Assert.fail("Failed to load file", e);
            }
        } else {
            try (ClickHouseClient client = getClient();
                    ClickHouseResponse response = newRequest(client, server).write()
                            .table("test_load_file").data(wrappedFile).executeAndWait()) {
                // ignore
            }
        }
        try (ClickHouseClient client = getClient();
                ClickHouseResponse response = newRequest(client, server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .set("select_sequential_consistency", isCloud() ? 1 : null)
                        .query("select * from test_load_file order by a").executeAndWait()) {
            int row = 0;
            for (ClickHouseRecord r : response.records()) {
                Assert.assertEquals(r.getValue(0).asObject(), data[row][0]);
                if (row == 2) {
                    Assert.assertNull(r.getValue(1).asObject());
                } else {
                    Assert.assertEquals(r.getValue(1).asObject(), data[row][1]);
                }
                row++;
            }
        } finally {
            file.delete();
        }
    }

    @Test(groups = { "integration" })
    public void testLoadRawData() throws ClickHouseException, IOException {
        ClickHouseNode server = getServer();
        sendAndWait(server, "drop table if exists test_load_raw_data",
                "create table test_load_raw_data(a Int64)engine=Memory");
        int rows = 10;

        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest.Mutation request = newRequest(client, server).write()
                    .table("test_load_raw_data")
                    .format(ClickHouseFormat.RowBinary)
                    // this is needed to get meaningful response summary
                    .set("async_insert", isCloud() ? 0 : null);
            ClickHouseConfig config = request.getConfig();

            CompletableFuture<ClickHouseResponse> future = null;
            // single producer → single consumer
            // important to close the stream *before* retrieving response
            try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                    .createPipedOutputStream(config)) {
                // start the worker thread which transfer data from the input into ClickHouse
                future = request.data(stream.getInputStream()).execute();
                // write bytes into the piped stream
                for (int i = 0; i < rows; i++) {
                    BinaryStreamUtils.writeInt64(stream, i);
                }
            } catch (Exception e) {
                Assert.fail("Failed to execute", e);
            }

            ClickHouseResponseSummary summary = null;
            try (ClickHouseResponse response = future.get()) {
                summary = response.getSummary();
            } catch (Exception e) {
                Assert.fail("Failed to get result", e);
            }

            Assert.assertEquals(summary.getWrittenRows(), rows);
        }
    }

    @Test(groups = { "integration" })
    public void testMultipleQueries() throws ClickHouseException {
        ClickHouseNode server = getServer();
        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest<?> req = newRequest(client, server)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes);

            int result1 = 1;
            int result2 = 2;
            ClickHouseResponse queryResp = req.copy().query("select 1").executeAndWait();

            try (ClickHouseResponse resp = req.copy().query("select 2").executeAndWait()) {
                Assert.assertEquals(resp.firstRecord().getValue(0).asInteger(), result2);
            }

            result2 = 0;
            for (ClickHouseRecord r : queryResp.records()) {
                Assert.assertEquals(r.getValue(0).asInteger(), result1);
                result2++;
            }
            Assert.assertEquals(result2, 1, "Should have only one record");
        }
    }

    @Test(groups = { "integration" })
    public void testExternalTableAsParameter() throws ClickHouseException {
        ClickHouseNode server = getServer();
        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = newRequest(client, server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query("select toString(number) as query_id from numbers(100) "
                                + "where query_id not in (select query_id from ext_table) limit 10")
                        .external(ClickHouseExternalTable.builder().name("ext_table")
                                .columns("query_id String, a_num Nullable(Int32)").format(ClickHouseFormat.CSV)
                                .content(new ByteArrayInputStream(
                                        "\"1,2,3\",\\N\n2,333".getBytes(StandardCharsets.US_ASCII)))
                                .build())
                        .executeAndWait()) {
            for (ClickHouseRecord r : resp.records()) {
                Assert.assertNotNull(r);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testInsertWithCustomFormat() throws ClickHouseException {
        ClickHouseNode server = getServer();
        sendAndWait(server, "drop table if exists test_custom_input_format",
                "create table test_custom_input_format(i Int8, f String) engine=MergeTree ORDER BY i");
        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest<?> request = newRequest(client, server)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes);
            try (ClickHouseResponse response = request.write().table("test_custom_input_format")
                    .data(o -> o.writeByte((byte) 1).writeUnicodeString("RowBinary")).executeAndWait()) {
                // ignore
            }
            try (ClickHouseResponse response = request.write().format(ClickHouseFormat.CSVWithNames)
                    .table("test_custom_input_format")
                    .data(o -> o.writeBytes("i,f\n2,CSVWithNames\n3,CSVWithNames".getBytes(StandardCharsets.US_ASCII)))
                    .executeAndWait()) {
                // ignore
            }

            Path temp = Files.createTempFile("data", ".csv");
            Assert.assertEquals(Files.size(temp), 0L);
            Files.write(temp, "i,f\n4,CSVWithNames\n5,CSVWithNames\n".getBytes(StandardCharsets.US_ASCII));
            Assert.assertTrue(Files.size(temp) > 0L);
            try (ClickHouseResponse response = request.write()
                    .table("test_custom_input_format")
                    .data(temp.toFile().getAbsolutePath()) // format is now CSV
                    .format(ClickHouseFormat.CSVWithNames) // change format to CSVWithNames
                    .executeAndWait()) {
                // ignore
            }

            String selectQuery = "SELECT * FROM test_custom_input_format ORDER BY i";

            try (ClickHouseResponse response = request
                    .query(selectQuery)
                    .set("select_sequential_consistency", isCloud() ? 1 : null)
                    .executeAndWait()) {
                int count = 0;
                for (ClickHouseRecord r : response.records()) {
                    Assert.assertEquals(r.getValue(0).asInteger(), count + 1);
                    Assert.assertEquals(r.getValue(1).asString(), count < 1 ? "RowBinary" : "CSVWithNames");
                    count++;
                }
                Assert.assertEquals(count, 5);
            }
        } catch (IOException e) {
            throw ClickHouseException.of(e, server);
        }
    }

    @Test(groups = { "integration" })
    public void testInsertRawDataSimple() throws Exception {
        if (isCloud()) return; // TODO: This test is really just for performance purposes
        testInsertRawDataSimple(1000);
    }
    public void testInsertRawDataSimple(int numberOfRecords) throws Exception {
        String tableName = "test_insert_raw_data_simple";
        ClickHouseNode server = getServer();
        sendAndWait(server, "DROP TABLE IF EXISTS " + tableName,
                "CREATE TABLE IF NOT EXISTS "+ tableName + " (i Int16, f String) engine=MergeTree ORDER BY i");
        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest.Mutation request = client.read(server).write().table(tableName).format(ClickHouseFormat.JSONEachRow);
            ClickHouseConfig config = request.getConfig();
            CompletableFuture<ClickHouseResponse> future;
            try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance().createPipedOutputStream(config)) {
                // start the worker thread which transfer data from the input into ClickHouse
                future = request.data(stream.getInputStream()).execute();
                for (int i = 0; i < numberOfRecords; i++) {
                    BinaryStreamUtils.writeBytes(stream, String.format("{\"i\": %s, \"\": \"JSON\"}", i).getBytes(StandardCharsets.UTF_8));
                }
            }

            ClickHouseResponseSummary summary = future.get().getSummary();
            Assert.assertEquals(summary.getWrittenRows(), numberOfRecords);
        }
    }


    @Test(groups = { "integration" })
    public void testInsertWithInputFunction() throws ClickHouseException {
        ClickHouseNode server = getServer();
        sendAndWait(server, "drop table if exists test_input_function",
                "create table test_input_function(name String, value Nullable(Int32)) engine=MergeTree ORDER BY name");

        try (ClickHouseClient client = getClient()) {
            // default format ClickHouseFormat.TabSeparated
            ClickHouseRequest<?> req = newRequest(client, server);
            try (ClickHouseResponse resp = req.write().query(
                    "insert into test_input_function select col2, col3 from "
                            + "input('col1 UInt8, col2 String, col3 Int32')")
                    .data(new ByteArrayInputStream("1\t2\t33\n2\t3\t333".getBytes(StandardCharsets.US_ASCII)))
                    .executeAndWait()) {

            }

            String selectQuery = "SELECT * FROM test_input_function ORDER BY value";

            List<Object[]> values = new ArrayList<>();
            try (ClickHouseResponse resp = req
                    .query(selectQuery)
                    .set("select_sequential_consistency", isCloud() ? 1 : null)
                    .executeAndWait()) {
                for (ClickHouseRecord r : resp.records()) {
                    values.add(new Object[] { r.getValue(0).asObject() });
                }
            }
            Assert.assertEquals(values.toArray(new Object[0][]),
                    new Object[][] { new Object[] { "2\t33" }, new Object[] { "3\t333" } });
        }
    }

    @Test(dataProvider = "renameMethods", groups = "integration")
    public void testRenameResponseColumns(ClickHouseRenameMethod m, String col1, String col2, String col3)
            throws ClickHouseException {
        ClickHouseNode server = getServer();
        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = newRequest(client, server)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .option(ClickHouseClientOption.RENAME_RESPONSE_COLUMN, m)
                        .query("select 1 `a b c`, 2 ` `, 3 `d.E_f`").executeAndWait()) {
            Assert.assertEquals(resp.getColumns().get(0).getColumnName(), col1);
            Assert.assertEquals(resp.getColumns().get(1).getColumnName(), col2);
            Assert.assertEquals(resp.getColumns().get(2).getColumnName(), col3);
        }
    }

    @Test(groups = "integration")
    public void testTempTable() throws ClickHouseException {
        if (isCloud()) {return;}
        ClickHouseNode server = getServer();
        String sessionId = UUID.randomUUID().toString();
        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest<?> request = newRequest(client, server)
                    .format(ClickHouseFormat.RowBinary).session(sessionId);
            execute(request, "drop temporary table if exists my_temp_table");
            execute(request, "create temporary table my_temp_table(a Int8)");
            execute(request, "insert into my_temp_table values(2)");

            try (ClickHouseResponse resp = request.write().table("my_temp_table")
                    .data(new ByteArrayInputStream(new byte[] { 3 })).executeAndWait()) {
                // ignore
            }

            int count = 0;
            try (ClickHouseResponse resp = request.format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .query("select * from my_temp_table order by a").executeAndWait()) {
                for (ClickHouseRecord r : resp.records()) {
                    Assert.assertEquals(r.getValue(0).asInteger(), count++ == 0 ? 2 : 3);
                }
            }
            Assert.assertEquals(count, 2);
        }
    }

    @Test(groups = "integration")
    public void testErrorDuringInsert() throws ClickHouseException {
        ClickHouseNode server = getServer();
        sendAndWait(server, "drop table if exists error_during_insert",
                "create table error_during_insert(n UInt64, flag UInt8)engine=Null");
        boolean success = true;
        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = newRequest(client, server).write()
                        .format(ClickHouseFormat.RowBinary)
                        .query("insert into error_during_insert select number, throwIf(number>=100000000) from numbers(500000000)")
                        .executeAndWait()) {
            for (ClickHouseRecord r : resp.records()) {
                Assert.fail("Should have no record");
            }
            Assert.fail("Insert should be aborted");
        } catch (UncheckedIOException e) {
            ClickHouseException ex = ClickHouseException.of(e, server);
            Assert.assertEquals(ex.getErrorCode(), 395, "Expected error code 395 but we got: " + ex.getMessage());
            Assert.assertTrue(ex.getCause() instanceof IOException, "Should end up with IOException");
            success = false;
        } catch (ClickHouseException e) {
            Assert.assertEquals(e.getErrorCode(), 395, "Expected error code 395 but we got: " + e.getMessage());
            Assert.assertTrue(e.getCause() instanceof IOException, "Should end up with IOException");
            success = false;
        }

        Assert.assertFalse(success, "Should fail due insert error");
    }

    @Test(groups = "integration")
    public void testErrorDuringQuery() throws ClickHouseException {
        // Note: server may return no records but only error
        ClickHouseNode server = getServer();
        String query = "select number, throwIf(number>=10000) from numbers(12000)";
        Map<ClickHouseOption, Object> options = new HashMap<>();
        try (ClickHouseClient client = getClient();
                ClickHouseResponse resp = newRequest(client, server)
                        .format(ClickHouseFormat.TabSeparated)
                        .query(query).executeAndWait()) {
            Assert.fail("Query should be terminated before all rows returned");
        } catch (UncheckedIOException e) {
            Assert.assertTrue(e.getCause() instanceof IOException,
                    "Should end up with IOException due to deserialization failure");
        } catch (ClickHouseException e) {
            Assert.assertEquals(e.getErrorCode(), 395, "Expected error code 395 but we got: " + e.getErrorCode());
        }
    }

    @Test(groups = "integration")
    public void testSession() throws ClickHouseException {
        ClickHouseNode server = getServer();
        String sessionId = ClickHouseRequestManager.getInstance().createSessionId();
        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest<?> req = newRequest(client, server).session(sessionId)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes);
            try (ClickHouseResponse resp = req.copy()
                    .query("drop temporary table if exists test_session")
                    .executeAndWait()) {
                // ignore
            }
            try (ClickHouseResponse resp = req.copy().clearSession().set("session_id", sessionId)
                    .query("create temporary table test_session(a String)engine=Memory as select '7'")
                    .executeAndWait()) {
                // ignore
            }
            try (ClickHouseResponse resp = req.copy().clearSession()
                    .option(ClickHouseClientOption.CUSTOM_SETTINGS, "session_id=" + sessionId)
                    .query("select * from test_session").executeAndWait()) {
                Assert.assertEquals(resp.firstRecord().getValue(0).asInteger(), 7);
            }
        }
    }

    @Test(groups = "integration")
    public void testSessionLock() throws ClickHouseException {
        if (isCloud()) return; //TODO: testSessionLock - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        ClickHouseNode server = getServer();
        String sessionId = ClickHouseRequestManager.getInstance().createSessionId();
        try (ClickHouseClient client = getClient()) {
            ClickHouseRequest<?> req1 = newRequest(client, server).session(sessionId)
                    .query("select * from numbers(10000000)");
            ClickHouseRequest<?> req2 = newRequest(client, server)
                    .option(ClickHouseClientOption.REPEAT_ON_SESSION_LOCK, true)
                    .option(ClickHouseClientOption.CONNECTION_TIMEOUT, 500)
                    .session(sessionId).query("select 1");

            ClickHouseResponse resp1 = req1.executeAndWait();
            try (ClickHouseResponse resp = req2.executeAndWait()) {
                Assert.fail("Should fail due to session is locked by previous query");
            } catch (ClickHouseException e) {
                Assert.assertEquals(e.getErrorCode(), ClickHouseException.ERROR_SESSION_IS_LOCKED,
                        "Expected error code 373 but we got: " + e.getMessage());
            }
            new Thread(() -> {
                try {
                    Thread.sleep(1000);
                    resp1.close();
                } catch (InterruptedException e) {
                    // ignore
                }
            }).start();

            try (ClickHouseResponse resp = req2.option(ClickHouseClientOption.CONNECTION_TIMEOUT, 30000)
                    .executeAndWait()) {
                Assert.assertNotNull(resp);
            }
        }
    }

    @Test(groups = "integration")
    public void testAbortTransaction() throws ClickHouseException {
        if (isCloud()) return; //TODO: testAbortTransaction - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        ClickHouseNode server = getServer();
        String tableName = "test_abort_transaction";
        sendAndWait(server, "drop table if exists " + tableName,
                "create table " + tableName + " (id Int64)engine=MergeTree order by id");
        try (ClickHouseClient client = getClient()) {
            if (!checkServerVersion(client, server, "[22.7,)")) {
                throw new SkipException("Transaction was supported since 22.7");
            }

            ClickHouseRequest<?> txRequest = newRequest(client, server).transaction();
            try (ClickHouseResponse response = txRequest.query("insert into " + tableName + " values(1)(2)(3)")
                    .executeAndWait()) {
                // ignore
            }
            checkRowCount(txRequest, tableName, 3);
            checkRowCount(tableName, 3);
            Assert.assertEquals(txRequest.getTransaction().getState(), ClickHouseTransaction.ACTIVE);

            txRequest.getTransaction().abort();
            Assert.assertEquals(txRequest.getTransaction().getState(), ClickHouseTransaction.FAILED);
            checkRowCount(tableName, 0);

            try {
                checkRowCount(txRequest, tableName, 0);
                Assert.fail("Should fail as the transaction is invalid");
            } catch (ClickHouseException e) {
                Assert.assertEquals(e.getErrorCode(), ClickHouseTransactionException.ERROR_INVALID_TRANSACTION,
                        "Expected error code 649 but we got: " + e.getMessage());
            }
        }
    }

    @Test(groups = "integration")
    public void testNewTransaction() throws ClickHouseException {
        if (isCloud()) return; //TODO: testNewTransaction - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        ClickHouseNode server = getServer();
        try (ClickHouseClient client = getClient()) {
            if (!checkServerVersion(client, server, "[22.7,)")) {
                throw new SkipException("Transaction was supported since 22.7");
            }

            ClickHouseRequest<?> request = newRequest(client, server);
            Assert.assertNull(request.getSessionId().orElse(null), "Should have no session");
            Assert.assertNull(request.getTransaction(), "Should have no transaction");

            request.transaction();
            Assert.assertNotNull(request.getSessionId().orElse(null), "Should have session now");
            ClickHouseTransaction tx = request.getTransaction();
            Assert.assertNotNull(tx, "Should have transaction now");
            Assert.assertEquals(tx.getSessionId(), request.getSessionId().orElse(null));
            Assert.assertEquals(tx.getServer(), server);
            Assert.assertEquals(tx.getState(), ClickHouseTransaction.ACTIVE);
            Assert.assertNotEquals(tx.getId(), XID.EMPTY);

            request.transaction(0); // current transaction should be reused
            Assert.assertEquals(request.getTransaction(), tx);
            Assert.assertEquals(ClickHouseRequestManager.getInstance().getOrStartTransaction(request, 0), tx);
            Assert.assertNotEquals(ClickHouseRequestManager.getInstance().createTransaction(server, 0), tx);

            request.transaction(30); // same transaction ID but with different timeout settings
            Assert.assertNotEquals(request.getTransaction(), tx);
            Assert.assertEquals(request.getTransaction().getId().getSnapshotVersion(), tx.getId().getSnapshotVersion());
            Assert.assertEquals(request.getTransaction().getId().getHostId(), tx.getId().getHostId());
            Assert.assertNotEquals(request.getTransaction().getId().getLocalTransactionCounter(),
                    tx.getId().getLocalTransactionCounter());
            Assert.assertNotEquals(request.getTransaction().getSessionId(), tx.getSessionId());

            request.transaction(0);
            Assert.assertNotEquals(request.getTransaction(), tx);

            ClickHouseRequest<?> otherRequest = newRequest(client, server).transaction(tx);
            Assert.assertEquals(otherRequest.getSessionId().orElse(null), tx.getSessionId());
            Assert.assertEquals(otherRequest.getTransaction(), tx);
        }
    }

    @Test(groups = "integration")
    public void testJoinTransaction() throws ClickHouseException {
        if (isCloud()) return; //TODO: testJoinTransaction - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        ClickHouseNode server = getServer();
        try (ClickHouseClient client = getClient()) {
            if (!checkServerVersion(client, server, "[22.7,)")) {
                throw new SkipException("Transaction was supported since 22.7");
            }

            ClickHouseRequest<?> request = newRequest(client, server).transaction();
            ClickHouseTransaction tx = request.getTransaction();

            ClickHouseRequest<?> otherRequest = newRequest(client, server).transaction(tx);
            Assert.assertEquals(otherRequest.getSessionId().orElse(null), request.getSessionId().orElse(null));
            Assert.assertEquals(otherRequest.getTransaction(), request.getTransaction());

            ClickHouseTransaction newTx = ClickHouseRequestManager.getInstance().createTransaction(server, 0);
            Assert.assertNotEquals(newTx, XID.EMPTY);
            Assert.assertNotEquals(tx, newTx);
            Assert.assertEquals(newTx.getState(), ClickHouseTransaction.NEW);

            // now replace the existing transaction to the new one
            request.transaction(newTx);
            Assert.assertEquals(request.getTransaction(), newTx);
            Assert.assertNotEquals(request.getSessionId().orElse(null), otherRequest.getSessionId().orElse(null));
            Assert.assertNotEquals(request.getTransaction(), otherRequest.getTransaction());
        }
    }

    @Test(groups = "integration")
    public void testCommitTransaction() throws ClickHouseException {
        if (isCloud()) return; //TODO: testCommitTransaction - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        ClickHouseNode server = getServer();
        sendAndWait(server, "drop table if exists test_tx_commit",
                "create table test_tx_commit(a Int64, b String)engine=MergeTree order by a");
        try (ClickHouseClient client = getClient()) {
            if (!checkServerVersion(client, server, "[22.7,)")) {
                throw new SkipException("Transaction was supported since 22.7");
            }

            ClickHouseRequest<?> request = newRequest(client, server).transaction();
            ClickHouseTransaction tx = request.getTransaction();

            ClickHouseRequest<?> otherRequest = newRequest(client, server).transaction(tx);
            Assert.assertEquals(otherRequest.getSessionId().orElse(null), request.getSessionId().orElse(null));
            Assert.assertEquals(otherRequest.getTransaction(), request.getTransaction());

            ClickHouseTransaction newTx = ClickHouseRequestManager.getInstance().createTransaction(server, 0);
            Assert.assertNotEquals(newTx, XID.EMPTY);
            Assert.assertNotEquals(tx, newTx);
            Assert.assertEquals(newTx.getState(), ClickHouseTransaction.NEW);

            // now replace the existing transaction to the new one
            request.transaction(newTx);
            Assert.assertEquals(request.getTransaction(), newTx);
            Assert.assertNotEquals(request.getSessionId().orElse(null), otherRequest.getSessionId().orElse(null));
            Assert.assertNotEquals(request.getTransaction(), otherRequest.getTransaction());
        }
    }

    @Test(groups = "integration")
    public void testRollbackTransaction() throws ClickHouseException {
        if (isCloud()) return; //TODO: testRollbackTransaction - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        String tableName = "test_tx_rollback";
        ClickHouseNode server = getServer();
        sendAndWait(server, "drop table if exists " + tableName,
                "create table " + tableName + "(a Int64, b String)engine=MergeTree order by a");

        checkRowCount(tableName, 0);
        try (ClickHouseClient client = getClient()) {
            if (!checkServerVersion(client, server, "[22.7,)")) {
                throw new SkipException("Transaction was supported since 22.7");
            }

            ClickHouseRequest<?> request = newRequest(client, server).transaction();
            ClickHouseTransaction tx = request.getTransaction();
            try (ClickHouseResponse response = newRequest(client, server)
                    .query("insert into " + tableName + " values(0, '?')").executeAndWait()) {
                // ignore
            }
            int rows = 1;
            checkRowCount(tableName, rows);
            checkRowCount(request, tableName, rows);

            try (ClickHouseResponse response = request
                    .query("insert into " + tableName + " values(1,'x')(2,'y')(3,'z')")
                    .executeAndWait()) {
                // ignore
            }
            rows += 3;

            checkRowCount(request, tableName, rows);
            ClickHouseRequest<?> otherRequest = newRequest(client, server).transaction(tx);
            checkRowCount(otherRequest, tableName, rows);
            checkRowCount(tableName, rows);

            try (ClickHouseResponse response = newRequest(client, server)
                    .query("insert into " + tableName + " values(-1, '?')").executeAndWait()) {
                // ignore
            }
            rows++;

            checkRowCount(request, tableName, rows);
            checkRowCount(otherRequest, tableName, rows);
            checkRowCount(tableName, rows);

            try (ClickHouseResponse response = otherRequest.query("insert into " + tableName + " values(4,'.')")
                    .executeAndWait()) {
                // ignore
            }
            rows++;

            checkRowCount(request, tableName, rows);
            checkRowCount(otherRequest, tableName, rows);
            checkRowCount(tableName, rows);

            rows -= 4;
            for (int i = 0; i < 10; i++) {
                tx.rollback();
                checkRowCount(tableName, rows);
                checkRowCount(otherRequest, tableName, rows);
                checkRowCount(request, tableName, rows);
            }
        }
    }

    @Test(groups = "integration")
    public void testTransactionSnapshot() throws ClickHouseException {
        if (isCloud()) return; //TODO: testTransactionSnapshot - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        String tableName = "test_tx_snapshots";
        ClickHouseNode server = getServer();
        sendAndWait(server, "drop table if exists " + tableName,
                "create table " + tableName + "(a Int64)engine=MergeTree order by a");
        try (ClickHouseClient client = getClient()) {
            if (!checkServerVersion(client, server, "[22.7,)")) {
                throw new SkipException("Transaction was supported since 22.7");
            }

            ClickHouseRequest<?> req1 = newRequest(client, server).transaction();
            ClickHouseRequest<?> req2 = newRequest(client, server).transaction();
            try (ClickHouseResponse response = req1.query("insert into " + tableName + " values(1)").executeAndWait()) {
                // ignore
            }
            req2.getTransaction().snapshot(1);
            checkRowCount(tableName, 1);
            checkRowCount(req1, tableName, 1);
            checkRowCount(req2, tableName, 0);
            try (ClickHouseResponse response = req2.query("insert into " + tableName + " values(2)").executeAndWait()) {
                // ignore
            }
            checkRowCount(tableName, 2);
            checkRowCount(req1, tableName, 1);
            checkRowCount(req2, tableName, 1);

            req1.getTransaction().snapshot(1);
            try (ClickHouseResponse response = req1.query("insert into " + tableName + " values(3)").executeAndWait()) {
                // ignore
            }
            checkRowCount(tableName, 3);
            checkRowCount(req1, tableName, 2);
            checkRowCount(req2, tableName, 1);

            try (ClickHouseResponse response = req2.query("insert into " + tableName + " values(4)").executeAndWait()) {
                // ignore
            }
            checkRowCount(tableName, 4);
            checkRowCount(req1, tableName, 2);
            checkRowCount(req2, tableName, 2);

            req2.getTransaction().snapshot(3);
            checkRowCount(tableName, 4);
            checkRowCount(req1, tableName, 2);
            checkRowCount(req2, tableName, 4);

            req1.getTransaction().snapshot(3);
            checkRowCount(tableName, 4);
            checkRowCount(req1, tableName, 4);
            checkRowCount(req2, tableName, 4);

            req1.getTransaction().snapshot(1);
            try (ClickHouseResponse response = req1.query("insert into " + tableName + " values(5)").executeAndWait()) {
                // ignore
            }
            checkRowCount(tableName, 5);
            checkRowCount(req1, tableName, 3);
            checkRowCount(req2, tableName, 5);

            req2.getTransaction().commit();
            checkRowCount(tableName, 5);
            checkRowCount(req1, tableName, 3);
            checkRowCount(req2, tableName, 5);
            try {
                req2.getTransaction().snapshot(5);
            } catch (ClickHouseTransactionException e) {
                Assert.assertEquals(e.getErrorCode(), ClickHouseTransactionException.ERROR_INVALID_TRANSACTION,
                        "Expected error code 649 but we got: " + e.getMessage());
            }

            req1.getTransaction().commit();
            checkRowCount(tableName, 5);
            checkRowCount(req1, tableName, 5);
            checkRowCount(req2, tableName, 5);
            try {
                req1.getTransaction().snapshot(5);
            } catch (ClickHouseTransactionException e) {
                Assert.assertEquals(e.getErrorCode(), ClickHouseTransactionException.ERROR_INVALID_TRANSACTION,
                        "Expected error code 649 but we got: " + e.getMessage());
            }
        }
    }

    @Test(groups = "integration")
    public void testTransactionTimeout() throws ClickHouseException {
        if (isCloud()) return; //TODO: testTransactionTimeout - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        String tableName = "test_tx_timeout";
        ClickHouseNode server = getServer();
        sendAndWait(server, "drop table if exists " + tableName,
                "create table " + tableName + "(a UInt64)engine=MergeTree order by a");
        try (ClickHouseClient client = getClient()) {
            if (!checkServerVersion(client, server, "[22.7,)")) {
                throw new SkipException("Transaction was supported since 22.7");
            }

            ClickHouseRequest<?> request = newRequest(client, server).transaction(1);
            ClickHouseTransaction tx = request.getTransaction();
            Assert.assertEquals(tx.getState(), ClickHouseTransaction.ACTIVE);
            tx.commit();
            Assert.assertEquals(tx.getState(), ClickHouseTransaction.COMMITTED);

            tx.begin();
            Assert.assertEquals(tx.getState(), ClickHouseTransaction.ACTIVE);
            tx.rollback();
            Assert.assertEquals(tx.getState(), ClickHouseTransaction.ROLLED_BACK);

            tx.begin();
            try {
                Thread.sleep(4000L);
            } catch (InterruptedException ex) {
                Assert.fail("Sleep was interrupted", ex);
            }
            try (ClickHouseResponse response = newRequest(client, server).transaction(tx)
                    .query("select 1").executeAndWait()) {
                Assert.fail("Query should fail due to session timed out");
            } catch (ClickHouseException e) {
                // session not found(since it's timed out)
                Assert.assertEquals(e.getErrorCode(), ClickHouseException.ERROR_SESSION_NOT_FOUND,
                        "Expected error code 372 but we got: " + e.getMessage());
            }
            Assert.assertEquals(tx.getState(), ClickHouseTransaction.ACTIVE);

            try {
                tx.commit();
                Assert.fail("Should fail to commit due to session timed out");
            } catch (ClickHouseTransactionException e) {
                Assert.assertEquals(e.getErrorCode(), ClickHouseTransactionException.ERROR_INVALID_TRANSACTION,
                        "Expected error code 649 but we got: " + e.getMessage());
            }
            Assert.assertEquals(tx.getState(), ClickHouseTransaction.FAILED);

            try {
                tx.rollback();
                Assert.fail("Should fail to roll back due to session timed out");
            } catch (ClickHouseTransactionException e) {
                Assert.assertEquals(e.getErrorCode(), ClickHouseTransactionException.ERROR_INVALID_TRANSACTION,
                        "Expected error code 649 but we got: " + e.getMessage());
            }
            Assert.assertEquals(tx.getState(), ClickHouseTransaction.FAILED);

            try {
                tx.begin();
                Assert.fail("Should fail to restart due to session timed out");
            } catch (ClickHouseTransactionException e) {
                Assert.assertEquals(e.getErrorCode(), ClickHouseTransactionException.ERROR_INVALID_TRANSACTION,
                        "Expected error code 649 but we got: " + e.getMessage());
            }
            Assert.assertEquals(tx.getState(), ClickHouseTransaction.FAILED);

            request.transaction(null);
            Assert.assertNull(request.getTransaction(), "Should have no transaction");
            checkRowCount(tableName, 0);
            request.transaction(1);
            try (ClickHouseResponse response = request.write().query("insert into " + tableName + " values(1)(2)(3)")
                    .executeAndWait()) {
                // ignore
            }
            Assert.assertEquals(request.getTransaction().getState(), ClickHouseTransaction.ACTIVE);
            checkRowCount(tableName, 3);
            checkRowCount(request, tableName, 3);
            try {
                Thread.sleep(3000L);
            } catch (InterruptedException ex) {
                Assert.fail("Sleep was interrupted", ex);
            }
            checkRowCount(tableName, 0);
            try {
                checkRowCount(request, tableName, 3);
                Assert.fail("Should fail to query due to session timed out");
            } catch (ClickHouseException e) {
                Assert.assertEquals(e.getErrorCode(), ClickHouseException.ERROR_SESSION_NOT_FOUND,
                        "Expected error code 372 but we got: " + e.getMessage());
            }
            Assert.assertEquals(request.getTransaction().getState(), ClickHouseTransaction.ACTIVE);
        }
    }

    @Test(groups = "integration")
    public void testImplicitTransaction() throws ClickHouseException {
        if (isCloud()) return; //TODO: testImplicitTransaction - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        ClickHouseNode server = getServer();
        String tableName = "test_implicit_transaction";
        sendAndWait(server, "drop table if exists " + tableName,
                "create table " + tableName + " (id Int64)engine=MergeTree order by id");
        try (ClickHouseClient client = getClient()) {
            if (!checkServerVersion(client, server, "[22.7,)")) {
                throw new SkipException("Transaction was supported since 22.7");
            }
            ClickHouseRequest<?> request = newRequest(client, server);
            ClickHouseTransaction.setImplicitTransaction(request, true);
            try (ClickHouseResponse response = request.query("insert into " + tableName + " values(1)")
                    .executeAndWait()) {
                // ignore
            }
            checkRowCount(tableName, 1);
            ClickHouseTransaction.setImplicitTransaction(request, false);
            try (ClickHouseResponse response = request.query("insert into " + tableName + " values(2)")
                    .executeAndWait()) {
                // ignore
            }
            checkRowCount(tableName, 2);

            ClickHouseTransaction.setImplicitTransaction(request, true);
            try (ClickHouseResponse response = request.transaction().query("insert into " + tableName + " values(3)")
                    .executeAndWait()) {
                // ignore
            }
            checkRowCount(tableName, 3);
            request.getTransaction().rollback();
            checkRowCount(tableName, 2);
        }
    }
    @Test(groups = "integration")
    public void testRowBinaryWithDefaults() throws ClickHouseException, IOException, ExecutionException, InterruptedException {
        ClickHouseNode server = getServer();

        String tableName = "test_row_binary_with_defaults";

        String tableColumns = String.format("id Int64, updated_at DateTime DEFAULT now(), updated_at_date Date DEFAULT toDate(updated_at)");
        sendAndWait(server, "drop table if exists " + tableName,
                "create table " + tableName + " (" + tableColumns + ")engine=Memory");

        long numRows = 1000;

        try {
            try (ClickHouseClient client = getClient()) {
                ClickHouseRequest.Mutation request = client.read(server)
                        .write()
                        .table(tableName)
                        .set("async_insert", isCloud() ? 0 : null)
                        .format(ClickHouseFormat.RowBinaryWithDefaults);
                ClickHouseConfig config = request.getConfig();
                CompletableFuture<ClickHouseResponse> future;

                try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                        .createPipedOutputStream(config)) {
                    // start the worker thread which transfer data from the input into ClickHouse
                    future = request.data(stream.getInputStream()).execute();
                    // write bytes into the piped stream
                    LongStream.range(0, numRows).forEachOrdered(
                            n -> {
                                try {
                                    BinaryStreamUtils.writeNonNull(stream);
                                    BinaryStreamUtils.writeInt64(stream, n);
                                    BinaryStreamUtils.writeNull(stream); // When using the default
                                    BinaryStreamUtils.writeNull(stream); // When using the default
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                    );

                    // We need to close the stream before getting a response
                    stream.close();
                    try (ClickHouseResponse response = future.get()) {
                        ClickHouseResponseSummary summary = response.getSummary();
                        Assert.assertEquals(summary.getWrittenRows(), numRows, "Num of written rows");
                    }
                }

            }
        } catch (Exception e) {
            Throwable th = e.getCause();
            if (th instanceof ClickHouseException) {
                ClickHouseException ce = (ClickHouseException) th;
                Assert.assertEquals(73, ce.getErrorCode(), "It's Code: 73. DB::Exception: Unknown format RowBinaryWithDefaults. a server that not support the format");
            } else {
                Assert.assertTrue(false, e.getMessage());
            }
        }
    }

    @Test(dataProvider = "loadBalancingPolicies", groups = {"unit"})
    public void testLoadBalancingPolicyFailover(ClickHouseLoadBalancingPolicy loadBalancingPolicy) {
        String firstEndpoint = "111.1.1.1";
        String secondEndpoint = "222.2.2.2";

        Properties props = new Properties();
        props.setProperty("failover", "1");

        // nodes where the first node is failed
        ClickHouseNodes nodes = ClickHouseNodes.of(
                getProtocol() + "://" + firstEndpoint + "," + secondEndpoint,
                props
        );

        try (ClickHouseClient client = getClient();
             ClickHouseResponse response = newRequest(client, nodes.nodes.getFirst())
                     .query("select 1")
                     .executeAndWait()) {
            Assert.fail("Exception expected for query on failed node");
        } catch (Exception failoverException) {
            ClickHouseNode failoverNode = loadBalancingPolicy.suggest(nodes, nodes.nodes.getFirst(), failoverException);
            Assert.assertEquals(failoverNode.getHost(), secondEndpoint, "The next node is expected to be suggested by the load balancing policy");
        }
    }

    @Test(groups = {"integration"})
    public void testFailover() throws ClickHouseException {
        if (isCloud()) return; //TODO: testFailover - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        ClickHouseNode availableNode = getServer();
        Properties props = new Properties();
        props.setProperty("failover", "1");
        props.setProperty(ClickHouseDefaults.PASSWORD.getKey(), getPassword());

        // nodes with the first node is unavailable
        ClickHouseNodes nodes = ClickHouseNodes.of(
                getProtocol() + "://111.1.1.1," + availableNode.getBaseUri(),
                props
        );

        // should fail over to next node and successfully perform request if the first node is failed
        try (ClickHouseClient client = getClient();
             ClickHouseResponse response = newRequest(client, nodes)
                     .query("select 1")
                     .executeAndWait()) {
            Assert.assertEquals(response.firstRecord().getValue(0).asInteger(), 1);
        }
    }

    @Test(groups = {"integration"}, dataProvider = "testServerTimezoneAppliedFromHeaderProvider")
    public void testServerTimezoneAppliedFromHeader(ClickHouseFormat format) throws Exception {
        System.out.println("Testing with " + format + " format");
        ClickHouseNode server = getServer();

        ZoneId custZoneId = ZoneId.of("America/Los_Angeles");

        final String sql = "SELECT now(), toDateTime(now(), 'UTC') as utc_time, serverTimezone() SETTINGS session_timezone = 'America/Los_Angeles'";
        try (ClickHouseClient client = getClient();
             ClickHouseResponse response = newRequest(client, server)
                     .query(sql, UUID.randomUUID().toString())
                     .option(ClickHouseClientOption.FORMAT, format)
                     .executeAndWait()) {

            Assert.assertEquals(response.getTimeZone().getID(), "America/Los_Angeles", "Timezone should be applied from the query settings");

            ClickHouseRecord record = response.firstRecord();
            final ZonedDateTime now = record.getValue(0).asZonedDateTime();
            final ZonedDateTime utcTime = record.getValue(1).asZonedDateTime();
            final String serverTimezone = record.getValue(2).asString();
            Assert.assertNotEquals(serverTimezone, "America/Los_Angeles", "Server timezone should be applied from the query settings");


            System.out.println("Now in America/Los_Angeles: " + now);
            System.out.println("UTC Time: " + utcTime);
            System.out.println("UTC Time: " + utcTime.withZoneSameInstant(custZoneId));
            Assert.assertEquals(now, utcTime.withZoneSameInstant(custZoneId));
        }
    }

    @DataProvider(name = "testServerTimezoneAppliedFromHeaderProvider")
    public static ClickHouseFormat[] testServerTimezoneAppliedFromHeaderProvider() {
        return new ClickHouseFormat[]{
                ClickHouseFormat.TabSeparatedWithNamesAndTypes,
                ClickHouseFormat.RowBinaryWithNamesAndTypes
        };
    }
}
