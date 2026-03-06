package com.clickhouse.client;


import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public abstract class BaseReaderWriterTests<R> extends BaseIntegrationTest {

    private Client sharedClient;

    private final DatasetBuilder datasetBuilder;


    protected BaseReaderWriterTests() {
        this.datasetBuilder = new DatasetBuilder();
    }


    @BeforeClass()
    public void setupClass() {
        sharedClient = createSharedClient();
    }
    //IPv6 value fe80::1%eth0 contains a zone ID (%eth0), which ClickHouse doesn't support — the % and everything after it breaks TSV parsing because ClickHouse sees the %eth0\t... as garbage.

    @Test(groups = {"integration"})
    void doReaderTest() throws Exception {

        DatasetBuilder.Dataset ds = new DatasetBuilder.SchemaBasedBuilder(datasetBuilder.getDefaultGenerators())
                .int8()
                .int16()
                .int32()
                .int64()
                .int128()
                .int256()
                .uint8()
                .uint16()
                .uint32()
                .uint64()
                .bool()
//                .date()
//                .date32()
//                .dateTime()
                .uint128()
                .uint256()
                .float32()
                .float64()
//                .string()
//                .dateTime64(3)
//                .decimal(18, 4)
                .ipv4()
//                .ipv6()
                .uuid()
                .build();

        sharedClient.execute(ds.getCreateTable()).get().close();

        DatasetBuilder.DatasetSnapshot snapshot = writeDatasetAsTSV(ds, 100);

        readDatasetFromTSV(ds, snapshot);
    }


    protected abstract Map<ClickHouseDataType, BiFunction<R, String, String>> getTypeReaders();

    protected void readDatasetFromTSV(DatasetBuilder.Dataset dataset, DatasetBuilder.DatasetSnapshot snapshot) throws Exception {
        String query = "SELECT * FROM " + dataset.getTableName() + " ORDER BY id";
        readAndVerifyRecords(query, snapshot, dataset.getChColumns());
    }

    protected abstract void readAndVerifyRecords(String query, DatasetBuilder.DatasetSnapshot snapshot,
                                                 List<ClickHouseColumn> columns) throws Exception;

    protected void verifyRecord(R record, List<String> expectedRow,
                                List<ClickHouseColumn> columns, int rowIndex,
                                Map<ClickHouseDataType, BiFunction<R, String, String>> typeReaders) {
        for (int col = 0; col < columns.size(); col++) {
            ClickHouseColumn column = columns.get(col);
            String colName = column.getColumnName();
            ClickHouseDataType dataType = column.getDataType();

            BiFunction<R, String, String> columnReader = typeReaders.get(dataType);
            Assert.assertNotNull(columnReader,
                    "No reader registered for type " + dataType + " (column '" + colName + "')");

            String actual = columnReader.apply(record, colName);
            String expected = expectedRow.get(col);
            String ctx = "row " + rowIndex + ", column '" + colName + "' (" + dataType + ")";

            if (dataType == ClickHouseDataType.Float32) {
                assertFloat32Equals(actual, expected, ctx);
            } else if (dataType == ClickHouseDataType.Float64) {
                assertFloat64Equals(actual, expected, ctx);
            } else if (dataType == ClickHouseDataType.IPv6) {
                Assert.assertEquals(actual, normalizeIPv6(expected), "Mismatch at " + ctx);
            } else {
                Assert.assertEquals(actual, expected, "Mismatch at " + ctx);
            }
        }
    }

    private static String normalizeSpecialFloatValues(String value) {
        switch (value) {
            case "inf":  return "Infinity";
            case "-inf": return "-Infinity";
            case "nan":  return "NaN";
            default:     return value;
        }
    }

    /**
     * Compares Float32 values numerically to handle precision differences
     * between Java's and ClickHouse's float parsing. Allows up to 1 ULP of
     * difference for finite values.
     */
    private static void assertFloat32Equals(String actual, String expected, String ctx) {
        float a = Float.parseFloat(actual);
        float e = Float.parseFloat(normalizeSpecialFloatValues(expected));
        if (Float.isNaN(e)) {
            Assert.assertTrue(Float.isNaN(a), "Expected NaN at " + ctx + " but got " + actual);
        } else if (Float.isInfinite(e)) {
            Assert.assertEquals(a, e, "Mismatch at " + ctx);
        } else {
            Assert.assertEquals(a, e, Math.ulp(e),
                    "Float32 mismatch at " + ctx + ": expected " + expected + " but got " + actual);
        }
    }

    /**
     * Compares Float64 values numerically, same rationale as Float32.
     */
    private static void assertFloat64Equals(String actual, String expected, String ctx) {
        double a = Double.parseDouble(actual);
        double e = Double.parseDouble(normalizeSpecialFloatValues(expected));
        if (Double.isNaN(e)) {
            Assert.assertTrue(Double.isNaN(a), "Expected NaN at " + ctx + " but got " + actual);
        } else if (Double.isInfinite(e)) {
            Assert.assertEquals(a, e, "Mismatch at " + ctx);
        } else {
            Assert.assertEquals(a, e, Math.ulp(e),
                    "Float64 mismatch at " + ctx + ": expected " + expected + " but got " + actual);
        }
    }

    /**
     * Parses the IPv6 string through Java's InetAddress to produce the same
     * expanded representation that Inet6Address.getHostAddress() returns
     * (e.g. "::" becomes "0:0:0:0:0:0:0:0").
     */
    private static String normalizeIPv6(String value) {
        try {
            return InetAddress.getByName(value).getHostAddress();
        } catch (UnknownHostException e) {
            return value;
        }
    }

    /**
     * Generates TSV data from the dataset and inserts it into the corresponding table.
     *
     * @param dataset the dataset describing the table schema and value generators
     * @param numRows number of rows to generate and insert
     * @return snapshot containing the generated values
     */
    protected DatasetBuilder.DatasetSnapshot writeDatasetAsTSV(DatasetBuilder.Dataset dataset, int numRows) throws Exception {
        DatasetBuilder.DatasetSnapshot snapshot = dataset.toTsvInputStream(numRows);
        sharedClient.insert(dataset.getTableName(),
                snapshot.getInputStream(), ClickHouseFormat.TabSeparated).get();
        return snapshot;
    }

    protected Client getSharedClient() {
        return sharedClient;
    }

    protected Client createSharedClient() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .compressClientRequest(false)
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .serverSetting(ServerSettings.WAIT_ASYNC_INSERT, "1")
                .serverSetting(ServerSettings.ASYNC_INSERT, "0").build();
    }
}
