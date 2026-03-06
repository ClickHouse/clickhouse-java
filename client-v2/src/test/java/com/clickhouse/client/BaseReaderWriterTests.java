package com.clickhouse.client;


import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Test(groups = {"integration"})

public class BaseReaderWriterTests extends BaseIntegrationTest {

    private Client sharedClient;

    private final DatasetBuilder datasetBuilder;


    protected BaseReaderWriterTests() {
        this.datasetBuilder = new DatasetBuilder();
    }


    @BeforeTest()
    public void setupClass() {
        sharedClient = createSharedClient();
    }

    @Test(groups = {"integration"})
    void doTest() throws Exception {

        DatasetBuilder.Dataset ds = new DatasetBuilder.SchemaBasedBuilder(datasetBuilder.getDefaultGenerators())
                .int8()
                .int16()
                .int32()
                .build();

        sharedClient.execute(ds.getCreateTable()).get().close();

        DatasetBuilder.DatasetSnapshot snapshot = writeDatasetAsTSV(ds, 100);

        readDatasetFromTSV(ds, snapshot);

    }

    
    protected Map<ClickHouseDataType, BiFunction<ClickHouseBinaryFormatReader, String, String>> getTypeReaders() {
        Map<ClickHouseDataType, BiFunction<ClickHouseBinaryFormatReader, String, String>> readers =
                new EnumMap<>(ClickHouseDataType.class);

        readers.put(ClickHouseDataType.Int8, (r, c) -> String.valueOf(r.getByte(c)));
        readers.put(ClickHouseDataType.Int16, (r, c) -> String.valueOf(r.getShort(c)));
        readers.put(ClickHouseDataType.Int32, (r, c) -> String.valueOf(r.getInteger(c)));
        readers.put(ClickHouseDataType.Int64, (r, c) -> String.valueOf(r.getLong(c)));
        readers.put(ClickHouseDataType.Int128, (r, c) -> r.getBigInteger(c).toString());
        readers.put(ClickHouseDataType.Int256, (r, c) -> r.getBigInteger(c).toString());

        readers.put(ClickHouseDataType.UInt8, (r, c) -> String.valueOf(r.getShort(c)));
        readers.put(ClickHouseDataType.UInt16, (r, c) -> String.valueOf(r.getInteger(c)));
        readers.put(ClickHouseDataType.UInt32, (r, c) -> String.valueOf(r.getLong(c)));
        readers.put(ClickHouseDataType.UInt64, (r, c) -> r.getBigInteger(c).toString());
        readers.put(ClickHouseDataType.UInt128, (r, c) -> r.getBigInteger(c).toString());
        readers.put(ClickHouseDataType.UInt256, (r, c) -> r.getBigInteger(c).toString());

        readers.put(ClickHouseDataType.Float32, (r, c) -> String.valueOf(r.getFloat(c)));
        readers.put(ClickHouseDataType.Float64, (r, c) -> String.valueOf(r.getDouble(c)));

        readers.put(ClickHouseDataType.Bool, (r, c) -> String.valueOf(r.getBoolean(c)));

        readers.put(ClickHouseDataType.Date, (r, c) -> r.getLocalDate(c).toString());
        readers.put(ClickHouseDataType.Date32, (r, c) -> r.getLocalDate(c).toString());
        readers.put(ClickHouseDataType.DateTime, (r, c) -> r.getLocalDateTime(c).toString());
        readers.put(ClickHouseDataType.DateTime32, (r, c) -> r.getLocalDateTime(c).toString());
        readers.put(ClickHouseDataType.DateTime64, (r, c) -> r.getLocalDateTime(c).toString());

        readers.put(ClickHouseDataType.Decimal, (r, c) -> r.getBigDecimal(c).toPlainString());
        readers.put(ClickHouseDataType.Decimal32, (r, c) -> r.getBigDecimal(c).toPlainString());
        readers.put(ClickHouseDataType.Decimal64, (r, c) -> r.getBigDecimal(c).toPlainString());
        readers.put(ClickHouseDataType.Decimal128, (r, c) -> r.getBigDecimal(c).toPlainString());
        readers.put(ClickHouseDataType.Decimal256, (r, c) -> r.getBigDecimal(c).toPlainString());

        readers.put(ClickHouseDataType.IPv4, (r, c) -> r.getInet4Address(c).getHostAddress());
        readers.put(ClickHouseDataType.IPv6, (r, c) -> r.getInet6Address(c).getHostAddress());

        readers.put(ClickHouseDataType.UUID, (r, c) -> r.getUUID(c).toString());

        readers.put(ClickHouseDataType.Enum8, (r, c) -> String.valueOf(r.getEnum8(c)));
        readers.put(ClickHouseDataType.Enum16, (r, c) -> String.valueOf(r.getEnum16(c)));

        return readers;
    }

    protected void readDatasetFromTSV(DatasetBuilder.Dataset dataset, DatasetBuilder.DatasetSnapshot snapshot) throws Exception {
        Map<ClickHouseDataType, BiFunction<ClickHouseBinaryFormatReader, String, String>> typeReaders = getTypeReaders();
        String query = "SELECT * FROM " + dataset.getTableName() + " ORDER BY id";
        try (QueryResponse response = sharedClient.query(query).get()) {
            ClickHouseBinaryFormatReader reader = sharedClient.newBinaryFormatReader(response);
            List<List<String>> expectedValues = snapshot.getValues();
            List<ClickHouseColumn> columns = dataset.getChColumns();
            int rowIndex = 0;

            while (reader.hasNext()) {
                reader.next();
                Assert.assertTrue(rowIndex < expectedValues.size(),
                        "Server returned more rows than expected (" + expectedValues.size() + ")");

                List<String> expectedRow = expectedValues.get(rowIndex);
                for (int col = 0; col < columns.size(); col++) {
                    ClickHouseColumn column = columns.get(col);
                    String colName = column.getColumnName();
                    ClickHouseDataType dataType = column.getDataType();

                    BiFunction<ClickHouseBinaryFormatReader, String, String> columnReader = typeReaders.get(dataType);
                    Assert.assertNotNull(columnReader,
                            "No reader registered for type " + dataType + " (column '" + colName + "')");

                    String actual = columnReader.apply(reader, colName);
                    String expected = expectedRow.get(col);
                    Assert.assertEquals(actual, expected,
                            "Mismatch at row " + rowIndex + ", column '" + colName + "' (" + dataType + ")");
                }
                rowIndex++;
            }

            Assert.assertEquals(rowIndex, expectedValues.size(),
                    "Server returned fewer rows than expected");
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
