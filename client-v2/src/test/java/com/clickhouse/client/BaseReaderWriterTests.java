package com.clickhouse.client;


import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;

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

    @Test(groups = {"integration"})
    void doReaderTest() throws Exception {

        DatasetBuilder.Dataset ds = new DatasetBuilder.SchemaBasedBuilder(datasetBuilder.getDefaultGenerators())
                .int8()
                .int16()
                .int32()
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
            Assert.assertEquals(actual, expected,
                    "Mismatch at row " + rowIndex + ", column '" + colName + "' (" + dataType + ")");
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
