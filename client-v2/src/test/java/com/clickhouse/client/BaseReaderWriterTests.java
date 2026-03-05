package com.clickhouse.client;


import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.data.ClickHouseFormat;
import org.testng.annotations.BeforeSuite;
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

        writeDatasetAsTSV(ds, 100).close();
        System.out.println("break point");
    }

    /**
     * Generates TSV data from the dataset and inserts it into the corresponding table.
     *
     * @param dataset the dataset describing the table schema and value generators
     * @param numRows number of rows to generate and insert
     */
    protected InsertResponse writeDatasetAsTSV(DatasetBuilder.Dataset dataset, int numRows) throws Exception {
        return sharedClient.insert(dataset.getTableName(),
                dataset.toTsvInputStream(numRows), ClickHouseFormat.TabSeparated).get();
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
