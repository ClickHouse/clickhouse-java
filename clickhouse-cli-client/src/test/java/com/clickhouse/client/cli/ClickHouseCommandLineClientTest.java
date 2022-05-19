package com.clickhouse.client.cli;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseClientBuilder;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.ClientIntegrationTest;
import com.clickhouse.client.cli.config.ClickHouseCommandLineOption;

import org.testcontainers.containers.GenericContainer;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ClickHouseCommandLineClientTest extends ClientIntegrationTest {
    @BeforeClass
    static void init() {
        System.setProperty(ClickHouseCommandLineOption.CLI_CONTAINER_DIRECTORY.getSystemProperty(),
                ClickHouseServerForTest.getClickHouseContainerTmpDir());
    }

    @Override
    protected ClickHouseProtocol getProtocol() {
        return ClickHouseProtocol.TCP;
    }

    @Override
    protected Class<? extends ClickHouseClient> getClientClass() {
        return ClickHouseCommandLineClient.class;
    }

    @Override
    protected ClickHouseClientBuilder initClient(ClickHouseClientBuilder builder) {
        return super.initClient(builder).option(ClickHouseCommandLineOption.CLI_CONTAINER_DIRECTORY,
                ClickHouseServerForTest.getClickHouseContainerTmpDir());
    }

    @Override
    protected ClickHouseNode getServer() {
        GenericContainer<?> container = ClickHouseServerForTest.getClickHouseContainer();
        if (container != null) {
            return ClickHouseNode.of("localhost", getProtocol(), getProtocol().getDefaultPort(), null);
        }

        return super.getServer();
    }

    @Test(groups = { "integration" })
    @Override
    public void testLoadRawData() throws Exception {
        throw new SkipException("Skip due to response summary is always empty");
    }

    @Test(groups = { "integration" })
    @Override
    public void testReadWriteGeoTypes() {
        throw new SkipException("Skip due to session is not supported");
    }

    @Test(groups = { "integration" })
    @Override
    public void testTempTable() {
        throw new SkipException("Skip due to session is not supported");
    }
}
