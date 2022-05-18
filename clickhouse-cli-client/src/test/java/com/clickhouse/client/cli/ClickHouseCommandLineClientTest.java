package com.clickhouse.client.cli;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClientIntegrationTest;

import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;

public class ClickHouseCommandLineClientTest extends ClientIntegrationTest {
    @Override
    protected ClickHouseProtocol getProtocol() {
        return ClickHouseProtocol.TCP;
    }

    @Override
    protected Class<? extends ClickHouseClient> getClientClass() {
        return ClickHouseCommandLineClient.class;
    }

    @Test(groups = { "integration" })
    @Override
    public void testLoadRawData() {
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
