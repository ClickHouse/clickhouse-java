package com.clickhouse.client.cli;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseClientBuilder;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseLoadBalancingPolicy;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.ClientIntegrationTest;
import com.clickhouse.client.cli.config.ClickHouseCommandLineOption;
import com.clickhouse.data.ClickHouseCompression;
import org.testcontainers.containers.GenericContainer;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
// deprecate from version 0.6.0
@Deprecated
public class ClickHouseCommandLineClientTest extends ClientIntegrationTest {
    @BeforeClass
    static void init() {
        System.setProperty(ClickHouseCommandLineOption.CLI_CONTAINER_DIRECTORY.getSystemProperty(),
                ClickHouseServerForTest.getClickHouseContainerTmpDir());
    }

    @DataProvider(name = "requestCompressionMatrix")
    protected Object[][] getRequestCompressionMatrix() {
        return new Object[][] {
                { ClickHouseCompression.NONE, -2, 2, 1 },
                { ClickHouseCompression.LZ4, -2, 19, 1 }, // [0, 18]
        };
    }

    @DataProvider(name = "mixedCompressionMatrix")
    protected Object[][] getMixedCompressionMatrix() {
        ClickHouseCompression[] supportedRequestCompression = { ClickHouseCompression.NONE, ClickHouseCompression.LZ4 };
        ClickHouseCompression[] supportedResponseCompression = { ClickHouseCompression.NONE,
                // ClickHouseCompression.LZ4
        };
        Object[][] matrix = new Object[supportedRequestCompression.length * supportedResponseCompression.length][];
        int i = 0;
        for (ClickHouseCompression reqComp : supportedRequestCompression) {
            for (ClickHouseCompression respComp : supportedResponseCompression) {
                matrix[i++] = new Object[] { reqComp, respComp };
            }
        }
        return matrix;
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
        if (container != null && !ClickHouseCommandLine.DEFAULT_CLI_IS_AVAILALBE) {
            return ClickHouseNode.of("localhost", getProtocol(), getProtocol().getDefaultPort(), null);
        }

        return super.getServer();
    }

    @Test(groups = { "integration" })
    public void testNothing() throws Exception {
    }

    @Test(groups = { "integration" })
    @Override
    public void testCustomLoad() throws ClickHouseException {
        throw new SkipException("Skip due to time out error");
    }

    @Test(groups = { "integration" })
    @Override
    public void testLoadRawData() throws ClickHouseException, IOException {
        throw new SkipException("Skip due to response summary is always empty");
    }

    @Test(groups = { "integration" })
    @Override
    public void testMultipleQueries() throws ClickHouseException {
        // FIXME not sure if the occasional "Stream closed" exception is related to
        // zeroturnaround/zt-exec#30 or not
        /*
         * Caused by: java.io.IOException: Stream closed
         * at java.io.BufferedInputStream.getBufIfOpen(BufferedInputStream.java:170)
         * at java.io.BufferedInputStream.read(BufferedInputStream.java:336)
         * at com.clickhouse.client.stream.WrappedInputStream.updateBuffer(
         * WrappedInputStream.java:32)
         * at com.clickhouse.client.stream.AbstractByteArrayInputStream.available(
         * AbstractByteArrayInputStream.java:56)
         * at
         * com.clickhouse.client.ClickHouseDataProcessor.hasNext(ClickHouseDataProcessor
         * .java:126)
         */
        throw new SkipException("Skip due to unknown cause");
    }

    @Test(groups = { "integration" })
    @Override
    public void testReadWriteGeoTypes() {
        throw new SkipException("Skip due to session is not supported");
    }

    @Test(groups = { "integration" })
    @Override
    public void testSession() {
        throw new SkipException("Skip due to session is not supported");
    }

    @Test(groups = { "integration" })
    @Override
    public void testSessionLock() {
        throw new SkipException("Skip due to session is not supported");
    }

    @Test(groups = { "integration" })
    @Override
    public void testTempTable() {
        throw new SkipException("Skip due to session is not supported");
    }

    @Test(groups = { "integration" })
    @Override
    public void testAbortTransaction() throws ClickHouseException {
        throw new SkipException("Skip due to session is not supported");
    }

    @Test(groups = { "integration" })
    @Override
    public void testCommitTransaction() throws ClickHouseException {
        throw new SkipException("Skip due to session is not supported");
    }

    @Test(groups = { "integration" })
    @Override
    public void testImplicitTransaction() throws ClickHouseException {
        throw new SkipException("Skip due to session is not supported");
    }

    @Test(groups = { "integration" })
    @Override
    public void testJoinTransaction() throws ClickHouseException {
        throw new SkipException("Skip due to session is not supported");
    }

    @Test(groups = { "integration" })
    @Override
    public void testNewTransaction() throws ClickHouseException {
        throw new SkipException("Skip due to session is not supported");
    }

    @Test(groups = { "integration" })
    @Override
    public void testRollbackTransaction() throws ClickHouseException {
        throw new SkipException("Skip due to session is not supported");
    }

    @Test(groups = { "integration" })
    @Override
    public void testTransactionSnapshot() throws ClickHouseException {
        throw new SkipException("Skip due to session is not supported");
    }

    @Test(groups = { "integration" })
    @Override
    public void testTransactionTimeout() throws ClickHouseException {
        throw new SkipException("Skip due to session is not supported");
    }
    @Test(groups = { "integration" })
    @Override
    public void testRowBinaryWithDefaults() throws ClickHouseException, IOException, ExecutionException, InterruptedException {
        throw new SkipException("Skip due to supported");
    }

    @Test(dataProvider = "loadBalancingPolicies", groups = {"unit"})
    public void testLoadBalancingPolicyFailover(ClickHouseLoadBalancingPolicy loadBalancingPolicy) {
        throw new SkipException("Skip due to failover is not supported");
    }

    @Test(groups = {"integration"})
    public void testFailover() {
        throw new SkipException("Skip due to failover is not supported");
    }
}
