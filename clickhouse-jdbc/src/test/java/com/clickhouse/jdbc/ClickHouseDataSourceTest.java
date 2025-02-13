package com.clickhouse.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.clickhouse.client.ClickHouseServerForTest;
import junit.runner.Version;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import com.clickhouse.client.ClickHouseLoadBalancingPolicy;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;

public class ClickHouseDataSourceTest extends JdbcIntegrationTest {
    @BeforeMethod(groups = "integration")
    public void setV1() {
        System.setProperty("clickhouse.jdbc.v1","true");
    }
    @Test(groups = "integration")
    public void testHighAvailabilityConfig() throws SQLException {
        if (isCloud() || ClickHouseDriver.isV2()) return; //TODO: testHighAvailabilityConfig - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        String httpEndpoint = getEndpointString();
        String grpcEndpoint = "grpc://" + getServerAddress(ClickHouseProtocol.GRPC) + "/";
        String tcpEndpoint = "tcp://" + getServerAddress(ClickHouseProtocol.TCP) + "/";

        String url = "jdbc:ch://(" + httpEndpoint + "),(" + grpcEndpoint + "),(" + tcpEndpoint + ")/system";
        Properties props = new Properties();
        props.setProperty("failover", "21");
        props.setProperty("load_balancing_policy", "roundRobin");
        props.setProperty("password",ClickHouseServerForTest.getPassword());
        try (Connection conn = DriverManager.getConnection(url, props)) {
            Assert.assertEquals(conn.unwrap(ClickHouseRequest.class).getConfig().getFailover(), 21);
            Assert.assertEquals(conn.unwrap(ClickHouseRequest.class).getConfig().getOption(
                    ClickHouseClientOption.LOAD_BALANCING_POLICY), ClickHouseLoadBalancingPolicy.ROUND_ROBIN);
        }
    }

    @Test // (groups = "integration")
    @Ignore
    public void testMultiEndpoints() throws SQLException {
        if (isCloud() || ClickHouseDriver.isV2()) return; //TODO: testMultiEndpoints - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        String httpEndpoint = getEndpointString();
        String grpcEndpoint = "grpc://" + getServerAddress(ClickHouseProtocol.GRPC) + "/";
        String tcpEndpoint = "tcp://" + getServerAddress(ClickHouseProtocol.TCP) + "/";

        String url = "jdbc:ch://(" + httpEndpoint + "),(" + grpcEndpoint + "),(" + tcpEndpoint
                + ")/system?load_balancing_policy=roundRobin";
        Properties props = new Properties();
        props.setProperty("user", "default");
        props.setProperty("password", ClickHouseServerForTest.getPassword());
        ClickHouseDataSource ds = new ClickHouseDataSource(url, props);
        for (int i = 0; i < 10; i++) {
            try (Connection httpConn = ds.getConnection();
                    Connection grpcConn = ds.getConnection("default", "");
                    Connection tcpConn = DriverManager.getConnection(url, props)) {
                Assert.assertEquals(httpConn.unwrap(ClickHouseRequest.class).getServer().getBaseUri(), httpEndpoint);
                Assert.assertEquals(grpcConn.unwrap(ClickHouseRequest.class).getServer().getBaseUri(), grpcEndpoint);
                Assert.assertEquals(tcpConn.unwrap(ClickHouseRequest.class).getServer().getBaseUri(), tcpEndpoint);
            }
        }
    }

    @Test(groups = "integration")
    public void testGetConnection() throws SQLException {
        String url = "jdbc:ch:" + getEndpointString();
//        String urlWithCredentials = "jdbc:ch:" + (isCloud() ? "https" : DEFAULT_PROTOCOL.name()) + "://default@"
//                + getServerAddress(DEFAULT_PROTOCOL);
        String urlWithCredentials = "jdbc:ch:" + DEFAULT_PROTOCOL.name() + "://default:" + getPassword() + "@" + getServerAddress(DEFAULT_PROTOCOL);
        if (isCloud()) {
            urlWithCredentials = "jdbc:ch:https://default:" + getPassword() + "@" + getServerAddress(DEFAULT_PROTOCOL);
        }
        String clientName = "client1";
        int maxExecuteTime = 1234;
        boolean continueBatchOnError = true;

        Properties properties = new Properties();
        properties.setProperty(ClickHouseDefaults.USER.getKey(), "default");
        properties.setProperty(ClickHouseDefaults.PASSWORD.getKey(), getPassword());
        properties.setProperty(ClickHouseClientOption.CLIENT_NAME.getKey(), clientName);
        properties.setProperty(ClickHouseClientOption.MAX_EXECUTION_TIME.getKey(), Integer.toString(maxExecuteTime));
        properties.setProperty(JdbcConfig.PROP_CONTINUE_BATCH, Boolean.toString(continueBatchOnError));
        String params = String.format("?%s=%s&%s=%d&%s", ClickHouseClientOption.CLIENT_NAME.getKey(), clientName,
                ClickHouseClientOption.MAX_EXECUTION_TIME.getKey(), maxExecuteTime, JdbcConfig.PROP_CONTINUE_BATCH);

        for (DataSourceV1 ds : new DataSourceV1[] {
                new DataSourceV1(url, properties),
                new DataSourceV1(urlWithCredentials, properties),
                new DataSourceV1(url + params),
                new DataSourceV1(urlWithCredentials + params),
        }) {
            for (ClickHouseConnection connection : new ClickHouseConnection[] {
                    ds.getConnection("default", getPassword()),
                    new DriverV1().connect(url, properties),
                    new DriverV1().connect(urlWithCredentials, properties),
                    new DriverV1().connect(urlWithCredentials + params, new Properties()),
                    (ClickHouseConnection) DriverManager.getConnection(url, properties),
                    (ClickHouseConnection) DriverManager.getConnection(urlWithCredentials, properties),
                    (ClickHouseConnection) DriverManager.getConnection(urlWithCredentials + params),
                    (ClickHouseConnection) DriverManager.getConnection(url + params, "default", getPassword()),
                    (ClickHouseConnection) DriverManager.getConnection(urlWithCredentials + params, "default", getPassword()),
            }) {
                try (ClickHouseConnection conn = connection; Statement stmt = conn.createStatement()) {
                    Assert.assertEquals(conn.getClientInfo(ClickHouseConnection.PROP_APPLICATION_NAME), clientName);
                    Assert.assertEquals(stmt.getQueryTimeout(), maxExecuteTime);
                    Assert.assertEquals(conn.getJdbcConfig().isContinueBatchOnError(), continueBatchOnError);
                }
            }
        }
    }
}
