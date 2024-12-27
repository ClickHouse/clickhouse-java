package com.clickhouse.jdbc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Properties;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.jdbc.internal.ClientInfoProperties;
import com.clickhouse.jdbc.internal.DriverProperties;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.apache.hc.core5.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.fail;


public class ConnectionTest extends JdbcIntegrationTest {

    @Test(groups = { "integration" }, enabled = false)
    public void createAndCloseStatementTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Statement statement = localConnection.createStatement();
        Assert.assertNotNull(statement);

        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT));
    }

    @Test(groups = { "integration" }, enabled = false)
    public void prepareStatementTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        PreparedStatement statement = localConnection.prepareStatement("SELECT 1");
        Assert.assertNotNull(statement);
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS));
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareStatement("SELECT 1", new int[] { 1 }));
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT));
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareStatement("SELECT 1", new String[] { "1" }));
    }

    @Test(groups = { "integration" })
    public void prepareCallTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareCall("SELECT 1"));
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT));
    }

    @Test(groups = { "integration" }, enabled = false)
    public void nativeSQLTest() throws SQLException {
        // TODO: implement
        Connection localConnection = this.getJdbcConnection();
        String sql = "SELECT 1";
        Assert.assertEquals(localConnection.nativeSQL(sql), sql);
    }

    @Test(groups = { "integration" })
    public void setAutoCommitTest() throws SQLException {
        try (Connection localConnection = this.getJdbcConnection()) {
            assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setAutoCommit(false));
            Assert.assertTrue(localConnection.getAutoCommit());
            localConnection.setAutoCommit(true);
        }

        Properties prop = new Properties();
        prop.setProperty(DriverProperties.IGNORE_UNSUPPORTED_VALUES.getKey(), "true");
        try (Connection localConnection = getJdbcConnection(prop)) {
            localConnection.setAutoCommit(false);
            Assert.assertTrue(localConnection.getAutoCommit());
            localConnection.setAutoCommit(true);
            Assert.assertTrue(localConnection.getAutoCommit());
            localConnection.setAutoCommit(false);
        }
    }

    @Test(groups = { "integration" })
    public void testCommitRollback() throws SQLException {
        try (Connection localConnection = this.getJdbcConnection()) {
            assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.commit());
            assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.rollback());
            assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.rollback(null));
        }

        Properties prop = new Properties();
        prop.setProperty(DriverProperties.IGNORE_UNSUPPORTED_VALUES.getKey(), "true");
        try (Connection localConnection = this.getJdbcConnection(prop)) {
            localConnection.commit();
            localConnection.rollback();
            localConnection.rollback(null);
        }
    }


    @Test(groups = { "integration" })
    public void closeTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertFalse(localConnection.isClosed());
        localConnection.close();
        Assert.assertTrue(localConnection.isClosed());
        assertThrows(SQLException.class, localConnection::createStatement);
        assertThrows(SQLException.class, () -> localConnection.prepareStatement("SELECT 1"));
    }

    @Test(groups = { "integration" })
    public void getMetaDataTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        DatabaseMetaData metaData = localConnection.getMetaData();
        Assert.assertNotNull(metaData);
        Assert.assertEquals(metaData.getConnection(), localConnection);
    }

    @Test(groups = { "integration" })
    public void setReadOnlyTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        localConnection.setReadOnly(false);
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setReadOnly(true));
    }

    @Test(groups = { "integration" })
    public void isReadOnlyTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertFalse(localConnection.isReadOnly());
    }

    @Test(groups = { "integration" })
    public void setCatalogTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        localConnection.setCatalog("catalog-name");
        Assert.assertNull(localConnection.getCatalog());
    }

    @Test(groups = { "integration" })
    public void setTransactionIsolationTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        localConnection.setTransactionIsolation(Connection.TRANSACTION_NONE);
        Assert.assertEquals(localConnection.getTransactionIsolation(), Connection.TRANSACTION_NONE);
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED));
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED));
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ));
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE));
    }

    @Test(groups = { "integration" })
    public void getTransactionIsolationTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertEquals(localConnection.getTransactionIsolation(), Connection.TRANSACTION_NONE);
    }

    @Test(groups = { "integration" })
    public void getWarningsTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertNull(localConnection.getWarnings());
    }

    @Test(groups = { "integration" })
    public void clearWarningsTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        localConnection.clearWarnings();
    }


    @Test(groups = { "integration" })
    public void getTypeMapTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.getTypeMap());
    }

    @Test(groups = { "integration" })
    public void setTypeMapTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setTypeMap(null));
    }

    @Test(groups = { "integration" })
    public void setHoldabilityTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        localConnection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);//No-op
    }

    @Test(groups = { "integration" })
    public void getHoldabilityTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertEquals(localConnection.getHoldability(), ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Test(groups = { "integration" })
    public void setSavepointTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setSavepoint());
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setSavepoint("savepoint-name"));
    }

    @Test(groups = { "integration" })
    public void releaseSavepointTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.releaseSavepoint(null));
    }

    @Test(groups = { "integration" })
    public void createClobTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        assertThrows(SQLFeatureNotSupportedException.class, localConnection::createClob);
    }

    @Test(groups = { "integration" })
    public void createBlobTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        assertThrows(SQLFeatureNotSupportedException.class, localConnection::createBlob);
    }

    @Test(groups = { "integration" })
    public void createNClobTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        assertThrows(SQLFeatureNotSupportedException.class, localConnection::createNClob);
    }

    @Test(groups = { "integration" })
    public void createSQLXMLTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        assertThrows(SQLFeatureNotSupportedException.class, localConnection::createSQLXML);
    }

    @Test(groups = { "integration" })
    public void isValidTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        assertThrows(SQLException.class, () -> localConnection.isValid(-1));
        Assert.assertTrue(localConnection.isValid(0));
    }

    @Test(groups = { "integration" })
    public void setAndGetClientInfoTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        localConnection.setClientInfo("custom-property", "client-name");
        Assert.assertNull(localConnection.getClientInfo("custom-property"));
        localConnection.setClientInfo(ClientInfoProperties.APPLICATION_NAME.getKey(), "client-name");
        Assert.assertEquals(localConnection.getClientInfo(ClientInfoProperties.APPLICATION_NAME.getKey()), "client-name");
    }


    @Test(groups = { "integration" })
    public void createArrayOfTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Array array = localConnection.createArrayOf("Int8", new Object[] { 1, 2, 3 });
        Assert.assertNotNull(array);
        Assert.assertEquals(array.getArray(), new Object[] { 1, 2, 3 });
    }

    @Test(groups = { "integration" })
    public void createStructTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.createStruct("type-name", new Object[] { 1, 2, 3 }));
    }

    @Test(groups = { "integration" })
    public void setSchemaTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        localConnection.setSchema("schema-name");
        Assert.assertEquals(localConnection.getSchema(), "schema-name");
    }

    @Test(groups = { "integration" })
    public void abortTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.abort(null));
    }

    @Test(groups = { "integration" })
    public void setNetworkTimeoutTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setNetworkTimeout(null, 0));
    }

    @Test(groups = { "integration" })
    public void getNetworkTimeoutTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.getNetworkTimeout());
    }

    @Test(groups = { "integration" })
    public void beginRequestTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        localConnection.beginRequest();//No-op
    }

    @Test(groups = { "integration" })
    public void endRequestTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        localConnection.endRequest();//No-op
    }

    @Test(groups = { "integration" })
    public void setShardingKeyIfValidTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setShardingKeyIfValid(null, 0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setShardingKeyIfValid(null, null, 0));
    }

    @Test(groups = { "integration" })
    public void setShardingKeyTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
       assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setShardingKey(null));
       assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setShardingKey(null, null));
    }

    @Test(groups = { "integration" })
    public void testMaxResultRowsProperty() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(Driver.chSettingKey(ServerSettings.MAX_RESULT_ROWS), "5");
        try (Connection conn = getJdbcConnection(properties)) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT toInt32(number) FROM system.numbers LIMIT 20");
                fail("Exception expected");
            } catch (SQLException e) {
                Assert.assertTrue(e.getCause() instanceof ServerException);
                Assert.assertEquals(((ServerException)e.getCause()).getCode(), 396);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSecureConnection() throws Exception {
        if (isCloud()) {
            return; // this test uses self-signed cert
        }
        ClickHouseNode secureServer = getSecureServer(ClickHouseProtocol.HTTP);

        Properties properties = new Properties();
        properties.put(ClientConfigProperties.USER.getKey(), "default");
        properties.put(ClientConfigProperties.PASSWORD.getKey(), "");
        properties.put(ClientConfigProperties.CA_CERTIFICATE.getKey(), "containers/clickhouse-server/certs/localhost.crt");

        try (Connection conn = new ConnectionImpl("jdbc:clickhouse:" + secureServer.getBaseUri(), properties);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT number FROM system.numbers LIMIT 10")) {

            int count = 0;
            while (rs.next()) { count ++ ; }
            Assert.assertEquals(count, 10);
        }

        properties.put(DriverProperties.SECURE_CONNECTION.getKey(), "true");
        String jdbcUrl = "jdbc:clickhouse://"+secureServer.getHost() + ":" + secureServer.getPort() + "/";

        try (Connection conn = new ConnectionImpl(jdbcUrl, properties);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT number FROM system.numbers LIMIT 10")) {

            int count = 0;
            while (rs.next()) { count ++ ; }
            Assert.assertEquals(count, 10);
        }
    }

    @Test(groups = { "integration" })
    public void testSelectingDatabase() throws Exception {
        if (isCloud()) {
            return; // no need to test in cloud
        }
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);
        Properties properties = new Properties();
        properties.put(ClientConfigProperties.USER.getKey(), "default");
        properties.put(ClientConfigProperties.PASSWORD.getKey(), "");

        String jdbcUrl = "jdbc:clickhouse://" + server.getHost() + ":" + server.getPort();
        try (Connection conn = new ConnectionImpl(jdbcUrl, properties);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT database()")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "default");
        }

        properties.put(ClientConfigProperties.DATABASE.getKey(), "system");
        jdbcUrl = "jdbc:clickhouse://"+server.getHost() + ":" + server.getPort() + "/default";

        try (Connection conn = new ConnectionImpl(jdbcUrl, properties);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT database()")) {

            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "default");
        }

        properties.put(ClientConfigProperties.DATABASE.getKey(), "default1");
        jdbcUrl = "jdbc:clickhouse://"+server.getHost() + ":" + server.getPort() + "/system";

        try (Connection conn = new ConnectionImpl(jdbcUrl, properties);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT database()")) {

            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "system");
        }
    }

    @Test(groups = { "integration" })
    public void testUnwrapping() throws Exception {
        Connection conn = getJdbcConnection();
        Assert.assertTrue(conn.isWrapperFor(Connection.class));
        Assert.assertTrue(conn.isWrapperFor(JdbcV2Wrapper.class));
        Assert.assertEquals(conn.unwrap(Connection.class), conn);
        Assert.assertEquals(conn.unwrap(JdbcV2Wrapper.class), conn);
        assertThrows(SQLException.class, () -> conn.unwrap(ResultSet.class));
    }

    @Test(groups = { "integration" })
    public void testBearerTokenAuth() throws Exception {
        if (isCloud()) {
            return; // mocked server
        }

        WireMockServer mockServer = new WireMockServer( WireMockConfiguration
                .options().port(9090).notifier(new ConsoleNotifier(false)));
        mockServer.start();

        try {
            String jwtToken1 = Arrays.stream(
                            new String[]{"header", "payload", "signature"})
                    .map(s -> Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8)))
                    .reduce((s1, s2) -> s1 + "." + s2).get();

            // WIP
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.put((byte) 1);

            mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                    .withHeader("Authorization", WireMock.equalTo("Bearer " + jwtToken1))
                    .willReturn(
                            WireMock.ok(buffer.toString())
                            .withHeader("X-ClickHouse-Summary",
                                    "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}")).build());

            Properties properties = new Properties();
            properties.put("access_token", jwtToken1);
            properties.put("compress", "false");
            String jdbcUrl = "jdbc:clickhouse://" + "localhost" + ":" + mockServer.port();
            try (Connection conn = new ConnectionImpl(jdbcUrl, properties);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 1")) {

            }
        } finally {
            mockServer.stop();
        }
    }
    @Test(groups = { "integration" })
    public void testJWTWithCloud() throws Exception {
        if (!isCloud()) {
            return; // only for cloud
        }

        String jwt = System.getenv("CLIENT_JWT");
        Properties properties = new Properties();
        properties.put("access_token", jwt);
        try (Connection conn = getJdbcConnection(properties);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
             Assert.assertTrue(rs.next());
        }
    }

}
