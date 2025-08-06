package com.clickhouse.jdbc;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.internal.ServerSettings;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class ConnectionTest extends JdbcIntegrationTest {

    @Test(groups = { "integration" })
    public void createAndCloseStatementTest() throws SQLException {
        Connection conn = getJdbcConnection();
        Statement stmt = conn.createStatement();
        PreparedStatement pStmt = conn.prepareStatement("SELECT ? as v");
        pStmt.setString(1, "test string");
        conn.close();
        conn.close(); // check second attempt doesn't throw anything
        assertThrows(SQLException.class, conn::createStatement);

        try {
            stmt.executeQuery("SELECT 1");
            fail("Exception expected");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }

        try {
            pStmt.executeQuery();
            fail("Exception expected");
        } catch (SQLException e) {
           Assert.assertTrue(e.getMessage().contains("closed"));

        }
    }

    @Test(groups = { "integration" })
    public void testCreateUnsupportedStatements() throws Throwable {

        boolean[] throwUnsupportedException = new boolean[] {false, true};

        for (boolean flag : throwUnsupportedException) {
            Properties props = new Properties();
            if (flag) {
                props.setProperty(DriverProperties.IGNORE_UNSUPPORTED_VALUES.getKey(), "true");
            }

            try (Connection conn = this.getJdbcConnection(props)) {
                Assert.ThrowingRunnable[] createStatements = new Assert.ThrowingRunnable[]{
                        () -> conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY),
                        () -> conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE),
                        () -> conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT),
                        () -> conn.prepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS),
                        () -> conn.prepareStatement("SELECT 1", new int[]{1}),
                        () -> conn.prepareStatement("SELECT 1", new String[]{"1"}),
                        () -> conn.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE),
                        () -> conn.prepareStatement("SELECT 1", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY),
                        () -> conn.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT),
                        conn::setSavepoint,
                        () -> conn.setSavepoint("save point"),
                        () -> conn.createStruct("simple", null),
                };

                for (Assert.ThrowingRunnable createStatement : createStatements) {
                    if (!flag) {
                        Assert.assertThrows(SQLFeatureNotSupportedException.class, createStatement );
                    } else {
                        createStatement.run();
                    }
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void prepareCallTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareCall("SELECT 1"));
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT));
    }

    @Test(groups = { "integration" })
    public void nativeSQLTest() throws SQLException {
        try (Connection conn = this.getJdbcConnection()) {
            String escapedSQL = "SELECT \n{ts '2024-01-02 02:01:01'} as v1,\n {d '2024-01-02 02:01:01'} as v2,\n {d ?} as v3";
            String nativeSQL = "SELECT \ntimestamp('2024-01-02 02:01:01') as v1,\n toDate('2024-01-02 02:01:01') as v2,\n {d ?} as v3";
            Assert.assertEquals(conn.nativeSQL(escapedSQL), nativeSQL);

            Assert.expectThrows(IllegalArgumentException.class, () -> conn.nativeSQL(null));
            Assert.assertEquals(conn.nativeSQL("SELECT 1 as t"), "SELECT 1 as t");
        }
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
            assertThrows(SQLFeatureNotSupportedException.class, localConnection::commit);
            assertThrows(SQLFeatureNotSupportedException.class, localConnection::rollback);
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
        assertThrows(SQLFeatureNotSupportedException.class, localConnection::getTypeMap);
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
        assertThrows(SQLFeatureNotSupportedException.class, localConnection::setSavepoint);
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

    @Test(groups = { "integration" }, dataProvider = "setAndGetClientInfoTestDataProvider")
    public void setAndGetClientInfoTest(String clientName) throws SQLException {
        final String unsupportedProperty = "custom-unsupported-property";
        try (Connection localConnection = this.getJdbcConnection();
                Statement stmt = localConnection.createStatement()) {
            localConnection.setClientInfo(unsupportedProperty, "i-am-unsupported-property");
            Assert.assertNull(localConnection.getClientInfo("custom-property"));
            localConnection.setClientInfo(ClientInfoProperties.APPLICATION_NAME.getKey(), clientName);
            Assert.assertEquals(localConnection.getClientInfo(ClientInfoProperties.APPLICATION_NAME.getKey()), clientName);
            Assert.assertNull(localConnection.getClientInfo(unsupportedProperty));

            final String testQuery = "SELECT '" + UUID.randomUUID() + "'";
            stmt.execute(testQuery);
            stmt.execute("SYSTEM FLUSH LOGS");

            final String logQuery ="SELECT http_user_agent " +
                    " FROM system.query_log WHERE query = '" + testQuery.replaceAll("'", "\\\\'") + "'";
            try (ResultSet rs = stmt.executeQuery(logQuery)) {
                Assert.assertTrue(rs.next());
                String userAgent = rs.getString("http_user_agent");
                if (clientName != null && !clientName.isEmpty()) {
                    Assert.assertTrue(userAgent.startsWith(clientName), "Expected to start with '" + clientName + "' but value was '" + userAgent + "'");
                }
                Assert.assertTrue(userAgent.contains(Client.CLIENT_USER_AGENT), "Expected to contain '" + Client.CLIENT_USER_AGENT + "' but value was '" + userAgent + "'");
                Assert.assertTrue(userAgent.contains(Driver.DRIVER_CLIENT_NAME), "Expected to contain '" + Driver.DRIVER_CLIENT_NAME + "' but value was '" + userAgent + "'");
            }
        }
    }

    @Test(groups = { "integration" })
    public void influenceUserAgentClientNameTest() throws SQLException {
        String clientName = UUID.randomUUID().toString().replace("-", "");
        influenceUserAgentTest(clientName, "?" + ClientConfigProperties.CLIENT_NAME.getKey() + "=" + clientName);
        influenceUserAgentTest(clientName, "?" + ClientConfigProperties.PRODUCT_NAME.getKey() + "=" + clientName);
    }

    private void influenceUserAgentTest(String clientName, String urlParam) throws SQLException {
        Properties info = new Properties();
        info.setProperty("user", "default");
        info.setProperty("password", ClickHouseServerForTest.getPassword());
        info.setProperty(ClientConfigProperties.DATABASE.getKey(), ClickHouseServerForTest.getDatabase());

        try (Connection localConnection = new ConnectionImpl(getEndpointString() + urlParam, info);
             Statement stmt = localConnection.createStatement()) {

            final String testQuery = "SELECT '" + UUID.randomUUID() + "'";
            stmt.execute(testQuery);
            stmt.execute("SYSTEM FLUSH LOGS");

            final String logQuery ="SELECT http_user_agent " +
                    " FROM system.query_log WHERE query = '" + testQuery.replaceAll("'", "\\\\'") + "'";
            try (ResultSet rs = stmt.executeQuery(logQuery)) {
                Assert.assertTrue(rs.next());
                String userAgent = rs.getString("http_user_agent");
                Assert.assertTrue(userAgent.startsWith(clientName), "Expected to start with '" + clientName + "' but value was '" + userAgent + "'");
            }
        }
    }

    @DataProvider(name = "setAndGetClientInfoTestDataProvider")
    public static Object[][] setAndGetClientInfoTestDataProvider() {
        return new Object[][] {
                {"product (version 1.0)"},
                {null},
                {""}
        };
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
        try (Connection conn = this.getJdbcConnection()) {
            conn.abort(Executors.newSingleThreadExecutor());
            assertTrue(conn.isClosed());
        }
    }

    @Test(groups = { "integration" })
    public void testNetworkTimeout() throws SQLException {
        try {
            Connection conn = this.getJdbcConnection();
            int t1 = (int) TimeUnit.SECONDS.toMillis(20);
            conn.setNetworkTimeout(Executors.newSingleThreadExecutor(), t1);
            Assert.assertEquals(t1, conn.getNetworkTimeout());

        } catch (Exception e) {

        }
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
        properties.put(ClientConfigProperties.PASSWORD.getKey(), ClickHouseServerForTest.getPassword());
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
        properties.put(ClientConfigProperties.PASSWORD.getKey(), ClickHouseServerForTest.getPassword());

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

            // From wireshark dump as C Array
            char select_server_info[] = { /* Packet 11901 */
                    0x03, 0x04, 0x75, 0x73, 0x65, 0x72, 0x08, 0x74,
                    0x69, 0x6d, 0x65, 0x7a, 0x6f, 0x6e, 0x65, 0x07,
                    0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x06,
                    0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x06, 0x53,
                    0x74, 0x72, 0x69, 0x6e, 0x67, 0x06, 0x53, 0x74,
                    0x72, 0x69, 0x6e, 0x67, 0x07, 0x64, 0x65, 0x66,
                    0x61, 0x75, 0x6c, 0x74, 0x03, 0x55, 0x54, 0x43,
                    0x0b, 0x32, 0x34, 0x2e, 0x33, 0x2e, 0x31, 0x2e,
                    0x32, 0x36, 0x37, 0x32 };

            char select1_res[] = { /* Packet 11909 */
                    0x01, 0x01, 0x31, 0x05, 0x55, 0x49, 0x6e, 0x74,
                    0x38, 0x01 };

            mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                    .withHeader("Authorization", WireMock.equalTo("Bearer " + jwtToken1))
                    .withRequestBody(WireMock.matching(".*SELECT 1.*"))
                    .willReturn(
                            WireMock.ok(new String(select1_res))
                            .withHeader("X-ClickHouse-Summary",
                                    "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}")).build());

            mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                    .withHeader("Authorization", WireMock.equalTo("Bearer " + jwtToken1))
                    .withRequestBody(WireMock.equalTo("SELECT currentUser() AS user, timezone() AS timezone, version() AS version LIMIT 1"))
                    .willReturn(
                            WireMock.ok(new String(select_server_info))
                                    .withHeader("X-ClickHouse-Summary",
                                            "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}")).build());

            Properties properties = new Properties();
            properties.put(ClientConfigProperties.BEARERTOKEN_AUTH.getKey(), jwtToken1);
            properties.put("compress", "false");
            String jdbcUrl = "jdbc:clickhouse://" + "localhost" + ":" + mockServer.port();
            try (Connection conn = new ConnectionImpl(jdbcUrl, properties);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 1")) {
                 Assert.assertTrue(rs.next());
                 Assert.assertEquals(rs.getInt(1), 1);
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
    @Test(groups = { "integration" })
    public void testDisableExtraCallToServer() throws Exception {
        Properties properties = new Properties();
        properties.put(ClientConfigProperties.SERVER_TIMEZONE.getKey(), "GMT");
        properties.put(ClientConfigProperties.SERVER_VERSION.getKey(), "1.0.0");
        try (Connection conn = getJdbcConnection(properties);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
             Assert.assertTrue(rs.next());
             ConnectionImpl connImpl = (ConnectionImpl) conn;

             Assert.assertEquals(connImpl.getClient().getServerVersion(), "1.0.0");
             Assert.assertEquals(connImpl.getClient().getServerTimeZone(), "GMT");
        }

    }

    @Test(groups = { "integration" }, dataProvider = "validDatabaseNames")
    public void testConnectionWithValidDatabaseName(String dbName) throws Exception {
        if (isCloud()) {
            return;
        }
        Connection connCreate = this.getJdbcConnection();
        connCreate.createStatement().executeUpdate("CREATE DATABASE `" + dbName + "`");
        Properties properties = new Properties();
        properties.put(ClientConfigProperties.DATABASE.getKey(), dbName);
        Connection connCheck = this.getJdbcConnection(properties);
        ResultSet rs = connCheck.createStatement().executeQuery("SELECT 1");
        rs.next();
        Assert.assertEquals(rs.getInt(1), Integer.valueOf(1));
        Assert.assertEquals(dbName, rs.getMetaData().getSchemaName(1));
        connCreate.createStatement().executeUpdate("DROP DATABASE `" + dbName + "`");
        connCreate.close();
        connCheck.close();
    }

    @Test(groups = { "integration" })
    public void closedConnectionIsInvalid() throws Exception {
        if (isCloud()) {
            return;
        }
        Connection connection = this.getJdbcConnection();
        Assert.assertTrue(connection.isValid(3));
        connection.close();
        Assert.assertFalse(connection.isValid(3));
    }

    @Test(groups = { "integration" })
    public void connectionWithWrongCredentialsIsInvalid() throws Exception {
        if (isCloud()) {
            return;
        }
        Connection connection = this.getJdbcConnection();
        Assert.assertTrue(connection.isValid(3));
        Properties properties = new Properties();
        properties.put("password", "invalid");
        connection = this.getJdbcConnection(properties);
        Assert.assertFalse(connection.isValid(3));
    }

    @DataProvider(name = "validDatabaseNames")
    private static Object[][] createValidDatabaseNames() {
        return new Object[][] {
            { "foo" },
            { "with-dashes" },
            { "☺" },
            { "foo/bar" },
            { "foobar 20" },
            { " leading_and_trailing_spaces   " },
            { "multi\nline\r\ndos" },
        };
    }

    @Test(groups = {"integration"})
    public void testClientInfoProperties() throws Exception {
        try (Connection conn = this.getJdbcConnection()) {

            Properties properties = conn.getClientInfo();
            assertEquals(properties.get(ClientInfoProperties.APPLICATION_NAME.getKey()), "");

            properties.put(ClientInfoProperties.APPLICATION_NAME.getKey(), "test");
            conn.setClientInfo(properties);

            assertEquals(properties.get(ClientInfoProperties.APPLICATION_NAME.getKey()),  "test");

            conn.setClientInfo(new Properties());
            assertNull(conn.getClientInfo(ClientInfoProperties.APPLICATION_NAME.getKey()));

            conn.setClientInfo(ClientInfoProperties.APPLICATION_NAME.getKey(), "test 2");
            assertEquals(conn.getClientInfo(ClientInfoProperties.APPLICATION_NAME.getKey()),  "test 2");

            assertNull(conn.getClientInfo("unknown"));
        }
    }
}
