package com.clickhouse.jdbc;

import java.sql.*;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ConnectionTest extends JdbcIntegrationTest {
    private Connection connection;

    @BeforeTest
    public void setUp() {
        try {
            this.connection = this.getJdbcConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterTest
    public void tearDown() {
        try {
            this.connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void createStatementTest() throws SQLException {
        Statement statement = this.connection.createStatement();
        Assert.assertNotNull(statement);
        statement.close();

        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT));
    }

    @Test
    public void prepareStatementTest() throws SQLException {
        PreparedStatement statement = this.connection.prepareStatement("SELECT 1");
        Assert.assertNotNull(statement);
        statement.close();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.prepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.prepareStatement("SELECT 1", new int[] { 1 }));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.prepareStatement("SELECT 1", new String[] { "1" }));
    }

    @Test
    public void prepareCallTest() {
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.prepareCall("SELECT 1"));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT));
    }

    @Test
    public void nativeSQLTest() throws SQLException {
        String sql = "SELECT 1";
        Assert.assertEquals(this.connection.nativeSQL(sql), sql);
    }

    @Test
    public void setAutoCommitTest() {
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.setAutoCommit(false));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.setAutoCommit(true));
    }

    @Test
    public void getAutoCommitTest() throws SQLException {
        Assert.assertTrue(this.connection.getAutoCommit());
    }

    @Test
    public void commitTest() {
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.commit());
    }

    @Test
    public void rollbackTest() {
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.rollback());
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.rollback(null));
    }

    @Test
    public void closeTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertFalse(localConnection.isClosed());
        localConnection.close();
        Assert.assertTrue(localConnection.isClosed());
        Assert.assertThrows(SQLException.class, localConnection::createStatement);
        Assert.assertThrows(SQLException.class, () -> localConnection.prepareStatement("SELECT 1"));
    }

    @Test
    public void getMetaDataTest() throws SQLException {
        Assert.assertNotNull(this.connection.getMetaData());
    }

    @Test
    public void setReadOnlyTest() throws SQLException {
        this.connection.setReadOnly(true);
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.setReadOnly(false));
    }

    @Test
    public void isReadOnlyTest() throws SQLException {
        Assert.assertTrue(this.connection.isReadOnly());
    }

    @Test
    public void setCatalogTest() throws SQLException {
        this.connection.setCatalog("catalog-name");
        Assert.assertEquals(this.connection.getCatalog(), "catalog-name");
    }

    @Test
    public void setTransactionIsolationTest() throws SQLException {
        this.connection.setTransactionIsolation(Connection.TRANSACTION_NONE);
        Assert.assertEquals(this.connection.getTransactionIsolation(), Connection.TRANSACTION_NONE);
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE));
    }

    @Test
    public void getTransactionIsolationTest() throws SQLException {
        Assert.assertEquals(this.connection.getTransactionIsolation(), Connection.TRANSACTION_NONE);
    }

    @Test
    public void getWarningsTest() throws SQLException {
        Assert.assertNull(this.connection.getWarnings());
    }

    @Test
    public void clearWarningsTest() throws SQLException {
        this.connection.clearWarnings();
    }


    @Test
    public void getTypeMapTest() {
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.getTypeMap());
    }

    @Test
    public void setTypeMapTest() {
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.setTypeMap(null));
    }

    @Test
    public void setHoldabilityTest() throws SQLException {
        this.connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);//No-op
    }

    @Test
    public void getHoldabilityTest() throws SQLException {
        Assert.assertEquals(this.connection.getHoldability(), ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Test
    public void setSavepointTest() {
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.setSavepoint());
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.setSavepoint("savepoint-name"));
    }

    @Test
    public void releaseSavepointTest(){
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.releaseSavepoint(null));
    }

    @Test
    public void createClobTest(){
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.createClob());
    }

    @Test
    public void createBlobTest(){
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.createBlob());
    }

    @Test
    public void createNClobTest(){
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.createNClob());
    }

    @Test
    public void createSQLXMLTest(){
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.createSQLXML());
    }

    @Test
    public void isValidTest() throws SQLException {
        Assert.assertThrows(SQLException.class, () -> this.connection.isValid(-1));
        Assert.assertTrue(this.connection.isValid(0));
    }

    @Test
    public void setClientInfoTest() {
        Assert.assertThrows(SQLClientInfoException.class, () -> this.connection.setClientInfo("key", "value"));
        Assert.assertThrows(SQLClientInfoException.class, () -> this.connection.setClientInfo(new Properties()));
    }

    @Test
    public void getClientInfoTest() throws SQLException {
        Assert.assertNull(this.connection.getClientInfo("key"));
        Assert.assertNotNull(this.connection.getClientInfo());
    }

    @Test
    public void createArrayOfTest() throws SQLException {
        Assert.assertNull(this.connection.createArrayOf("type-name", new Object[] { 1, 2, 3 }));
    }

    @Test
    public void createStructTest() throws SQLException {
        Assert.assertNull(this.connection.createStruct("type-name", new Object[] { 1, 2, 3 }));
    }

    @Test
    public void setSchemaTest() throws SQLException {
        this.connection.setSchema("schema-name");
        Assert.assertEquals(this.connection.getSchema(), "schema-name");
    }

    @Test
    public void abortTest() {
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.abort(null));
    }

    @Test
    public void setNetworkTimeoutTest() {
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.setNetworkTimeout(null, 0));
    }

    @Test
    public void getNetworkTimeoutTest() {
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.getNetworkTimeout());
    }

    @Test
    public void beginRequestTest() throws SQLException {
        this.connection.beginRequest();//No-op
    }

    @Test
    public void endRequestTest() throws SQLException {
        this.connection.endRequest();//No-op
    }

    @Test
    public void setShardingKeyIfValidTest() {
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.setShardingKeyIfValid(null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.setShardingKeyIfValid(null, null, 0));
    }

    @Test
    public void setShardingKeyTest() {
       Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.setShardingKey(null));
       Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> this.connection.setShardingKey(null, null));
    }
}
