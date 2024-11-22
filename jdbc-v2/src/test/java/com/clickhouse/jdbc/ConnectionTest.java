package com.clickhouse.jdbc;

import java.sql.*;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ConnectionTest extends JdbcIntegrationTest {

    @Test
    public void createAndCloseStatementTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Statement statement = localConnection.createStatement();
        Assert.assertNotNull(statement);

        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT));
    }

    @Test
    public void prepareStatementTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        PreparedStatement statement = localConnection.prepareStatement("SELECT 1");
        Assert.assertNotNull(statement);
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareStatement("SELECT 1", new int[] { 1 }));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareStatement("SELECT 1", new String[] { "1" }));
    }

    @Test
    public void prepareCallTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareCall("SELECT 1"));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT));
    }

    @Test
    public void nativeSQLTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        String sql = "SELECT 1";
        Assert.assertEquals(localConnection.nativeSQL(sql), sql);
    }

    @Test
    public void setAutoCommitTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setAutoCommit(false));
        localConnection.setAutoCommit(true);
    }

    @Test
    public void getAutoCommitTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertTrue(localConnection.getAutoCommit());
    }

    @Test
    public void commitTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.commit());
    }

    @Test
    public void rollbackTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.rollback());
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.rollback(null));
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
        Connection localConnection = this.getJdbcConnection();
        Assert.assertNotNull(localConnection.getMetaData());
    }

    @Test
    public void setReadOnlyTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        localConnection.setReadOnly(true);
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setReadOnly(false));
    }

    @Test
    public void isReadOnlyTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertFalse(localConnection.isReadOnly());
    }

    @Test
    public void setCatalogTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        localConnection.setCatalog("catalog-name");
        Assert.assertEquals(localConnection.getCatalog(), "catalog-name");
    }

    @Test
    public void setTransactionIsolationTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        localConnection.setTransactionIsolation(Connection.TRANSACTION_NONE);
        Assert.assertEquals(localConnection.getTransactionIsolation(), Connection.TRANSACTION_NONE);
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE));
    }

    @Test
    public void getTransactionIsolationTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertEquals(localConnection.getTransactionIsolation(), Connection.TRANSACTION_NONE);
    }

    @Test
    public void getWarningsTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertNull(localConnection.getWarnings());
    }

    @Test
    public void clearWarningsTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        localConnection.clearWarnings();
    }


    @Test
    public void getTypeMapTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.getTypeMap());
    }

    @Test
    public void setTypeMapTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setTypeMap(null));
    }

    @Test
    public void setHoldabilityTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        localConnection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);//No-op
    }

    @Test
    public void getHoldabilityTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertEquals(localConnection.getHoldability(), ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Test
    public void setSavepointTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setSavepoint());
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setSavepoint("savepoint-name"));
    }

    @Test
    public void releaseSavepointTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.releaseSavepoint(null));
    }

    @Test
    public void createClobTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.createClob());
    }

    @Test
    public void createBlobTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.createBlob());
    }

    @Test
    public void createNClobTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.createNClob());
    }

    @Test
    public void createSQLXMLTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.createSQLXML());
    }

    @Test
    public void isValidTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLException.class, () -> localConnection.isValid(-1));
        Assert.assertTrue(localConnection.isValid(0));
    }

    @Test
    public void setClientInfoTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLClientInfoException.class, () -> localConnection.setClientInfo("key", "value"));
        Assert.assertThrows(SQLClientInfoException.class, () -> localConnection.setClientInfo(new Properties()));
    }

    @Test
    public void getClientInfoTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertNull(localConnection.getClientInfo("key"));
        Assert.assertNotNull(localConnection.getClientInfo());
    }

    @Test
    public void createArrayOfTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertNull(localConnection.createArrayOf("type-name", new Object[] { 1, 2, 3 }));
    }

    @Test
    public void createStructTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertNull(localConnection.createStruct("type-name", new Object[] { 1, 2, 3 }));
    }

    @Test
    public void setSchemaTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        localConnection.setSchema("schema-name");
        Assert.assertEquals(localConnection.getSchema(), "schema-name");
    }

    @Test
    public void abortTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.abort(null));
    }

    @Test
    public void setNetworkTimeoutTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setNetworkTimeout(null, 0));
    }

    @Test
    public void getNetworkTimeoutTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.getNetworkTimeout());
    }

    @Test
    public void beginRequestTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        localConnection.beginRequest();//No-op
    }

    @Test
    public void endRequestTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        localConnection.endRequest();//No-op
    }

    @Test
    public void setShardingKeyIfValidTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setShardingKeyIfValid(null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setShardingKeyIfValid(null, null, 0));
    }

    @Test
    public void setShardingKeyTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
       Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setShardingKey(null));
       Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setShardingKey(null, null));
    }
}
