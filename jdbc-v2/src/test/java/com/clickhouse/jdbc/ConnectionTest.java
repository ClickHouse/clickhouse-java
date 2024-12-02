package com.clickhouse.jdbc;

import java.sql.*;
import java.util.Properties;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.jdbc.internal.ClientInfoProperties;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ConnectionTest extends JdbcIntegrationTest {

    @Test(groups = { "integration" })
    public void createAndCloseStatementTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Statement statement = localConnection.createStatement();
        Assert.assertNotNull(statement);

        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT));
    }

    @Test(groups = { "integration" })
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

    @Test(groups = { "integration" })
    public void prepareCallTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareCall("SELECT 1"));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT));
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
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setAutoCommit(false));
        localConnection.setAutoCommit(true);
    }

    @Test(groups = { "integration" })
    public void getAutoCommitTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertTrue(localConnection.getAutoCommit());
    }

    @Test(groups = { "integration" })
    public void commitTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.commit());
    }

    @Test(groups = { "integration" })
    public void rollbackTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.rollback());
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.rollback(null));
    }

    @Test(groups = { "integration" })
    public void closeTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertFalse(localConnection.isClosed());
        localConnection.close();
        Assert.assertTrue(localConnection.isClosed());
        Assert.assertThrows(SQLException.class, localConnection::createStatement);
        Assert.assertThrows(SQLException.class, () -> localConnection.prepareStatement("SELECT 1"));
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
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setReadOnly(true));
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
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE));
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
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.getTypeMap());
    }

    @Test(groups = { "integration" })
    public void setTypeMapTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setTypeMap(null));
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
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setSavepoint());
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setSavepoint("savepoint-name"));
    }

    @Test(groups = { "integration" })
    public void releaseSavepointTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.releaseSavepoint(null));
    }

    @Test(groups = { "integration" })
    public void createClobTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, localConnection::createClob);
    }

    @Test(groups = { "integration" })
    public void createBlobTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, localConnection::createBlob);
    }

    @Test(groups = { "integration" })
    public void createNClobTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, localConnection::createNClob);
    }

    @Test(groups = { "integration" })
    public void createSQLXMLTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, localConnection::createSQLXML);
    }

    @Test(groups = { "integration" })
    public void isValidTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLException.class, () -> localConnection.isValid(-1));
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
        Array array = localConnection.createArrayOf("type-name", new Object[] { 1, 2, 3 });
        Assert.assertNotNull(array);
        Assert.assertEquals(array.getArray(), new Object[] { 1, 2, 3 });
    }

    @Test(groups = { "integration" })
    public void createStructTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertNull(localConnection.createStruct("type-name", new Object[] { 1, 2, 3 }));
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
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.abort(null));
    }

    @Test(groups = { "integration" })
    public void setNetworkTimeoutTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setNetworkTimeout(null, 0));
    }

    @Test(groups = { "integration" })
    public void getNetworkTimeoutTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.getNetworkTimeout());
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
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setShardingKeyIfValid(null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setShardingKeyIfValid(null, null, 0));
    }

    @Test(groups = { "integration" })
    public void setShardingKeyTest() throws SQLException {
        Connection localConnection = this.getJdbcConnection();
       Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setShardingKey(null));
       Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> localConnection.setShardingKey(null, null));
    }
}
