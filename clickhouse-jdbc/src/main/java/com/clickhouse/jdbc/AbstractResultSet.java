package com.clickhouse.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;

@Deprecated
public abstract class AbstractResultSet extends JdbcWrapper implements ResultSet {
    protected void ensureOpen() throws SQLException {
        if (isClosed()) {
            throw SqlExceptionUtils.clientError("Cannot operate on a closed ResultSet");
        }
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("absolute not implemented");
    }

    @Override
    public void afterLast() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("afterLast not implemented");
    }

    @Override
    public void beforeFirst() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("beforeFirst not implemented");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("cancelRowUpdates not implemented");
    }

    @Override
    public void clearWarnings() throws SQLException {
        ensureOpen();
    }

    @Override
    public void deleteRow() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("deleteRow not implemented");
    }

    @Override
    public boolean first() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("first not implemented");
    }

    @Override
    public int getConcurrency() throws SQLException {
        ensureOpen();

        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        ensureOpen();

        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public int getHoldability() throws SQLException {
        ensureOpen();

        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getType() throws SQLException {
        ensureOpen();

        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        ensureOpen();

        return null;
    }

    @Override
    public void insertRow() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("insertRow not implemented");
    }

    @Override
    public boolean last() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("last not implemented");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("moveToCurrentRow not implemented");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("moveToInsertRow not implemented");
    }

    @Override
    public boolean previous() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("previous not implemented");
    }

    @Override
    public void refreshRow() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("refreshRow not implemented");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("relative not implemented");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        ensureOpen();

        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        ensureOpen();

        return false;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        ensureOpen();

        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        ensureOpen();

        if (direction != ResultSet.FETCH_FORWARD) {
            throw SqlExceptionUtils.unsupportedError("only FETCH_FORWARD is supported in setFetchDirection");
        }
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateArray not implemented");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        updateArray(findColumn(columnLabel), x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateAsciiStream not implemented");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateAsciiStream not implemented");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateAsciiStream not implemented");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateBigDecimal not implemented");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        updateBigDecimal(findColumn(columnLabel), x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateBinaryStream not implemented");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateBinaryStream not implemented");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateBinaryStream not implemented");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateBlob not implemented");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        updateBlob(findColumn(columnLabel), x);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateBlob not implemented");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        updateBlob(findColumn(columnLabel), inputStream);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateBlob not implemented");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        updateBlob(findColumn(columnLabel), inputStream, length);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateBoolean not implemented");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        updateBoolean(findColumn(columnLabel), x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateByte not implemented");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        updateByte(findColumn(columnLabel), x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateBytes not implemented");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        updateBytes(findColumn(columnLabel), x);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader reader) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateCharacterStream not implemented");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader reader, int length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateCharacterStream not implemented");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader reader, long length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateCharacterStream not implemented");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateClob not implemented");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        updateClob(findColumn(columnLabel), x);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateClob not implemented");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        updateClob(findColumn(columnLabel), reader);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateClob not implemented");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateClob(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateDate not implemented");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        updateDate(findColumn(columnLabel), x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateDouble not implemented");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        updateDouble(findColumn(columnLabel), x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateFloat not implemented");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        updateFloat(findColumn(columnLabel), x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateInt not implemented");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        updateInt(findColumn(columnLabel), x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateLong not implemented");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        updateLong(findColumn(columnLabel), x);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader reader) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateNCharacterStream not implemented");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        updateNCharacterStream(findColumn(columnLabel), reader);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader reader, long length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateNCharacterStream not implemented");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        updateNCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateNClob not implemented");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        updateNClob(findColumn(columnLabel), nClob);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateNClob not implemented");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        updateNClob(findColumn(columnLabel), reader);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateNClob not implemented");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateNClob(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateNString not implemented");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        updateNString(findColumn(columnLabel), nString);
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateNull not implemented");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        updateNull(findColumn(columnLabel));
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateObject not implemented");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateObject not implemented");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        updateObject(findColumn(columnLabel), x, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateObject not implemented");
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        updateObject(findColumn(columnLabel), x, targetSqlType);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateObject not implemented");
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)
            throws SQLException {
        updateObject(findColumn(columnLabel), x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateRef not implemented");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        updateRef(findColumn(columnLabel), x);
    }

    @Override
    public void updateRow() throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateRow not implemented");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateRowId not implemented");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        updateRowId(findColumn(columnLabel), x);
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateSQLXML not implemented");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        updateSQLXML(findColumn(columnLabel), xmlObject);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateShort not implemented");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        updateShort(findColumn(columnLabel), x);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateString not implemented");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        updateString(findColumn(columnLabel), x);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateTime not implemented");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        updateTime(findColumn(columnLabel), x);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        ensureOpen();

        throw SqlExceptionUtils.unsupportedError("updateTimestamp not implemented");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        updateTimestamp(findColumn(columnLabel), x);
    }
}
