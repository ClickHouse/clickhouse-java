package com.clickhouse.jdbc;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.format.BinaryStreamUtils;

@Deprecated
public interface ClickHousePreparedStatement extends PreparedStatement {
    @Override
    default void setNull(int parameterIndex, int sqlType) throws SQLException {
        setNull(parameterIndex, sqlType, null);
    }

    @Override
    default void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setByte(parameterIndex, x ? (byte) 1 : (byte) 0);
    }

    @Override
    default void setDate(int parameterIndex, Date x) throws SQLException {
        setDate(parameterIndex, x, null);
    }

    @Override
    default void setTime(int parameterIndex, Time x) throws SQLException {
        setTime(parameterIndex, x, null);
    }

    @Override
    default void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setTimestamp(parameterIndex, x, null);
    }

    @Override
    default void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setCharacterStream(parameterIndex, new InputStreamReader(x, StandardCharsets.US_ASCII), length);
    }

    @Override
    default void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setCharacterStream(parameterIndex, new InputStreamReader(x, StandardCharsets.UTF_8), length);
    }

    @Override
    default void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setBinaryStream(parameterIndex, x, (long) length);
    }

    @Override
    default void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x, targetSqlType, 0);
    }

    @Override
    default void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        String s = null;
        if (reader != null) {
            try {
                s = BinaryStreamUtils.readString(reader, length);
            } catch (Throwable e) { // IOException and potentially OOM error
                throw SqlExceptionUtils.clientError(e);
            }
        }

        setString(parameterIndex, s);
    }

    @Override
    default void setRef(int parameterIndex, Ref x) throws SQLException {
        throw SqlExceptionUtils.unsupportedError("setRef not implemented");
    }

    @Override
    default void setBlob(int parameterIndex, Blob x) throws SQLException {
        if (x != null) {
            setBinaryStream(parameterIndex, x.getBinaryStream());
        } else {
            setNull(parameterIndex, Types.BLOB);
        }
    }

    @Override
    default void setClob(int parameterIndex, Clob x) throws SQLException {
        if (x != null) {
            setCharacterStream(parameterIndex, x.getCharacterStream());
        } else {
            setNull(parameterIndex, Types.CLOB);
        }
    }

    @Override
    default ResultSetMetaData getMetaData() throws SQLException {
        ResultSet currentResult = getResultSet();
        if (currentResult != null) {
            return currentResult.getMetaData();
        } else if (getLargeUpdateCount() != -1L) {
            return null; // Update query
        }

        return describeQueryResult();
    }

    default ResultSetMetaData describeQueryResult() throws SQLException {
        return null;
    }

    @Override
    default void setURL(int parameterIndex, URL x) throws SQLException {
        if (x != null) {
            setString(parameterIndex, String.valueOf(x));
        } else {
            setNull(parameterIndex, Types.VARCHAR);
        }
    }

    @Override
    default void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw SqlExceptionUtils.unsupportedError("setRowId not implemented");
    }

    @Override
    default void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    default void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        setCharacterStream(parameterIndex, value, length);
    }

    @Override
    default void setNClob(int parameterIndex, NClob value) throws SQLException {
        setClob(parameterIndex, value);
    }

    @Override
    default void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    default void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        setBinaryStream(parameterIndex, inputStream, length);
    }

    @Override
    default void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setClob(parameterIndex, reader, length);
    }

    @Override
    default void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw SqlExceptionUtils.unsupportedError("setSQLXML not implemented");
    }

    @Override
    default void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setCharacterStream(parameterIndex, new InputStreamReader(x, StandardCharsets.US_ASCII), length);
    }

    @Override
    default void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setBinaryStream(parameterIndex, length < 0L ? x : ClickHouseInputStream.wrap(x, 0, length, null));
    }

    @Override
    default void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw SqlExceptionUtils.unsupportedError("setCharacterStream not implemented");
    }

    @Override
    default void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        setCharacterStream(parameterIndex, new InputStreamReader(x, StandardCharsets.US_ASCII));
    }

    @Override
    default void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw SqlExceptionUtils.unsupportedError("setBinaryStream not implemented");
    }

    @Override
    default void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        setCharacterStream(parameterIndex, reader, -1L);
    }

    @Override
    default void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setCharacterStream(parameterIndex, value);
    }

    @Override
    default void setClob(int parameterIndex, Reader reader) throws SQLException {
        setCharacterStream(parameterIndex, reader);
    }

    @Override
    default void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        setBinaryStream(parameterIndex, inputStream);
    }

    @Override
    default void setNClob(int parameterIndex, Reader reader) throws SQLException {
        setClob(parameterIndex, reader);
    }
}
