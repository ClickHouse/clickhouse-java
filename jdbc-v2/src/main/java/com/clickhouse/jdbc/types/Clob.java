package com.clickhouse.jdbc.types;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;

public class Clob implements java.sql.Clob {
    String data;

    public Clob(String data) {
        this.data = data;
    }

    @Override
    public long length() throws SQLException {
        try {
            return data.length();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        try {
            int adjustedPos = (int) pos - 1;
            return data.substring(adjustedPos, adjustedPos + length);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        try {
            return new java.io.StringReader(data);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        try {
            return new java.io.ByteArrayInputStream(data.getBytes());
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public long position(String searchstr, long start) throws SQLException {
        try {
            return data.indexOf(searchstr, (int) start);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public long position(java.sql.Clob searchstr, long start) throws SQLException {
        try {
            return data.indexOf(searchstr.getSubString(1, (int) searchstr.length()), (int) start);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        try {
            int adjustedPos = (int) pos - 1;
            data = data.substring(0, adjustedPos) + str + data.substring(adjustedPos + str.length());
            return str.length();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        try {
            int adjustedPos = (int) pos - 1;
            data = data.substring(0, adjustedPos) + str.substring(offset, offset + len) + data.substring(adjustedPos + len);
            return len;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        throw new SQLException("setAsciiStream is not supported.");
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        throw new SQLException("setCharacterStream is not supported.");
    }

    @Override
    public void truncate(long len) throws SQLException {
        try {
            data = data.substring(0, (int) len);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void free() throws SQLException {
        data = null;
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        try {
            return new java.io.StringReader(data.substring((int) pos - 1, (int) pos - 1 + (int) length));
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
