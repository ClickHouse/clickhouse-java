package com.clickhouse.jdbc.types;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;

public class Blob implements java.sql.Blob {
    String data;

    public Blob(String data) {
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
    public byte[] getBytes(long pos, int length) throws SQLException {
        try {
            int adjustedPos = (int) pos - 1;
            return data.substring(adjustedPos, adjustedPos + length).getBytes();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        try {
            return new java.io.ByteArrayInputStream(data.getBytes());
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        try {
            int adjustedStart = (int) start - 1;
            return data.indexOf(new String(pattern), adjustedStart);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public long position(java.sql.Blob pattern, long start) throws SQLException {
        try {
            return position(pattern.getBytes(1, (int) pattern.length()), start);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        try {
            int adjustedPos = (int) pos - 1;
            data = data.substring(0, adjustedPos) + new String(bytes) + data.substring(adjustedPos + bytes.length);
            return bytes.length;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        try {
            int adjustedPos = (int) pos - 1;
            data = data.substring(0, adjustedPos) + new String(bytes, offset, len) + data.substring(adjustedPos + len);
            return len;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        throw new SQLException("setBinaryStream is not supported.");
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
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        try {
            int adjustedPos = (int) pos - 1;
            return new java.io.ByteArrayInputStream(data.substring(adjustedPos, adjustedPos + (int) length).getBytes());
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
