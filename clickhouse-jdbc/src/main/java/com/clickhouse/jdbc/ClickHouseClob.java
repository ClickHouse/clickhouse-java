package com.clickhouse.jdbc;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;

@Deprecated
public class ClickHouseClob implements NClob {

    @Override
    public long length() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long position(String searchstr, long start) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long position(Clob searchstr, long start) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void truncate(long len) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void free() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

}
