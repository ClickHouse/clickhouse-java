package com.clickhouse.jdbc;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;
import java.sql.SQLXML;

import javax.xml.transform.Result;
import javax.xml.transform.Source;

@Deprecated
public class ClickHouseXml implements SQLXML {

    @Override
    public void free() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public OutputStream setBinaryStream() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Writer setCharacterStream() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getString() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setString(String value) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends Result> T setResult(Class<T> resultClass) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

}
