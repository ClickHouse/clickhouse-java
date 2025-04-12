package com.clickhouse.jdbc.metadata;

import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.Types;

import static org.testng.Assert.*;


public class ParameterMetaDataImplTest {
    @Test(groups = {"integration"})
    public void testGetParameterCount() throws SQLException {
        ParameterMetaDataImpl metaData = new ParameterMetaDataImpl(0);
        assertEquals(metaData.getParameterCount(), 0);

        metaData = new ParameterMetaDataImpl(1);
        assertEquals(metaData.getParameterCount(), 1);
    }

    @Test(groups = {"integration"})
    public void testIsNullable() throws SQLException {
        ParameterMetaDataImpl metaData = new ParameterMetaDataImpl(1);
        assertEquals(metaData.isNullable(1), ParameterMetaDataImpl.parameterNullableUnknown);

        assertThrows(() -> metaData.isNullable(0));
        assertThrows(() -> metaData.isNullable(2));
    }

    @Test(groups = {"integration"})
    public void testIsSigned() throws SQLException {
        ParameterMetaDataImpl metaData = new ParameterMetaDataImpl(1);
        assertFalse(metaData.isSigned(1));

        assertThrows(() -> metaData.isSigned(0));
        assertThrows(() -> metaData.isSigned(2));
    }

    @Test(groups = {"integration"})
    public void testGetPrecisionAndScale() throws SQLException {
        ParameterMetaDataImpl metaData = new ParameterMetaDataImpl(1);
        assertEquals(metaData.getPrecision(1), 0);
        assertEquals(metaData.getScale(1), 0);

        assertThrows(() -> metaData.getPrecision(0));
        assertThrows(() -> metaData.getPrecision(2));
    }

    @Test(groups = {"integration"})
    public void testGetParameterType() throws SQLException {
        ParameterMetaDataImpl metaData = new ParameterMetaDataImpl(1);
        assertEquals(metaData.getParameterType(1), Types.OTHER);

        assertThrows(() -> metaData.getParameterType(0));
        assertThrows(() -> metaData.getParameterType(2));
    }

    @Test(groups = {"integration"})
    public void testGetParameterTypeName() throws SQLException {
        ParameterMetaDataImpl metaData = new ParameterMetaDataImpl(1);
        assertEquals(metaData.getParameterTypeName(1), "UNKNOWN");

        assertThrows(() -> metaData.getParameterTypeName(0));
        assertThrows(() -> metaData.getParameterTypeName(2));
    }

    @Test(groups = {"integration"})
    public void testGetParameterClassName() throws SQLException {
        ParameterMetaDataImpl metaData = new ParameterMetaDataImpl(1);
        assertEquals(metaData.getParameterClassName(1), Object.class.getName());

        assertThrows(() -> metaData.getParameterClassName(0));
        assertThrows(() -> metaData.getParameterClassName(2));
    }

    @Test(groups = {"integration"})
    public void testGetParameterMode() throws SQLException {
        ParameterMetaDataImpl metaData = new ParameterMetaDataImpl(1);
        assertEquals(metaData.getParameterMode(1), ParameterMetaDataImpl.parameterModeIn);

        assertThrows(() -> metaData.getParameterMode(0));
        assertThrows(() -> metaData.getParameterMode(2));
    }
}
