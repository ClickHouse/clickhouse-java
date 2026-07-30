package com.clickhouse.jdbc.metadata;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.sql.JDBCType;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.testng.Assert.assertEquals;

/**
 * Unit tests for the {@link DatabaseMetaDataImpl} result-set mutators that map a ClickHouse type
 * name to a JDBC {@code DATA_TYPE}. They pin the fallback behaviour: a type name that cannot be
 * resolved must not fail the metadata lookup, it degrades to {@link JDBCType#OTHER} (logging the
 * cause at DEBUG). The mutators are internal to the class and run without a live server, so they
 * are driven directly here rather than through a {@code getColumns}/{@code getTypeInfo} round trip.
 */
public class DatabaseMetaDataImplMutatorTest {

    @SuppressWarnings("unchecked")
    private static Consumer<Map<String, Object>> mutator(String fieldName) throws Exception {
        Field field = DatabaseMetaDataImpl.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (Consumer<Map<String, Object>>) field.get(null);
    }

    private static Object resolveDataType(String fieldName, String typeName) throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("TYPE_NAME", typeName);
        mutator(fieldName).accept(row);
        return row.get("DATA_TYPE");
    }

    @DataProvider(name = "typeNameMutators")
    public Object[][] typeNameMutators() {
        return new Object[][] {
                {"DATA_TYPE_VALUE_FUNCTION"},
                {"TYPE_INFO_VALUE_FUNCTION"},
        };
    }

    @Test(groups = {"unit"}, dataProvider = "typeNameMutators")
    public void testUnknownTypeNameFallsBackToOther(String mutatorField) throws Exception {
        Object dataType = resolveDataType(mutatorField, "ThisTypeDoesNotExist_" + System.nanoTime());
        assertEquals(dataType, JDBCType.OTHER.getVendorTypeNumber(),
                mutatorField + " must map an unrecognised type name to JDBCType.OTHER");
    }

    @Test(groups = {"unit"}, dataProvider = "typeNameMutators")
    public void testNullTypeNameFallsBackToOther(String mutatorField) throws Exception {
        Object dataType = resolveDataType(mutatorField, null);
        assertEquals(dataType, JDBCType.OTHER.getVendorTypeNumber(),
                mutatorField + " must map a null type name to JDBCType.OTHER");
    }
}
