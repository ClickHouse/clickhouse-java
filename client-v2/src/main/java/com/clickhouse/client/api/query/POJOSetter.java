package com.clickhouse.client.api.query;


import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.data.ClickHouseColumn;

/**
 * Class used to set value for individual fields in a POJO.
 * Implementation will have reference to a specific POJO property.
 * Caller will use this class to set value for the property.
 * Methods are overloaded to support primitive types and avoid boxing.
 */
public interface POJOSetter {

    void setValue(Object obj, BinaryStreamReader reader, ClickHouseColumn column) throws Exception;
}
