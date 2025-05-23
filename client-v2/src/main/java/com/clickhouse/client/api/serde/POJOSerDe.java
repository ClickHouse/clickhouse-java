package com.clickhouse.client.api.serde;

import com.clickhouse.client.api.data_formats.RowBinaryFormatSerializer;
import com.clickhouse.client.api.data_formats.internal.SerializerUtils;
import com.clickhouse.client.api.metadata.ColumnToMethodMatchingStrategy;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class glues serialization staff together
 */
public class POJOSerDe {
    private static final Logger LOG = LoggerFactory.getLogger(POJOSerDe.class);

    private final ColumnToMethodMatchingStrategy columnToMethodMatchingStrategy;

    // POJO serializer mapping (class -> (schemaKey -> (column -> serializer)))
    private final Map<Class<?>, Map<String, Map<String, POJOFieldSerializer>>> serializers;

    // POJO deserializer mapping (class -> (schemaKey -> (column -> deserializer)))
    private final Map<Class<?>, Map<String, Map<String, POJOFieldDeserializer>>> deserializers;


    public POJOSerDe(ColumnToMethodMatchingStrategy matchingStrategy) {
        this.columnToMethodMatchingStrategy = matchingStrategy;
        this.serializers = new ConcurrentHashMap<>();
        this.deserializers = new ConcurrentHashMap<>();
    }

    public void registerClass(Class<?> clazz, TableSchema schema) {
        ColumnToMethodMatchingStrategy matchingStrategy = columnToMethodMatchingStrategy;

        //Create a new POJOSerializer with static .serialize(object, columns) methods
        Map<String, Method> classGetters = new HashMap<>();
        Map<String, Method> classSetters = new HashMap<>();
        for (Method method : clazz.getMethods()) {//Clean up the method names
            if (matchingStrategy.isGetter(method.getName())) {
                String methodName = matchingStrategy.normalizeMethodName(method.getName());
                classGetters.put(methodName, method);
            } else if (matchingStrategy.isSetter(method.getName())) {
                String methodName = matchingStrategy.normalizeMethodName(method.getName());
                classSetters.put(methodName, method);
            }
        }

        Map<String, POJOFieldSerializer> schemaSerializers = new HashMap<>();
        Map<String, POJOFieldDeserializer> schemaDeserializers = new ConcurrentHashMap<>();
        boolean defaultsSupport = schema.hasDefaults();

        for (ClickHouseColumn column : schema.getColumns()) {
            String propertyName = columnToMethodMatchingStrategy.normalizeColumnName(column.getColumnName());
            Method getterMethod = classGetters.get(propertyName);
            if (getterMethod != null) {
                schemaSerializers.put(column.getColumnName(), (obj, stream) -> {
                    Object value = getterMethod.invoke(obj);

                    if (RowBinaryFormatSerializer.writeValuePreamble(stream, defaultsSupport, column, value)) {
                        SerializerUtils.serializeData(stream, value, column);
                    }
                });
            } else {
                LOG.warn("No getter method found for column: {}", propertyName);
            }

            // Deserialization stuff
            Method setterMethod = classSetters.get(propertyName);
            if (setterMethod != null) {
                schemaDeserializers.put(column.getColumnName(), SerializerUtils.compilePOJOSetter(setterMethod, column));
            } else {
                LOG.warn("No setter method found for column: {}", propertyName);
            }
        }

        Map<String, Map<String, POJOFieldSerializer>> classSerializers = serializers.computeIfAbsent(clazz, k -> new HashMap<>());
        Map<String, Map<String, POJOFieldDeserializer>> classDeserializers = deserializers.computeIfAbsent(clazz, k -> new HashMap<>());

        String schemaKey;
        if (schema.getTableName() != null && schema.getQuery() == null) {
            schemaKey = schema.getTableName();
        } else if (schema.getQuery() != null && schema.getTableName() == null) {
            schemaKey = schema.getQuery();
        } else {
            throw new IllegalArgumentException("Table schema has both query and table name set. Only one is allowed.");
        }
        classSerializers.put(schemaKey, schemaSerializers);
        classDeserializers.put(schemaKey, schemaDeserializers);
    }

    public Map<String, POJOFieldSerializer> getFieldSerializers(Class<?> clazz, TableSchema schema) {
        // TODO: instead of storing a query list of columns should be stored and it will work both for read and write
        return serializers.getOrDefault(clazz, Collections.emptyMap())
                .getOrDefault(schema.getTableName() == null? schema.getQuery() : schema.getTableName(), Collections.emptyMap());
    }

    public Map<String, POJOFieldDeserializer> getFieldDeserializers(Class<?> clazz, TableSchema schema) {
        return deserializers.getOrDefault(clazz,
                Collections.emptyMap()).getOrDefault(schema.getTableName() == null?
                schema.getQuery() : schema.getTableName(), Collections.emptyMap());
    }
}
