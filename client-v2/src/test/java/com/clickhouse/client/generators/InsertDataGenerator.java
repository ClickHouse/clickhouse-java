package com.clickhouse.client.generators;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.insert.SamplePOJO;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class InsertDataGenerator {
    public static InputStream generateSimpleRowBinaryData() {
        return new InputStream() {//Placeholder
            @Override
            public int read() throws IOException {
                return 0;
            }
        };
    }

    public static List<Object> generateSimplePOJOs() {
        List<Object> pojos = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            pojos.add(new SamplePOJO(i, "pojo" + i, i * 1.0));
        }
        return pojos;
    }

    public static TableSchema generateSimpleTableSchema(String tableName) {
        TableSchema schema = new TableSchema();
        schema.setDatabaseName("default");
        schema.setTableName(tableName);
        schema.addColumn("id", "Int32");
        schema.addColumn("name", "String");
        schema.addColumn("value", "Float64");
        return schema;
    }
}
