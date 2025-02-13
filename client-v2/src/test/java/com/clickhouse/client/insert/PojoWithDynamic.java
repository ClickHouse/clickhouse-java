package com.clickhouse.client.insert;

import com.clickhouse.client.datatypes.DataTypeTests;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class PojoWithDynamic {

    int rowId;

    Object any;

    Object nullableAny;

    public static String getTableDef(String tableName) {
        return DataTypeTests.tableDefinition(tableName,
                "rowId Int32",
                "any Dynamic",
                "nullableAny Dynamic");
    }
}
