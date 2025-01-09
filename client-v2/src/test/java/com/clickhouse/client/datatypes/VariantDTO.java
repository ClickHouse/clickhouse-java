package com.clickhouse.client.datatypes;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import static com.clickhouse.client.datatypes.DataTypeTests.tableDefinition;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class VariantDTO {

    private int rowId;

    private Object a;

    public static String tblCreateSQL(String table) {
        return tableDefinition(table, "rowId Int16, a Variant(String, Int16)");
    }
}
