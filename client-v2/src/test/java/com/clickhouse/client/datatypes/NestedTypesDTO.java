package com.clickhouse.client.datatypes;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.testng.annotations.Test;

import static com.clickhouse.client.datatypes.DataTypeTests.tableDefinition;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NestedTypesDTO {

    private int rowId;

    private Object[] tuple1;

    private double[] point1;

    public static String tblCreateSQL(String table) {
        return tableDefinition(table,
                "rowId Int16",
                "tuple1 Tuple(Int16, String)",
                "point1 Point");
    }
}
