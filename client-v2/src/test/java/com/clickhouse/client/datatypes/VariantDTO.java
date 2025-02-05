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

    private Object b;

    private Object c;

    private Object d;

    private Object e;

    private Object f;

    public static String tblCreateSQL(String table) {
        return tableDefinition(table,
                "rowId Int16",
                "a Variant(String, Int16)",
                "b Variant(String, Int128)",
                "c Variant(String, Decimal128(4))",
                "d Variant(String, Float32)",
                "e Variant(Int128, Decimal128(4))",
                "f Variant(Float64, Int128)");
    }
}
