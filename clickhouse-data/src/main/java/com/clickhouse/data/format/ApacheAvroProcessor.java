package com.clickhouse.data.format;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseDeserializer;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseSerializer;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

public class ApacheAvroProcessor extends ClickHouseDataProcessor {
    static final ClickHouseColumn toColumn(Field field) {
        return toColumn(field.name(), field.schema());
    }

    static final ClickHouseColumn toColumn(String name, Schema schema) {
        ClickHouseColumn column;
        switch (schema.getType()) {
            case BOOLEAN:
                column = ClickHouseColumn.of(name, ClickHouseDataType.Bool, schema.isNullable());
                break;
            case ENUM:
                String[] enumTypes = schema.getEnumSymbols().toArray(new String[0]);
                for (int i = 0, len = enumTypes.length; i < len; i++) {
                    enumTypes[i] = new StringBuilder(ClickHouseValues.convertToQuotedString(enumTypes[i])).append('=')
                            .append(i).toString();
                }
                column = ClickHouseColumn.of(name, ClickHouseDataType.Enum16, schema.isNullable(),
                        false, enumTypes);
                break;
            case INT:
                column = ClickHouseColumn.of(name, ClickHouseDataType.Int32, schema.isNullable());
                break;
            case LONG:
                column = ClickHouseColumn.of(name, ClickHouseDataType.Int64, schema.isNullable());
                break;
            case FLOAT:
                column = ClickHouseColumn.of(name, ClickHouseDataType.Float32, schema.isNullable());
                break;
            case DOUBLE:
                column = ClickHouseColumn.of(name, ClickHouseDataType.Float64, schema.isNullable());
                break;
            case FIXED:
                column = ClickHouseColumn.of(name, ClickHouseDataType.FixedString, schema.isNullable(),
                        false, Integer.toString(schema.getFixedSize()));
                break;
            case BYTES:
            case STRING:
                column = ClickHouseColumn.of(name, ClickHouseDataType.String, schema.isNullable());
                break;
            case ARRAY:
                column = ClickHouseColumn.of(name, ClickHouseDataType.Array, false,
                        toColumn(schema.getFields().get(0)));
                break;
            case MAP:
                column = null;
                break;
            case RECORD:
                List<Field> fields = schema.getFields();
                ClickHouseColumn[] nestedCols = new ClickHouseColumn[fields.size()];
                for (Field f : fields) {
                    nestedCols[0] = toColumn(f.name(), f.schema());
                }
                column = ClickHouseColumn.of(name, ClickHouseDataType.Nested, false, nestedCols);
                break;
            case UNION:
                for (Schema s : schema.getTypes()) {
                    if (s.getType() != Schema.Type.NULL) {
                        return toColumn(name, s);
                    }
                }
            case NULL:
            default:
                column = ClickHouseColumn.of(name, ClickHouseDataType.Nothing, true);
                break;
        }
        return column;
    }

    protected ApacheAvroProcessor(ClickHouseDataConfig config, ClickHouseInputStream input,
            ClickHouseOutputStream output, List<ClickHouseColumn> columns, Map<String, Serializable> settings)
            throws IOException {
        super(config, input, output, columns, settings);
    }

    @Override
    protected ClickHouseRecord createRecord() {

        return null;
    }

    @Override
    protected void readAndFill(ClickHouseValue value) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    protected List<ClickHouseColumn> readColumns() throws IOException {
        if (input.available() < 1) {
            input.close();
            return Collections.emptyList();
        }

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileStream<GenericRecord> reader = new DataFileStream<>(input, datumReader);
        Schema schema = reader.getSchema();
        if (schema.getType() != Type.RECORD) {
            throw new IOException("Root element of schema must be RECORD");
        }
        List<Field> flds = schema.getFields();
        List<ClickHouseColumn> cols = new ArrayList<>(flds.size());
        for (Field f : flds) {
            cols.add(toColumn(f));
        }
        return Collections.unmodifiableList(cols);
    }

    @Override
    public void write(ClickHouseValue value) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public ClickHouseDeserializer getDeserializer(ClickHouseDataConfig config, ClickHouseColumn column) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClickHouseSerializer getSerializer(ClickHouseDataConfig config, ClickHouseColumn column) {
        // TODO Auto-generated method stub
        return null;
    }
}
