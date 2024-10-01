package com.clickhouse.client.internal;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.POJOSetter;
import com.clickhouse.client.query.QuerySamplePOJO;
import com.clickhouse.client.query.SimplePOJO;
import com.clickhouse.data.ClickHouseColumn;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

public class SerializerUtilsTests {


    public static class SamplePOJOInt256Setter implements POJOSetter {

        @Override
        public void setValue(Object obj, BinaryStreamReader reader, ClickHouseColumn column) throws IOException {
            ((QuerySamplePOJO)obj).setDateTime(((ZonedDateTime)reader.readValue(column)).toLocalDateTime());
        }

        public void readValue(Object obj, BinaryStreamReader reader, ClickHouseColumn column) throws IOException {
//            ((SamplePOJO)obj).setDateTime(((ZonedDateTime)reader.readValue(column)).toLocalDateTime());
            ((SimplePOJO)obj).setId(reader.readIntLE());
        }
    }
}
