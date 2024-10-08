package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.format.BinaryStreamUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.TimeZone;
import java.util.function.BiConsumer;

public class ClickHouseBinaryFormatReaderTest {
    @Test
    public void testReadingNumbers() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        String[] names = new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
        String[] types = new String[]{"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64", "Float32", "Float64"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        BinaryStreamUtils.writeInt8(out, 1);
        BinaryStreamUtils.writeInt16(out, 2);
        BinaryStreamUtils.writeInt32(out, 3);
        BinaryStreamUtils.writeInt64(out, 4);
        BinaryStreamUtils.writeUnsignedInt8(out, 5);
        BinaryStreamUtils.writeUnsignedInt16(out, 6);
        BinaryStreamUtils.writeUnsignedInt32(out, 7);
        BinaryStreamUtils.writeUnsignedInt64(out, 8);
        BinaryStreamUtils.writeFloat32(out, 9.0f);
        BinaryStreamUtils.writeFloat64(out, 10.0);


        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId());
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());

        reader.next();

        for (int i = 0; i < names.length; i++) {
            String name = names[i];
            Assert.assertEquals(reader.getBoolean(name), Boolean.TRUE);

            if (types[i].equalsIgnoreCase("int8")) {
                Assert.assertEquals(reader.getByte(name), (i + 1));
            }
            if (types[i].equalsIgnoreCase("int8") || types[i].equalsIgnoreCase("int16")) {
                Assert.assertEquals(reader.getShort(name), (i + 1));
            }

            if (types[i].equalsIgnoreCase("int8") || types[i].equalsIgnoreCase("int16") || types[i].equalsIgnoreCase("int32")) {
                Assert.assertEquals(reader.getInteger(name), (i + 1));
            }

            if (types[i].equalsIgnoreCase("int8") || types[i].equalsIgnoreCase("int16") || types[i].equalsIgnoreCase("int32") || types[i].equalsIgnoreCase("int64")) {
                Assert.assertEquals(reader.getLong(name), (i + 1));
            }

            if (types[i].equalsIgnoreCase("int8") || types[i].equalsIgnoreCase("int16") || types[i].equalsIgnoreCase("int32") || types[i].equalsIgnoreCase("int64") || types[i].equalsIgnoreCase("uint8")) {
                Assert.assertEquals(reader.getFloat(name), (i + 1));
            }

            if (types[i].equalsIgnoreCase("int8") || types[i].equalsIgnoreCase("int16") || types[i].equalsIgnoreCase("int32") || types[i].equalsIgnoreCase("int64") || types[i].equalsIgnoreCase("uint8") || types[i].equalsIgnoreCase("uint16")) {
                Assert.assertEquals(reader.getDouble(name), (i + 1));
            }

            if (types[i].equalsIgnoreCase("int8") || types[i].equalsIgnoreCase("int16") || types[i].equalsIgnoreCase("int32") || types[i].equalsIgnoreCase("int64") || types[i].equalsIgnoreCase("uint8") || types[i].equalsIgnoreCase("uint16") || types[i].equalsIgnoreCase("uint32")) {
                Assert.assertEquals(reader.getBigInteger(name), BigInteger.valueOf((i + 1)));
            }

            if (types[i].equalsIgnoreCase("int8") || types[i].equalsIgnoreCase("int16") || types[i].equalsIgnoreCase("int32") || types[i].equalsIgnoreCase("int64") || types[i].equalsIgnoreCase("uint8") || types[i].equalsIgnoreCase("uint16") || types[i].equalsIgnoreCase("uint32") || types[i].equalsIgnoreCase("uint64")) {
                Assert.assertTrue(reader.getBigDecimal(name).compareTo(BigDecimal.valueOf((i+ 1.0f))) == 0);
            }
        }
    }
}